"""Tests for juice.overload — the pure sustained-overload detector."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from juice.flipfix import ReportResult
from juice.overload import (
    CLOUD_MAX_GAP_S,
    FLOOR_WATTS,
    REL_MULTIPLIER,
    SUSTAIN_SECONDS,
    TAP_MAX_GAP_S,
    OverloadWindow,
    check_overload,
    resolve_overload_mode,
    threshold_for,
)
from juice.store import Store

T0 = datetime(2026, 6, 13, 20, 0, 0, tzinfo=UTC)


def _at(sec: float) -> datetime:
    return T0 + timedelta(seconds=sec)


class TestThresholdFor:
    def test_relative_dominates_for_high_baseline(self) -> None:
        # 2.5 * 100 = 250 > floor
        assert threshold_for(100.0) == REL_MULTIPLIER * 100.0

    def test_floor_dominates_for_low_baseline(self) -> None:
        # 2.5 * 20 = 50 < floor 80
        assert threshold_for(20.0) == FLOOR_WATTS

    def test_trade_winds_baseline(self) -> None:
        # Real Trade Winds baseline p99 ~= 49W -> threshold ~122W.
        assert threshold_for(49.0) == REL_MULTIPLIER * 49.0


class TestOverloadWindow:
    def _feed(self, win: OverloadWindow, watts: float, *, start: float, stop: float, step: float):
        """Feed a constant load and return the last verdict."""
        fire, mean = False, 0.0
        t = start
        while t <= stop:
            win.add(_at(t), watts)
            fire, mean = win.verdict(baseline=49.0)
            t += step
        return fire, mean

    def test_sustained_overload_fires(self) -> None:
        # Mirror the Trade Winds incident: ~170W held well past the sustain window.
        win = OverloadWindow()
        fire, mean = self._feed(win, 170.0, start=0, stop=SUSTAIN_SECONDS + 30, step=5)
        assert fire is True
        assert mean > threshold_for(49.0)

    def test_does_not_fire_before_window_is_full(self) -> None:
        # High load, but only for half the sustain window -> no full-window coverage yet.
        win = OverloadWindow()
        fire, _ = self._feed(win, 170.0, start=0, stop=SUSTAIN_SECONDS / 2, step=5)
        assert fire is False

    def test_spiky_normal_play_does_not_fire(self) -> None:
        # Normal play: ~45W with isolated single-reading spikes to 150W. The mean
        # over the window stays well under threshold, so no trigger.
        win = OverloadWindow()
        fire = False
        for i in range(120):
            watts = 150.0 if i % 20 == 0 else 45.0  # a spike every ~100s
            win.add(_at(i * 5), watts)
            fire, _ = win.verdict(baseline=49.0)
            assert fire is False, f"false positive at i={i}"

    def test_low_load_does_not_fire(self) -> None:
        win = OverloadWindow()
        fire, _ = self._feed(win, 45.0, start=0, stop=SUSTAIN_SECONDS + 60, step=5)
        assert fire is False

    def test_sustained_just_above_floor_for_low_baseline(self) -> None:
        # A low-baseline machine (baseline 20 -> threshold = floor 80). Sustained
        # 100W should fire; sustained 70W (below floor) should not.
        win_hi = OverloadWindow()
        t = 0.0
        fire_hi = False
        while t <= SUSTAIN_SECONDS + 20:
            win_hi.add(_at(t), 100.0)
            fire_hi, _ = win_hi.verdict(baseline=20.0)
            t += 5
        assert fire_hi is True

        win_lo = OverloadWindow()
        t = 0.0
        fire_lo = False
        while t <= SUSTAIN_SECONDS + 20:
            win_lo.add(_at(t), 70.0)
            fire_lo, _ = win_lo.verdict(baseline=20.0)
            t += 5
        assert fire_lo is False

    def test_window_trims_old_samples(self) -> None:
        # An old high burst then sustained low: once the burst ages out of the
        # window, the mean reflects only the recent low load.
        win = OverloadWindow()
        for t in range(0, 40, 5):
            win.add(_at(t), 300.0)  # early burst
        # Now feed low load for longer than the sustain window.
        t = 40.0
        fire = True
        while t <= 40 + SUSTAIN_SECONDS + 30:
            win.add(_at(t), 40.0)
            fire, _ = win.verdict(baseline=49.0)
            t += 5
        assert fire is False

    def test_reset_clears_window(self) -> None:
        win = OverloadWindow()
        self._feed(win, 170.0, start=0, stop=SUSTAIN_SECONDS + 30, step=5)
        win.reset()
        fire, _ = win.verdict(baseline=49.0)
        assert fire is False

    def test_gap_longer_than_window_resets(self) -> None:
        # High load, then a polling gap longer than the sustain window, then a
        # single high reading. The stale pre-gap samples must not bridge the gap
        # and make a 2-sample window look "full" — no fire until a fresh window
        # accumulates.
        win = OverloadWindow()
        for t in range(0, SUSTAIN_SECONDS, 5):
            win.add(_at(t), 170.0)
        # Gap of 2x the window, then one high sample.
        win.add(_at(SUSTAIN_SECONDS + 2 * SUSTAIN_SECONDS), 170.0)
        fire, _ = win.verdict(baseline=49.0)
        assert fire is False


class TestCoverage:
    """A window that claims two minutes must have *seen* two minutes.

    Live frames are droppable and the cloud recorder skips reads, so samples
    can be sparse and uneven. Three samples at 800 W near t=0, a 100 s hole,
    three more at t=120: span 120, per-sample mean 800, fires -- having
    observed six seconds of the two minutes it claims. That is the case this
    gate exists to refuse, and the reason the mean is now weighted by how long
    each sample held rather than counted.
    """

    def test_a_hole_in_the_window_refuses_to_fire(self) -> None:
        win = OverloadWindow(max_gap_seconds=TAP_MAX_GAP_S)
        for t in (0, 1, 2):
            win.add(_at(t), 800.0)
        for t in (120, 121, 122):
            win.add(_at(t), 800.0)
            fire, _ = win.verdict(baseline=49.0)
            assert fire is False, "six seconds of evidence is not two minutes"

    def test_the_hole_ages_out_and_the_window_fires_again(self) -> None:
        """A refusal is a delay, never a permanent disarm: once the gap has
        left the trailing window, continuous evidence fires as before."""
        win = OverloadWindow(max_gap_seconds=TAP_MAX_GAP_S)
        for t in (0, 1, 2):
            win.add(_at(t), 800.0)
        fired_at = None
        for t in range(120, 400):
            win.add(_at(t), 800.0)
            fire, _ = win.verdict(baseline=49.0)
            if fire:
                fired_at = t
                break
        assert fired_at is not None
        assert 120 + SUSTAIN_SECONDS <= fired_at <= 120 + SUSTAIN_SECONDS + TAP_MAX_GAP_S + 1

    def test_gaps_within_the_bound_do_not_refuse(self) -> None:
        """tap's measured worst case is a 2.4 s reconnect; the bound has room."""
        win = OverloadWindow(max_gap_seconds=TAP_MAX_GAP_S)
        t = 0.0
        fire = False
        while t <= SUSTAIN_SECONDS + 10:
            win.add(_at(t), 170.0)
            fire, _ = win.verdict(baseline=49.0)
            t += 4.0  # under the bound every time
        assert fire is True

    def test_the_bound_is_per_collector(self) -> None:
        """The cloud recorder's cadence is 6-9 s with a p99.9 of 22 s on a
        drawing outlet (measured on a production week): a 5 s bound there
        would refuse every window and silently disarm protection in the mode
        running in production today."""
        assert TAP_MAX_GAP_S < CLOUD_MAX_GAP_S
        cloud = OverloadWindow(max_gap_seconds=CLOUD_MAX_GAP_S)
        fire = False
        t = 0.0
        while t <= SUSTAIN_SECONDS + 30:
            cloud.add(_at(t), 170.0)
            fire, _ = cloud.verdict(baseline=49.0)
            t += 9.0  # the cloud's p90
        assert fire is True

    def test_the_default_bound_is_the_cloud_recorders(self) -> None:
        """A window built without saying which collector feeds it must assume
        the one running in production today, or a caller that forgot would
        disarm it."""
        win = OverloadWindow()
        fire = False
        t = 0.0
        while t <= SUSTAIN_SECONDS + 30:
            win.add(_at(t), 170.0)
            fire, _ = win.verdict(baseline=49.0)
            t += 20.0  # the cloud's p99.9
        assert fire is True
        assert win.max_gap_seconds == CLOUD_MAX_GAP_S

    def test_one_held_sample_at_the_bound_cannot_fire_alone(self) -> None:
        """The accepted risk, executable: a single sample held for exactly the
        tap bound weighs 10/120 of the window. At the floor's lowest threshold
        (Trade Winds: 46 W baseline, 115 W) it would need ~900 W to fire by
        itself; nothing on the floor draws that. 500 W does not."""
        win = OverloadWindow(max_gap_seconds=TAP_MAX_GAP_S)
        t = 0.0
        while t < SUSTAIN_SECONDS + 5:
            win.add(_at(t), 46.0)
            t += 1.0
        win.add(_at(t), 500.0)
        win.add(_at(t + TAP_MAX_GAP_S), 46.0)
        win.add(_at(t + TAP_MAX_GAP_S + 1), 46.0)
        fire, mean = win.verdict(baseline=46.0)
        assert fire is False
        assert mean < threshold_for(46.0)


class TestTheMeanIsWeightedByTime:
    def test_a_brief_spike_counts_for_its_duration_not_its_sample(self) -> None:
        """Uneven sampling must not bias the mean. One second at 1000 W inside
        two minutes at 40 W is ~48 W however many samples land on the spike."""
        win = OverloadWindow(max_gap_seconds=CLOUD_MAX_GAP_S)
        t = 0.0
        while t < SUSTAIN_SECONDS:
            win.add(_at(t), 40.0)
            t += 10.0
        # Five samples inside one second of a 1000 W spike, then back to 40 W.
        for i in range(5):
            win.add(_at(SUSTAIN_SECONDS + i * 0.2), 1000.0)
        win.add(_at(SUSTAIN_SECONDS + 1.0), 40.0)
        win.add(_at(SUSTAIN_SECONDS + 10.0), 40.0)
        fire, mean = win.verdict(baseline=49.0)
        assert fire is False
        assert mean < 60, f"five samples in one second must not weigh like fifty seconds: {mean}"

    def test_a_steady_load_reads_as_itself(self) -> None:
        win = OverloadWindow()
        t = 0.0
        while t <= SUSTAIN_SECONDS + 30:
            win.add(_at(t), 170.0)
            t += 7.0
        _, mean = win.verdict(baseline=49.0)
        assert abs(mean - 170.0) < 1e-9

    def test_the_straddler_counts_only_inside_the_window(self) -> None:
        """`add()` keeps one sample at or before the cutoff so the span
        brackets a full window. Its hold must be clipped at the cutoff: 1000 W
        held from t=0 to t=10 then 40 W to t=121 is 112 W over the trailing
        two minutes (t=1..121), not 119 W over t=0..121 -- and at a 46 W
        baseline (threshold 115 W) that difference is a shutdown."""
        win = OverloadWindow()
        win.add(_at(0), 1000.0)
        t = 10.0
        while t <= 121:
            win.add(_at(t), 40.0)
            t += 1.0
        fire, mean = win.verdict(baseline=46.0)
        assert fire is False
        assert abs(mean - 112.0) < 0.5, mean

    def test_the_verdict_mean_is_the_weighted_one(self) -> None:
        """Half the window at 100 W, half at 300 W, sampled ten times as
        densely on the low half: per-sample says ~118, time says 200."""
        win = OverloadWindow()
        t = 0.0
        while t < SUSTAIN_SECONDS / 2:
            win.add(_at(t), 100.0)
            t += 1.0
        while t <= SUSTAIN_SECONDS + 5:
            win.add(_at(t), 300.0)
            t += 10.0
        _, mean = win.verdict(baseline=49.0)
        assert 195 < mean < 205, mean


class TestResolveOverloadMode:
    def test_valid_modes_passthrough(self) -> None:
        assert resolve_overload_mode("live") == "live"
        assert resolve_overload_mode("shadow") == "shadow"
        assert resolve_overload_mode("off") == "off"

    def test_case_insensitive(self) -> None:
        assert resolve_overload_mode("OFF") == "off"

    def test_none_defaults_to_live(self) -> None:
        assert resolve_overload_mode(None) == "live"

    def test_typo_fails_safe_to_live(self) -> None:
        # "disable"/"false" are NOT recognized — must not silently disable.
        assert resolve_overload_mode("disable") == "live"
        assert resolve_overload_mode("false") == "live"


@pytest.fixture
def store():
    with Store(":memory:") as s:
        yield s


class TestCheckOverload:
    BASE_TS = datetime(2026, 6, 13, 20, 0, 0, tzinfo=UTC)

    def _setup(self, store: Store, *, baseline: float | None = 49.0, mode: str = "live"):
        from juice.server import RecorderState

        plug_id = store.ensure_plug("d1", "c01", "Trade Winds - M0003")
        store.ensure_machine("M0003", "Trade Winds")
        state = RecorderState()
        state.overload_mode = mode
        state.assignments[plug_id] = ("Trade Winds", "M0003", None)
        if baseline is not None:
            state.power_baselines["M0003"] = baseline
        fake = AsyncMock()
        state.plug_objects[plug_id] = fake
        self._sustain = SUSTAIN_SECONDS
        return state, plug_id, fake

    async def _feed(self, state, store, plug_id, watts, *, seconds=None, start=0, settle=True):
        """Feed a constant load across a span longer than the sustain window.

        `check_overload` hands the actuation to its own task and returns, so by
        default the feed also waits for any shutdown it started -- what a test
        asserting on `turn_off` wants. `settle=False` leaves it in flight.
        """
        seconds = seconds if seconds is not None else self._sustain + 30
        t = start
        while t <= start + seconds:
            await check_overload(state, store, plug_id, self.BASE_TS + timedelta(seconds=t), watts)
            t += 5
        if settle:
            await self._settle(state)

    @staticmethod
    async def _settle(state) -> None:
        pending = list(state.overload_shutdowns.values())
        if pending:
            await asyncio.gather(*pending)

    @pytest.mark.asyncio
    async def test_sustained_overload_shuts_down_and_locks_off(self, store: Store) -> None:
        state, plug_id, fake = self._setup(store)
        q: asyncio.Queue = asyncio.Queue(maxsize=8)
        state.event_subscribers.add(q)

        await self._feed(state, store, plug_id, 175.0)

        fake.turn_off.assert_awaited()
        assert state.lock_modes["M0003"] == "off"
        assert store.get_lock_modes() == {"M0003": "off"}
        rows = store.recent_power_events(limit=10)
        overload_row = next(r for r in rows if r["source"] == "overload")
        assert overload_row["action"] == "turn_off"
        assert overload_row["result"] == "ok"
        # FlipFix isn't configured here, so the skip is surfaced as an error audit row.
        assert any(r["source"] == "flipfix" and r["result"] == "error" for r in rows)
        ev = q.get_nowait()
        assert ev["type"] == "overload_shutdown"
        assert ev["asset_id"] == "M0003"
        assert ev["shadow"] is False

    @pytest.mark.asyncio
    async def test_normal_play_does_not_shut_down(self, store: Store) -> None:
        state, plug_id, fake = self._setup(store)
        await self._feed(state, store, plug_id, 45.0)
        fake.turn_off.assert_not_called()
        assert "M0003" not in state.lock_modes
        assert store.recent_power_events(limit=10) == []

    @pytest.mark.asyncio
    async def test_shadow_mode_audits_but_does_not_act(self, store: Store) -> None:
        state, plug_id, fake = self._setup(store, mode="shadow")
        await self._feed(state, store, plug_id, 175.0)
        fake.turn_off.assert_not_called()
        assert "M0003" not in state.lock_modes
        rows = store.recent_power_events(limit=10)
        assert len(rows) == 1
        assert rows[0]["result"] == "shadow"

    @pytest.mark.asyncio
    async def test_off_mode_does_nothing(self, store: Store) -> None:
        state, plug_id, fake = self._setup(store, mode="off")
        await self._feed(state, store, plug_id, 175.0)
        fake.turn_off.assert_not_called()
        assert store.recent_power_events(limit=10) == []

    @pytest.mark.asyncio
    async def test_unarmed_machine_skipped(self, store: Store) -> None:
        # No baseline yet -> not armed -> never auto-shut-down.
        state, plug_id, fake = self._setup(store, baseline=None)
        await self._feed(state, store, plug_id, 175.0)
        fake.turn_off.assert_not_called()

    @pytest.mark.asyncio
    async def test_already_locked_off_skipped(self, store: Store) -> None:
        state, plug_id, fake = self._setup(store)
        state.lock_modes["M0003"] = "off"
        await self._feed(state, store, plug_id, 175.0)
        fake.turn_off.assert_not_called()

    @pytest.mark.asyncio
    async def test_turn_off_failure_audited_as_error(self, store: Store) -> None:
        state, plug_id, fake = self._setup(store)
        fake.turn_off.side_effect = RuntimeError("Device is offline")
        await self._feed(state, store, plug_id, 175.0)
        rows = store.recent_power_events(limit=10)
        assert len(rows) == 1
        assert rows[0]["result"] == "error"
        # Lock NOT engaged when the power-off didn't succeed.
        assert "M0003" not in state.lock_modes

    @pytest.mark.asyncio
    async def test_failed_shutdown_waits_out_the_cooldown(self, store: Store) -> None:
        # A shutdown that fails must not re-fire on the next full window: that
        # is six more commands, an ERROR and an audit row every two minutes at
        # a strip that just refused six. It waits OVERLOAD_RETRY_COOLDOWN_S,
        # then tries again.
        from juice.overload import OVERLOAD_RETRY_COOLDOWN_S

        state, plug_id, fake = self._setup(store)
        fake.turn_off.side_effect = RuntimeError("Device is offline")

        # One continuous overload, sampled every 5 s: the first attempt fires
        # at the end of the sustain window, and the second exactly one cooldown
        # after it -- not one window after it, and not one window after the
        # cooldown either (the window kept filling, so it is warm when the
        # cooldown ends).
        fired_at: int | None = None
        t = 0
        while t <= self._sustain + OVERLOAD_RETRY_COOLDOWN_S + 60:
            await check_overload(state, store, plug_id, self.BASE_TS + timedelta(seconds=t), 175.0)
            await self._settle(state)
            if fake.turn_off.await_count == 1 and fired_at is None:
                fired_at = t
            if fired_at is not None and t < fired_at + OVERLOAD_RETRY_COOLDOWN_S:
                assert fake.turn_off.await_count == 1, f"retried early at t={t}"
            t += 5
        assert fired_at == self._sustain
        assert fake.turn_off.await_count == 2
        assert [r["result"] for r in store.recent_power_events(limit=10)] == ["error", "error"]

    @pytest.mark.asyncio
    async def test_shutdown_tasks_are_cancelled_on_collector_exit(self, store: Store) -> None:
        from juice.overload import cancel_overload_shutdowns

        state, plug_id, fake = self._setup(store)
        release = asyncio.Event()

        async def blocked_turn_off():
            await release.wait()

        fake.turn_off.side_effect = blocked_turn_off
        await self._feed(state, store, plug_id, 175.0, settle=False)
        await asyncio.sleep(0)
        task = state.overload_shutdowns[plug_id]

        await cancel_overload_shutdowns(state)
        await asyncio.sleep(0)  # the done-callback runs
        assert task.cancelled()
        assert state.overload_shutdowns == {}
        assert "M0003" not in state.lock_modes
        assert store.recent_power_events(limit=10) == []

    @pytest.mark.asyncio
    async def test_a_relabel_mid_actuation_locks_nothing(self, store: Store) -> None:
        # The outlet is off either way; the lock, audit row and report are
        # about a machine, and the machine this task knew is no longer there.
        state, plug_id, fake = self._setup(store)
        release = asyncio.Event()

        async def blocked_turn_off():
            await release.wait()

        fake.turn_off.side_effect = blocked_turn_off
        await self._feed(state, store, plug_id, 175.0, settle=False)
        await asyncio.sleep(0)
        state.assignments[plug_id] = ("Blackout", "M0013", None)
        release.set()
        await self._settle(state)
        fake.turn_off.assert_awaited_once()
        assert state.lock_modes == {}
        assert store.get_lock_modes() == {}
        assert store.recent_power_events(limit=10) == []

    @pytest.mark.asyncio
    async def test_shutdown_task_crash_is_logged_not_lost(self, store: Store, caplog) -> None:
        state, plug_id, fake = self._setup(store)

        # `RuntimeError` is handled; a non-Exception escaping `call_with_retry`
        # is the "this is a bug" path the done-callback exists for.
        class Boom(BaseException):
            pass

        fake.turn_off.side_effect = Boom
        with caplog.at_level(logging.ERROR, logger="juice.overload"):
            await self._feed(state, store, plug_id, 175.0, settle=False)
            await asyncio.sleep(0)
            await asyncio.sleep(0)
        assert state.overload_shutdowns == {}
        assert any("crashed" in r.message for r in caplog.records)

    @pytest.mark.asyncio
    async def test_actuation_runs_off_the_caller(self, store: Store) -> None:
        # `check_overload` returns while `turn_off` is still in flight: the
        # caller is the poll loop or the live-frame apply, and awaiting a
        # minute of retries there stalls every other machine.
        state, plug_id, fake = self._setup(store)
        release = asyncio.Event()

        async def blocked_turn_off():
            await release.wait()

        fake.turn_off.side_effect = blocked_turn_off

        await self._feed(state, store, plug_id, 175.0, settle=False)
        await asyncio.sleep(0)  # let the task start
        assert fake.turn_off.await_count == 1
        assert plug_id in state.overload_shutdowns
        assert "M0003" not in state.lock_modes

        # More overload while it is in flight spawns nothing new.
        await self._feed(state, store, plug_id, 175.0, start=self._sustain + 35, settle=False)
        await asyncio.sleep(0)
        assert fake.turn_off.await_count == 1

        release.set()
        await self._settle(state)
        assert state.lock_modes["M0003"] == "off"
        assert plug_id not in state.overload_shutdowns

    @pytest.mark.asyncio
    async def test_other_plug_fires_while_one_shutdown_is_in_flight(self, store: Store) -> None:
        state, plug_id, fake = self._setup(store)
        other_id = store.ensure_plug("d1", "c02", "Blackout - M0013")
        store.ensure_machine("M0013", "Blackout")
        state.assignments[other_id] = ("Blackout", "M0013", None)
        state.power_baselines["M0013"] = 49.0
        other = AsyncMock()
        state.plug_objects[other_id] = other
        release = asyncio.Event()

        async def blocked_turn_off():
            await release.wait()

        fake.turn_off.side_effect = blocked_turn_off

        await self._feed(state, store, plug_id, 175.0, settle=False)
        await asyncio.sleep(0)
        assert fake.turn_off.await_count == 1

        await self._feed(state, store, other_id, 175.0, settle=False)
        await asyncio.sleep(0)
        other.turn_off.assert_awaited_once()

        release.set()
        await self._settle(state)
        assert state.lock_modes == {"M0003": "off", "M0013": "off"}

    def _patch_flipfix(self, monkeypatch, *, report=None, log_ok=True):
        """Patch the FlipFix client; return (report_calls, logentry_calls)."""
        if report is None:
            report = ReportResult(201, 42)
        report_calls: list = []
        log_calls: list = []

        async def _report(url, key, asset_id, description, *, occurred_at=None, mark_broken=True):
            report_calls.append({"asset_id": asset_id, "description": description})
            return report

        async def _log(url, key, report_id, text, *, occurred_at=None):
            log_calls.append({"report_id": report_id, "text": text})
            return log_ok

        monkeypatch.setattr("juice.overload.report_unplayable", _report)
        monkeypatch.setattr("juice.overload.add_log_entry", _log)
        return report_calls, log_calls

    @pytest.mark.asyncio
    async def test_files_new_report_on_first_overload(self, store: Store, monkeypatch) -> None:
        state, plug_id, fake = self._setup(store)
        state.flipfix_url = "https://flipfix.example.com/api/v1/"
        state.flipfix_key = "write-key"
        reports, logs = self._patch_flipfix(monkeypatch, report=ReportResult(201, 42))

        await self._feed(state, store, plug_id, 175.0)

        fake.turn_off.assert_awaited()
        assert len(reports) == 1 and reports[0]["asset_id"] == "M0003"
        assert "overload" in reports[0]["description"]
        assert logs == []  # 201 created -> no separate log entry
        flip = next(r for r in store.recent_power_events(limit=10) if r["source"] == "flipfix")
        assert flip["result"] == "ok" and "#42" in flip["error"]

    @pytest.mark.asyncio
    async def test_recurrence_appends_log_entry(self, store: Store, monkeypatch) -> None:
        state, plug_id, fake = self._setup(store)
        state.flipfix_url = "https://flipfix.example.com/api/v1/"
        state.flipfix_key = "write-key"
        reports, logs = self._patch_flipfix(monkeypatch, report=ReportResult(200, 7))

        await self._feed(state, store, plug_id, 175.0)

        # 200 = an open unplayable report already exists -> log the recurrence onto it.
        assert len(logs) == 1 and logs[0]["report_id"] == 7
        flip = next(r for r in store.recent_power_events(limit=10) if r["source"] == "flipfix")
        assert (
            flip["result"] == "ok" and "#7" in flip["error"] and "append" in flip["error"].lower()
        )

    @pytest.mark.asyncio
    async def test_report_failure_audited_as_error(self, store: Store, monkeypatch) -> None:
        state, plug_id, fake = self._setup(store)
        state.flipfix_url = "https://flipfix.example.com/api/v1/"
        state.flipfix_key = "write-key"
        reports, logs = self._patch_flipfix(monkeypatch, report=ReportResult(403, None))

        await self._feed(state, store, plug_id, 175.0)

        assert logs == []
        flip = next(r for r in store.recent_power_events(limit=10) if r["source"] == "flipfix")
        assert flip["result"] == "error" and "403" in flip["error"]

    @pytest.mark.asyncio
    async def test_description_has_duration_peak_and_link(self, store: Store, monkeypatch) -> None:
        state, plug_id, fake = self._setup(store)
        state.flipfix_url = "https://flipfix.example.com/api/v1/"
        state.flipfix_key = "write-key"
        state.public_url = "https://juice.example.com"
        reports, _ = self._patch_flipfix(monkeypatch)

        await self._feed(state, store, plug_id, 175.0)

        desc = reports[0]["description"]
        assert "peak 175W" in desc
        assert "for " in desc and "m " in desc  # a formatted duration like "2m 00s"
        assert f"https://juice.example.com/machine/{plug_id}" in desc

    @pytest.mark.asyncio
    async def test_no_flipfix_report_when_unconfigured(self, store: Store, monkeypatch) -> None:
        state, plug_id, fake = self._setup(store)  # no flipfix creds set
        reports, logs = self._patch_flipfix(monkeypatch)

        await self._feed(state, store, plug_id, 175.0)

        fake.turn_off.assert_awaited()
        assert reports == [] and logs == []
        flip = next(r for r in store.recent_power_events(limit=10) if r["source"] == "flipfix")
        assert flip["result"] == "error" and "not configured" in flip["error"].lower()

    @pytest.mark.asyncio
    async def test_shadow_mode_does_not_report(self, store: Store, monkeypatch) -> None:
        state, plug_id, fake = self._setup(store, mode="shadow")
        state.flipfix_url = "https://flipfix.example.com/api/v1/"
        state.flipfix_key = "write-key"
        reports, logs = self._patch_flipfix(monkeypatch)

        await self._feed(state, store, plug_id, 175.0)

        fake.turn_off.assert_not_called()
        assert reports == [] and logs == []
