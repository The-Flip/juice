"""Tests for juice.recorder."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from juice.flipfix import ReportResult
from juice.recorder import (
    check_overload,
    extract_asset_tag,
    hydrate_assignments,
    mark_device_offline,
    note_device_ok,
)
from juice.store import Store

# ---------------------------------------------------------------------------
# Asset tag extraction
# ---------------------------------------------------------------------------


class TestExtractAssetTag:
    def test_standard_format(self) -> None:
        assert extract_asset_tag("Blackout - M0013") == "M0013"

    def test_tag_at_end(self) -> None:
        assert extract_asset_tag("M0001") == "M0001"

    def test_tag_in_middle(self) -> None:
        assert extract_asset_tag("foo M0042 bar") == "M0042"

    def test_no_tag(self) -> None:
        assert extract_asset_tag("cooktop") is None

    def test_generic_plug_name(self) -> None:
        assert extract_asset_tag("Plug 2") is None

    def test_multiple_tags_returns_first(self) -> None:
        assert extract_asset_tag("M0001 and M0002") == "M0001"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def store():
    with Store(":memory:") as s:
        yield s


# ---------------------------------------------------------------------------
# device health
# ---------------------------------------------------------------------------


class TestDeviceHealth:
    def test_ok_clears_offline(self) -> None:
        from juice.server import RecorderState

        state = RecorderState()
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        mark_device_offline(state, "d1", ts, reason="unseen in live frames")
        assert "d1" in state.offline_since

        note_device_ok(state, "d1")
        assert "d1" not in state.offline_since

    def test_helpers_noop_without_state(self) -> None:
        note_device_ok(None, "d1")  # must not raise


# ---------------------------------------------------------------------------
# hydrate_assignments
# ---------------------------------------------------------------------------


class TestHydrateAssignments:
    def test_fills_state_from_open_assignments(self, store: Store) -> None:
        from juice.server import RecorderState

        plug_id = store.ensure_plug("d-ep10", "", "Blackout - M0013", has_emeter=False)
        mid = store.ensure_machine("M0013", "Blackout")
        store.update_assignment(plug_id, mid, datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC))

        state = RecorderState()
        hydrate_assignments(state, store)

        assert state.assignments[plug_id] == ("Blackout", "M0013", None)
        assert state.plugs[plug_id] == ("d-ep10", "", "Blackout - M0013")
        assert state.plug_has_emeter[plug_id] is False

    def test_noop_without_state(self, store: Store) -> None:
        hydrate_assignments(None, store)  # must not raise

    def test_populates_lock_modes(self, store: Store) -> None:
        from juice.server import RecorderState

        plug_id = store.ensure_plug("d-ep10", "", "Blackout - M0013", has_emeter=False)
        mid = store.ensure_machine("M0013", "Blackout")
        store.update_assignment(plug_id, mid, datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC))
        store.set_machine_lock_mode(mid, "off")

        state = RecorderState()
        hydrate_assignments(state, store)

        assert state.lock_modes == {"M0013": "off"}

    def test_populates_strip_names(self, store: Store) -> None:
        from juice.server import RecorderState

        store.set_strip_name("d1", "Back Wall")

        state = RecorderState()
        hydrate_assignments(state, store)

        assert state.strip_names == {"d1": "Back Wall"}

    def test_populates_circuit_devices(self, store: Store) -> None:
        from juice.server import RecorderState

        cid = store.create_circuit("P1", "B20", "coin-op", 20.0)
        store.set_device_circuit("d1", cid)

        state = RecorderState()
        hydrate_assignments(state, store)

        assert state.circuit_devices == {"d1": cid}
        assert state.circuits[cid]["panel"] == "P1"

    def test_populates_strip_orders(self, store: Store) -> None:
        from juice.server import RecorderState

        store.set_strip_orders(["d1", "d2"])

        state = RecorderState()
        hydrate_assignments(state, store)

        assert state.strip_orders == {"d1": 0, "d2": 1}

    def test_populates_unassigned_plugs_too(self, store: Store) -> None:
        # The strip outlet map must show every outlet of an offline-at-boot
        # strip, not just the assigned ones — so plugs hydrate from the full
        # plugs table, not only open assignments.
        from juice.server import RecorderState

        assigned = store.ensure_plug("d1", "c00", "Blackout - M0013")
        unassigned = store.ensure_plug("d1", "c01", "Unused", has_emeter=False)
        mid = store.ensure_machine("M0013", "Blackout")
        store.update_assignment(assigned, mid, datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC))

        state = RecorderState()
        hydrate_assignments(state, store)

        assert state.plugs[unassigned] == ("d1", "c01", "Unused")
        assert state.plug_has_emeter[unassigned] is False
        assert unassigned not in state.assignments
        assert state.plugs[assigned] == ("d1", "c00", "Blackout - M0013")


# ---------------------------------------------------------------------------
# Overload detection + auto-shutdown
# ---------------------------------------------------------------------------


class TestCheckOverload:
    BASE_TS = datetime(2026, 6, 13, 20, 0, 0, tzinfo=UTC)

    def _setup(self, store: Store, *, baseline: float | None = 49.0, mode: str = "live"):
        from juice.overload import SUSTAIN_SECONDS
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
        from juice.recorder import cancel_overload_shutdowns

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
        with caplog.at_level(logging.ERROR, logger="juice.recorder"):
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

        monkeypatch.setattr("juice.recorder.report_unplayable", _report)
        monkeypatch.setattr("juice.recorder.add_log_entry", _log)
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


# ---------------------------------------------------------------------------
# Air monitoring poll
# ---------------------------------------------------------------------------


class TestAirPollOnce:
    @pytest.mark.asyncio
    async def test_persists_sensors_and_readings(self) -> None:
        from juice.air_collector import AirReading, AirSensor
        from juice.recorder import air_poll_once

        ts = datetime(2026, 6, 20, 12, 0, 0, tzinfo=UTC)
        reading = AirReading(
            mac="MAC1",
            ts=ts,
            temperature=22.5,
            humidity=45.0,
            co2=620.0,
            pm25=8.0,
            pm10=12.0,
            tvoc=130.0,
            noise=None,
            battery=88.0,
        )
        air_account = MagicMock()
        air_account.devices = AsyncMock(
            return_value=[(AirSensor(mac="MAC1", name="Main Floor", online=True), reading)]
        )

        with Store(":memory:") as store:
            count = await air_poll_once(air_account, store, ts)
            assert count == 1
            sensors = store.list_air_sensors()
            assert sensors[0]["mac"] == "MAC1"
            assert sensors[0]["name"] == "Main Floor"
            latest = store.air_latest()
            assert latest["MAC1"]["co2"] == 620.0

    @pytest.mark.asyncio
    async def test_no_sensors_is_a_noop(self) -> None:
        from juice.recorder import air_poll_once

        air_account = MagicMock()
        air_account.devices = AsyncMock(return_value=[])
        with Store(":memory:") as store:
            count = await air_poll_once(air_account, store, datetime.now(UTC))
            assert count == 0
            assert store.list_air_sensors() == []


class TestAirBackfill:
    @pytest.mark.asyncio
    async def test_inserts_history(self) -> None:
        from juice.air_collector import AirReading, AirSensor
        from juice.recorder import air_backfill

        t = datetime(2026, 6, 20, 12, 0, 0, tzinfo=UTC)
        hist = [
            AirReading(mac="MAC1", ts=t, co2=600.0),
            AirReading(mac="MAC1", ts=t + timedelta(minutes=15), co2=620.0),
        ]
        acct = MagicMock()
        acct.history = AsyncMock(return_value=hist)
        with Store(":memory:") as store:
            n = await air_backfill(
                acct, store, [AirSensor("MAC1", "Main", True)], datetime(2026, 6, 21, tzinfo=UTC)
            )
            assert n == 2
            assert store.air_latest()["MAC1"]["co2"] == 620.0

    @pytest.mark.asyncio
    async def test_first_run_uses_default_lookback(self) -> None:
        from juice.air_collector import AirSensor
        from juice.recorder import air_backfill

        acct = MagicMock()
        acct.history = AsyncMock(return_value=[])
        now = datetime(2026, 6, 21, 0, 0, 0, tzinfo=UTC)
        with Store(":memory:") as store:
            await air_backfill(acct, store, [AirSensor("MAC1", "Main", True)], now, default_days=30)
        _mac, start_unix, end_unix = acct.history.call_args.args[:3]
        assert end_unix == int(now.timestamp())
        assert start_unix == end_unix - 30 * 86_400

    @pytest.mark.asyncio
    async def test_gap_fill_starts_after_last_stored(self) -> None:
        from juice.air_collector import AirSensor
        from juice.recorder import air_backfill

        last = datetime(2026, 6, 20, 12, 0, 0, tzinfo=UTC)
        acct = MagicMock()
        acct.history = AsyncMock(return_value=[])
        with Store(":memory:") as store:
            # (ts, mac, temperature, humidity, co2, pm25, pm10, tvoc, noise, battery)
            store.insert_air_readings(
                [(last, "MAC1", 22.0, 44.0, 600.0, 7.0, 10.0, 120.0, None, 90.0)]
            )
            await air_backfill(
                acct, store, [AirSensor("MAC1", "Main", True)], datetime(2026, 6, 21, tzinfo=UTC)
            )
        _mac, start_unix, _end = acct.history.call_args.args[:3]
        assert start_unix == int(last.timestamp()) + 1

    @pytest.mark.asyncio
    async def test_one_sensor_failure_does_not_abort_others(self) -> None:
        from juice.air_collector import AirReading, AirSensor
        from juice.recorder import air_backfill

        t = datetime(2026, 6, 20, 12, 0, 0, tzinfo=UTC)

        async def _history(mac, *_args, **_kw):
            if mac == "BAD":
                raise RuntimeError("boom")
            return [AirReading(mac="GOOD", ts=t, co2=500.0)]

        acct = MagicMock()
        acct.history = AsyncMock(side_effect=_history)
        with Store(":memory:") as store:
            n = await air_backfill(
                acct,
                store,
                [AirSensor("BAD", "Bad", True), AirSensor("GOOD", "Good", True)],
                datetime(2026, 6, 21, tzinfo=UTC),
            )
            assert n == 1
            assert "GOOD" in store.air_latest()
