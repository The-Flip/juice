"""Tests for juice.overload — the pure sustained-overload detector."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from juice.overload import (
    CLOUD_MAX_GAP_S,
    FLOOR_WATTS,
    REL_MULTIPLIER,
    SUSTAIN_SECONDS,
    TAP_MAX_GAP_S,
    OverloadWindow,
    resolve_overload_mode,
    threshold_for,
)

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
