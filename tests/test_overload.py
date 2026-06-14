"""Tests for juice.overload — the pure sustained-overload detector."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from juice.overload import (
    FLOOR_WATTS,
    REL_MULTIPLIER,
    SUSTAIN_SECONDS,
    OverloadWindow,
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
