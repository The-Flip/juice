"""The replayer's pure transforms.

These are unit-tested with no database because the replayer's own input is a
249 MB production backup that is not in git and never will be, so the CLI itself
cannot run in CI. The reshaping logic is where the judgement calls live, and it
is all here.
"""

from __future__ import annotations

from datetime import UTC, datetime

from tests.e2e.replay import Sample, derive_relay_on, to_milli, to_sweeps, upsample


class TestRelayDerivation:
    """juice's `readings` has no relay column, so tap's `relay_on` has to be
    reconstructed from how the cloud recorder wrote rows."""

    def test_all_zeroes_is_the_recorders_relay_off_convention(self) -> None:
        assert derive_relay_on(0.0, 0.0, 0.0, 0.0) is False

    def test_all_nulls_is_on_without_a_meter(self) -> None:
        """`juice/recorder.py` writes all-NULL for an outlet that is switched on
        but has no energy meter."""
        assert derive_relay_on(None, None, None, None) is True

    def test_a_metered_reading_is_on(self) -> None:
        assert derive_relay_on(212.5, 119.0, 1.8, 4.2) is True

    def test_zero_watts_with_live_voltage_is_on(self) -> None:
        """The case that makes `watts > 0` the wrong rule. 5% of rows on
        2026-09-02 look like this -- an outlet switched on at the strip and off
        at the machine's own switch. Calling them off would invent thousands of
        state transitions that never happened."""
        assert derive_relay_on(0.0, 118.7, 0.0, 122.1) is True


class TestUnitConversion:
    def test_units_become_milli_units(self) -> None:
        assert to_milli(42.0) == 42_000
        assert to_milli(0.35) == 350

    def test_none_stays_none(self) -> None:
        """Null means unmeasured, never zero. A null flattened to 0 mW would
        arrive at juice as a machine drawing nothing."""
        assert to_milli(None) is None

    def test_zero_is_not_none(self) -> None:
        assert to_milli(0.0) == 0


def sample(second, child="A00", device="A", **kw) -> Sample:
    base = {
        "relay_on": True,
        "power_mw": 42_000,
        "voltage_mv": 119_000,
        "current_ma": 350,
        "energy_wh": 1,
    }
    base.update(kw)
    return Sample(second=second, device_id=device, child_id=child, **base)


class TestUpsampling:
    def test_a_held_value_fills_the_gap_between_samples(self) -> None:
        """Production recorded at a p50 of 6.7 s; tap polls at 1 Hz. The fill is
        what turns a 215k-row day into the ~4.2M-row day tap will really produce."""
        seconds = [s for s, _ in upsample([sample(100), sample(105)], max_hold_s=300)]
        assert seconds == list(range(100, 106))

    def test_the_held_value_is_the_last_one_seen(self) -> None:
        out = dict(upsample([sample(100, power_mw=1000), sample(103, power_mw=2000)]))
        assert out[102][0].power_mw == 1000
        assert out[103][0].power_mw == 2000

    def test_a_plug_emits_nothing_before_its_first_sample(self) -> None:
        out = dict(upsample([sample(100, child="A00"), sample(103, child="A01")]))
        assert {s.child_id for s in out[100]} == {"A00"}
        assert {s.child_id for s in out[103]} == {"A00", "A01"}

    def test_an_outlet_drops_out_after_the_hold_limit(self) -> None:
        """Otherwise a plug that went offline for six hours replays as six hours
        of invented steady draw -- and the "an outlet vanishes from its strip's
        sweep" path is never exercised at all."""
        out = dict(upsample([sample(100), sample(400)], max_hold_s=60))
        assert 160 in out
        assert 200 not in out, "the outlet must go quiet, not hold forever"
        assert 400 in out

    def test_an_empty_input_yields_nothing(self) -> None:
        assert list(upsample([])) == []


class TestSweepBucketing:
    def test_one_sweep_per_device(self) -> None:
        sweeps = to_sweeps(
            1788000000,
            [
                sample(0, device="A", child="A00"),
                sample(0, device="A", child="A01"),
                sample(0, device="B", child="B00"),
            ],
        )
        assert sorted(s.device_id for s in sweeps) == ["A", "B"]
        assert sorted(len(s.outlets) for s in sweeps) == [1, 2]

    def test_outlets_of_one_strip_share_a_timestamp(self) -> None:
        """`hourly_strip_peak` reconstructs simultaneous draw by grouping on an
        exact timestamp, so a strip whose outlets disagreed by a millisecond
        would report each outlet as its own peak instead of their sum."""
        sweeps = to_sweeps(1788000000, [sample(0, child="A00"), sample(0, child="A01")])
        assert len(sweeps) == 1
        assert sweeps[0].ts == datetime.fromtimestamp(1788000000, UTC)

    def test_nulls_reach_the_outlet_reading(self) -> None:
        sweeps = to_sweeps(1788000000, [sample(0, power_mw=None, relay_on=True)])
        outlet = sweeps[0].outlets[0]
        assert outlet.power_mw is None
        assert outlet.relay_on is True

    def test_the_alias_is_left_empty(self) -> None:
        """tap learns aliases from the device roster, never from readings, and
        juice must not have one invented for it -- an alias is what drives
        machine assignment."""
        sweeps = to_sweeps(1788000000, [sample(0)])
        assert sweeps[0].outlets[0].alias == ""


class TestWindowResolution:
    """A local-clock window is not a UTC one. "9am to 9pm Saturday" is
    14:00-02:00 UTC and straddles two dates; getting that wrong replays the
    wrong twelve hours and nothing about the result would look unusual."""

    def test_a_local_window_becomes_the_right_utc_range(self) -> None:
        from tests.e2e.replay import resolve_window

        begin, end = resolve_window("2026-08-29", "09:00", "21:00")
        assert (begin.hour, end.hour) == (14, 2)
        assert begin.date().isoformat() == "2026-08-29"
        assert end.date().isoformat() == "2026-08-30", "the window crosses UTC midnight"
        assert (end - begin).total_seconds() == 12 * 3600

    def test_the_window_is_twelve_hours_of_museum_time(self) -> None:
        """Not 12 hours of UTC offset arithmetic that happens to look right --
        the local clock times must come back out unchanged."""
        from tests.e2e.replay import LOCAL_TZ, resolve_window

        begin, end = resolve_window("2026-08-29", "09:00", "21:00")
        assert begin.astimezone(LOCAL_TZ).strftime("%H:%M") == "09:00"
        assert end.astimezone(LOCAL_TZ).strftime("%H:%M") == "21:00"

    def test_an_end_before_the_start_runs_past_midnight(self) -> None:
        from tests.e2e.replay import resolve_window

        begin, end = resolve_window("2026-08-29", "21:00", "02:00")
        assert (end - begin).total_seconds() == 5 * 3600

    def test_no_times_means_the_whole_utc_day(self) -> None:
        """The earlier runs' behaviour, kept."""
        from tests.e2e.replay import resolve_window

        begin, end = resolve_window("2026-09-02", None, None)
        assert begin.isoformat() == "2026-09-02T00:00:00+00:00"
        assert (end - begin).total_seconds() == 24 * 3600


class TestTheVerifyWindowFollowsTheAnchor:
    """`--anchor start|end` shifts every replayed timestamp so the day lands at
    "now". `verify` derives its window from `--day`, so without the shift it
    queries the original dates, finds none of the rows that were just ingested,
    and reports "nothing was ingested for this day" after a successful replay.

    The shift is chosen at replay time from the wall clock, so verify cannot
    recompute it -- the replay has to leave it behind.
    """

    def test_an_unanchored_run_leaves_the_window_alone(self, tmp_path) -> None:
        from tests.e2e.replay import resolve_window, verify_window

        begin, end = resolve_window("2026-09-02", None, None)
        assert verify_window(begin, end, tmp_path, "2026-09-02") == (begin, end)

    def test_a_recorded_shift_moves_the_window(self, tmp_path) -> None:
        from tests.e2e.replay import record_anchor_shift, resolve_window, verify_window

        begin, end = resolve_window("2026-09-02", None, None)
        record_anchor_shift(tmp_path, "2026-09-02", 86_400)

        shifted_begin, shifted_end = verify_window(begin, end, tmp_path, "2026-09-02")
        assert (shifted_begin - begin).total_seconds() == 86_400
        assert (shifted_end - end).total_seconds() == 86_400

    def test_a_shift_recorded_for_another_day_is_ignored(self, tmp_path) -> None:
        """The buffer dir is reused across runs. A stale sidecar from yesterday's
        replay must not silently move today's verification window."""
        from tests.e2e.replay import record_anchor_shift, resolve_window, verify_window

        begin, end = resolve_window("2026-09-02", None, None)
        record_anchor_shift(tmp_path, "2026-08-30", 86_400)
        assert verify_window(begin, end, tmp_path, "2026-09-02") == (begin, end)

    def test_a_zero_shift_is_recorded_as_no_shift(self, tmp_path) -> None:
        from tests.e2e.replay import record_anchor_shift, resolve_window, verify_window

        begin, end = resolve_window("2026-09-02", None, None)
        record_anchor_shift(tmp_path, "2026-09-02", 0)
        assert verify_window(begin, end, tmp_path, "2026-09-02") == (begin, end)

    def test_a_missing_or_unreadable_sidecar_is_not_fatal(self, tmp_path) -> None:
        from tests.e2e.replay import resolve_window, verify_window

        begin, end = resolve_window("2026-09-02", None, None)
        assert verify_window(begin, end, tmp_path / "nope", "2026-09-02") == (begin, end)
        (tmp_path / "replay-anchor.json").write_text("{not json")
        assert verify_window(begin, end, tmp_path, "2026-09-02") == (begin, end)
