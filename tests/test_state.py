"""Tests for juice.state — machine state detection from real power data."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from juice.state import (
    DEFAULT_CALIBRATION,
    LEGACY_STATE_TOKEN,
    OFF_WATTS,
    UNCALIBRATED_CALIBRATION,
    Activity,
    Calibration,
    CalibrationError,
    auto_calibrate,
    classify,
)

# Checked in under tests/data as parquet, ~670 KB. It used to read a hand-placed
# copy of production at data/juice.duckdb, which meant every test here skipped
# silently whenever that file was absent — and it had been skipping unnoticed.
# The readings those tests described are gone regardless: production now starts
# at 2026-03-23, two days after the last window they used. Regenerate with
# `uv run python scripts/make-state-fixture.py`.
FIXTURE = Path(__file__).resolve().parent / "data"

# Per-machine calibrations (must match what's seeded in the DB / used at runtime)
BLACKOUT_CAL = Calibration(idle_max_rsd=None, play_min_rsd=2.0)
EBD_CAL = Calibration(idle_max_rsd=1.0, play_min_rsd=8.0)
GODZILLA_CAL = Calibration(idle_max_rsd=2.0, play_min_rsd=12.0)
HYPERBALL_CAL = Calibration(idle_max_rsd=None, play_min_rsd=13.0)
RFM_CAL = Calibration(idle_max_rsd=None, play_min_rsd=5.0)
TAF_CAL = Calibration(idle_max_rsd=2.1, play_min_rsd=7.0)


def _fetch_watts(
    con: duckdb.DuckDBPyConnection,
    machine: str,
    utc_start: str,
    utc_end: str,
) -> list[float]:
    """Fetch watts for a machine in a UTC time range."""
    rows = con.sql(
        f"""
        SELECT r.watts
        FROM readings r
        JOIN assignments a ON r.plug_id = a.plug_id
            AND r.ts >= a.assigned_from
            AND (a.assigned_until IS NULL OR r.ts < a.assigned_until)
        JOIN machines m USING(machine_id)
        WHERE m.name = '{machine}'
          AND r.ts >= '{utc_start}' AND r.ts < '{utc_end}'
        ORDER BY r.ts
        """
    ).fetchall()
    watts = [r[0] for r in rows]
    # Most assertions below are of the form "this fraction is 0.0", which an
    # empty fetch satisfies for free. A wrong window, a renamed machine or a
    # fixture that lost a day would then pass silently, which is exactly how
    # this suite came to be skipping unnoticed in the first place. The
    # classifier also needs ~30 samples to fill its rolling window, and at the
    # current ~7s cadence that is several minutes of readings.
    assert len(watts) >= 25, (
        f"{machine} {utc_start}..{utc_end} returned {len(watts)} readings; "
        "the window is outside the fixture or the machine name is wrong"
    )
    return watts


@pytest.fixture(scope="module")
def con():
    """The fixture data as views, so the queries below are the production ones."""
    c = duckdb.connect(":memory:")
    for table in ("machines", "assignments", "plugs", "readings"):
        path = FIXTURE / f"{table}.parquet"
        assert path.exists(), f"missing {path}; run scripts/make-state-fixture.py"
        c.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{path}')")
    yield c
    c.close()


# -- Helpers ------------------------------------------------------------------


def _majority_state(states: list[Activity | None]) -> Activity | None:
    """Return the most common state in the list."""
    counts: dict[Activity | None, int] = {}
    for s in states:
        counts[s] = counts.get(s, 0) + 1
    return max(counts, key=lambda s: counts[s])


def _state_fraction(states: list[Activity | None], target: Activity | None) -> float:
    """Fraction of readings classified as target state."""
    if not states:
        return 0.0
    return sum(1 for s in states if s == target) / len(states)


# -- OFF ----------------------------------------------------------------------


class TestOffThreshold:
    """The shared OFF_WATTS cutoff: below it is OFF regardless of the relay."""

    CAL = Calibration(idle_max_rsd=None, play_min_rsd=10.0)

    def test_just_below_threshold_is_off(self) -> None:
        states = classify([OFF_WATTS - 0.1] * 40, self.CAL)
        assert all(s is None for s in states)

    def test_just_above_threshold_is_not_off(self) -> None:
        states = classify([OFF_WATTS + 0.1] * 40, self.CAL)
        assert all(s is not None for s in states)

    def test_lightning_low_power_attract_reads_as_on(self) -> None:
        # Lightning (M0019) draws a steady ~3.5W in attract; it must read as on
        # (ATTRACT), not OFF. Pinned to the real-world value, NOT OFF_WATTS, so a
        # future threshold bump that re-broke this fails here.
        states = classify([3.5] * 40, self.CAL)
        assert all(s == Activity.ATTRACT for s in states)


class TestUncalibratedCalibration:
    """The fallback for machines with no usable calibration: powered -> ATTRACT
    (blue/on), never PLAYING or IDLE, so they don't render an unclassified gray."""

    def test_steady_low_power_is_attract(self) -> None:
        states = classify([3.5] * 40, UNCALIBRATED_CALIBRATION)
        assert all(s == Activity.ATTRACT for s in states)

    def test_off_when_below_threshold(self) -> None:
        states = classify([0.0] * 40, UNCALIBRATED_CALIBRATION)
        assert all(s is None for s in states)

    def test_high_variance_still_attract_never_playing(self) -> None:
        # A swingy signal that a *real* calibration would call PLAYING must stay
        # ATTRACT here — with no calibration we can't claim a machine is playing.
        watts = [100.0, 300.0, 120.0, 340.0, 90.0, 360.0, 110.0, 320.0] * 6
        states = classify(watts, UNCALIBRATED_CALIBRATION)
        assert Activity.PLAYING not in states
        assert Activity.ABANDONED not in states
        assert set(states) <= {Activity.ATTRACT}  # every drawing reading is ATTRACT


class TestOff:
    def test_ebd_before_power_on(self, con: duckdb.DuckDBPyConnection) -> None:
        """EBD reads 0W in the quiet hours after the museum closes."""
        watts = _fetch_watts(
            con,
            "Eight Ball Deluxe Limited Edition",
            "2026-08-26 01:08:00",  # 5:00 PM CT
            "2026-08-26 01:38:00",  # 5:08 PM CT
        )
        assert len(watts) > 0
        states = classify(watts, EBD_CAL)
        assert all(s is None for s in states)

    def test_godzilla_before_power_on(self, con: duckdb.DuckDBPyConnection) -> None:
        watts = _fetch_watts(
            con,
            "Godzilla (Premium)",
            "2026-08-26 01:08:00",
            "2026-08-26 01:38:00",
        )
        assert len(watts) > 0
        states = classify(watts, GODZILLA_CAL)
        assert all(s is None for s in states)


# -- ATTRACT ------------------------------------------------------------------


class TestAttract:
    def test_ebd_early_attract(self, con: duckdb.DuckDBPyConnection) -> None:
        """EBD just after opening — pure attract, no players yet."""
        watts = _fetch_watts(
            con,
            "Eight Ball Deluxe Limited Edition",
            "2026-08-25 21:03:00",
            "2026-08-25 21:13:00",
        )
        states = classify(watts, EBD_CAL)
        assert _state_fraction(states, Activity.ATTRACT) > 0.9

    def test_hyperball_early_attract(self, con: duckdb.DuckDBPyConnection) -> None:
        """Hyperball just after opening — attract mode."""
        watts = _fetch_watts(
            con,
            "Hyperball",
            "2026-08-25 21:03:00",
            "2026-08-25 21:13:00",
        )
        states = classify(watts, HYPERBALL_CAL)
        assert _state_fraction(states, Activity.ATTRACT) > 0.9

    def test_rfm_quiet_attract_is_not_idle(self, con: duckdb.DuckDBPyConnection) -> None:
        """RFM's quiet attract phase must not be classified as IDLE."""
        watts = _fetch_watts(
            con,
            "Revenge From Mars",
            "2026-08-25 21:03:00",  # 5:15 PM CT
            "2026-08-25 21:18:00",  # 5:30 PM CT
        )
        states = classify(watts, RFM_CAL)
        assert _state_fraction(states, Activity.ABANDONED) == 0.0
        assert _state_fraction(states, Activity.ATTRACT) > 0.8

    def test_rfm_no_idle_all_evening(self, con: duckdb.DuckDBPyConnection) -> None:
        """RFM should never show IDLE across the entire evening."""
        watts = _fetch_watts(
            con,
            "Revenge From Mars",
            "2026-08-25 22:00:00",
            "2026-08-26 02:00:00",
        )
        states = classify(watts, RFM_CAL)
        assert _state_fraction(states, Activity.ABANDONED) == 0.0

    def test_hyperball_no_idle_all_evening(self, con: duckdb.DuckDBPyConnection) -> None:
        """Hyperball should never show IDLE."""
        watts = _fetch_watts(
            con,
            "Hyperball",
            "2026-08-25 22:00:00",
            "2026-08-26 02:00:00",
        )
        states = classify(watts, HYPERBALL_CAL)
        assert _state_fraction(states, Activity.ABANDONED) == 0.0


class TestNotIdle:
    """Periods that were falsely classified as IDLE before calibration fix."""

    def test_taf_steady_attract_is_not_idle(self, con: duckdb.DuckDBPyConnection) -> None:
        """Attract steady enough to look idle, but not steady enough to be.

        Chosen as the tightest near-miss available: the window's minimum
        rolling RSD sits within 0.4% of TAF's 2.1% idle threshold without
        crossing it, so any drift toward false IDLE trips this.
        """
        watts = _fetch_watts(
            con,
            "The Addams Family (Coin Op)",
            "2026-08-28 23:15:00",
            "2026-08-28 23:22:00",
        )
        states = classify(watts, TAF_CAL)
        assert _state_fraction(states, Activity.ABANDONED) == 0.0

    def test_taf_steady_attract_is_not_idle_second_evening(
        self, con: duckdb.DuckDBPyConnection
    ) -> None:
        """A second near-miss on the idle threshold, on a different evening."""
        watts = _fetch_watts(
            con,
            "The Addams Family (Coin Op)",
            "2026-09-01 21:37:00",
            "2026-09-01 21:44:00",
        )
        states = classify(watts, TAF_CAL)
        assert _state_fraction(states, Activity.ABANDONED) == 0.0


class TestOff2:
    """OFF detection when machine powers down mid-session."""

    def test_taf_off_after_close(self, con: duckdb.DuckDBPyConnection) -> None:
        """TAF reads 0W after the 01:07 UTC power-down — must be OFF."""
        watts = _fetch_watts(
            con,
            "The Addams Family (Coin Op)",
            "2026-08-26 01:08:00",
            "2026-08-26 01:38:00",
        )
        states = classify(watts, TAF_CAL)
        assert all(s is None for s in states)

    def test_taf_off_boundary(self, con: duckdb.DuckDBPyConnection) -> None:
        """TAF around the 2026-08-26 01:07:29 power-down — on before, off after."""
        # Last reading before off
        watts_before = _fetch_watts(
            con,
            "The Addams Family (Coin Op)",
            "2026-08-26 01:00:00",
            "2026-08-26 01:07:00",
        )
        states_before = classify(watts_before, TAF_CAL)
        assert _state_fraction(states_before, None) == 0.0

        # During off
        watts_during = _fetch_watts(
            con,
            "The Addams Family (Coin Op)",
            "2026-08-26 01:08:00",
            "2026-08-26 01:38:00",
        )
        states_during = classify(watts_during, TAF_CAL)
        assert all(s is None for s in states_during)


class TestNotPlaying:
    """Periods falsely classified as PLAYING before calibration fix."""

    def test_rfm_lively_attract_is_not_playing(self, con: duckdb.DuckDBPyConnection) -> None:
        """Attract lively enough to look like play, but not enough to be.

        The tightest near-miss found: peak rolling RSD reaches 99.8% of RFM's
        5.0% play threshold without crossing it.
        """
        watts = _fetch_watts(
            con,
            "Revenge From Mars",
            "2026-09-01 21:33:00",
            "2026-09-01 21:38:00",
        )
        states = classify(watts, RFM_CAL)
        assert _state_fraction(states, Activity.PLAYING) == 0.0

    def test_rfm_lively_attract_is_not_playing_second_evening(
        self, con: duckdb.DuckDBPyConnection
    ) -> None:
        """A second near-miss on the play threshold, on a different evening."""
        watts = _fetch_watts(
            con,
            "Revenge From Mars",
            "2026-08-27 00:00:00",
            "2026-08-27 00:05:00",
        )
        states = classify(watts, RFM_CAL)
        assert _state_fraction(states, Activity.PLAYING) == 0.0

    def test_rfm_lively_attract_is_not_playing_third_evening(
        self, con: duckdb.DuckDBPyConnection
    ) -> None:
        """A third near-miss on the play threshold, on a different evening."""
        watts = _fetch_watts(
            con,
            "Revenge From Mars",
            "2026-08-30 00:31:00",
            "2026-08-30 00:36:00",
        )
        states = classify(watts, RFM_CAL)
        assert _state_fraction(states, Activity.PLAYING) == 0.0


# -- PLAYING ------------------------------------------------------------------


class TestPlaying:
    def test_ebd_playing(self, con: duckdb.DuckDBPyConnection) -> None:
        """EBD 7:08-7:09 PM CT — active play. EBD is an older EM machine
        whose play spikes are subtle (~6.5% RSD), so it mostly reads as
        ATTRACT with occasional PLAYING spikes. Key test: it is NOT IDLE."""
        watts = _fetch_watts(
            con,
            "Eight Ball Deluxe Limited Edition",
            "2026-08-25 21:03:00",
            "2026-08-25 21:08:00",
        )
        states = classify(watts, EBD_CAL)
        assert _state_fraction(states, Activity.ABANDONED) < 0.1
        assert _state_fraction(states, None) == 0.0

    def test_godzilla_playing(self, con: duckdb.DuckDBPyConnection) -> None:
        """Godzilla under active play — big spikes."""
        watts = _fetch_watts(
            con,
            "Godzilla (Premium)",
            "2026-08-29 00:06:00",
            "2026-08-29 00:11:00",
        )
        states = classify(watts, GODZILLA_CAL)
        assert _state_fraction(states, Activity.PLAYING) > 0.5

    def test_hyperball_playing(self, con: duckdb.DuckDBPyConnection) -> None:
        """Hyperball under active play."""
        watts = _fetch_watts(
            con,
            "Hyperball",
            "2026-09-02 23:01:00",
            "2026-09-02 23:08:00",
        )
        states = classify(watts, HYPERBALL_CAL)
        assert _state_fraction(states, Activity.PLAYING) > 0.5


# -- IDLE (confirmed periods) ------------------------------------------------


class TestIdle:
    def test_ebd_idle(self, con: duckdb.DuckDBPyConnection) -> None:
        """EBD idle. Rare — EBD reads ABANDONED for 1.8% of its readings."""
        watts = _fetch_watts(
            con,
            "Eight Ball Deluxe Limited Edition",
            "2026-08-27 01:04:00",
            "2026-08-27 01:11:00",
        )
        states = classify(watts, EBD_CAL)
        assert _state_fraction(states, Activity.ABANDONED) > 0.6

    def test_godzilla_idle_long_stretch(self, con: duckdb.DuckDBPyConnection) -> None:
        """Godzilla idle — the longest clean stretch in the fixture."""
        watts = _fetch_watts(
            con,
            "Godzilla (Premium)",
            "2026-08-27 00:10:00",
            "2026-08-27 00:17:00",
        )
        states = classify(watts, GODZILLA_CAL)
        assert _state_fraction(states, Activity.ABANDONED) > 0.6

    def test_godzilla_idle_second_evening(self, con: duckdb.DuckDBPyConnection) -> None:
        """Godzilla idle, a second evening."""
        watts = _fetch_watts(
            con,
            "Godzilla (Premium)",
            "2026-09-02 00:31:00",
            "2026-09-02 00:38:00",
        )
        states = classify(watts, GODZILLA_CAL)
        assert _state_fraction(states, Activity.ABANDONED) > 0.6

    def test_godzilla_idle_third_evening(self, con: duckdb.DuckDBPyConnection) -> None:
        """Godzilla idle, a third evening."""
        watts = _fetch_watts(
            con,
            "Godzilla (Premium)",
            "2026-09-03 00:06:00",
            "2026-09-03 00:13:00",
        )
        states = classify(watts, GODZILLA_CAL)
        assert _state_fraction(states, Activity.ABANDONED) > 0.6

    def test_godzilla_idle_fourth_evening(self, con: duckdb.DuckDBPyConnection) -> None:
        """Godzilla idle, a fourth evening."""
        watts = _fetch_watts(
            con,
            "Godzilla (Premium)",
            "2026-08-29 00:45:00",
            "2026-08-29 00:52:00",
        )
        states = classify(watts, GODZILLA_CAL)
        assert _state_fraction(states, Activity.ABANDONED) > 0.6

    def test_godzilla_idle_across_midnight(self, con: duckdb.DuckDBPyConnection) -> None:
        """Godzilla idle across a UTC midnight, which the day-partitioned
        fixture must still join correctly."""
        watts = _fetch_watts(
            con,
            "Godzilla (Premium)",
            "2026-08-29 23:55:00",
            "2026-08-30 00:02:00",
        )
        states = classify(watts, GODZILLA_CAL)
        assert _state_fraction(states, Activity.ABANDONED) > 0.5


class TestTransitions:
    def test_godzilla_playing_to_idle_to_playing(self, con: duckdb.DuckDBPyConnection) -> None:
        """Godzilla PLAYING -> IDLE -> PLAYING across a UTC midnight."""

        # Before idle: playing
        before = _fetch_watts(
            con, "Godzilla (Premium)", "2026-08-29 23:46:00", "2026-08-29 23:51:00"
        )
        states_before = classify(before, GODZILLA_CAL)
        assert _state_fraction(states_before, Activity.PLAYING) > 0.5

        # During idle
        during = _fetch_watts(
            con, "Godzilla (Premium)", "2026-08-29 23:53:00", "2026-08-30 00:00:00"
        )
        states_during = classify(during, GODZILLA_CAL)
        assert _state_fraction(states_during, Activity.ABANDONED) > 0.6

        # After idle: playing resumes
        after = _fetch_watts(
            con, "Godzilla (Premium)", "2026-08-30 00:02:00", "2026-08-30 00:07:00"
        )
        states_after = classify(after, GODZILLA_CAL)
        assert _state_fraction(states_after, Activity.PLAYING) > 0.5

    def test_ebd_playing_to_idle_to_playing(self, con: duckdb.DuckDBPyConnection) -> None:
        """EBD PLAYING -> IDLE -> PLAYING.

        The only such transition in range: EBD is idle for 1.8% of readings,
        which is why the fixture reaches back to July for this one evening.
        """

        before = _fetch_watts(
            con, "Eight Ball Deluxe Limited Edition", "2026-07-16 23:49:00", "2026-07-16 23:54:00"
        )
        states_before = classify(before, EBD_CAL)
        assert _state_fraction(states_before, Activity.ABANDONED) < 0.1  # not idle while playing

        during = _fetch_watts(
            con, "Eight Ball Deluxe Limited Edition", "2026-07-17 00:03:00", "2026-07-17 00:09:00"
        )
        states_during = classify(during, EBD_CAL)
        assert _state_fraction(states_during, Activity.ABANDONED) > 0.6  # idle

        after = _fetch_watts(
            con, "Eight Ball Deluxe Limited Edition", "2026-07-17 00:11:00", "2026-07-17 00:16:00"
        )
        states_after = classify(after, EBD_CAL)
        assert (
            _state_fraction(states_after, Activity.ABANDONED) < 0.1
        )  # not idle after play resumes


# -- Auto-calibration ---------------------------------------------------------


class TestAutoCalibrate:
    """Derive calibration from real power data and verify against known values."""

    def test_godzilla(self, con: duckdb.DuckDBPyConnection) -> None:
        """Godzilla separates IDLE, ATTRACT and PLAYING.

        Only about half of recent evenings let `auto_calibrate` find the idle
        cluster; the fixture carries one that does.
        """
        watts = _fetch_watts(
            con, "Godzilla (Premium)", "2026-09-02 22:00:00", "2026-09-03 02:00:00"
        )
        cal = auto_calibrate(watts)
        assert 8.0 <= cal.play_min_rsd <= 18.0
        assert cal.idle_max_rsd is not None
        assert cal.idle_max_rsd <= 5.0

    def test_hyperball(self, con: duckdb.DuckDBPyConnection) -> None:
        """Hyperball has no IDLE state."""
        watts = _fetch_watts(con, "Hyperball", "2026-08-29 22:00:00", "2026-08-30 02:00:00")
        cal = auto_calibrate(watts)
        assert 10.0 <= cal.play_min_rsd <= 18.0
        assert cal.idle_max_rsd is None

    def test_rfm(self, con: duckdb.DuckDBPyConnection) -> None:
        """RFM has no IDLE state and a low play threshold."""
        watts = _fetch_watts(con, "Revenge From Mars", "2026-08-29 22:00:00", "2026-08-30 02:00:00")
        cal = auto_calibrate(watts)
        assert 3.0 <= cal.play_min_rsd <= 10.0
        assert cal.idle_max_rsd is None

    def test_ebd(self, con: duckdb.DuckDBPyConnection) -> None:
        """EBD has a low play threshold."""
        watts = _fetch_watts(
            con, "Eight Ball Deluxe Limited Edition", "2026-08-29 22:00:00", "2026-08-30 02:00:00"
        )
        cal = auto_calibrate(watts)
        assert 5.0 <= cal.play_min_rsd <= 12.0

    def test_taf(self, con: duckdb.DuckDBPyConnection) -> None:
        """TAF's play threshold is derivable; its idle cluster no longer is.

        This used to assert `idle_max_rsd is not None`. Across 35 consecutive
        evenings of current data `auto_calibrate` finds TAF's idle cluster on
        exactly none of them, and TAF's minimum rolling RSD is 2.72% against
        the 2.1% threshold its stored calibration still carries — so the
        configured idle detection cannot fire in production either. Godzilla,
        by contrast, still separates on 17 of those 35 evenings.
        """
        watts = _fetch_watts(
            con, "The Addams Family (Coin Op)", "2026-08-29 22:00:00", "2026-08-30 02:00:00"
        )
        cal = auto_calibrate(watts)
        assert 5.0 <= cal.play_min_rsd <= 14.0

    def test_too_few_readings(self) -> None:
        with pytest.raises(CalibrationError, match="Not enough non-OFF"):
            auto_calibrate([0.0] * 100)

    def test_all_same_power(self) -> None:
        """Uniform power = no state separation."""
        with pytest.raises(CalibrationError):
            auto_calibrate([100.0] * 3600)


# -- Periodic dip (Blackout) --------------------------------------------------


class TestPeriodicDip:
    """Blackout has a brief power dip every ~270s during attract. Must not
    trigger false PLAYING."""

    def test_synthetic_periodic_dip_not_playing(self) -> None:
        """Stable ~215W with periodic 4-reading dips to ~100W should be ATTRACT."""
        watts: list[float] = []
        for i in range(600):
            if i % 270 < 4:
                watts.append(100.0)
            else:
                watts.append(215.0)
        states = classify(watts, BLACKOUT_CAL)
        on_states = [s for s in states if s is not None]
        playing_frac = _state_fraction(on_states, Activity.PLAYING)
        assert playing_frac < 0.05, f"Got {playing_frac:.1%} PLAYING"

    def test_blackout_attract_with_dip(self, con: duckdb.DuckDBPyConnection) -> None:
        """Blackout attract around its real periodic dip must stay ATTRACT."""
        watts = _fetch_watts(
            con,
            "Blackout",
            "2026-08-25 21:03:00",
            "2026-08-25 21:13:00",
        )
        states = classify(watts, BLACKOUT_CAL)
        on_states = [s for s in states if s is not None]
        playing_frac = _state_fraction(on_states, Activity.PLAYING)
        assert playing_frac < 0.05, f"Got {playing_frac:.1%} PLAYING"


class TestActivityVocabulary:
    """`State` → `Activity` (status_vocabulary.md §3).

    Activity is only what a *drawing* machine can be doing. Not drawing is
    `None` — the absence of an activity, not an activity called OFF. OFFLINE was
    never a classifier output at all; the server injected it at the presentation
    layer, which is the conflation the rename exists to end.
    """

    def test_activity_has_exactly_three_members(self) -> None:
        assert {a.name for a in Activity} == {"ATTRACT", "PLAYING", "ABANDONED"}

    def test_not_drawing_is_none_not_an_activity(self) -> None:
        states = classify([0.0] * 60, DEFAULT_CALIBRATION)
        assert all(s is None for s in states)

    def test_abandoned_replaces_idle(self) -> None:
        """IDLE meant 'game in progress, player walked away' — the opposite of
        what the word suggests, since ATTRACT is the state that is actually idle."""
        cal = Calibration(idle_max_rsd=5.0, play_min_rsd=50.0)
        states = classify([100.0] * 60, cal)
        assert Activity.ABANDONED in states
        assert not hasattr(Activity, "IDLE")

    def test_legacy_token_preserves_the_v1_wire_format(self) -> None:
        """v1's JSON must not change: `state` keeps emitting the old tokens."""
        assert LEGACY_STATE_TOKEN[None] == "OFF"
        assert LEGACY_STATE_TOKEN[Activity.ATTRACT] == "ATTRACT"
        assert LEGACY_STATE_TOKEN[Activity.PLAYING] == "PLAYING"
        assert LEGACY_STATE_TOKEN[Activity.ABANDONED] == "IDLE"

    def test_legacy_token_covers_every_activity(self) -> None:
        """A new Activity can't be added without deciding its v1 token."""
        for activity in Activity:
            assert activity in LEGACY_STATE_TOKEN
