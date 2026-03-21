"""Tests for juice.state — machine state detection from real power data."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from juice.state import Calibration, CalibrationError, State, auto_calibrate, classify

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "juice.duckdb"

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
    return [r[0] for r in rows]


@pytest.fixture(scope="module")
def con():
    if not DB_PATH.exists():
        pytest.skip("juice.duckdb not available")
    c = duckdb.connect(str(DB_PATH), read_only=True)
    yield c
    c.close()


# -- Helpers ------------------------------------------------------------------


def _majority_state(states: list[State]) -> State:
    """Return the most common state in the list."""
    counts: dict[State, int] = {}
    for s in states:
        counts[s] = counts.get(s, 0) + 1
    return max(counts, key=lambda s: counts[s])


def _state_fraction(states: list[State], target: State) -> float:
    """Fraction of readings classified as target state."""
    if not states:
        return 0.0
    return sum(1 for s in states if s == target) / len(states)


# -- OFF ----------------------------------------------------------------------


class TestOff:
    def test_ebd_before_power_on(self, con: duckdb.DuckDBPyConnection) -> None:
        """EBD reads 0W before machines are turned on at ~5:08 PM CT."""
        watts = _fetch_watts(
            con,
            "Eight Ball Deluxe Limited Edition",
            "2026-03-19 22:00:00",  # 5:00 PM CT
            "2026-03-19 22:08:00",  # 5:08 PM CT
        )
        assert len(watts) > 0
        states = classify(watts, EBD_CAL)
        assert all(s == State.OFF for s in states)

    def test_godzilla_before_power_on(self, con: duckdb.DuckDBPyConnection) -> None:
        watts = _fetch_watts(
            con,
            "Godzilla (Premium)",
            "2026-03-19 22:00:00",
            "2026-03-19 22:08:00",
        )
        assert len(watts) > 0
        states = classify(watts, GODZILLA_CAL)
        assert all(s == State.OFF for s in states)


# -- ATTRACT ------------------------------------------------------------------


class TestAttract:
    def test_ebd_early_attract(self, con: duckdb.DuckDBPyConnection) -> None:
        """EBD 5:15-5:25 PM CT — pure attract, no players yet."""
        watts = _fetch_watts(
            con,
            "Eight Ball Deluxe Limited Edition",
            "2026-03-19 22:15:00",
            "2026-03-19 22:25:00",
        )
        states = classify(watts, EBD_CAL)
        assert _state_fraction(states, State.ATTRACT) > 0.9

    def test_hyperball_early_attract(self, con: duckdb.DuckDBPyConnection) -> None:
        """Hyperball 5:15-5:25 PM CT — attract mode, ~10% RSD."""
        watts = _fetch_watts(
            con,
            "Hyperball",
            "2026-03-19 22:15:00",
            "2026-03-19 22:25:00",
        )
        states = classify(watts, HYPERBALL_CAL)
        assert _state_fraction(states, State.ATTRACT) > 0.9

    def test_rfm_quiet_attract_is_not_idle(
        self, con: duckdb.DuckDBPyConnection
    ) -> None:
        """RFM's quiet attract phase (RSD ~0.6%) must not be classified as IDLE."""
        watts = _fetch_watts(
            con,
            "Revenge From Mars",
            "2026-03-19 22:15:00",  # 5:15 PM CT
            "2026-03-19 22:30:00",  # 5:30 PM CT
        )
        states = classify(watts, RFM_CAL)
        assert _state_fraction(states, State.IDLE) == 0.0
        assert _state_fraction(states, State.ATTRACT) > 0.8

    def test_rfm_no_idle_all_evening(self, con: duckdb.DuckDBPyConnection) -> None:
        """RFM should never show IDLE across the entire evening."""
        watts = _fetch_watts(
            con,
            "Revenge From Mars",
            "2026-03-19 22:00:00",
            "2026-03-20 02:00:00",
        )
        states = classify(watts, RFM_CAL)
        assert _state_fraction(states, State.IDLE) == 0.0

    def test_hyperball_no_idle_all_evening(
        self, con: duckdb.DuckDBPyConnection
    ) -> None:
        """Hyperball should never show IDLE."""
        watts = _fetch_watts(
            con,
            "Hyperball",
            "2026-03-19 22:00:00",
            "2026-03-20 02:00:00",
        )
        states = classify(watts, HYPERBALL_CAL)
        assert _state_fraction(states, State.IDLE) == 0.0


class TestNotIdle:
    """Periods that were falsely classified as IDLE before calibration fix."""

    def test_taf_not_idle_6_21_09(self, con: duckdb.DuckDBPyConnection) -> None:
        """TAF 6:21:09 PM CT — attract, not idle."""
        watts = _fetch_watts(
            con, "The Addams Family",
            "2026-03-19 23:20:30", "2026-03-19 23:22:00",
        )
        states = classify(watts, TAF_CAL)
        assert _state_fraction(states, State.IDLE) == 0.0

    def test_taf_not_idle_6_45_42(self, con: duckdb.DuckDBPyConnection) -> None:
        """TAF 6:45:42 PM CT — attract, not idle."""
        watts = _fetch_watts(
            con, "The Addams Family",
            "2026-03-19 23:45:00", "2026-03-19 23:46:30",
        )
        states = classify(watts, TAF_CAL)
        assert _state_fraction(states, State.IDLE) == 0.0


class TestOff2:
    """OFF detection when machine powers down mid-session."""

    def test_taf_off_6_48_to_6_52(self, con: duckdb.DuckDBPyConnection) -> None:
        """TAF reads 0W from ~6:48:33 to 6:52:37 PM CT — must be OFF."""
        watts = _fetch_watts(
            con, "The Addams Family",
            "2026-03-19 23:48:33", "2026-03-19 23:52:37",
        )
        states = classify(watts, TAF_CAL)
        assert all(s == State.OFF for s in states)

    def test_taf_off_boundary(self, con: duckdb.DuckDBPyConnection) -> None:
        """TAF around the OFF boundary — on before, off after."""
        # Last reading before off
        watts_before = _fetch_watts(
            con, "The Addams Family",
            "2026-03-19 23:48:00", "2026-03-19 23:48:33",
        )
        states_before = classify(watts_before, TAF_CAL)
        assert _state_fraction(states_before, State.OFF) == 0.0

        # During off
        watts_during = _fetch_watts(
            con, "The Addams Family",
            "2026-03-19 23:49:00", "2026-03-19 23:52:00",
        )
        states_during = classify(watts_during, TAF_CAL)
        assert all(s == State.OFF for s in states_during)


class TestNotPlaying:
    """Periods falsely classified as PLAYING before calibration fix."""

    def test_rfm_not_playing_6_34(self, con: duckdb.DuckDBPyConnection) -> None:
        """RFM 6:34 PM CT — level transition, not playing."""
        watts = _fetch_watts(
            con, "Revenge From Mars",
            "2026-03-19 23:33:30", "2026-03-19 23:34:30",
        )
        states = classify(watts, RFM_CAL)
        assert _state_fraction(states, State.PLAYING) == 0.0

    def test_rfm_not_playing_7_02(self, con: duckdb.DuckDBPyConnection) -> None:
        """RFM 7:02 PM CT — level transition, not playing."""
        watts = _fetch_watts(
            con, "Revenge From Mars",
            "2026-03-20 00:02:00", "2026-03-20 00:03:00",
        )
        states = classify(watts, RFM_CAL)
        assert _state_fraction(states, State.PLAYING) == 0.0

    def test_rfm_not_playing_8_15(self, con: duckdb.DuckDBPyConnection) -> None:
        """RFM 8:15 PM CT — level transition, not playing."""
        watts = _fetch_watts(
            con, "Revenge From Mars",
            "2026-03-20 01:14:30", "2026-03-20 01:15:30",
        )
        states = classify(watts, RFM_CAL)
        assert _state_fraction(states, State.PLAYING) == 0.0


# -- PLAYING ------------------------------------------------------------------


class TestPlaying:
    def test_ebd_playing(self, con: duckdb.DuckDBPyConnection) -> None:
        """EBD 7:08-7:09 PM CT — active play. EBD is an older EM machine
        whose play spikes are subtle (~6.5% RSD), so it mostly reads as
        ATTRACT with occasional PLAYING spikes. Key test: it is NOT IDLE."""
        watts = _fetch_watts(
            con,
            "Eight Ball Deluxe Limited Edition",
            "2026-03-20 00:08:00",
            "2026-03-20 00:09:00",
        )
        states = classify(watts, EBD_CAL)
        assert _state_fraction(states, State.IDLE) < 0.1
        assert _state_fraction(states, State.OFF) == 0.0

    def test_godzilla_playing(self, con: duckdb.DuckDBPyConnection) -> None:
        """Godzilla 8:02-8:04 PM CT — active play, big spikes."""
        watts = _fetch_watts(
            con,
            "Godzilla (Premium)",
            "2026-03-20 01:02:00",
            "2026-03-20 01:04:00",
        )
        states = classify(watts, GODZILLA_CAL)
        assert _state_fraction(states, State.PLAYING) > 0.5

    def test_hyperball_playing(self, con: duckdb.DuckDBPyConnection) -> None:
        """Hyperball 7:15-7:20 PM CT — active play, ~20% RSD."""
        watts = _fetch_watts(
            con,
            "Hyperball",
            "2026-03-20 00:15:00",
            "2026-03-20 00:20:00",
        )
        states = classify(watts, HYPERBALL_CAL)
        assert _state_fraction(states, State.PLAYING) > 0.5


# -- IDLE (confirmed periods) ------------------------------------------------


class TestIdle:
    def test_ebd_idle_7_10(self, con: duckdb.DuckDBPyConnection) -> None:
        """EBD 7:10:08-7:11:09 PM CT — confirmed IDLE, std=0.56W."""
        watts = _fetch_watts(
            con,
            "Eight Ball Deluxe Limited Edition",
            "2026-03-20 00:10:08",
            "2026-03-20 00:11:09",
        )
        states = classify(watts, EBD_CAL)
        assert _state_fraction(states, State.IDLE) > 0.6

    def test_godzilla_idle_8_04(self, con: duckdb.DuckDBPyConnection) -> None:
        """Godzilla 8:04:45-8:13:30 PM CT — confirmed IDLE, std=0.67W."""
        watts = _fetch_watts(
            con,
            "Godzilla (Premium)",
            "2026-03-20 01:04:45",
            "2026-03-20 01:13:30",
        )
        states = classify(watts, GODZILLA_CAL)
        assert _state_fraction(states, State.IDLE) > 0.6

    def test_godzilla_idle_7_12(self, con: duckdb.DuckDBPyConnection) -> None:
        """Godzilla 7:12:10-7:13:12 PM CT — IDLE, 62s."""
        watts = _fetch_watts(
            con,
            "Godzilla (Premium)",
            "2026-03-20 00:12:10",
            "2026-03-20 00:13:12",
        )
        states = classify(watts, GODZILLA_CAL)
        assert _state_fraction(states, State.IDLE) > 0.6

    def test_godzilla_idle_7_49(self, con: duckdb.DuckDBPyConnection) -> None:
        """Godzilla 7:49:05-7:52:53 PM CT — IDLE, 228s."""
        watts = _fetch_watts(
            con,
            "Godzilla (Premium)",
            "2026-03-20 00:49:05",
            "2026-03-20 00:52:53",
        )
        states = classify(watts, GODZILLA_CAL)
        assert _state_fraction(states, State.IDLE) > 0.6

    def test_godzilla_idle_7_56(self, con: duckdb.DuckDBPyConnection) -> None:
        """Godzilla 7:56:41-7:57:37 PM CT — IDLE, 56s."""
        watts = _fetch_watts(
            con,
            "Godzilla (Premium)",
            "2026-03-20 00:56:41",
            "2026-03-20 00:57:37",
        )
        states = classify(watts, GODZILLA_CAL)
        assert _state_fraction(states, State.IDLE) > 0.6

    def test_godzilla_idle_8_46(self, con: duckdb.DuckDBPyConnection) -> None:
        """Godzilla 8:46:01-8:46:40 PM CT — IDLE, 39s."""
        watts = _fetch_watts(
            con,
            "Godzilla (Premium)",
            "2026-03-20 01:46:01",
            "2026-03-20 01:46:40",
        )
        states = classify(watts, GODZILLA_CAL)
        assert _state_fraction(states, State.IDLE) > 0.5

    def test_taf_idle_7_38(self, con: duckdb.DuckDBPyConnection) -> None:
        """TAF 7:38:48-7:39:55 PM CT — confirmed IDLE, std=2.88W."""
        watts = _fetch_watts(
            con,
            "The Addams Family",
            "2026-03-20 00:38:48",
            "2026-03-20 00:39:55",
        )
        states = classify(watts, TAF_CAL)
        assert _state_fraction(states, State.IDLE) > 0.5


# -- Transitions --------------------------------------------------------------


class TestTransitions:
    def test_godzilla_playing_to_idle_to_playing(
        self, con: duckdb.DuckDBPyConnection
    ) -> None:
        """Godzilla 8:02-8:15 PM CT — PLAYING -> IDLE -> PLAYING transition."""

        # Before idle: playing
        before = _fetch_watts(con, "Godzilla (Premium)", "2026-03-20 01:02:00", "2026-03-20 01:04:00")
        states_before = classify(before, GODZILLA_CAL)
        assert _state_fraction(states_before, State.PLAYING) > 0.5

        # During idle
        during = _fetch_watts(con, "Godzilla (Premium)", "2026-03-20 01:05:00", "2026-03-20 01:13:00")
        states_during = classify(during, GODZILLA_CAL)
        assert _state_fraction(states_during, State.IDLE) > 0.6

        # After idle: playing resumes
        after = _fetch_watts(con, "Godzilla (Premium)", "2026-03-20 01:14:00", "2026-03-20 01:15:00")
        states_after = classify(after, GODZILLA_CAL)
        assert _state_fraction(states_after, State.PLAYING) > 0.5

    def test_ebd_playing_to_idle_to_playing(
        self, con: duckdb.DuckDBPyConnection
    ) -> None:
        """EBD 7:08-7:12 PM CT — PLAYING -> IDLE -> PLAYING transition."""

        before = _fetch_watts(con, "Eight Ball Deluxe Limited Edition", "2026-03-20 00:08:00", "2026-03-20 00:09:50")
        states_before = classify(before, EBD_CAL)
        assert _state_fraction(states_before, State.IDLE) < 0.1  # not idle while playing

        during = _fetch_watts(con, "Eight Ball Deluxe Limited Edition", "2026-03-20 00:10:08", "2026-03-20 00:11:09")
        states_during = classify(during, EBD_CAL)
        assert _state_fraction(states_during, State.IDLE) > 0.6  # idle

        after = _fetch_watts(con, "Eight Ball Deluxe Limited Edition", "2026-03-20 00:11:20", "2026-03-20 00:12:30")
        states_after = classify(after, EBD_CAL)
        assert _state_fraction(states_after, State.IDLE) < 0.1  # not idle after play resumes


# -- Auto-calibration ---------------------------------------------------------


class TestAutoCalibrate:
    """Derive calibration from real power data and verify against known values."""

    def test_godzilla(self, con: duckdb.DuckDBPyConnection) -> None:
        """Godzilla has clear IDLE, ATTRACT, PLAYING separation."""
        watts = _fetch_watts(con, "Godzilla (Premium)", "2026-03-19 22:08:00", "2026-03-20 02:00:00")
        cal = auto_calibrate(watts)
        assert 8.0 <= cal.play_min_rsd <= 18.0
        assert cal.idle_max_rsd is not None
        assert cal.idle_max_rsd <= 5.0

    def test_hyperball(self, con: duckdb.DuckDBPyConnection) -> None:
        """Hyperball has no IDLE state."""
        watts = _fetch_watts(con, "Hyperball", "2026-03-19 22:08:00", "2026-03-20 02:00:00")
        cal = auto_calibrate(watts)
        assert 10.0 <= cal.play_min_rsd <= 18.0
        assert cal.idle_max_rsd is None

    def test_rfm(self, con: duckdb.DuckDBPyConnection) -> None:
        """RFM has no IDLE state and a low play threshold."""
        watts = _fetch_watts(con, "Revenge From Mars", "2026-03-19 22:08:00", "2026-03-20 02:00:00")
        cal = auto_calibrate(watts)
        assert 3.0 <= cal.play_min_rsd <= 10.0
        assert cal.idle_max_rsd is None

    def test_ebd(self, con: duckdb.DuckDBPyConnection) -> None:
        """EBD has a low play threshold."""
        watts = _fetch_watts(con, "Eight Ball Deluxe Limited Edition", "2026-03-19 22:08:00", "2026-03-20 02:00:00")
        cal = auto_calibrate(watts)
        assert 5.0 <= cal.play_min_rsd <= 12.0

    def test_taf(self, con: duckdb.DuckDBPyConnection) -> None:
        """TAF has detectable IDLE with two-phase approach."""
        watts = _fetch_watts(con, "The Addams Family", "2026-03-19 22:08:00", "2026-03-20 02:00:00")
        cal = auto_calibrate(watts)
        assert 5.0 <= cal.play_min_rsd <= 14.0
        assert cal.idle_max_rsd is not None
        assert cal.idle_max_rsd <= 4.0

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
        on_states = [s for s in states if s != State.OFF]
        playing_frac = _state_fraction(on_states, State.PLAYING)
        assert playing_frac < 0.05, f"Got {playing_frac:.1%} PLAYING"

    def test_blackout_attract_with_dip(self, con: duckdb.DuckDBPyConnection) -> None:
        """Blackout attract around a real dip (~01:40:57 UTC) must be ATTRACT."""
        watts = _fetch_watts(
            con, "Blackout",
            "2026-03-21 01:39:00", "2026-03-21 01:43:00",
        )
        states = classify(watts, BLACKOUT_CAL)
        on_states = [s for s in states if s != State.OFF]
        playing_frac = _state_fraction(on_states, State.PLAYING)
        assert playing_frac < 0.05, f"Got {playing_frac:.1%} PLAYING"
