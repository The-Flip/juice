"""Tests for juice.status — the single status derivation.

status_vocabulary.md's central claim is that the "on-ness" cascade exists in
exactly ONE place. These tests make that a checked invariant rather than a
comment: the projection property test below asserts v1's four-value vocabulary
is a pure function of the v2 seven-value one, so a second cascade cannot quietly
reappear.
"""

from __future__ import annotations

import itertools

import pytest

from juice.collector import PlugReading
from juice.state import OFF_WATTS, Activity
from juice.status import Axes, derive_status, legacy_power_status, read_axes


def _reading(is_on: bool, watts: float | None) -> PlugReading:
    return PlugReading(
        child_id="c", alias="a", is_on=is_on, watts=watts, voltage=120.0, amps=1.0, total_kwh=0.0
    )


def _axes(
    *,
    reachable: bool = True,
    relay: str = "on",
    draw: float | None = 100.0,
    activity: Activity | None = Activity.ATTRACT,
    because: str | None = None,
) -> Axes:
    return Axes(
        reachable=reachable,
        relay=relay,
        draw=draw,
        activity=activity,
        activity_unknown_because=because,
    )


class TestDeriveStatus:
    """The cascade, one row per reachable outcome."""

    def test_unreachable_wins_over_everything(self) -> None:
        # Even with a stale relay-on, drawing reading: we don't know, so say so.
        assert derive_status(_axes(reachable=False, relay="on", draw=250.0)) == "unreachable"

    def test_relay_off_is_off(self) -> None:
        assert derive_status(_axes(relay="off", draw=None, activity=None)) == "off"

    def test_relay_on_but_not_drawing_is_no_draw(self) -> None:
        assert derive_status(_axes(draw=0.4, activity=None)) == "no_draw"

    def test_drawing_but_unclassifiable_is_powered(self) -> None:
        """The Lightning/Centipede/Tempest case: honestly on, activity unknown."""
        assert derive_status(_axes(draw=3.5, activity=None)) == "powered"

    def test_unmetered_relay_on_is_powered(self) -> None:
        """No emeter ⇒ draw is None ⇒ we can assert 'on' and nothing more."""
        assert derive_status(_axes(draw=None, activity=None)) == "powered"

    @pytest.mark.parametrize(
        ("activity", "expected"),
        [
            (Activity.ATTRACT, "attract"),
            (Activity.PLAYING, "playing"),
            (Activity.ABANDONED, "abandoned"),
        ],
    )
    def test_activity_surfaces_when_known(self, activity: Activity, expected: str) -> None:
        assert derive_status(_axes(draw=200.0, activity=activity)) == expected

    def test_off_watts_is_the_boundary(self) -> None:
        assert derive_status(_axes(draw=OFF_WATTS - 0.01, activity=None)) == "no_draw"
        assert derive_status(_axes(draw=OFF_WATTS, activity=None)) == "powered"


class TestReadAxes:
    """Axes come off a live reading; `activity_unknown_because` must always
    explain a null activity, so the UI never shows a confident blank."""

    def test_offline_reading(self) -> None:
        axes = read_axes(_reading(True, 100.0), has_emeter=True, offline=True)
        assert axes.reachable is False
        assert axes.activity_unknown_because == "unreachable"

    def test_missing_reading_is_relay_off(self) -> None:
        axes = read_axes(None, has_emeter=True, offline=False)
        assert axes.relay == "off"
        assert axes.draw is None

    def test_no_emeter_has_no_draw_axis(self) -> None:
        axes = read_axes(_reading(True, None), has_emeter=False, offline=False)
        assert axes.draw is None
        assert axes.activity_unknown_because == "unmetered"

    def test_metered_outlet_with_no_watts_is_not_called_unmetered(self) -> None:
        """A metered plug whose reading carried no watts is a *missing
        measurement*, not an outlet without a meter. Both leave `draw` None, but
        conflating them makes the UI explain the wrong thing — and v1 already
        treated a missing watts value as "we don't know" rather than a fact."""
        axes = read_axes(_reading(True, None), has_emeter=True, offline=False)
        assert axes.draw is None
        assert axes.activity_unknown_because == "no_measurement"

    def test_the_two_no_draw_axis_cases_are_distinguishable(self) -> None:
        unmetered = read_axes(_reading(True, None), has_emeter=False, offline=False)
        unmeasured = read_axes(_reading(True, None), has_emeter=True, offline=False)
        assert unmetered.draw is unmeasured.draw is None
        assert unmetered.activity_unknown_because != unmeasured.activity_unknown_because
        # Both are honestly "on, and that is all we know".
        assert derive_status(unmetered) == derive_status(unmeasured) == "powered"

    def test_not_drawing_explains_itself(self) -> None:
        axes = read_axes(_reading(True, 0.2), has_emeter=True, offline=False)
        assert axes.activity_unknown_because == "not_drawing"

    def test_uncalibrated_reports_powered_not_attract(self) -> None:
        """An uncalibrated machine is honestly 'powered'. v1 forced these through
        ATTRACT to avoid a gray tile (#74); v2 keeps the same colour but stops
        claiming knowledge we don't have."""
        axes = read_axes(
            _reading(True, 300.0),
            has_emeter=True,
            offline=False,
            activity=Activity.ATTRACT,
            calibrated=False,
        )
        assert axes.activity is None
        assert axes.activity_unknown_because == "uncalibrated"
        assert derive_status(axes) == "powered"

    def test_calibrated_activity_passes_through(self) -> None:
        axes = read_axes(
            _reading(True, 300.0),
            has_emeter=True,
            offline=False,
            activity=Activity.PLAYING,
            calibrated=True,
        )
        assert axes.activity is Activity.PLAYING
        assert axes.activity_unknown_because is None

    def test_activity_is_discarded_when_the_machine_is_not_drawing(self) -> None:
        """An activity is something a *drawing* machine does. The rolling buffer
        can still hold a machine's last busy minute after its draw collapses, so
        the classifier will happily report PLAYING for an outlet reading 0 W —
        `read_axes` must not pass that through."""
        axes = read_axes(
            _reading(True, 0.0),
            has_emeter=True,
            offline=False,
            activity=Activity.PLAYING,
            calibrated=True,
        )
        assert axes.activity is None
        assert axes.activity_unknown_because == "not_drawing"
        assert derive_status(axes) == "no_draw"

    def test_activity_is_discarded_when_unreachable(self) -> None:
        axes = read_axes(
            _reading(True, 300.0),
            has_emeter=True,
            offline=True,
            activity=Activity.PLAYING,
            calibrated=True,
        )
        assert axes.activity is None
        assert axes.activity_unknown_because == "unreachable"

    def test_activity_is_discarded_when_the_relay_is_off(self) -> None:
        axes = read_axes(
            _reading(False, 0.0),
            has_emeter=True,
            offline=False,
            activity=Activity.ATTRACT,
            calibrated=True,
        )
        assert axes.activity is None

    def test_axes_are_always_self_consistent(self) -> None:
        """The invariant: a reported activity implies an activity-bearing status.
        Anything else is a payload that contradicts itself."""
        for relay_on in (True, False):
            for watts in (None, 0.0, 1.0, 300.0):
                for offline in (True, False):
                    for activity in (None, *Activity):
                        axes = read_axes(
                            _reading(relay_on, watts),
                            has_emeter=True,
                            offline=offline,
                            activity=activity,
                            calibrated=True,
                        )
                        status = derive_status(axes)
                        if axes.activity is not None:
                            assert status in {"attract", "playing", "abandoned"}, (
                                relay_on,
                                watts,
                                offline,
                                activity,
                                status,
                            )
                        else:
                            assert axes.activity_unknown_because is not None

    def test_a_known_activity_never_carries_a_reason(self) -> None:
        for activity in Activity:
            axes = read_axes(
                _reading(True, 300.0),
                has_emeter=True,
                offline=False,
                activity=activity,
                calibrated=True,
            )
            assert axes.activity_unknown_because is None


def _v1_reference(reading: PlugReading | None, has_emeter: bool, offline: bool) -> str:
    """Independent restatement of v1's original `_power_status` logic.

    Deliberately a copy of the ORIGINAL, not a call into the shipped one — this
    is the oracle the projection is checked against, so it must not move when
    the implementation does.
    """
    if offline:
        return "offline"
    if reading is None or not reading.is_on:
        return "off"
    if has_emeter and reading.watts is not None and reading.watts < OFF_WATTS:
        return "no_draw"
    return "on"


class TestV1Projection:
    """v1's vocabulary must be a pure projection of v2's — the guard that keeps
    'exactly one cascade' true as the code changes."""

    def test_projection_matches_v1_over_the_whole_input_space(self) -> None:
        watt_values = [None, 0.0, 0.5, OFF_WATTS - 0.01, OFF_WATTS, 3.5, 250.0]
        cases = itertools.product([True, False], [True, False], watt_values, [True, False])
        checked = 0
        for offline, has_emeter, watts, is_on in cases:
            for reading in (None, _reading(is_on, watts)):
                expected = _v1_reference(reading, has_emeter, offline)
                actual = legacy_power_status(
                    read_axes(reading, has_emeter=has_emeter, offline=offline)
                )
                assert actual == expected, (offline, has_emeter, watts, is_on, reading)
                checked += 1
        assert checked == 112  # the space is actually being walked

    def test_every_v2_status_projects_to_a_v1_value(self) -> None:
        """A new status can't be added without deciding what v1 shows for it."""
        from juice.status import _V1_PROJECTION, STATUSES

        assert set(_V1_PROJECTION) == set(STATUSES)
        assert set(_V1_PROJECTION.values()) == {"offline", "off", "no_draw", "on"}


class TestTotality:
    def test_derive_status_is_total(self) -> None:
        """Never raises, never returns None — a status always has a name."""
        from juice.status import STATUSES

        for reachable, relay, draw, activity in itertools.product(
            [True, False],
            ["on", "off"],
            [None, 0.0, 1.0, 500.0],
            [None, *Activity],
        ):
            result = derive_status(
                Axes(
                    reachable=reachable,
                    relay=relay,
                    draw=draw,
                    activity=activity,
                    activity_unknown_because=None,
                )
            )
            assert result in STATUSES
