"""The e2e fixture must contain problems, or the Problems section has nothing to show.

`tests/e2e/seed.py` builds a production-shaped fixture, but every machine in it is
healthy: `_snapshot_plug_readings` proxies `is_on` from draw, so no plug can ever
be relay-on-with-no-draw, and no device is ever marked offline. The new floor
view's headline feature — a Problems section filtering on status — would render
empty against it, and any spec asserting on it would pass vacuously.

These tests pin the injected states so the fixture keeps its teeth.
"""

from __future__ import annotations

from juice.server import RecorderState, _power_status
from juice.state import Activity, classify
from tests.e2e.serve import PROBLEM_PLUG_COUNT, inject_problem_states


def _state_with_plugs(n: int = 24, devices: int = 6) -> RecorderState:
    """A hydrated-looking state shaped like the real fixture.

    Plug count and device spread matter: the seeded fixture is ~31 machines over
    8 strips, so one unreachable device removes a small minority. A toy state
    with two devices would have one offline device knock out half the floor and
    make the "most machines stay healthy" assertion meaningless.
    """
    from juice.collector import PlugReading

    state = RecorderState()
    per_device = max(1, n // devices)
    for plug_id in range(1, n + 1):
        device_id = f"DEV_{(plug_id - 1) // per_device}"
        child_id = f"{device_id}_{plug_id:02d}"
        state.plugs[plug_id] = (device_id, child_id, f"Machine {plug_id} - M{plug_id:04d}")
        state.plug_has_emeter[plug_id] = True
        state.assignments[plug_id] = (f"Machine {plug_id}", f"M{plug_id:04d}", 1980)
        state.plug_readings[plug_id] = PlugReading(
            child_id=child_id,
            alias=f"Machine {plug_id}",
            is_on=True,
            watts=180.0,
            voltage=120.0,
            amps=1.5,
            total_kwh=10.0,
        )
    return state


class TestInjectProblemStates:
    def test_creates_no_draw_plugs(self) -> None:
        """Relay on, drawing nothing — someone switched the machine off at the
        machine, or it's faulted. The most operationally useful signal juice has."""
        state = _state_with_plugs()
        inject_problem_states(state)

        no_draw = [
            plug_id
            for plug_id in state.plugs
            if _power_status(state.plug_readings.get(plug_id), True, False) == "no_draw"
        ]
        assert len(no_draw) == PROBLEM_PLUG_COUNT
        for plug_id in no_draw:
            reading = state.plug_readings[plug_id]
            assert reading.is_on is True  # relay energized...
            assert reading.watts is not None and reading.watts < 2.0  # ...but nothing drawn

    def test_marks_a_device_offline(self) -> None:
        state = _state_with_plugs()
        inject_problem_states(state)

        assert len(state.offline_since) == 1
        offline_device = next(iter(state.offline_since))
        affected = [p for p, (dev, _c, _a) in state.plugs.items() if dev == offline_device]
        assert affected, "an offline device with no plugs proves nothing"

    def test_creates_an_abandoned_game(self) -> None:
        """Ultra-stable draw with idle_max_rsd set — a game in progress whose
        player walked away. Needs a calibration, or it isn't measurable at all."""
        state = _state_with_plugs()
        inject_problem_states(state)

        abandoned = []
        for plug_id, buf in state.watt_buffers.items():
            cal = state.calibrations.get(plug_id)
            if cal is None:
                continue
            classified = classify(list(buf), cal)
            if classified and classified[-1] is Activity.ABANDONED:
                abandoned.append(plug_id)
        assert abandoned, "fixture has no abandoned game for the Problems section"

    def test_leaves_most_machines_healthy(self) -> None:
        """A fixture that is all problems is as useless as one with none."""
        state = _state_with_plugs()
        inject_problem_states(state)

        healthy = [
            p
            for p in state.plugs
            if state.plugs[p][0] not in state.offline_since
            and _power_status(state.plug_readings.get(p), True, False) == "on"
        ]
        assert len(healthy) > len(state.plugs) // 2

    def test_is_idempotent(self) -> None:
        """The harness may hydrate more than once; injecting twice must not
        cascade more machines into problem states."""
        state = _state_with_plugs()
        inject_problem_states(state)
        offline_once = dict(state.offline_since)
        no_draw_once = {
            p
            for p in state.plugs
            if _power_status(state.plug_readings.get(p), True, False) == "no_draw"
        }

        inject_problem_states(state)

        assert dict(state.offline_since) == offline_once
        no_draw_twice = {
            p
            for p in state.plugs
            if _power_status(state.plug_readings.get(p), True, False) == "no_draw"
        }
        assert no_draw_twice == no_draw_once

    def test_no_op_on_an_empty_state(self) -> None:
        state = RecorderState()
        inject_problem_states(state)  # must not raise
        assert state.offline_since == {}
