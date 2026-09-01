"""Tests for status_since — how long an item has been in its current status.

Without it the Problems section can't tell a machine five seconds into a reboot
from one that has been dead for four hours, so it fills with machines that are
simply still starting every time someone opens the museum. Operators stop
trusting it within a week.

It has to be tracked continuously rather than computed on read: if nobody looks
for an hour, a value derived at request time would report a duration of zero.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from juice.collector import PlugReading
from juice.server import RecorderState, track_status

T0 = datetime(2026, 8, 31, 12, 0, 0, tzinfo=UTC)


def _reading(is_on: bool, watts: float | None) -> PlugReading:
    return PlugReading(
        child_id="c", alias="a", is_on=is_on, watts=watts, voltage=120.0, amps=1.0, total_kwh=0.0
    )


class TestTrackStatus:
    def test_records_when_a_status_is_first_seen(self) -> None:
        state = RecorderState()
        track_status(state, 1, _reading(True, 200.0), has_emeter=True, offline=False, now=T0)

        status, since = state.status_since[1]
        assert status == "powered"
        assert since == T0

    def test_an_unchanged_status_keeps_its_original_timestamp(self) -> None:
        """The whole point — the duration must accumulate, not reset each tick."""
        state = RecorderState()
        for offset in range(5):
            track_status(
                state,
                1,
                _reading(True, 0.0),
                has_emeter=True,
                offline=False,
                now=T0 + timedelta(seconds=offset),
            )

        status, since = state.status_since[1]
        assert status == "no_draw"
        assert since == T0

    def test_a_changed_status_resets_the_timestamp(self) -> None:
        state = RecorderState()
        track_status(state, 1, _reading(True, 200.0), has_emeter=True, offline=False, now=T0)
        later = T0 + timedelta(minutes=5)
        track_status(state, 1, _reading(True, 0.0), has_emeter=True, offline=False, now=later)

        status, since = state.status_since[1]
        assert status == "no_draw"
        assert since == later

    def test_going_offline_is_a_change(self) -> None:
        state = RecorderState()
        track_status(state, 1, _reading(True, 200.0), has_emeter=True, offline=False, now=T0)
        later = T0 + timedelta(minutes=1)
        track_status(state, 1, _reading(True, 200.0), has_emeter=True, offline=True, now=later)

        assert state.status_since[1] == ("unreachable", later)

    def test_plugs_are_tracked_independently(self) -> None:
        state = RecorderState()
        track_status(state, 1, _reading(True, 200.0), has_emeter=True, offline=False, now=T0)
        track_status(
            state,
            2,
            _reading(False, 0.0),
            has_emeter=True,
            offline=False,
            now=T0 + timedelta(minutes=1),
        )

        assert state.status_since[1][0] == "powered"
        assert state.status_since[2][0] == "off"

    def test_activity_is_deliberately_not_part_of_the_tracked_status(self) -> None:
        """Tracking is done on the recorder's hot path, once per plug per second.
        Resolving attract/playing/abandoned means running the classifier over a
        3600-sample buffer for every machine every tick, which is real cost for a
        duration nobody reads at that resolution.

        So this tracks the *physical* status, where the durations that matter are
        exact: no_draw, unreachable and off. A drawing machine reads `powered`,
        and its status_since means "drawing since" — which the floor endpoint
        documents rather than dressing up as something finer.
        """
        state = RecorderState()
        track_status(state, 1, _reading(True, 200.0), has_emeter=True, offline=False, now=T0)
        assert state.status_since[1][0] == "powered"


class TestOfflineTransition:
    """A device going offline must move status_since too.

    track_status runs on a successful read, so without an explicit hook the
    plugs of a device that just stopped answering keep the timestamp from
    whenever they were last read in some other status. The floor then reports
    "unreachable" with a duration that could be hours old or minutes short —
    and a misleading duration is worse than none, because it is what an operator
    triages on.
    """

    def test_marking_a_device_offline_stamps_its_plugs(self) -> None:
        from juice.recorder import note_device_failure

        state = RecorderState()
        state.plugs[1] = ("DEV", "DEV01", "a - M0001")
        state.plugs[2] = ("DEV", "DEV02", "b - M0002")
        state.plugs[3] = ("OTHER", "OTHER01", "c - M0003")
        for plug_id in (1, 2, 3):
            state.plug_has_emeter[plug_id] = True
            track_status(
                state, plug_id, _reading(True, 200.0), has_emeter=True, offline=False, now=T0
            )

        went_down = T0 + timedelta(minutes=10)
        for _ in range(3):  # OFFLINE_FAILURE_THRESHOLD
            note_device_failure(state, "DEV", went_down, RuntimeError("Device is offline"))

        assert state.status_since[1] == ("unreachable", went_down)
        assert state.status_since[2] == ("unreachable", went_down)
        assert state.status_since[3] == ("powered", T0)  # untouched

    def test_below_the_threshold_nothing_changes(self) -> None:
        from juice.recorder import note_device_failure

        state = RecorderState()
        state.plugs[1] = ("DEV", "DEV01", "a - M0001")
        state.plug_has_emeter[1] = True
        track_status(state, 1, _reading(True, 200.0), has_emeter=True, offline=False, now=T0)

        note_device_failure(state, "DEV", T0 + timedelta(minutes=1), RuntimeError("blip"))

        assert state.status_since[1] == ("powered", T0)

    def test_recovery_to_the_same_status_restamps(self) -> None:
        """Otherwise a machine that was unreachable for an hour and came back
        would claim it had been drawing all along."""
        from juice.recorder import note_device_failure

        state = RecorderState()
        state.plugs[1] = ("DEV", "DEV01", "a - M0001")
        state.plug_has_emeter[1] = True
        track_status(state, 1, _reading(True, 200.0), has_emeter=True, offline=False, now=T0)

        down = T0 + timedelta(minutes=10)
        for _ in range(3):
            note_device_failure(state, "DEV", down, RuntimeError("Device is offline"))
        assert state.status_since[1][0] == "unreachable"

        back = down + timedelta(hours=1)
        track_status(state, 1, _reading(True, 200.0), has_emeter=True, offline=False, now=back)

        assert state.status_since[1] == ("powered", back)
