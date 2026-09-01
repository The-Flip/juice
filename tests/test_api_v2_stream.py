"""Tests for GET /api/v2/stream — the v2 projection of the shared event bus.

v1 and v2 read the same bus but speak different vocabularies: v1's pages read
power_change / reboot / operation_step and must keep doing so, while v2 clients
read `command`, which expresses the same actions with a lifecycle attached.

The dense-seq contract is what lets the new UI drop v1's blind polling, so most
of these are about not breaking it.
"""

from __future__ import annotations

import asyncio

import pytest

from juice.api.v2.stream import project
from juice.server import RecorderState, _publish, _sse_stream


def _reading_event() -> dict:
    return {
        "type": "readings",
        "machines": [
            {
                "plug_id": 1,
                "status": "playing",
                "activity": "playing",
                "activity_unknown_because": None,
                "status_since": "2026-09-01T12:00:00+00:00",
                "is_on": True,
                "watt": 210.0,
                "power": {"watts": 210.0},
                "power_status": "on",
                "state": "PLAYING",
                "offline": False,
            }
        ],
    }


class TestProjection:
    def test_readings_becomes_a_reading_tick_in_v2_vocabulary(self) -> None:
        out = project(_reading_event(), public=False)

        assert out is not None
        assert out["type"] == "reading_tick"
        machine = out["machines"][0]
        assert machine["status"] == "playing"
        assert machine["relay"] == "on"
        assert machine["draw_watts"] == 210.0
        assert machine["status_since"] == "2026-09-01T12:00:00+00:00"

    def test_v1_only_keys_are_not_carried_into_v2(self) -> None:
        """A v2 client must not be able to read the old vocabulary by accident —
        that is how six render sites came to disagree in the first place."""
        out = project(_reading_event(), public=False)
        machine = out["machines"][0]

        for legacy in ("state", "power_status", "is_on", "watt", "power", "offline"):
            assert legacy not in machine

    def test_v1_action_events_are_dropped(self) -> None:
        """power_change and reboot are v1's way of saying what `command` says
        with a lifecycle; delivering both would be noise a client must dedupe."""
        for kind in ("power_change", "reboot", "lock_change", "operation_step_retry"):
            assert project({"type": kind, "plug_id": 1}, public=False) is None

    def test_command_events_pass_through(self) -> None:
        out = project({"type": "command", "command_id": "abc", "phase": "confirmed"}, public=False)
        assert out is not None
        assert out["command_id"] == "abc"

    def test_resync_reaches_public_subscribers_too(self) -> None:
        """A gap is equally true for an anonymous viewer, and the notice names
        nobody."""
        assert project({"type": "resync_required"}, public=True) is not None

    def test_command_traffic_is_withheld_from_public_subscribers(self) -> None:
        """Commands name the operator who issued them."""
        assert project({"type": "command", "actor": "dana@theflip.museum"}, public=True) is None


class TestSeqDensityUnderProjection:
    """Dropping an event must not consume a sequence number.

    This is the subtle one: _sse_stream assigns seq before handing the event to
    `write`, so filtering in the write callback would leave gaps and every v2
    client would resync constantly — defeating the mechanism entirely. The
    projection therefore runs inside _sse_stream, ahead of numbering.
    """

    @pytest.mark.asyncio
    async def test_dropped_events_do_not_leave_gaps(self) -> None:
        state = RecorderState()
        captured: list = []

        async def write(event: dict) -> None:
            captured.append(event)

        task = asyncio.create_task(
            _sse_stream(state, write, project=lambda e: project(e, public=False))
        )
        for _ in range(10):
            await asyncio.sleep(0)

        # Three events a v2 subscriber drops, interleaved with two it keeps.
        _publish(state, {"type": "power_change", "plug_id": 1, "on": True})
        _publish(state, _reading_event())
        _publish(state, {"type": "reboot", "plug_id": 1, "phase": "start"})
        _publish(state, {"type": "command", "command_id": "x", "phase": "accepted"})
        _publish(state, {"type": "lock_change", "plug_id": 1})
        for _ in range(30):
            await asyncio.sleep(0)

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        kinds = [e["type"] for e in captured]
        assert kinds == ["hello", "reading_tick", "command"]
        assert [e["seq"] for e in captured] == [1, 2, 3]
