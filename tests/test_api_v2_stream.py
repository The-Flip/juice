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
                # Every real snapshot entry carries this; without it the
                # fixture drifts from the producer and the projection's
                # moved-machine filter has nothing to act on.
                "asset_id": "M0001",
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


class TestEndpointDeliversRealValues:
    """Exercises the actual route, not just the projection function.

    The unit tests above call _sse_stream directly with a projection, so they
    cannot see a handler that projects a second time in its write callback. That
    is exactly the bug that shipped here: the double pass re-read an
    already-projected event, found no `is_on` or `watt`, and reported every
    machine as relay "off" drawing nothing. Statuses still looked right, so a
    spot check of the payload missed it.
    """

    @pytest.mark.asyncio
    async def test_relay_and_draw_survive_the_handler(self) -> None:
        import json as _json

        from aiohttp.test_utils import TestClient, TestServer

        from juice.collector import PlugReading
        from juice.server import create_app
        from juice.store import Store

        with Store(":memory:") as store:
            state = RecorderState()
            state.plugs[1] = ("DEV", "DEV01", "Godzilla - M0001")
            state.plug_has_emeter[1] = True
            state.assignments[1] = ("Godzilla", "M0001", 2021)
            state.plug_readings[1] = PlugReading(
                child_id="DEV01",
                alias="Godzilla",
                is_on=True,
                watts=210.0,
                voltage=120.0,
                amps=1.8,
                total_kwh=1.0,
            )

            app = create_app(state, store, dev_auth=True)
            async with TestClient(TestServer(app)) as client:
                resp = await client.get("/api/v2/stream")
                assert resp.status == 200

                # hello, then publish one tick and read it back off the wire.
                await resp.content.readuntil(b"\n\n")
                from juice.server import _publish, _readings_snapshot

                _publish(
                    state,
                    {"type": "readings", "machines": _readings_snapshot(state)},
                )
                raw = await asyncio.wait_for(resp.content.readuntil(b"\n\n"), timeout=5)

        event = _json.loads(raw.decode().removeprefix("data: ").strip())
        machine = event["machines"][0]
        assert event["type"] == "reading_tick"
        assert machine["relay"] == "on", "a second projection blanked the relay"
        assert machine["draw_watts"] == 210.0, "a second projection blanked the draw"


class TestTickIdentity:
    """A tick a client cannot join to a machine is a tick it cannot use."""

    def test_a_reading_tick_names_the_machine_by_asset_id(self) -> None:
        out = project(_reading_event(), public=False)

        assert out is not None
        assert out["machines"][0]["asset_id"] == "M0001"

    def test_an_anonymous_subscriber_gets_the_asset_id_too(self) -> None:
        """`plug_id` is operator-only in the machine view, so without this an
        anonymous client receives ticks it has no way to attribute."""
        out = project(_reading_event(), public=True)

        assert out is not None
        assert out["machines"][0]["asset_id"] == "M0001"


class TestUnreachableIsNotStaleData:
    def test_an_unreachable_tick_carries_no_relay_or_draw(self) -> None:
        event = _reading_event()
        event["machines"][0] |= {"status": "unreachable", "offline": True}

        out = project(event, public=False)

        assert out is not None
        machine = out["machines"][0]
        assert machine["relay"] is None
        assert machine["draw_watts"] is None

    def test_a_reachable_tick_is_unchanged(self) -> None:
        out = project(_reading_event(), public=False)

        assert out is not None
        assert out["machines"][0]["relay"] == "on"
        assert out["machines"][0]["draw_watts"] == 210.0


class TestTickRedaction:
    def test_an_anonymous_tick_drops_the_operator_only_plug_id(self) -> None:
        """`plug_id` is operator-only in every other v2 payload. Sending it
        here anyway made §8's redaction boundary mean two different things."""
        out = project(_reading_event(), public=True)

        assert out is not None
        assert "plug_id" not in out["machines"][0]
        assert out["machines"][0]["asset_id"] == "M0001"

    def test_an_operator_tick_keeps_it(self) -> None:
        out = project(_reading_event(), public=False)

        assert out is not None
        assert out["machines"][0]["plug_id"] == 1


class TestSnapshotIsTheProducer:
    """`project` only forwards `asset_id`; these cover the half that mints it,
    which no test touched — stripping it from the snapshot left the suite green.
    """

    def test_the_snapshot_names_every_machine(self) -> None:
        from juice.collector import PlugReading
        from juice.server import RecorderState, _readings_snapshot

        state = RecorderState()
        state.plugs[1] = ("DEV", "DEV01", "X - M0001")
        state.plug_has_emeter[1] = True
        state.assignments[1] = ("Blackout", "M0001", 1980)
        state.plug_readings[1] = PlugReading(
            child_id="DEV01",
            alias="x",
            is_on=True,
            watts=210.0,
            voltage=120.0,
            amps=1.0,
            total_kwh=1.0,
        )

        snapshot = _readings_snapshot(state)
        assert [m["asset_id"] for m in snapshot] == ["M0001"]

        tick = project({"type": "readings", "machines": snapshot}, public=True)
        assert tick is not None
        assert tick["machines"][0]["asset_id"] == "M0001"

    def test_a_moved_machine_appears_once_on_the_wire(self) -> None:
        """Two open assignments — stale outlet on a dead strip, live outlet on a
        good one. v1 keys tiles by plug_id and needs both; v2 joins on asset_id,
        so two rows would let the dead outlet overwrite the live one, invisibly
        for an anonymous client that cannot see plug_id at all.
        """
        from datetime import UTC, datetime

        from juice.collector import PlugReading
        from juice.server import RecorderState, _readings_snapshot

        state = RecorderState()
        for plug_id, device in ((2, "LIVE"), (9, "DEAD")):
            state.plugs[plug_id] = (device, f"{device}{plug_id:02d}", "X - M0001")
            state.plug_has_emeter[plug_id] = True
            state.assignments[plug_id] = ("Blackout", "M0001", 1980)
            state.plug_readings[plug_id] = PlugReading(
                child_id=f"{device}{plug_id:02d}",
                alias="x",
                is_on=True,
                watts=210.0,
                voltage=120.0,
                amps=1.0,
                total_kwh=1.0,
            )
        state.offline_since["DEAD"] = datetime(2026, 9, 1, tzinfo=UTC)

        snapshot = _readings_snapshot(state)
        assert [m["plug_id"] for m in snapshot] == [2, 9]  # v1 still gets both

        for public in (True, False):
            tick = project({"type": "readings", "machines": snapshot}, public=public)
            assert tick is not None
            rows = tick["machines"]
            assert [m["asset_id"] for m in rows] == ["M0001"], f"public={public}"
            # The live outlet, not the dead one.
            assert rows[0]["status"] == "powered"
            assert rows[0]["draw_watts"] == 210.0
