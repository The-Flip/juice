"""The ingest receiver, driven by a scripted client.

Mirrors `tests/tap/test_uplink.py`, which tests the same protocol from the other
end against a fake server. What matters is not that bytes move: it is that an
ack is never sent for rows we have not stored, that a reconnect can neither skip
nor duplicate a row, that a poison batch cannot wedge the stream, and that
backfill never touches live state.
"""

from __future__ import annotations

import asyncio
import json

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from juice.server import RecorderState, create_app
from juice.store import Store

TOKEN = "ingest-token"  # noqa: S105
TS = 1788000000000
AUTH = {"Authorization": f"Bearer {TOKEN}"}


def cur(n: int) -> str:
    """A cursor in tap's format: fixed-width zero-padded decimal."""
    return f"{n:018d}"


def row(ts_ms=TS, device="DEV", child="DEV00", relay=1, mw=42000, mv=119000, ma=350, wh=12345):
    return [ts_ms, device, child, relay, mw, mv, ma, wh]


@pytest.fixture
def store():
    with Store(":memory:") as s:
        yield s


@pytest.fixture
def state():
    return RecorderState()


class Tap:
    """A scripted tap. Deliberately not the real uplink -- that one retries and
    backs off, which hides exactly the responses these tests are about."""

    def __init__(self, ws):
        self.ws = ws

    async def hello(self, tap_id="tap-1", buffer_id="buf-1", protocol=1, **extra):
        await self.ws.send_json(
            {
                "type": "hello",
                "tap_id": tap_id,
                "version": "0.1.0",
                "protocol": protocol,
                "buffer_oldest": None,
                "buffer_newest": None,
                "buffer_id": buffer_id,
                **extra,
            }
        )
        return await self.recv()

    async def readings(self, rows, batch="b1", cursor=None):
        await self.ws.send_json(
            {"type": "readings", "batch": batch, "cursor": cursor or cur(len(rows)), "rows": rows}
        )
        return await self.recv()

    async def recv(self, timeout=5.0):
        msg = await asyncio.wait_for(self.ws.receive(), timeout=timeout)
        if msg.type is web.WSMsgType.TEXT:
            return json.loads(msg.data)
        return msg  # CLOSE / CLOSED / ERROR


async def _client(
    state,
    store,
    token=TOKEN,
    tap_devices=None,
    tap_live=None,
    tap_control=None,
    tap_shadow=False,
):
    app = create_app(
        state,
        store,
        dev_auth=True,
        ingest_token=token,
        tap_shadow=tap_shadow,
        tap_devices=tap_devices,
        tap_live=tap_live,
        tap_control=tap_control,
    )
    client = TestClient(TestServer(app))
    await client.start_server()
    return client


async def _tap(client, **kw):
    return Tap(await client.ws_connect("/api/v2/ingest", headers=AUTH, **kw))


class TestHandshake:
    async def test_hello_is_answered_with_a_welcome(self, state, store) -> None:
        client = await _client(state, store)
        try:
            welcome = await (await _tap(client)).hello()
            assert welcome["type"] == "welcome"
            assert welcome["protocol"] == 1
            assert welcome["max_batch_rows"] >= 1
            assert welcome["window"] >= 1
        finally:
            await client.close()

    async def test_the_welcome_does_not_make_tap_flap(self, state, store) -> None:
        """A `live_max_lag_s` of 0 puts tap's suppression threshold exactly
        where a healthy collector's lag sits, so it logs a suppressed/resumed
        pair on every crossing -- 30 pairs in a one-hour replay, three quarters
        of the log. juice discards `live` frames either way; the quiet choice is
        the right one."""
        client = await _client(state, store)
        try:
            welcome = await (await _tap(client)).hello()
            assert welcome["live_max_lag_s"] > 0
        finally:
            await client.close()

    async def test_an_unseen_buffer_resumes_from_nothing(self, state, store) -> None:
        client = await _client(state, store)
        try:
            welcome = await (await _tap(client)).hello()
            assert welcome["resume_from"] is None
        finally:
            await client.close()

    async def test_a_known_buffer_resumes_from_the_stored_cursor(self, state, store) -> None:
        store.set_ingest_cursor("tap-1", "buf-1", cur(42))
        client = await _client(state, store)
        try:
            welcome = await (await _tap(client)).hello()
            assert welcome["resume_from"] == cur(42)
        finally:
            await client.close()

    async def test_a_replaced_buffer_resumes_from_nothing(self, state, store) -> None:
        """A new buffer_id means tap's storage was replaced and its sequence
        restarted, so our cursor names a row that no longer exists. Offering it
        back would silently skip everything the new buffer has
        (`tap/wire.py:57-62`)."""
        store.set_ingest_cursor("tap-1", "buf-1", cur(42))
        client = await _client(state, store)
        try:
            welcome = await (await _tap(client)).hello(buffer_id="buf-2")
            assert welcome["resume_from"] is None
        finally:
            await client.close()

    async def test_another_taps_cursor_is_not_offered(self, state, store) -> None:
        store.set_ingest_cursor("tap-1", "buf-1", cur(42))
        client = await _client(state, store)
        try:
            welcome = await (await _tap(client)).hello(tap_id="tap-2")
            assert welcome["resume_from"] is None
        finally:
            await client.close()

    async def test_a_protocol_mismatch_is_told_so_before_the_close(self, state, store) -> None:
        """tap logs "server speaks protocol N, tap speaks M" from the welcome.
        Closing without one leaves the operator with a bare disconnect and no
        clue that the two sides are on different versions."""
        client = await _client(state, store)
        try:
            tap = await _tap(client)
            welcome = await tap.hello(protocol=99)
            assert welcome["type"] == "welcome"
            assert welcome["protocol"] == 1
            assert (await tap.recv()).type in (web.WSMsgType.CLOSE, web.WSMsgType.CLOSED)
        finally:
            await client.close()

    async def test_a_hello_without_a_tap_id_is_refused(self, state, store) -> None:
        client = await _client(state, store)
        try:
            tap = await _tap(client)
            await tap.ws.send_json({"type": "hello", "protocol": 1, "buffer_id": "b"})
            assert (await tap.recv()).type in (web.WSMsgType.CLOSE, web.WSMsgType.CLOSED)
        finally:
            await client.close()

    async def test_readings_before_hello_close_the_connection(self, state, store) -> None:
        """Not a nack: without a tap_id there is no sequence space to scope a
        cursor to, so there is nothing coherent to refuse."""
        client = await _client(state, store)
        try:
            tap = await _tap(client)
            await tap.ws.send_json(
                {"type": "readings", "batch": "b1", "cursor": cur(1), "rows": [row()]}
            )
            assert (await tap.recv()).type in (web.WSMsgType.CLOSE, web.WSMsgType.CLOSED)
            assert store._conn.execute("SELECT count(*) FROM readings").fetchone()[0] == 0
        finally:
            await client.close()


class TestDurability:
    async def test_rows_are_stored_before_the_ack_is_sent(self, state, store) -> None:
        """The ack is a durability claim: tap advances its cursor on it and
        never replays what it believes we hold. If the ack could outrun the
        commit, every crash would be silent permanent loss."""
        client = await _client(state, store)
        try:
            tap = await _tap(client)
            await tap.hello()
            ack = await tap.readings([row(), row(TS + 1000)])
            assert ack["type"] == "ack"
            assert store._conn.execute("SELECT count(*) FROM readings").fetchone()[0] == 2
            assert store.ingest_cursor("tap-1", "buf-1") == cur(2)
        finally:
            await client.close()

    async def test_the_ack_echoes_the_batch_byte_for_byte(self, state, store) -> None:
        """tap matches acks on `batch` alone and ignores the cursor we return
        (`tap/uplink.py:419-425`). An id that is merely equivalent is dropped at
        DEBUG and the stream stalls at the window limit for 120 s -- a hang with
        no error anywhere."""
        client = await _client(state, store)
        try:
            tap = await _tap(client)
            await tap.hello()
            ack = await tap.readings([row()], batch="b-XYZ-007")
            assert ack["batch"] == "b-XYZ-007"
        finally:
            await client.close()

    async def test_a_replayed_batch_is_acked_without_storing_it_twice(self, state, store) -> None:
        """tap resends on the *same* socket when an ack goes missing
        (BATCH_ACK_TIMEOUT). Refusing or re-storing would both be wrong."""
        client = await _client(state, store)
        try:
            tap = await _tap(client)
            await tap.hello()
            first = await tap.readings([row()], batch="b1", cursor=cur(1))
            second = await tap.readings([row()], batch="b1", cursor=cur(1))
            assert first["type"] == second["type"] == "ack"
            assert store._conn.execute("SELECT count(*) FROM readings").fetchone()[0] == 1
        finally:
            await client.close()

    async def test_a_reconnect_resumes_where_the_commit_reached(self, state, store) -> None:
        client = await _client(state, store)
        try:
            tap = await _tap(client)
            await tap.hello()
            await tap.readings([row()], cursor=cur(7))
            await tap.ws.close()

            welcome = await (await _tap(client)).hello()
            assert welcome["resume_from"] == cur(7)
        finally:
            await client.close()


class TestTheWriterLeavesNoTransactionOpen:
    """A `duplicate` verdict ends on `_ingest_cursor`'s `fetchone()`, and the
    writer thread then idles until the next batch. Bounded by tap's cadence
    today, but it is the same pin as the retention worker's (`Store.settle`),
    and a tap that goes quiet would hold it for as long as the quiet lasts.
    """

    async def test_a_duplicate_verdict_pins_nothing(self, state, store) -> None:
        from tests.pinned import RELEASED_AT_MOST, hammer, pinned_bytes

        client = await _client(state, store)
        try:
            tap = await _tap(client)
            await tap.hello()
            await tap.readings([row()], batch="b1", cursor=cur(1))
            assert (await tap.readings([row()], batch="b1", cursor=cur(1)))["type"] == "ack"
            hammer(store)
            assert pinned_bytes(store) <= RELEASED_AT_MOST
        finally:
            await client.close()

    async def test_a_rehearsed_duplicate_pins_nothing(self, state, store) -> None:
        """Shadow mode is the path production runs today, at ~2.5 batches/s."""
        from tests.pinned import RELEASED_AT_MOST, hammer, pinned_bytes

        client = await _client(state, store, tap_shadow=True)
        try:
            tap = await _tap(client)
            await tap.hello()
            await tap.readings([row()], batch="b1", cursor=cur(1))
            assert (await tap.readings([row()], batch="b1", cursor=cur(1)))["type"] == "ack"
            hammer(store)
            assert pinned_bytes(store) <= RELEASED_AT_MOST
        finally:
            await client.close()

    async def test_the_summary_reports_the_pin(self, state, store, caplog) -> None:
        """A pin must be a number in the log, not an inference from tap's lag."""
        import logging

        from tests.pinned import hammer

        holder = store.new_connection()
        client = await _client(state, store)
        try:
            tap = await _tap(client)
            await tap.hello()
            await tap.readings([row()])
            holder.execute("SELECT MIN(ts) FROM readings").fetchone()
            hammer(store)
            with caplog.at_level(logging.INFO, logger="juice.api.v2.ingest"):
                await tap.ws.close()
                await asyncio.sleep(0.1)
        finally:
            holder.close()
            await client.close()
        summary = next(r.getMessage() for r in caplog.records if "batches," in r.getMessage())
        pinned = float(summary.rsplit("pinned ", 1)[1].split(" MB")[0])
        assert pinned >= 32, summary

    async def test_a_reconnect_hello_pins_nothing(self, state, store) -> None:
        """`hello` reads the cursor on the event loop's connection, which has no
        idle boundary to settle at; on a tap-only server the next statement on
        it may be the next HTTP request. A *hit* left half-fetched pins (an
        empty result does not), so this needs a tap that has been seen before."""
        from tests.pinned import RELEASED_AT_MOST, hammer, pinned_bytes

        client = await _client(state, store)
        try:
            tap = await _tap(client)
            await tap.hello()
            await tap.readings([row()])
            await tap.ws.close()
            assert (await (await _tap(client)).hello())["resume_from"] == cur(1)
            hammer(store)
            assert pinned_bytes(store) <= RELEASED_AT_MOST
        finally:
            await client.close()

    async def test_a_stored_batch_pins_nothing(self, state, store) -> None:
        """Ends on COMMIT, which is clean with or without `settle`; pinned so a
        reordering of the commit path cannot regress it."""
        from tests.pinned import RELEASED_AT_MOST, hammer, pinned_bytes

        client = await _client(state, store)
        try:
            tap = await _tap(client)
            await tap.hello()
            assert (await tap.readings([row()]))["type"] == "ack"
            hammer(store)
            assert pinned_bytes(store) <= RELEASED_AT_MOST
        finally:
            await client.close()


class TestPoisonBatches:
    async def test_a_malformed_row_is_a_bad_batch_and_stores_nothing(self, state, store) -> None:
        client = await _client(state, store)
        try:
            tap = await _tap(client)
            await tap.hello()
            nack = await tap.readings([row(), [1, 2, 3]])
            assert nack["type"] == "nack"
            assert nack["code"] == "bad_batch"
            assert store._conn.execute("SELECT count(*) FROM readings").fetchone()[0] == 0
            assert store.ingest_cursor("tap-1", "buf-1") is None
        finally:
            await client.close()

    async def test_a_bad_batch_does_not_end_the_connection(self, state, store) -> None:
        """tap steps over a poison batch and keeps going. Dropping the socket
        would turn one bad row into a reconnect loop."""
        client = await _client(state, store)
        try:
            tap = await _tap(client)
            await tap.hello()
            await tap.readings([[1, 2, 3]], batch="bad", cursor=cur(1))
            ack = await tap.readings([row()], batch="good", cursor=cur(2))
            assert ack["type"] == "ack"
        finally:
            await client.close()

    async def test_an_impossible_timestamp_drops_one_row_not_the_batch(self, state, store) -> None:
        client = await _client(state, store)
        try:
            tap = await _tap(client)
            await tap.hello()
            ack = await tap.readings([row(), row(ts_ms=1000)], cursor=cur(2))
            assert ack["type"] == "ack"
            assert store._conn.execute("SELECT count(*) FROM readings").fetchone()[0] == 1
        finally:
            await client.close()

    async def test_a_frame_with_no_batch_id_is_ignored(self, state, store) -> None:
        """A nack has to name a batch. One we cannot name is one we cannot
        refuse, so it is dropped and tap's ack timeout resends it."""
        client = await _client(state, store)
        try:
            tap = await _tap(client)
            await tap.hello()
            await tap.ws.send_json({"type": "readings", "cursor": cur(1), "rows": [row()]})
            await tap.ws.send_json(
                {"type": "readings", "batch": "b2", "cursor": cur(2), "rows": [row()]}
            )
            assert (await tap.recv())["batch"] == "b2"
        finally:
            await client.close()


class TestIgnoredFrames:
    async def test_frames_with_no_projection_wired_are_dropped(self, state, store) -> None:
        """An app with no collector projection -- cloud mode, and `create_app` in
        these tests -- drops these frames exactly as before. Worth keeping as a
        named property: it is what lets the receiver be exercised on its own, and
        what stops a cloud-mode server acting on a roster it is not driving from.
        """
        client = await _client(state, store)
        try:
            tap = await _tap(client)
            await tap.hello()
            await tap.ws.send_json({"type": "live", "rows": [row()]})
            await tap.ws.send_json(
                {"type": "devices", "devices": [{"device_id": "D", "child_id": "D0", "alias": "x"}]}
            )
            await tap.ws.send_json({"type": "pong", "token": 1})
            await tap.ws.send_json({"type": "some_future_frame", "whatever": True})
            ack = await tap.readings([row()], batch="after")
            assert ack["batch"] == "after"
            assert store._conn.execute("SELECT count(*) FROM readings").fetchone()[0] == 1
            # The roster frame named an outlet that does not exist; with nothing
            # projecting it, no plug was created for it.
            assert (
                store._conn.execute("SELECT count(*) FROM plugs WHERE device_id = 'D'").fetchone()[
                    0
                ]
                == 0
            )
        finally:
            await client.close()

    async def test_a_roster_frame_reaches_the_projection_when_one_is_wired(
        self, state, store
    ) -> None:
        """And the seam actually carries it -- otherwise the dispatch above is
        indistinguishable from the drop it replaced."""
        seen: list[list[dict]] = []
        client = await _client(state, store, tap_devices=seen.append)
        try:
            tap = await _tap(client)
            await tap.hello()
            await tap.ws.send_json(
                {"type": "devices", "devices": [{"device_id": "D", "child_id": "D0", "alias": "x"}]}
            )
            await tap.readings([row()], batch="after")
        finally:
            await client.close()

        assert seen == [[{"device_id": "D", "child_id": "D0", "alias": "x"}]]

    async def test_an_unusable_roster_frame_does_not_cost_the_connection(
        self, state, store
    ) -> None:
        """The `readings` stream on this socket is the durable channel; a bad
        roster must not take it down."""
        calls: list[object] = []
        client = await _client(state, store, tap_devices=calls.append)
        try:
            tap = await _tap(client)
            await tap.hello()
            await tap.ws.send_json({"type": "devices", "devices": "not a list"})
            ack = await tap.readings([row()], batch="after")
            assert ack["batch"] == "after", "the socket survived"
            assert calls == []
        finally:
            await client.close()

    async def test_a_projection_that_raises_does_not_cost_the_connection(
        self, state, store
    ) -> None:
        def boom(_entries):
            raise RuntimeError("projection blew up")

        client = await _client(state, store, tap_devices=boom)
        try:
            tap = await _tap(client)
            await tap.hello()
            await tap.ws.send_json(
                {"type": "devices", "devices": [{"device_id": "D", "child_id": "D0", "alias": "x"}]}
            )
            ack = await tap.readings([row()], batch="after")
            assert ack["batch"] == "after"
        finally:
            await client.close()

    async def test_a_live_frame_does_not_become_a_reading(self, state, store) -> None:
        """`live` is a present-tense snapshot, unacked and never replayed. If it
        landed in `readings` it would be indistinguishable from durable data."""
        client = await _client(state, store)
        try:
            tap = await _tap(client)
            await tap.hello()
            await tap.ws.send_json({"type": "live", "rows": [row()]})
            await tap.readings([row(TS + 5000)], batch="b9")
            assert store._conn.execute("SELECT count(*) FROM readings").fetchone()[0] == 1
        finally:
            await client.close()


class TestTheLiveFrameReachesItsProjection:
    """`live` is the one frame that *is* meant to drive live state -- through
    the same kind of seam as `devices`, so the receiver stays a protocol shim
    and a cloud-mode server keeps dropping it."""

    async def test_a_live_frame_reaches_the_projection_and_is_awaited(self, state, store) -> None:
        events: list[tuple[str, int]] = []
        finished = asyncio.Event()

        async def project(rows):
            events.append(("start", len(rows)))
            await asyncio.sleep(0.05)  # a real projection awaits
            events.append(("end", len(rows)))
            if len(rows) == 1:
                finished.set()

        client = await _client(state, store, tap_live=project)
        try:
            tap = await _tap(client)
            await tap.hello()
            await tap.ws.send_json({"type": "live", "rows": [row(), row(child="DEV01")]})
            await tap.ws.send_json({"type": "live", "rows": [row()]})
            await asyncio.wait_for(finished.wait(), timeout=5.0)
        finally:
            await client.close()

        # Awaited, not fired off: the second frame is not handed over until
        # the first projection has returned. A projection that needs real
        # work schedules it itself (`LiveProjector`), which is what keeps
        # this contract cheap to honour.
        assert events == [("start", 2), ("end", 2), ("start", 1), ("end", 1)]

    async def test_a_live_frame_before_hello_is_dropped(self, state, store) -> None:
        """Until hello the peer's protocol version is unchecked, so its claim
        about the floor is not applied -- and, unlike `readings`, not fatal
        either: nothing durable is at stake in a snapshot."""
        calls: list[object] = []

        async def project(rows):
            calls.append(rows)

        client = await _client(state, store, tap_live=project)
        try:
            tap = await _tap(client)
            await tap.ws.send_json({"type": "live", "rows": [row()]})
            welcome = await tap.hello()
            assert welcome["type"] == "welcome", "the socket survived"
            await tap.ws.send_json({"type": "live", "rows": [row()]})
            await tap.readings([row()], batch="after")
        finally:
            await client.close()

        assert calls == [[row()]], "only the frame after hello reached the projection"

    async def test_an_unusable_live_frame_does_not_cost_the_connection(self, state, store) -> None:
        calls: list[object] = []

        async def project(rows):
            calls.append(rows)

        client = await _client(state, store, tap_live=project)
        try:
            tap = await _tap(client)
            await tap.hello()
            await tap.ws.send_json({"type": "live", "rows": "not a list"})
            await tap.ws.send_json({"type": "live", "rows": [["too", "short"]]})
            ack = await tap.readings([row()], batch="after")
            assert ack["batch"] == "after", "the socket survived"
            assert calls == []
        finally:
            await client.close()

    async def test_a_projection_that_raises_does_not_cost_the_connection(
        self, state, store, caplog
    ) -> None:
        import logging

        async def boom(_rows):
            raise RuntimeError("projection blew up")

        client = await _client(state, store, tap_live=boom)
        try:
            tap = await _tap(client)
            await tap.hello()
            with caplog.at_level(logging.WARNING, logger="juice.api.v2.ingest"):
                await tap.ws.send_json({"type": "live", "rows": [row()]})
                ack = await tap.readings([row()], batch="after")
            assert ack["batch"] == "after"
        finally:
            await client.close()

        # It reached the projection and the failure was reported, rather than
        # the frame never having been dispatched at all.
        assert any("applying a live frame failed" in r.getMessage() for r in caplog.records)


async def _never_send(_frame: dict) -> None:
    raise AssertionError("a command was routed to the wrong tap")


class TestCommandsRideTheSameSocket:
    """The receiver is also the sender: with a `TapControl` wired, a tap that
    has said hello can take `command` frames, and its `command_result` frames
    come back to whoever is waiting."""

    async def test_hello_registers_and_disconnect_unregisters(self, state, store) -> None:
        from juice.collector_tap import TapControl

        control = TapControl()
        client = await _client(state, store, tap_control=control)
        try:
            tap = await _tap(client)
            assert control.connected == [], "a socket is not a tap until it says hello"
            await tap.hello(tap_id="bumper")
            assert control.connected == ["bumper"]
            await tap.ws.close()
            for _ in range(50):
                if not control.connected:
                    break
                await asyncio.sleep(0.02)
            assert control.connected == []
        finally:
            await client.close()

    async def test_a_command_reaches_the_tap_and_its_result_comes_back(self, state, store) -> None:
        from juice.collector_tap import TapControl

        control = TapControl()
        # A second, silent session: routing must come from the roster, not
        # from "there is only one tap".
        control.connect("elsewhere", _never_send)
        client = await _client(state, store, tap_control=control)
        try:
            tap = await _tap(client)
            await tap.hello(tap_id="bumper")
            await tap.ws.send_json(
                {
                    "type": "devices",
                    "devices": [{"device_id": "DEV", "child_id": "DEV00", "alias": "x"}],
                }
            )
            await tap.readings([row()], batch="sync")  # the roster has been read by now

            task = asyncio.create_task(control.command("turn_on", "DEV", "DEV00"))
            frame = await tap.recv()
            assert frame["type"] == "command"
            assert frame["kind"] == "turn_on" and frame["device_id"] == "DEV"
            await tap.ws.send_json(
                {
                    "type": "command_result",
                    "command_id": frame["command_id"],
                    "status": "ok",
                    "error": None,
                }
            )
            assert await asyncio.wait_for(task, timeout=5.0) == frame["command_id"]
            assert control.owner_of("DEV") == "bumper"
        finally:
            await client.close()

    async def test_an_unusable_result_does_not_cost_the_connection(self, state, store) -> None:
        from juice.collector_tap import TapControl

        control = TapControl()
        client = await _client(state, store, tap_control=control)
        try:
            tap = await _tap(client)
            await tap.hello()
            await tap.ws.send_json({"type": "command_result", "status": "maybe"})
            ack = await tap.readings([row()], batch="after")
            assert ack["batch"] == "after"
        finally:
            await client.close()

    async def test_without_control_results_are_dropped_as_before(self, state, store) -> None:
        client = await _client(state, store)
        try:
            tap = await _tap(client)
            await tap.hello()
            await tap.ws.send_json(
                {"type": "command_result", "command_id": "c1", "status": "ok", "error": None}
            )
            ack = await tap.readings([row()], batch="after")
            assert ack["batch"] == "after"
        finally:
            await client.close()


class TestLiveStateIsUntouched:
    async def test_ingest_publishes_nothing_to_the_event_bus(self, state, store) -> None:
        """Backfill must never drive live state. Replaying three days of
        history through the event bus would run overload detection over it and
        fire shutdowns for events that ended on Tuesday (`tap/wire.py:13-18`).

        Asserted **with a live projection wired**, because that is the
        configuration in which it would be easiest to get wrong: `readings`
        and `live` arrive on the same socket, and only one of them may reach
        the state."""
        from juice.collector_tap import LiveProjector

        plug_id = store.ensure_plug("DEV", "DEV00", "outlet")
        state.plugs[plug_id] = ("DEV", "DEV00", "outlet")
        projector = LiveProjector(state, store)
        client = await _client(state, store, tap_live=projector)
        queue: asyncio.Queue = asyncio.Queue()
        state.event_subscribers.add(queue)
        try:
            tap = await _tap(client)
            await tap.hello()
            await tap.readings([row(), row(TS + 1000)])
            assert queue.empty()
            assert state.plug_readings == {}
        finally:
            state.event_subscribers.discard(queue)
            await client.close()


class TestTheProjectionsAreOneCollectorOrTheOther:
    def test_shadow_mode_refuses_an_explicit_projection(self, state, store) -> None:
        """Shadow mode *is* a pair of projections; passing another beside it
        would mean two collectors' worth of opinion about one frame."""
        with pytest.raises(ValueError, match="tap_shadow"):
            create_app(state, store, dev_auth=True, tap_shadow=True, tap_live=lambda rows: None)
        with pytest.raises(ValueError, match="tap_shadow"):
            create_app(
                state, store, dev_auth=True, tap_shadow=True, tap_devices=lambda entries: None
            )
        with pytest.raises(ValueError, match="tap_shadow"):
            from juice.collector_tap import TapControl

            create_app(state, store, dev_auth=True, tap_shadow=True, tap_control=TapControl())


class TestHelloIdentityRejectsUnusableValues:
    """The cursor is scoped to `(tap_id, buffer_id)`, so a `buffer_id` juice
    cannot round-trip is not a cosmetic complaint: two taps whose ids both
    collapse to the same key share one cursor sequence, and each will be told
    to resume from the other's position.
    """

    @pytest.mark.parametrize("value", [0, False, [], {}, 0.0])
    def test_falsey_non_strings_are_refused(self, value) -> None:
        from juice.api.v2.tap_wire import BadFrameError, hello_identity

        with pytest.raises(BadFrameError, match="buffer_id"):
            hello_identity({"type": "hello", "tap_id": "t", "buffer_id": value})

    @pytest.mark.parametrize("value", [1, 1.5, ["a"], {"a": 1}, True])
    def test_truthy_non_strings_are_refused(self, value) -> None:
        from juice.api.v2.tap_wire import BadFrameError, hello_identity

        with pytest.raises(BadFrameError, match="buffer_id"):
            hello_identity({"type": "hello", "tap_id": "t", "buffer_id": value})

    def test_absent_and_null_and_empty_all_mean_the_empty_scope(self) -> None:
        """An old tap may omit the field, and `tap.wire.hello` defaults it to
        `""`. Those are the same buffer, and `""` is a usable key."""
        from juice.api.v2.tap_wire import hello_identity

        assert hello_identity({"type": "hello", "tap_id": "t"}) == ("t", "")
        assert hello_identity({"type": "hello", "tap_id": "t", "buffer_id": None}) == ("t", "")
        assert hello_identity({"type": "hello", "tap_id": "t", "buffer_id": ""}) == ("t", "")

    def test_a_real_buffer_id_survives(self) -> None:
        from juice.api.v2.tap_wire import hello_identity

        assert hello_identity({"type": "hello", "tap_id": "t", "buffer_id": "b7"}) == ("t", "b7")


class TestShadowMode:
    """Shadow mode rehearses a cutover on a production floor the cloud recorder
    is still driving. The one thing it must not do is write readings: the cloud
    recorder is already writing those hours at its own cadence, and a second
    writer at 1 Hz would double-count every rollup for the whole rehearsal --
    ~4.2M rows a day of it.

    But it must still **ack** them, and record the cursor. tap treats an ack as
    "the server holds this", so refusing would make it resend forever and grow its
    buffer; and a cursor that is not recorded means tap resumes from the start of
    its buffer at real cutover and replays the entire shadow period on top of the
    cloud recorder's rows. Acknowledged-and-discarded is the honest state: the
    data *is* durable, in the cloud recorder's copy.
    """

    async def test_readings_are_acked_but_not_stored(self, state, store) -> None:
        client = await _client(state, store, tap_shadow=True)
        try:
            tap = await _tap(client)
            await tap.hello()
            ack = await tap.readings([row(), row(TS + 1000)], batch="b1")
            assert ack["type"] == "ack" and ack["batch"] == "b1"
        finally:
            await client.close()

        assert store._conn.execute("SELECT count(*) FROM readings").fetchone()[0] == 0, (
            "shadow mode must never write readings: the cloud recorder is writing them"
        )

    async def test_the_cursor_is_still_recorded(self, state, store) -> None:
        """So that at real cutover tap resumes from here rather than replaying the
        whole shadow period over the cloud recorder's rows."""
        client = await _client(state, store, tap_shadow=True)
        try:
            tap = await _tap(client)
            await tap.hello()
            await tap.readings([row()], batch="b1", cursor="0" * 17 + "7")
        finally:
            await client.close()

        assert store.ingest_cursor("tap-1", "buf-1") == "0" * 17 + "7"

    async def test_a_reconnect_resumes_from_the_recorded_cursor(self, state, store) -> None:
        client = await _client(state, store, tap_shadow=True)
        try:
            tap = await _tap(client)
            await tap.hello()
            await tap.readings([row()], batch="b1", cursor="0" * 17 + "7")
            tap2 = await _tap(client)
            welcome = await tap2.hello()
            assert welcome["resume_from"] == "0" * 17 + "7"
        finally:
            await client.close()

    async def test_a_batch_cutover_would_refuse_is_refused_in_shadow_too(
        self, state, store
    ) -> None:
        """The rehearsal has to report what cutover would do. Acking a poison
        batch here would make shadow mode say "clean" about a tap whose frames
        the real path nacks -- and the cursor must stay put, as it would live,
        so the two modes resume from the same place."""
        client = await _client(state, store, tap_shadow=True)
        try:
            tap = await _tap(client)
            await tap.hello()
            nack = await tap.readings([row(), [1, 2, 3]], batch="bad", cursor=cur(1))
            assert nack["type"] == "nack" and nack["code"] == "bad_batch"
            assert store.ingest_cursor("tap-1", "buf-1") is None
            ack = await tap.readings([row()], batch="good", cursor=cur(2))
            assert ack["type"] == "ack"
        finally:
            await client.close()

        assert store.ingest_cursor("tap-1", "buf-1") == cur(2)
        assert store._conn.execute("SELECT count(*) FROM readings").fetchone()[0] == 0

    async def test_an_impossible_timestamp_is_counted_as_it_would_be_live(
        self, state, store, caplog
    ) -> None:
        """Live drops the row and warns; shadow must warn the same way, or a tap
        with a bad clock rehearses clean and drops half its rows at cutover."""
        import logging

        client = await _client(state, store, tap_shadow=True)
        try:
            tap = await _tap(client)
            await tap.hello()
            with caplog.at_level(logging.WARNING, logger="juice.api.v2.ingest"):
                ack = await tap.readings([row(), row(ts_ms=1000)], batch="b1")
            assert ack["type"] == "ack"
        finally:
            await client.close()

        assert any("dropped 1 row(s) of batch b1" in r.getMessage() for r in caplog.records), [
            r.getMessage() for r in caplog.records
        ]

    async def test_no_backfill_mark_is_left_behind(self, state, store) -> None:
        """A discarded batch must not widen the next rollup pass: nothing was
        written for it to cover."""
        client = await _client(state, store, tap_shadow=True)
        try:
            tap = await _tap(client)
            await tap.hello()
            await tap.readings([row(TS - 86_400_000 * 3)], batch="b1")
        finally:
            await client.close()

        assert store.pending_backfill_start() is None

    async def test_the_roster_still_reaches_the_projection(self, state, store) -> None:
        """Shadow mode discards readings, not the roster -- the roster is the
        entire point of the rehearsal."""
        client = await _client(state, store, tap_shadow=True)
        projector = client.app["tap_devices"]
        try:
            tap = await _tap(client)
            await tap.hello()
            await tap.ws.send_json(
                {"type": "devices", "devices": [{"device_id": "D", "child_id": "D0", "alias": "x"}]}
            )
            await tap.readings([row()], batch="after")
        finally:
            await client.close()

        assert projector.frames == 1
        assert projector.last_roster == [{"device_id": "D", "child_id": "D0", "alias": "x"}]
