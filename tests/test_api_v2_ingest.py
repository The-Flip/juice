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


async def _client(state, store, token=TOKEN):
    client = TestClient(TestServer(create_app(state, store, dev_auth=True, ingest_token=token)))
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
    async def test_live_devices_and_pong_are_accepted_and_stored_nowhere(
        self, state, store
    ) -> None:
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


class TestLiveStateIsUntouched:
    async def test_ingest_publishes_nothing_to_the_event_bus(self, state, store) -> None:
        """Backfill must never drive live state. Replaying three days of
        history through the event bus would run overload detection over it and
        fire shutdowns for events that ended on Tuesday (`tap/wire.py:13-18`)."""
        client = await _client(state, store)
        queue: asyncio.Queue = asyncio.Queue()
        state.event_subscribers.add(queue)
        try:
            tap = await _tap(client)
            await tap.hello()
            await tap.readings([row(), row(TS + 1000)])
            assert queue.empty()
        finally:
            state.event_subscribers.discard(queue)
            await client.close()


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
