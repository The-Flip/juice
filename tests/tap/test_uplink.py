"""The uplink, against a scriptable fake server.

What matters here is not that bytes move. It is that the cursor is never
advanced by anything but a durability claim, that a gap is impossible, that a
poison batch cannot wedge the stream forever, and that a redelivered command
does not throw a relay twice.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta

import aiohttp
import pytest
from aiohttp import web

from tap import wire
from tap.buffer import Buffer
from tap.config import Config, UplinkConfig
from tap.device import OutletReading, Sweep
from tap.health import Health
from tap.uplink import ACKED_STATE_KEY, Uplink


class FakeServer:
    """A juice-shaped WebSocket endpoint whose behaviour each test dictates."""

    def __init__(self, **welcome) -> None:
        self.welcome = {"type": wire.WELCOME, **welcome}
        self.batches: list[dict] = []
        self.hello: dict | None = None
        self.devices: dict | None = None
        self.command_results: list[dict] = []
        self.live_frames: list[dict] = []
        self.nack_next: dict | None = None
        self.to_send: list[dict] = []
        self.connections = 0
        self.drop_after_batches: int | None = None
        # Hold acks back so several batches are in flight at once — the only
        # way to exercise out-of-order acking and a tail rewind.
        self.hold_acks = False
        self.held: list[dict] = []
        self.rows_acked: list[list] = []
        self._ws = None

    async def handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.connections += 1
        async for message in ws:
            if message.type is not aiohttp.WSMsgType.TEXT:
                continue
            frame = message.json()
            kind = frame.get("type")
            if kind == wire.HELLO:
                self.hello = frame
                await ws.send_json(self.welcome)
                for extra in self.to_send:
                    await ws.send_json(extra)
            elif kind == wire.DEVICES:
                self.devices = frame
            elif kind == wire.LIVE:
                self.live_frames.append(frame)
            elif kind == wire.COMMAND_RESULT:
                self.command_results.append(frame)
            elif kind == wire.READINGS:
                self.batches.append(frame)
                self._ws = ws
                if self.nack_next is not None:
                    nack = {"type": wire.NACK, "batch": frame["batch"], **self.nack_next}
                    self.nack_next = None
                    await ws.send_json(nack)
                elif self.hold_acks:
                    self.held.append(frame)
                else:
                    await self._ack(ws, frame)
                while not self.hold_acks and self.held:
                    await self._ack(ws, self.held.pop(0))
                if (
                    self.drop_after_batches is not None
                    and len(self.batches) >= self.drop_after_batches
                ):
                    await ws.close()
                    return ws
        return ws

    async def _ack(self, ws, frame: dict) -> None:
        self.rows_acked.extend(frame["rows"])
        await ws.send_json({"type": wire.ACK, "batch": frame["batch"], "cursor": frame["cursor"]})

    async def ack_batch(self, batch_id: str) -> None:
        """Ack one held batch out of order."""
        for i, frame in enumerate(self.held):
            if frame["batch"] == batch_id:
                await self._ack(self._ws, self.held.pop(i))
                return

    async def nack(self, batch_id: str, code: str) -> None:
        self.held = [f for f in self.held if f["batch"] != batch_id]
        await self._ws.send_json({"type": wire.NACK, "batch": batch_id, "code": code})

    @property
    def rows(self) -> list[list]:
        return [row for batch in self.batches for row in batch["rows"]]


@pytest.fixture
async def buf(tmp_path):
    b = Buffer(tmp_path / "buffer", retention_days=30)
    await b.open()
    yield b
    await b.close()


async def _fill(buffer: Buffer, count: int, device_id: str = "DEV1") -> None:
    base = datetime.now(UTC) - timedelta(seconds=count)
    for i in range(count):
        buffer.submit(
            Sweep(
                device_id=device_id,
                ts=base + timedelta(seconds=i),
                outlets=[
                    OutletReading(
                        child_id=f"{device_id}00", alias="a", relay_on=True, power_mw=1000 + i
                    )
                ],
            )
        )
    await buffer.flush()


@contextlib.asynccontextmanager
async def _running(server: FakeServer, buf: Buffer, health: Health, pollers=None):
    app = web.Application()
    app.router.add_get("/ingest", server.handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = runner.addresses[0][1]
    config = Config(
        tap_id="test-tap",
        uplink=UplinkConfig(url=f"http://127.0.0.1:{port}/ingest", token="secret", enabled=True),
    )
    uplink = Uplink(config, buf, health, pollers)
    task = asyncio.create_task(uplink.run())
    try:
        yield uplink
    finally:
        uplink.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await runner.cleanup()


async def _wait_for(predicate, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("condition not met in time")


class TestStandalone:
    async def test_no_url_means_no_connection_and_no_complaint(self, buf):
        """tap must run happily with nowhere to send anything."""
        health = Health()
        uplink = Uplink(Config(), buf, health)
        task = asyncio.create_task(uplink.run())
        await asyncio.sleep(0.1)
        assert health.uplink.enabled is False
        assert health.uplink.connected is False
        uplink.stop()
        await asyncio.wait_for(task, timeout=2)


class TestStreaming:
    async def test_rows_arrive_in_cursor_order_exactly_once(self, buf):
        health = Health()
        await _fill(buf, 12)
        server = FakeServer()
        async with _running(server, buf, health):
            await _wait_for(lambda: len(server.rows) >= 12)
        powers = [row[4] for row in server.rows]
        assert powers == sorted(powers)
        assert len(powers) == len(set(powers))

    async def test_hello_carries_the_extent_and_the_token_is_a_header(self, buf):
        health = Health()
        await _fill(buf, 2)
        server = FakeServer()
        async with _running(server, buf, health):
            await _wait_for(lambda: server.hello is not None)
        assert server.hello["tap_id"] == "test-tap"
        assert server.hello["buffer_newest"] is not None

    async def test_aliases_travel_out_of_band(self, buf):
        """Not on every reading row: that would thrash the server's plug cache."""
        health = Health()
        await _fill(buf, 2)
        server = FakeServer()
        async with _running(server, buf, health):
            await _wait_for(lambda: server.devices is not None)
        assert server.devices["devices"][0]["alias"] == "a"
        assert len(server.rows[0]) == len(wire.ROW_FIELDS)

    async def test_the_acked_cursor_points_at_the_last_delivered_row(self, buf):
        """Persisted, and pointing where it should — not merely self-consistent."""
        health = Health()
        await _fill(buf, 4)
        server = FakeServer()
        async with _running(server, buf, health):
            await _wait_for(lambda: len(server.rows) >= 4)
            await _wait_for(lambda: health.uplink.acked_cursor is not None)

        rows = await buf.read_after(None)
        expected = buf.cursor_of(rows[-1])
        assert health.uplink.acked_cursor == expected
        assert await buf.get_state(ACKED_STATE_KEY) == expected
        # And nothing is left behind it.
        assert await buf.read_after(expected) == []

    async def test_batches_respect_the_servers_size_limit(self, buf):
        health = Health()
        await _fill(buf, 10)
        server = FakeServer(max_batch_rows=3)
        async with _running(server, buf, health):
            await _wait_for(lambda: len(server.rows) >= 10)
        assert all(len(b["rows"]) <= 3 for b in server.batches)


class TestResume:
    async def test_the_server_cursor_wins_when_it_is_behind(self, buf):
        """A server restored from backup must get the missing rows again."""
        health = Health()
        await _fill(buf, 6)
        rows = await buf.read_after(None)
        # Pretend we already acked everything, but the server only has row 2.
        await buf.set_state(ACKED_STATE_KEY, buf.cursor_of(rows[-1]))
        server = FakeServer(resume_from=buf.cursor_of(rows[1]))
        async with _running(server, buf, health):
            await _wait_for(lambda: len(server.rows) >= 4)
        assert len(server.rows) == 4  # rows 3..6 resent

    async def test_a_reconnect_does_not_skip_rows(self, buf):
        health = Health()
        await _fill(buf, 6)
        server = FakeServer(max_batch_rows=2)
        server.drop_after_batches = 1  # die mid-stream
        async with _running(server, buf, health):
            await _wait_for(lambda: server.connections >= 2, timeout=8)
            await _wait_for(lambda: len(server.rows) >= 6, timeout=8)
        powers = sorted(row[4] for row in server.rows)
        assert powers[:6] == [1000, 1001, 1002, 1003, 1004, 1005]


class TestNacks:
    async def test_a_transient_nack_resends_the_batch(self, buf):
        health = Health()
        await _fill(buf, 3)
        server = FakeServer(max_batch_rows=3)
        server.nack_next = {"code": wire.NACK_TRANSIENT}
        async with _running(server, buf, health):
            await _wait_for(lambda: len(server.batches) >= 2)
        assert server.batches[0]["rows"] == server.batches[1]["rows"]

    async def test_a_transient_nack_with_several_in_flight_resends_the_whole_tail(self, buf):
        """With window > 1 a rewind must not leave a hole behind it."""
        health = Health()
        await _fill(buf, 12)
        server = FakeServer(max_batch_rows=2, window=4)
        server.hold_acks = True
        async with _running(server, buf, health):
            await _wait_for(lambda: len(server.batches) >= 3)
            await server.nack(server.batches[0]["batch"], wire.NACK_TRANSIENT)
            server.hold_acks = False
            # Wait on distinct rows, not the count: duplicates from the rewind
            # would otherwise satisfy a length check before the tail arrives.
            await _wait_for(
                lambda: {r[4] for r in server.rows_acked} == {1000 + i for i in range(12)},
                timeout=10,
            )
        # Delivery is at-least-once, so a rewind legitimately repeats rows. The
        # invariant that matters is that nothing is *missing*.
        delivered = [r[4] for r in server.rows_acked]
        assert set(delivered) == {1000 + i for i in range(12)}
        assert len(delivered) >= 12

    async def test_a_poison_batch_is_skipped_not_retried_forever(self, buf):
        """Wedging on one bad batch is a worse failure than losing it."""
        health = Health()
        await _fill(buf, 6)
        server = FakeServer(max_batch_rows=3)
        server.nack_next = {"code": wire.NACK_BAD_BATCH, "message": "unparseable"}
        async with _running(server, buf, health):
            await _wait_for(lambda: health.uplink.batches_poisoned >= 1)
            await _wait_for(lambda: len(server.rows) >= 3)
        # It moved past the poison batch instead of resending it.
        assert health.uplink.batches_poisoned == 1
        assert [row[4] for row in server.rows[-3:]] == [1003, 1004, 1005]


class TestCommands:
    async def test_a_command_reaches_the_device_and_is_reported(self, buf):
        from tests.tap.fakes import FakeDevice

        health = Health()
        device = FakeDevice(device_id="DEV1", host="10.0.0.1")

        class Pollers:
            def find(self, device_id):
                return _Poller() if device_id == "DEV1" else None

        class _Poller:
            async def set_relay(self, child_id, on):
                await device.set_relay(child_id, on)

        server = FakeServer()
        server.to_send = [
            {
                "type": wire.COMMAND,
                "command_id": "c1",
                "kind": "turn_off",
                "device_id": "DEV1",
                "child_id": "DEV100",
            }
        ]
        async with _running(server, buf, health, Pollers()):
            await _wait_for(lambda: server.command_results)
        assert device.relay_calls == [("DEV100", False)]
        assert server.command_results[0]["status"] == "ok"

    async def test_a_redelivered_command_is_not_re_actuated(self, buf):
        """A relay is physical: doing it twice is not the same as doing it once."""
        from tests.tap.fakes import FakeDevice

        health = Health()
        device = FakeDevice(device_id="DEV1", host="10.0.0.1")

        class _Poller:
            async def set_relay(self, child_id, on):
                await device.set_relay(child_id, on)

        class Pollers:
            def find(self, device_id):
                return _Poller()

        command = {
            "type": wire.COMMAND,
            "command_id": "c1",
            "kind": "turn_on",
            "device_id": "DEV1",
            "child_id": "DEV100",
        }
        server = FakeServer()
        server.to_send = [command, dict(command)]
        async with _running(server, buf, health, Pollers()):
            await _wait_for(lambda: len(server.command_results) >= 2)
        assert device.relay_calls == [("DEV100", True)]  # once, not twice
        assert [r["status"] for r in server.command_results] == ["ok", "ok"]

    async def test_an_expired_command_is_refused(self, buf):
        """A message that sat in a dead socket must not throw a relay later."""
        health = Health()
        stale = (datetime.now(UTC) - timedelta(minutes=2)).isoformat()
        server = FakeServer()
        server.to_send = [
            {
                "type": wire.COMMAND,
                "command_id": "c9",
                "kind": "turn_on",
                "device_id": "DEV1",
                "child_id": "DEV100",
                "expires_at": stale,
            }
        ]
        async with _running(server, buf, health, None):
            await _wait_for(lambda: server.command_results)
        assert server.command_results[0]["status"] == "error"
        assert server.command_results[0]["error"] == "expired"

    async def test_an_unknown_command_kind_is_reported_not_guessed(self, buf):
        health = Health()
        server = FakeServer()
        server.to_send = [
            {
                "type": wire.COMMAND,
                "command_id": "c2",
                "kind": "self_destruct",
                "device_id": "DEV1",
                "child_id": "",
            }
        ]
        async with _running(server, buf, health, None):
            await _wait_for(lambda: server.command_results)
        assert server.command_results[0]["status"] == "error"
        assert "self_destruct" in server.command_results[0]["error"]


class TestAckOrdering:
    async def test_an_out_of_order_ack_does_not_skip_the_batches_before_it(self, buf):
        """The bug this defends against loses data permanently.

        Up to `window` batches are on the wire at once, so acks can arrive out
        of order. Advancing the durable cursor to whichever batch was acked last
        would skip every earlier one still in flight — and because the cursor is
        persisted, no future connection would ever send those rows again.
        """
        health = Health()
        await _fill(buf, 8)
        server = FakeServer(max_batch_rows=2, window=4)
        server.hold_acks = True
        async with _running(server, buf, health):
            await _wait_for(lambda: len(server.batches) >= 3)
            second = server.batches[1]["batch"]
            await server.ack_batch(second)
            await asyncio.sleep(0.2)
            # The second batch is acked, the first is not: the cursor must not
            # have moved past rows the server never confirmed.
            assert health.uplink.acked_cursor is None
            first_rows = server.batches[0]["rows"]
            assert first_rows[0][4] == 1000

            server.hold_acks = False
            await server.ack_batch(server.batches[0]["batch"])
            await _wait_for(lambda: health.uplink.acked_cursor is not None, timeout=8)

        # Now that the prefix is contiguous the cursor advances, and past both.
        assert health.uplink.acked_cursor is not None
        remaining = await buf.read_after(health.uplink.acked_cursor)
        assert [r.power_mw for r in remaining][:1] != [1000]

    async def test_the_durable_cursor_never_moves_backwards(self, buf):
        """Acks held back and then released out of order, plus a nack in the
        middle — the combination that used to regress the persisted cursor.

        A server acking in order can never move it backwards, so exercising
        that would prove nothing.
        """
        health = Health()
        await _fill(buf, 12)
        server = FakeServer(max_batch_rows=2, window=4)
        server.hold_acks = True
        seen: list[str] = []

        async def watch():
            while True:
                await asyncio.sleep(0.02)
                if health.uplink.acked_cursor:
                    seen.append(health.uplink.acked_cursor)

        async with _running(server, buf, health):
            watcher = asyncio.create_task(watch())
            try:
                await _wait_for(lambda: len(server.batches) >= 4, timeout=8)
                held = [f["batch"] for f in server.held]
                # Ack the third, then the second, then nack the first.
                await server.ack_batch(held[2])
                await server.ack_batch(held[1])
                await asyncio.sleep(0.1)
                await server.nack(held[0], wire.NACK_TRANSIENT)
                server.hold_acks = False
                await _wait_for(
                    lambda: {r[4] for r in server.rows_acked} == {1000 + i for i in range(12)},
                    timeout=10,
                )
                # Let the watcher see the cursor settle after the last ack.
                await _wait_for(lambda: health.uplink.acked_cursor is not None, timeout=5)
                await asyncio.sleep(0.1)
            finally:
                watcher.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await watcher

        assert seen, "the cursor never advanced at all"
        assert seen == sorted(seen), f"cursor regressed: {seen}"


class TestServerInputIsNotTrusted:
    async def test_a_malformed_resume_from_is_refused_not_crashed_on(self, buf):
        """It used to surface as a bare ValueError deep in the sender, killing
        the session — and the same welcome arrives on every reconnect, so
        nothing was ever delivered again."""
        health = Health()
        await _fill(buf, 4)
        server = FakeServer(resume_from="garbage")
        async with _running(server, buf, health):
            await _wait_for(lambda: health.uplink.reconnects >= 1 or health.uplink.last_error)
            await asyncio.sleep(0.3)
        assert server.rows == []
        assert "resume_from" in health.uplink.last_error

    async def test_a_naive_expires_at_does_not_tear_down_the_session(self, buf):
        """Comparing naive to aware raises TypeError, which used to escape the
        reader and drop the readings stream with it."""
        health = Health()
        await _fill(buf, 4)
        server = FakeServer()
        server.to_send = [
            {
                "type": wire.COMMAND,
                "command_id": "naive",
                "kind": "turn_on",
                "device_id": "DEV1",
                "child_id": "",
                # No offset: a plausible server bug.
                "expires_at": datetime.now().isoformat(),  # noqa: DTZ005 — deliberately naive
            }
        ]
        async with _running(server, buf, health, None):
            await _wait_for(lambda: server.command_results)
            await _wait_for(lambda: len(server.rows) >= 4)
        # The command is answered, and readings kept flowing throughout.
        assert server.command_results[0]["command_id"] == "naive"
        assert len(server.rows) >= 4


class TestLive:
    async def test_live_omits_devices_tap_cannot_currently_reach(self, buf):
        """Health keeps a parked device's last reading; live must not restamp it."""
        from tap.device import DeviceState
        from tap.health import OutletHealth

        health = Health()
        online = health.device("ON1", host="10.0.0.1")
        online.state = DeviceState.ONLINE
        online.outlets["ON100"] = OutletHealth(child_id="ON100", relay_on=True, power_mw=5000)
        parked = health.device("OFF1", host="10.0.0.2")
        parked.state = DeviceState.OFFLINE
        parked.outlets["OFF100"] = OutletHealth(child_id="OFF100", relay_on=True, power_mw=9999)

        server = FakeServer()
        async with _running(server, buf, health):
            await _wait_for(lambda: server.live_frames, timeout=8)
        devices = {row[1] for frame in server.live_frames for row in frame["rows"]}
        assert "ON1" in devices
        assert "OFF1" not in devices


class TestRewindBeatsAnInFlightSend:
    """`send_json` suspends whenever the transport is paused — which is exactly
    during a large backfill to a slow server, the case that matters. If the
    sender assigns `_sent` *after* that await, a nack arriving meanwhile is
    silently undone: `_rewind_to` drops the batch and everything after it, then
    the sender overwrites the rewind and nothing ever resends those rows.
    """

    class _ParkedWs:
        """A WebSocket whose first readings send parks until released."""

        def __init__(self):
            self.closed = False
            self.sent: list[dict] = []
            self.parked = asyncio.Event()
            self.release = asyncio.Event()

        async def send_json(self, frame):
            self.sent.append(frame)
            if frame.get("type") == wire.READINGS and not self.parked.is_set():
                self.parked.set()
                await self.release.wait()

    async def test_rows_nacked_during_a_suspended_send_are_still_resent(self, buf):
        health = Health()
        await _fill(buf, 6)
        config = Config(tap_id="t", uplink=UplinkConfig(url="ws://x/y", enabled=True))
        uplink = Uplink(config, buf, health)
        uplink._limits = wire.Welcome({"type": wire.WELCOME, "max_batch_rows": 2, "window": 4})

        ws = self._ParkedWs()
        sender = asyncio.create_task(uplink._sender(ws))
        await asyncio.wait_for(ws.parked.wait(), timeout=5)

        # The first batch is registered and its send is suspended. Nack it.
        (batch_id,) = list(uplink._inflight)
        await uplink._on_nack({"batch": batch_id, "code": wire.NACK_TRANSIENT})
        assert uplink._sent is None, "the nack should have rewound to the start"

        ws.release.set()
        await asyncio.sleep(0.2)
        sender.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await sender

        # The observable consequence: the rewound rows must appear again. If the
        # sender overwrote the rewind, everything sent after the parked batch
        # starts *past* them and they are lost for good.
        readings = [f for f in ws.sent if f.get("type") == wire.READINGS]
        assert len(readings) >= 2, "the sender should have carried on after the nack"
        resent = [row[4] for frame in readings[1:] for row in frame["rows"]]
        assert 1000 in resent, f"the nacked rows were never resent; saw {sorted(set(resent))}"


class TestResumeFromIsSanityChecked:
    async def test_a_cursor_beyond_our_buffer_is_not_adopted(self, buf):
        """A replaced buffer restarts the sequence below the server's cursor.

        Adopting it would make every future row sort before it: nothing would
        ever be sent again, and lag would read as zero because there is nothing
        'after' the cursor.
        """
        health = Health()
        await _fill(buf, 4)
        _oldest, newest = await buf.extent()
        way_ahead = f"{int(newest) + 1_000_000:018d}"

        server = FakeServer(resume_from=way_ahead)
        async with _running(server, buf, health):
            await _wait_for(lambda: len(server.rows) >= 4, timeout=8)

        # It sent what it has instead of going silent.
        assert [r[4] for r in server.rows][:4] == [1000, 1001, 1002, 1003]
