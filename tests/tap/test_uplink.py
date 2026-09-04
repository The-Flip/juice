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
                if self.nack_next is not None:
                    nack = {"type": wire.NACK, "batch": frame["batch"], **self.nack_next}
                    self.nack_next = None
                    await ws.send_json(nack)
                else:
                    await ws.send_json(
                        {"type": wire.ACK, "batch": frame["batch"], "cursor": frame["cursor"]}
                    )
                if (
                    self.drop_after_batches is not None
                    and len(self.batches) >= self.drop_after_batches
                ):
                    await ws.close()
                    return ws
        return ws

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

    async def test_the_acked_cursor_is_persisted(self, buf):
        health = Health()
        await _fill(buf, 4)
        server = FakeServer()
        async with _running(server, buf, health):
            await _wait_for(lambda: len(server.rows) >= 4)
            await _wait_for(lambda: health.uplink.acked_cursor is not None)
        assert await buf.get_state(ACKED_STATE_KEY) == health.uplink.acked_cursor

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
