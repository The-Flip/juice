"""The WebSocket uplink: a cursor tailing the buffer.

**There is no separate live path and backfill path.** Every reading is written
to the buffer first and the uplink is a cursor walking it. Caught up, that walk
produces one small batch per second. After a three-day outage it produces large
batches of old rows. Same code, same ordering, same acknowledgements — backfill
is not a feature, it is what "caught up" degrades into when it hasn't been.

The server is the authority on durability. tap offers its buffer extent in
`hello`; the server replies with the cursor it has actually stored, and tap
rewinds to it. That is what makes a server restored from backup correct rather
than quietly missing a day.

Flow control is a fixed window of unacked batches. A collector three days behind
therefore cannot flood the server: the ack rate sets the drain rate.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import logging
import random
from datetime import UTC, datetime

import aiohttp

from tap import wire
from tap.buffer import Buffer, rows_to_wire
from tap.config import Config
from tap.health import Health

log = logging.getLogger(__name__)

# Reconnect backoff, matching juice's SSE client, plus the jitter it lacks.
BACKOFF_INITIAL = 0.5
BACKOFF_MAX = 30.0
# A connection that survives this long is considered good, and resets the
# backoff — otherwise a daily blip would eventually leave us at a 30s delay.
BACKOFF_RESET_AFTER = 30.0
# Application-level keepalive. A proxy can kill a socket without closing it;
# an idle read looks identical to a healthy quiet link without this.
PING_INTERVAL = 20.0
STREAM_READ_TIMEOUT = 60.0
LIVE_INTERVAL = 1.0
# Idle poll when the buffer has nothing new. Short enough to feel live, long
# enough not to spin.
IDLE_POLL = 0.25
# Commands already answered, kept so a redelivery is not re-actuated. A relay is
# physical: doing it twice is not the same as doing it once.
COMMAND_CACHE_SIZE = 256

ACKED_STATE_KEY = "acked_cursor"


class _Batch:
    __slots__ = ("batch_id", "start_cursor", "end_cursor", "rows")

    def __init__(self, batch_id: str, start_cursor: str | None, end_cursor: str, rows: int) -> None:
        self.batch_id = batch_id
        self.start_cursor = start_cursor
        self.end_cursor = end_cursor
        self.rows = rows


class Uplink:
    """Streams the buffer to the server and carries commands back."""

    def __init__(
        self,
        config: Config,
        buffer: Buffer,
        health: Health,
        pollers=None,
        *,
        session_factory=None,
    ) -> None:
        self._config = config
        self._buffer = buffer
        self._health = health
        self._pollers = pollers
        self._session_factory = session_factory
        self._ids = itertools.count(1)
        self._acked: str | None = None
        self._sent: str | None = None
        self._inflight: dict[str, _Batch] = {}
        self._limits = wire.Welcome({"type": wire.WELCOME, "protocol": wire.PROTOCOL_VERSION})
        self._command_results: dict[str, dict] = {}
        self._stop = asyncio.Event()

    # ---- lifecycle ----------------------------------------------------------

    async def run(self) -> None:
        """Connect, stream, reconnect. Never returns until stopped."""
        health = self._health.uplink
        health.enabled = self._config.uplink.active
        health.url = self._config.uplink.url or ""
        if not self._config.uplink.active:
            log.info("uplink: not configured; running standalone (buffering only)")
            await self._stop.wait()
            return

        self._acked = await self._buffer.get_state(ACKED_STATE_KEY)
        backoff = BACKOFF_INITIAL
        while not self._stop.is_set():
            started = asyncio.get_running_loop().time()
            try:
                await self._connect_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 — the uplink must never die
                health.last_error = f"{type(e).__name__}: {e}"
                log.warning("uplink: %s", health.last_error)
            health.connected = False
            if self._stop.is_set():
                break
            if asyncio.get_running_loop().time() - started >= BACKOFF_RESET_AFTER:
                backoff = BACKOFF_INITIAL
            delay = backoff / 2 + random.uniform(0, backoff / 2)  # noqa: S311 — load spreading
            health.backoff_s = delay
            log.info("uplink: reconnecting in %.1fs", delay)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            backoff = min(backoff * 2, BACKOFF_MAX)

    def stop(self) -> None:
        self._stop.set()

    async def _connect_once(self) -> None:
        session_cm = (
            self._session_factory()
            if self._session_factory is not None
            else aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=None, sock_read=STREAM_READ_TIMEOUT)
            )
        )
        async with session_cm as session:
            headers = {}
            if self._config.uplink.token:
                # Header only, so the secret never lands in a URL or an access log.
                headers["Authorization"] = f"Bearer {self._config.uplink.token}"
            url = self._config.uplink.url
            if url is None:  # pragma: no cover - guarded by uplink.active in run()
                return
            async with session.ws_connect(url, headers=headers) as ws:
                await self._session(ws)

    # ---- one connection -----------------------------------------------------

    async def _session(self, ws) -> None:
        health = self._health.uplink
        oldest, newest = await self._buffer.extent()
        await ws.send_json(wire.hello(self._config.tap_id, self._health.version, oldest, newest))
        raw = await ws.receive_json()
        welcome = wire.Welcome(raw)
        self._limits = welcome

        # The server's cursor wins. Older than ours means it lost data and we
        # resend; newer means our buffer was wiped and it discards the overlap.
        if welcome.resume_from is not None:
            self._acked = welcome.resume_from
            await self._buffer.set_state(ACKED_STATE_KEY, self._acked)
        self._sent = self._acked
        self._inflight.clear()

        health.connected = True
        health.since = datetime.now(UTC)
        health.reconnects += 1
        health.backoff_s = 0.0
        health.acked_cursor = self._acked
        log.info(
            "uplink: connected to %s, resuming from %s",
            self._config.uplink.url,
            self._acked or "the start of the buffer",
        )

        await ws.send_json(wire.devices(await self._buffer.aliases()))

        # FIRST_COMPLETED, not a TaskGroup: when the socket closes it is the
        # reader that notices, and a TaskGroup would then wait for the pinger to
        # finish its 20-second sleep before letting us reconnect. The first task
        # to end means the connection is over, so the rest are cancelled.
        tasks = {
            asyncio.create_task(self._reader(ws), name="uplink:reader"),
            asyncio.create_task(self._sender(ws), name="uplink:sender"),
            asyncio.create_task(self._live(ws), name="uplink:live"),
            asyncio.create_task(self._pinger(ws), name="uplink:ping"),
        }
        try:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        for task in done:
            if task.cancelled():
                continue
            failure = task.exception()
            if failure is not None:
                raise failure

    async def _pinger(self, ws) -> None:
        while not ws.closed:
            await asyncio.sleep(PING_INTERVAL)
            if not ws.closed:
                await ws.send_json({"type": wire.PING, "token": datetime.now(UTC).timestamp()})

    async def _sender(self, ws) -> None:
        """Walk the buffer forward, never more than `window` batches unacked."""
        health = self._health.uplink
        while not ws.closed:
            if len(self._inflight) >= self._limits.window:
                await asyncio.sleep(IDLE_POLL)
                continue
            rows = await self._buffer.read_after(self._sent, self._limits.max_batch_rows)
            if not rows:
                await self._update_lag()
                await asyncio.sleep(IDLE_POLL)
                continue
            end_cursor = self._buffer.cursor_of(rows[-1])
            batch_id = f"b{next(self._ids)}"
            self._inflight[batch_id] = _Batch(batch_id, self._sent, end_cursor, len(rows))
            await ws.send_json(wire.readings(batch_id, end_cursor, rows_to_wire(rows)))
            self._sent = end_cursor
            health.sent_cursor = end_cursor
            health.batches_sent += 1
            await self._update_lag()

    async def _live(self, ws) -> None:
        """Best-effort current state, suppressed while deep in backfill."""
        health = self._health.uplink
        while not ws.closed:
            await asyncio.sleep(LIVE_INTERVAL)
            lag = health.lag_seconds
            suppressed = lag is not None and lag > self._limits.live_max_lag_s
            if suppressed != health.live_suppressed:
                health.live_suppressed = suppressed
                log.info(
                    "uplink: live frames %s (lag %.0fs)",
                    "suppressed while backfilling" if suppressed else "resumed",
                    lag or 0.0,
                )
            if suppressed or ws.closed:
                continue
            rows = self._live_rows()
            if rows:
                await ws.send_json(wire.live(rows))

    def _live_rows(self) -> list[list]:
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        rows = []
        for device in self._health.devices.values():
            for outlet in device.outlets.values():
                rows.append(
                    [
                        now_ms,
                        device.device_id,
                        outlet.child_id,
                        1 if outlet.relay_on else 0,
                        outlet.power_mw,
                        outlet.voltage_mv,
                        None,
                        None,
                    ]
                )
        return rows

    async def _update_lag(self) -> None:
        health = self._health.uplink
        _oldest, newest = await self._buffer.extent()
        if newest is None:
            health.lag_rows = 0
            health.lag_seconds = 0.0
            return
        health.lag_rows = max(0, len(await self._buffer.read_after(self._acked, 100_000)))
        newest_ts = self._health.buffer.newest_ts
        if newest_ts is None or health.lag_rows == 0:
            health.lag_seconds = 0.0
            return
        oldest_unsent = await self._buffer.read_after(self._acked, 1)
        if oldest_unsent:
            ts = datetime.fromtimestamp(oldest_unsent[0].ts_ms / 1000, UTC)
            health.lag_seconds = max(0.0, (datetime.now(UTC) - ts).total_seconds())

    async def _reader(self, ws) -> None:
        async for message in ws:
            if message.type is not aiohttp.WSMsgType.TEXT:
                continue
            try:
                frame = message.json()
            except ValueError:
                log.warning("uplink: server sent a non-JSON frame; ignoring")
                continue
            await self._handle(ws, frame)

    async def _handle(self, ws, frame: dict) -> None:
        health = self._health.uplink
        kind = frame.get("type")
        if kind == wire.ACK:
            await self._on_ack(frame)
        elif kind == wire.NACK:
            await self._on_nack(frame)
        elif kind == wire.COMMAND:
            await self._on_command(ws, frame)
        elif kind == wire.PING:
            await ws.send_json(wire.pong(frame.get("token")))
        elif kind == wire.PONG:
            pass
        else:
            log.debug("uplink: ignoring unknown frame type %r", kind)
            health.last_error = f"unknown frame {kind!r}"

    async def _on_ack(self, frame: dict) -> None:
        health = self._health.uplink
        batch = self._inflight.pop(frame.get("batch", ""), None)
        if batch is None:
            log.debug("uplink: ack for unknown batch %r", frame.get("batch"))
            return
        # An ack is a durability claim, so this is the only place the persisted
        # cursor moves forward.
        self._acked = batch.end_cursor
        await self._buffer.set_state(ACKED_STATE_KEY, self._acked)
        health.acked_cursor = self._acked
        health.batches_acked += 1
        health.rows_acked += batch.rows

    async def _on_nack(self, frame: dict) -> None:
        health = self._health.uplink
        batch = self._inflight.pop(frame.get("batch", ""), None)
        health.batches_nacked += 1
        if batch is None:
            return
        code = frame.get("code", wire.NACK_TRANSIENT)
        if code == wire.NACK_BAD_BATCH:
            # A poison pill. Wedging forever on one malformed batch is a worse
            # failure than losing it, so step over it — loudly, and counted.
            health.batches_poisoned += 1
            log.error(
                "uplink: server rejected batch %s as unusable (%s); skipping rows %s..%s",
                batch.batch_id,
                frame.get("message", ""),
                batch.start_cursor,
                batch.end_cursor,
            )
            self._acked = batch.end_cursor
            await self._buffer.set_state(ACKED_STATE_KEY, self._acked)
            health.acked_cursor = self._acked
            return
        # Transient: rewind and let the sender walk it again.
        log.warning("uplink: batch %s nacked (%s); resending", batch.batch_id, code)
        self._sent = batch.start_cursor

    # ---- commands -----------------------------------------------------------

    async def _on_command(self, ws, frame: dict) -> None:
        health = self._health.uplink
        command_id = frame.get("command_id") or ""
        health.commands_received += 1

        cached = self._command_results.get(command_id)
        if cached is not None:
            # Redelivery. Answer from cache: a relay is physical, and doing it
            # twice is not the same as doing it once.
            log.info("uplink: command %s already applied; replaying result", command_id)
            await ws.send_json(cached)
            return

        result = await self._apply_command(frame)
        self._remember(command_id, result)
        if result.get("status") != "ok":
            health.commands_failed += 1
        await ws.send_json(result)

    async def _apply_command(self, frame: dict) -> dict:
        command_id = frame.get("command_id") or ""
        kind = frame.get("kind")
        device_id = frame.get("device_id") or ""
        child_id = frame.get("child_id") or ""

        expires_at = frame.get("expires_at")
        if expires_at:
            try:
                deadline = datetime.fromisoformat(expires_at)
            except ValueError:
                return wire.command_result(command_id, "error", "unparseable expires_at")
            if datetime.now(UTC) > deadline:
                # Powering a machine on because of a message that sat in a dead
                # socket for two minutes is exactly the failure to avoid.
                log.warning("uplink: command %s arrived after it expired; refusing", command_id)
                return wire.command_result(command_id, "error", "expired")

        if kind not in ("turn_on", "turn_off"):
            return wire.command_result(command_id, "error", f"unknown command kind {kind!r}")
        if self._pollers is None:
            return wire.command_result(command_id, "error", "no devices")
        poller = self._pollers.find(device_id)
        if poller is None:
            return wire.command_result(command_id, "error", f"unknown device {device_id}")

        log.info("uplink: command %s %s %s/%s", command_id, kind, device_id[:12], child_id)
        try:
            await poller.set_relay(child_id, kind == "turn_on")
        except Exception as e:  # noqa: BLE001 — the failure belongs in the reply
            log.error("uplink: command %s failed: %s", command_id, e)
            return wire.command_result(command_id, "error", f"{type(e).__name__}: {e}")
        # "ok" means the device accepted the call. Whether the relay actually
        # moved is settled by the next reading, on the server, where the
        # command's lifecycle lives.
        return wire.command_result(command_id, "ok")

    def _remember(self, command_id: str, result: dict) -> None:
        if not command_id:
            return
        self._command_results[command_id] = result
        while len(self._command_results) > COMMAND_CACHE_SIZE:
            self._command_results.pop(next(iter(self._command_results)))
