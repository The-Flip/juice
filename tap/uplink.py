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
from tap.device import DeviceState
from tap.errors import FatalError
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
# The welcome exchange runs before any reader task exists, so it is bounded
# separately.
WELCOME_TIMEOUT = 30.0
LIVE_INTERVAL = 1.0
# Idle poll when the buffer has nothing new. Short enough to feel live, long
# enough not to spin.
IDLE_POLL = 0.25
# Commands already answered, kept so a redelivery is not re-actuated. A relay is
# physical: doing it twice is not the same as doing it once.
COMMAND_CACHE_SIZE = 256
# A batch with no answer after this long is assumed lost and resent. Without it,
# a single dropped ack on an otherwise healthy socket wedges the sender at the
# window limit forever, and nothing watches for that.
BATCH_ACK_TIMEOUT = 120.0

ACKED_STATE_KEY = "acked_cursor"


class _Batch:
    __slots__ = ("batch_id", "start_cursor", "end_cursor", "rows", "acked", "sent_at")

    def __init__(self, batch_id: str, start_cursor: str | None, end_cursor: str, rows: int) -> None:
        self.batch_id = batch_id
        self.start_cursor = start_cursor
        self.end_cursor = end_cursor
        self.rows = rows
        self.acked = False
        self.sent_at = 0.0


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
        # Commands being actuated right now. Dispatching commands off the reader
        # loop means a redelivery can arrive while the first is still running,
        # so in-flight ids are refused as well as completed ones.
        self._command_inflight: dict[str, asyncio.Task] = {}
        self._command_tasks: set[asyncio.Task] = set()
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
            except asyncio.CancelledError, FatalError:
                # FatalError means the buffer is unusable. Retrying the socket
                # forever would hide it; the supervisor should restart instead.
                raise
            except Exception as e:  # noqa: BLE001 — a bad connection must never kill the uplink
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
        for task in list(self._command_tasks):
            task.cancel()

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
            # `sock_read` does NOT bound WebSocket frame reads — aiohttp uses
            # its own ws_receive timeout, which ws_connect leaves unset unless
            # asked. Without these, a proxy that kills a socket without closing
            # it looks exactly like a healthy quiet link, forever.
            async with session.ws_connect(
                url,
                headers=headers,
                heartbeat=PING_INTERVAL,
                timeout=aiohttp.ClientWSTimeout(ws_receive=STREAM_READ_TIMEOUT),
            ) as ws:
                await self._session(ws)

    # ---- one connection -----------------------------------------------------

    async def _session(self, ws) -> None:
        health = self._health.uplink
        oldest, newest = await self._buffer.extent()
        await ws.send_json(wire.hello(self._config.tap_id, self._health.version, oldest, newest))
        # The welcome happens before the reader task exists, so it needs its own
        # deadline: nothing else would notice a server that accepts the socket
        # and then says nothing.
        async with asyncio.timeout(WELCOME_TIMEOUT):
            raw = await ws.receive_json()
        welcome = wire.Welcome(raw)
        self._limits = welcome

        # The server's cursor wins. Older than ours means it lost data and we
        # resend; newer means our buffer was wiped and it discards the overlap.
        if welcome.resume_from is not None:
            # Compare against the sequence space, not the current contents. A
            # buffer that is merely *behind* the server (pruned, or caught up)
            # still has a high-water mark above its cursor and should adopt it.
            # A buffer whose storage was replaced has a high-water mark below
            # it — and on a genuinely fresh volume there is no newest row at
            # all, so comparing against `extent()` would miss the very case
            # this guard exists for.
            high_water = await self._buffer.high_water()
            if welcome.resume_from > high_water:
                log.warning(
                    "uplink: server resumes from %s, beyond our whole sequence (high water "
                    "%s) — the buffer appears to have been replaced; sending what we have "
                    "instead, and the server should expect a restarted sequence",
                    welcome.resume_from,
                    high_water,
                )
            else:
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

    async def _sender(self, ws) -> None:
        """Walk the buffer forward, never more than `window` batches unacked."""
        health = self._health.uplink
        loop = asyncio.get_running_loop()
        while not ws.closed:
            self._expire_stalled_batches(loop.time())
            if len(self._inflight) >= self._limits.window:
                await asyncio.sleep(IDLE_POLL)
                continue
            # Remember what we read from: `_on_nack` can rewind `_sent` while
            # this await is suspended, and blindly assigning `end_cursor`
            # afterwards would silently discard the rewind and skip the batch
            # the server just asked us to resend.
            read_from = self._sent
            rows = await self._buffer.read_after(read_from, self._limits.max_batch_rows)
            if self._sent != read_from:
                continue
            if not rows:
                await self._update_lag()
                await asyncio.sleep(IDLE_POLL)
                continue
            end_cursor = self._buffer.cursor_of(rows[-1])
            batch_id = f"b{next(self._ids)}"
            batch = _Batch(batch_id, read_from, end_cursor, len(rows))
            batch.sent_at = loop.time()
            self._inflight[batch_id] = batch
            # Advance BEFORE sending, not after. `send_json` suspends whenever
            # the transport is paused — i.e. exactly during a large backfill to
            # a slow server — and `_on_nack` runs on the reader task meanwhile.
            # Assigning after the send would overwrite the rewind it performed
            # and strand every batch the rewind dropped.
            self._sent = end_cursor
            health.sent_cursor = end_cursor
            health.batches_sent += 1
            await ws.send_json(wire.readings(batch_id, end_cursor, rows_to_wire(rows)))
            await self._update_lag()

    def _expire_stalled_batches(self, now: float) -> None:
        """Resend anything the server never answered.

        One dropped ack on a socket that stays up would otherwise hold a window
        slot forever; four of them stop the stream entirely, with nothing
        watching.
        """
        stalled = [
            b for b in self._inflight.values() if b.sent_at and now - b.sent_at > BATCH_ACK_TIMEOUT
        ]
        if not stalled:
            return
        oldest = min(stalled, key=lambda b: b.sent_at)
        log.warning(
            "uplink: no answer for batch %s after %.0fs; resending from %s",
            oldest.batch_id,
            now - oldest.sent_at,
            oldest.start_cursor or "the start of the buffer",
        )
        self._rewind_to(oldest)

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
        """A snapshot of what each *reachable* outlet is doing now.

        Health keeps a device's last readings after it goes offline, which is
        right for the status page and wrong here: stamping them with the current
        time would tell the server an unreachable strip is still drawing. The
        buffer records the truth (a gap, and no rows), so live must agree.
        """
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        rows = []
        for device in self._health.devices.values():
            if device.state not in (DeviceState.ONLINE, DeviceState.DEGRADED):
                continue
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
        """How far behind durable acknowledgement we are, in rows and seconds.

        Lag is measured from the *acked* cursor, not the sent one: rows on the
        wire but not yet confirmed are still rows the server might not have.
        """
        health = self._health.uplink
        rows, oldest_ms = await self._buffer.lag_after(self._acked)
        health.lag_rows = rows
        if not rows or oldest_ms is None:
            health.lag_seconds = 0.0
            return
        oldest = datetime.fromtimestamp(oldest_ms / 1000, UTC)
        health.lag_seconds = max(0.0, (datetime.now(UTC) - oldest).total_seconds())

    async def _reader(self, ws) -> None:
        async for message in ws:
            if message.type is not aiohttp.WSMsgType.TEXT:
                continue
            try:
                frame = message.json()
            except ValueError:
                log.warning("uplink: server sent a non-JSON frame; ignoring")
                continue
            try:
                await self._handle(ws, frame)
            except asyncio.CancelledError, FatalError:
                raise
            except Exception as e:  # noqa: BLE001 — one bad frame must not drop the connection
                log.warning(
                    "uplink: ignoring a %r frame we could not handle: %s",
                    frame.get("type"),
                    e,
                )
                self._health.uplink.last_error = f"{type(e).__name__}: {e}"

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
        batch = self._inflight.get(frame.get("batch", ""))
        if batch is None:
            log.debug("uplink: ack for unknown batch %r", frame.get("batch"))
            return
        batch.acked = True
        await self._advance_acked()

    async def _advance_acked(self) -> None:
        """Move the durable cursor over the longest contiguous acked prefix.

        Up to `window` batches are on the wire at once, so acks can arrive out
        of order. Advancing to whichever batch was acked last would skip every
        earlier one still in flight — and because the cursor is persisted, those
        rows would never be sent again by any future connection. Only a prefix
        with no holes in it is safe.
        """
        health = self._health.uplink
        advanced: str | None = None
        while self._inflight:
            batch_id = next(iter(self._inflight))
            batch = self._inflight[batch_id]
            if not batch.acked:
                break
            del self._inflight[batch_id]
            advanced = batch.end_cursor
            health.batches_acked += 1
            health.rows_acked += batch.rows
        if advanced is None:
            return
        # An ack is a durability claim, so this is the only place the persisted
        # cursor moves forward — and it never moves backward.
        self._acked = advanced
        await self._buffer.set_state(ACKED_STATE_KEY, advanced)
        health.acked_cursor = advanced

    async def _on_nack(self, frame: dict) -> None:
        health = self._health.uplink
        batch = self._inflight.get(frame.get("batch", ""))
        health.batches_nacked += 1
        if batch is None:
            return
        code = frame.get("code", wire.NACK_TRANSIENT)
        if code == wire.NACK_BAD_BATCH:
            # A poison pill. Wedging forever on one malformed batch is a worse
            # failure than losing it, so step over it — loudly, and counted.
            # Treated as acked so the cursor still advances only over a
            # contiguous prefix; it must never jump past a batch still in doubt.
            health.batches_poisoned += 1
            log.error(
                "uplink: server rejected batch %s as unusable (%s); skipping rows %s..%s",
                batch.batch_id,
                frame.get("message", ""),
                batch.start_cursor,
                batch.end_cursor,
            )
            batch.acked = True
            await self._advance_acked()
            return
        log.warning("uplink: batch %s nacked (%s); resending", batch.batch_id, code)
        self._rewind_to(batch)

    def _rewind_to(self, batch: _Batch) -> None:
        """Resend `batch` and everything sent after it.

        Batches are strictly ordered, so anything sent later covers rows after
        this one. Dropping the whole tail keeps the stream contiguous rather
        than leaving a hole that the acked-prefix rule would then refuse to
        advance past.
        """
        seen = False
        for batch_id in list(self._inflight):
            if batch_id == batch.batch_id:
                seen = True
            if seen:
                del self._inflight[batch_id]
        self._sent = batch.start_cursor
        self._health.uplink.sent_cursor = batch.start_cursor

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
        running = self._command_inflight.get(command_id)
        if running is not None:
            # Redelivered while the first attempt is still actuating. Wait for
            # that one rather than throwing the relay again, then answer from
            # its result — silence would leave the server waiting forever.
            log.info("uplink: command %s already in flight; awaiting it", command_id)
            await asyncio.shield(running)
            cached = self._command_results.get(command_id)
            if cached is not None and not ws.closed:
                await ws.send_json(cached)
            return

        # Actuating can take COMMAND_ATTEMPTS x COMMAND_BUDGET, and doing it
        # inline would stop the reader answering acks for that whole time — the
        # send window fills and the server may conclude tap is dead.
        task = asyncio.create_task(self._run_command(ws, command_id, frame))
        self._command_inflight[command_id] = task
        self._command_tasks.add(task)
        task.add_done_callback(self._command_tasks.discard)

    async def _run_command(self, ws, command_id: str, frame: dict) -> None:
        health = self._health.uplink
        try:
            result = await self._apply_command(frame)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 — the failure belongs in the reply
            log.error("uplink: command %s raised: %s", command_id, e, exc_info=True)
            result = wire.command_result(command_id, "error", f"{type(e).__name__}: {e}")
        finally:
            self._command_inflight.pop(command_id, None)
        if result.get("status") == "ok":
            # Only successes are memoised. Caching a failure would mean a
            # command that failed because the device was briefly unreachable
            # returns the same stale error to every retry, forever — and that is
            # the emergency shutdown path.
            self._remember(command_id, result)
        else:
            health.commands_failed += 1
        if not ws.closed:
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
            except TypeError, ValueError:
                return wire.command_result(command_id, "error", "unparseable expires_at")
            if deadline.tzinfo is None:
                # A naive deadline is a server bug, but comparing it would raise
                # TypeError and take the whole session down with it. Read it as
                # UTC, which is what the protocol says every timestamp is.
                log.warning(
                    "uplink: command %s sent a naive expires_at (%s); reading it as UTC",
                    command_id,
                    expires_at,
                )
                deadline = deadline.replace(tzinfo=UTC)
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
