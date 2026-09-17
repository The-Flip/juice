"""The receiving half of the tap uplink: `GET /api/v2/ingest` (WebSocket).

tap buffers readings on the LAN and streams them here. `tap/wire.py` is the
normative protocol description -- read it first. juice's copy of the constants
is `juice/api/v2/tap_wire.py`, deliberately duplicated rather than imported.

Three invariants, each of which cost something to learn:

**An ack is a durability claim.** tap advances its cursor when we ack and never
replays what it believes we hold, so acking before the rows are committed turns
any crash into permanent, silent loss. The ack goes out after the commit. That
costs nothing in throughput: tap keeps four batches in flight and waits 120 s
for each ack, so it is never idle waiting on us.

**`readings` drives no live state.** No `RecorderState`, no `_publish`, no
overload check on that channel: it is the durable, replayable one and is allowed
to be days behind, so feeding it to the live layer would run overload detection
across history and fire shutdowns for events that ended on Tuesday
(`tap/wire.py:13-18`).

That is a statement about `readings`, not about this module. `devices` and
`live` are present-tense frames and *are* projected -- but through seams
(`app["tap_devices"]` and `app["tap_live"]`, see `_handle_devices` and
`_handle_live`), because what a roster or a snapshot means is the collector's
business rather than the protocol's. Wire nothing and the frame is dropped,
which is what a bare `create_app` does.

**`readings` rows never become Python objects.** The raw frame goes to DuckDB,
which parses, validates, converts units and resolves plug identity in one pass
-- see `Store.commit_ingest_batch`. Only the small envelope (`type`, `batch`,
`cursor`) is parsed here. (`live` rows do become objects -- one `PlugReading`
each, ~50 a second -- which is a different scale entirely.) That envelope parse is a real cost on a large frame (~11 ms for
5000 rows) and it is the one piece of per-batch work still on the event loop; if
it ever matters, it moves to the writer thread with the rest.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from aiohttp import WSMsgType, web

from juice.api.access import Access, access
from juice.api.v2 import tap_wire as wire
from juice.store import IngestResult, Store

log = logging.getLogger(__name__)

# Identifies this process to tap. tap stores it and does not act on it today;
# it exists so a future tap can notice that the server it is talking to is not
# the one it was talking to a moment ago.
SERVER_EPOCH = uuid.uuid4().hex

# How long a connection may stay silent before saying hello. Mirrors tap's own
# WELCOME_TIMEOUT from the other side.
HELLO_TIMEOUT = 30.0

# Ping a peer after this much silence, and give up half as long again later.
# tap sends at 1 Hz, so a healthy connection is never pinged; this is how long
# a *dead* one keeps its registration. Railway's edge ends tap's socket with a
# bare TCP FIN and never closes the backend leg (measured 2026-09-15, ~50
# times a day), so nothing but this heartbeat ends the zombie session -- at
# 30 s it lived 45 s beside the live one. 10 s bounds that at 15 s, and is
# still ten frames of silence before a ping goes out.
HEARTBEAT_S = 10.0

# A peer that sends this much consecutive garbage is not a peer. One malformed
# frame, by contrast, should not cost a connection.
MAX_CONSECUTIVE_JUNK = 10

# Protocol-level close code for "your frames are wrong".
WS_PROTOCOL_ERROR = 1002

# How often a connected tap's throughput is summarised. Per-batch logging would
# be a line a second in steady state; per-connection-only would tell you nothing
# until the collector disconnected, which for a healthy one is never.
SUMMARY_INTERVAL_S = 300.0


class _Stats:
    """Per-connection counters, summarised periodically.

    The commit timings are the interesting part: they are the number that says
    whether the writer thread is keeping up, and the one that would have to
    change before anything else about the ingest path needed rethinking.
    """

    __slots__ = (
        "batches",
        "rows",
        "dropped",
        "bad",
        "transient",
        "duplicate",
        "commit_ms_total",
        "commit_ms_max",
        "live_frames",
        "live_dropped",
        "since",
        "last_rx",
        "last_tx",
    )

    def __init__(self) -> None:
        # Loop-clock stamps of the last frame received and the last ack or
        # nack sent, for the disconnect diagnostic line. Not part of
        # `reset`: they describe the connection, not the interval.
        self.last_rx = self.last_tx = 0.0
        self.reset()

    def reset(self) -> None:
        self.batches = self.rows = self.dropped = 0
        self.bad = self.transient = self.duplicate = 0
        self.commit_ms_total = 0.0
        self.commit_ms_max = 0.0
        self.live_frames = self.live_dropped = 0
        self.since = time.monotonic()

    def due(self) -> bool:
        return time.monotonic() - self.since >= SUMMARY_INTERVAL_S

    def summarise(self, tap_id: str, pinned_bytes: int) -> None:
        """One line per interval. `pinned_bytes` is what DuckDB is holding for
        transactions it cannot clean up yet (`Store.pinned_transaction_bytes`):
        flat near zero when every connection is settled, and climbing across
        summaries -- alongside a climbing commit avg -- when one is not."""
        window = max(time.monotonic() - self.since, 1e-9)
        log.info(
            "ingest: tap %s | %d batches, %d rows (%.1f rows/s) | "
            "commit avg %.0fms max %.0fms | dropped=%d bad=%d transient=%d dup=%d | "
            "live %d frames, %d unusable | pinned %.0f MB",
            tap_id,
            self.batches,
            self.rows,
            self.rows / window,
            self.commit_ms_total / self.batches if self.batches else 0.0,
            self.commit_ms_max,
            self.dropped,
            self.bad,
            self.transient,
            self.duplicate,
            self.live_frames,
            self.live_dropped,
            pinned_bytes / 1e6,
        )
        self.reset()


class IngestWriter:
    """Serialises ingest commits onto a single thread with its own connection.

    Two jobs, and both matter.

    It keeps the ~70 ms DuckDB commit off the event loop: without it a tap
    catching up on a day of buffered readings would starve the recorder's 1 Hz
    poll, the SSE stream and every HTTP request for the ~45 s it takes to drain.
    Measured with the writer thread, main-thread latency stays at p50 4.5 ms /
    p99 22 ms while 100k rows land.

    And `max_workers=1` is load-bearing rather than conservative: one thread
    means one connection and one writer, so concurrent ingest commits cannot
    interleave their transactions on the same tables and there is no
    optimistic-concurrency conflict to reason about. `Store._conn` stays the
    event loop's, untouched -- DuckDB connections are not thread-safe, so the
    writer gets its own via `Store.new_connection`.
    """

    def __init__(self, store: Store) -> None:
        self._store = store
        self._local = threading.local()
        self._pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ingest-writer")

    def _conn(self):
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._local.conn = self._store.new_connection()
        return conn

    def _commit(self, tap_id: str, buffer_id: str, cursor: str, frame_text: str) -> IngestResult:
        conn = self._conn()
        try:
            return self._store.commit_ingest_batch(tap_id, buffer_id, cursor, frame_text, conn=conn)
        finally:
            # A `duplicate` or `bad_batch` verdict returns straight after a
            # `fetchone()`, and the thread then idles until the next batch --
            # for as long as tap stays quiet -- with that result's transaction
            # open (`Store.settle`).
            self._store.settle(conn)

    async def commit(
        self, tap_id: str, buffer_id: str, cursor: str, frame_text: str
    ) -> IngestResult:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._pool, self._commit, tap_id, buffer_id, cursor, frame_text
        )

    def close(self) -> None:
        self._pool.shutdown(wait=True)


@access(Access.SERVICE)
async def handle_ingest(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse(heartbeat=HEARTBEAT_S, receive_timeout=HELLO_TIMEOUT * 4)
    await ws.prepare(request)

    store: Store = request.app["store"]
    writer: IngestWriter = request.app["ingest_writer"]
    # The command channel, when the collector on duty has one. Registered on
    # hello and released in `finally`, so a tap is addressable exactly while
    # it is a peer. Absent (bare `create_app`),
    # `command_result` frames are dropped as they always were.
    control = request.app.get("tap_control")
    # One bound method object for the session's lifetime: the registry tells a
    # stale disconnect from a live one by identity, and `ws.send_json` is a
    # fresh bound method on every attribute access.
    sender = ws.send_json
    identity: tuple[str, str] | None = None
    stats = _Stats()
    junk = 0

    try:
        while True:
            try:
                message = await asyncio.wait_for(
                    ws.receive(), timeout=HELLO_TIMEOUT if identity is None else None
                )
            except TimeoutError:
                log.info("ingest: no hello within %.0fs; closing", HELLO_TIMEOUT)
                await ws.close(code=WS_PROTOCOL_ERROR, message=b"no hello")
                return ws

            stats.last_rx = time.monotonic()
            if message.type is not WSMsgType.TEXT:
                if message.type in (
                    WSMsgType.CLOSE,
                    WSMsgType.CLOSING,
                    WSMsgType.CLOSED,
                    WSMsgType.ERROR,
                ):
                    return ws
                continue

            try:
                frame = json.loads(message.data)
            except ValueError:
                junk += 1
                if junk >= MAX_CONSECUTIVE_JUNK:
                    await ws.close(code=WS_PROTOCOL_ERROR, message=b"unparseable frames")
                    return ws
                continue
            if not isinstance(frame, dict):
                junk += 1
                continue
            junk = 0

            kind = frame.get("type")

            if kind == wire.HELLO:
                if identity is not None:
                    # A benign client quirk. Closing would be a worse answer
                    # than ignoring it.
                    log.warning("ingest: second hello on one connection; ignoring")
                    continue
                try:
                    tap_id, buffer_id = wire.hello_identity(frame)
                    protocol = wire.hello_protocol(frame)
                except wire.BadFrameError as exc:
                    log.warning("ingest: unusable hello (%s); closing", exc)
                    await ws.close(code=WS_PROTOCOL_ERROR, message=b"bad hello")
                    return ws

                resume_from = store.ingest_cursor(tap_id, buffer_id)
                # Sent even on a version mismatch: the welcome is what lets tap
                # log "server speaks protocol N, tap speaks M". Closing without
                # one leaves an operator staring at a bare disconnect.
                await ws.send_json(wire.welcome(resume_from, SERVER_EPOCH))
                if protocol != wire.PROTOCOL_VERSION:
                    log.error(
                        "ingest: tap %s speaks protocol %s, juice speaks %s",
                        tap_id,
                        protocol,
                        wire.PROTOCOL_VERSION,
                    )
                    await ws.close(code=WS_PROTOCOL_ERROR, message=b"protocol mismatch")
                    return ws

                identity = (tap_id, buffer_id)
                if control is not None:
                    control.connect(tap_id, sender)
                log.info(
                    "ingest: tap %s buffer %s connected, resuming from %s",
                    tap_id,
                    buffer_id or "(none)",
                    resume_from or "the start",
                )
                continue

            if kind == wire.READINGS:
                if identity is None:
                    # Not a nack: with no tap_id there is no sequence space to
                    # scope a cursor to, so there is nothing coherent to refuse.
                    log.warning("ingest: readings before hello; closing")
                    await ws.close(code=WS_PROTOCOL_ERROR, message=b"readings before hello")
                    return ws
                await _handle_readings(ws, writer, identity, frame, message.data, stats)
                if stats.due():
                    stats.summarise(identity[0], store.pinned_transaction_bytes())
                continue

            if kind == wire.DEVICES:
                _handle_devices(request, frame)
                if control is not None and identity is not None:
                    _note_devices(control, identity[0], frame)
                continue

            if kind == wire.COMMAND_RESULT:
                if control is not None:
                    _handle_command_result(control, frame)
                continue

            if kind == wire.LIVE:
                if identity is None:
                    # Not yet a peer: the protocol version is checked on hello,
                    # and a live frame is a present-tense claim about the floor.
                    stats.live_dropped += 1
                    continue
                await _handle_live(request, frame, stats)
                if stats.due():
                    stats.summarise(identity[0], store.pinned_transaction_bytes())
                continue

            # `pong` and anything a future tap invents. Ignoring unknown
            # frames is what lets either side add one without a flag day
            # (`tap/wire.py:97-99`).
    finally:
        if identity is not None:
            if control is not None:
                control.disconnect(identity[0], sender)
            stats.summarise(identity[0], store.pinned_transaction_bytes())
            # Why the socket ended, for the reconnect flapping seen in
            # production: `close_code` 1000/1001 with no exception is a close
            # frame from tap (or a proxy); 1006 with an exception is our own
            # heartbeat giving up; None means the loop ended without a close
            # at all. The ages say whether tap had gone quiet first.
            now = time.monotonic()
            failure = ws.exception()
            log.info(
                "ingest: tap %s disconnected: close_code=%s exception=%s "
                "last_rx=%.1fs ago last_ack=%.1fs ago",
                identity[0],
                ws.close_code,
                f"{type(failure).__name__}: {failure}" if failure is not None else None,
                now - stats.last_rx,
                now - stats.last_tx if stats.last_tx else -1.0,
            )
    return ws


def _note_devices(control: Any, tap_id: str, frame: dict) -> None:
    """Which tap reports which device: the routing table for commands."""
    try:
        entries = wire.devices_of(frame)
    except wire.BadFrameError:
        return  # `_handle_devices` has already logged it
    control.note_devices(tap_id, {str(e["device_id"]) for e in entries if e.get("device_id")})


def _handle_command_result(control: Any, frame: dict) -> None:
    try:
        control.resolve(wire.command_result_of(frame))
    except wire.BadFrameError as exc:
        log.warning("ingest: unusable command_result (%s); ignoring", exc)
    except Exception:
        log.warning("ingest: resolving a command result failed", exc_info=True)


def _handle_devices(request: web.Request, frame: dict) -> None:
    """Hand a roster frame to whatever is projecting it, if anything is.

    A seam rather than a call, for the reason in this module's docstring: what a
    roster *means* -- plugs, machines, assignments -- is the collector's business,
    and doing it here would make this module a second recorder. `None` is the
    normal case for `create_app` in unit tests, and it
    means the frame is dropped exactly as before.

    Never raises: a bad roster must not cost the connection, because the
    `readings` stream on it is the durable channel.
    """
    project = request.app.get("tap_devices")
    if project is None:
        return
    try:
        entries = wire.devices_of(frame)
    except wire.BadFrameError as exc:
        log.warning("ingest: unusable devices frame (%s); ignoring", exc)
        return
    try:
        project(entries)
    except Exception:
        log.warning("ingest: applying the tap roster failed", exc_info=True)


async def _handle_live(request: web.Request, frame: dict, stats: _Stats) -> None:
    """Hand a live frame to whatever is projecting it, if anything is.

    The same seam as `_handle_devices`, awaited: the projection decides what
    the rows mean (`juice.collector_tap.LiveProjector`) and is expected to
    return promptly -- a frame that needs real
    work is applied on a task of the projection's own, because this loop owns
    the durable channel's acks and must never wait on live state. `None` is the
    normal case for a bare `create_app` and means the frame is dropped exactly
    as before. Only reached after `hello`: a frame from a peer whose protocol
    version has not been checked is not a claim worth applying.

    Never raises, for the same reason as `_handle_devices`.
    """
    stats.live_frames += 1
    project = request.app.get("tap_live")
    if project is None:
        return
    try:
        rows = wire.live_rows_of(frame)
    except wire.BadFrameError as exc:
        stats.live_dropped += 1
        log.warning("ingest: unusable live frame (%s); ignoring", exc)
        return
    if not rows:
        stats.live_dropped += 1
        return
    try:
        await project(rows)
    except Exception:
        log.warning("ingest: applying a live frame failed", exc_info=True)


async def _answer(ws: web.WebSocketResponse, stats: _Stats, frame: dict) -> None:
    """Send an ack or nack, and note when: `last_ack` on the disconnect line
    means the last answer that actually left, not the last frame handled."""
    await ws.send_json(frame)
    stats.last_tx = time.monotonic()


async def _handle_readings(
    ws: web.WebSocketResponse,
    writer: IngestWriter,
    identity: tuple[str, str],
    frame: dict,
    raw: str,
    stats: _Stats,
) -> None:
    tap_id, buffer_id = identity

    try:
        batch = wire.batch_id(frame)
    except wire.BadFrameError as exc:
        # Every refusal has to name a batch, so a frame we cannot name is one we
        # cannot refuse. Drop it and let tap's ack timeout resend.
        log.error("ingest: readings frame without a usable batch id (%s); dropping", exc)
        return

    try:
        cursor = wire.cursor_of(frame)
        rows = wire.rows_of(frame)
    except wire.BadFrameError as exc:
        await _answer(ws, stats, wire.nack(batch, wire.NACK_BAD_BATCH, str(exc)))
        return

    if not rows:
        # An empty batch is a no-op, not an error -- but the cursor still has to
        # advance or tap would resend it forever.
        await _answer(ws, stats, wire.ack(batch, cursor))
        return

    started = time.monotonic()
    try:
        result = await writer.commit(tap_id, buffer_id, cursor, raw)
    except Exception:
        stats.transient += 1
        # The batch is fine; we are not. `transient` asks tap to try again
        # rather than discarding rows over a full disk.
        log.warning("ingest: store write failed for batch %s", batch, exc_info=True)
        await _answer(ws, stats, wire.nack(batch, wire.NACK_TRANSIENT, "store write failed"))
        return

    commit_ms = (time.monotonic() - started) * 1000
    stats.batches += 1
    stats.commit_ms_total += commit_ms
    stats.commit_ms_max = max(stats.commit_ms_max, commit_ms)
    stats.rows += result.stored
    stats.dropped += result.dropped_ts
    if result.verdict == "duplicate":
        stats.duplicate += 1

    if result.verdict == "bad_batch":
        stats.bad += 1
        log.error(
            "ingest: batch %s has %d malformed row(s) of %d; refusing permanently",
            batch,
            result.bad,
            result.total,
        )
        await _answer(
            ws, stats, wire.nack(batch, wire.NACK_BAD_BATCH, f"{result.bad} malformed rows")
        )
        return

    if result.dropped_ts:
        log.warning(
            "ingest: dropped %d row(s) of batch %s for an impossible timestamp",
            result.dropped_ts,
            batch,
        )
    await _answer(ws, stats, wire.ack(batch, cursor))
