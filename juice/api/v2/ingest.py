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

**Ingest drives no live state.** No `RecorderState`, no `_publish`, no overload
check. `readings` is the durable, replayable channel and is allowed to be days
behind; feeding it to the live layer would run overload detection across history
and fire shutdowns for events that ended on Tuesday (`tap/wire.py:13-18`). tap
has a separate `live` frame for present-tense state, which this ignores for now.

**Rows never become Python objects.** The raw frame goes to DuckDB, which
parses, validates, converts units and resolves plug identity in one pass -- see
`Store.commit_ingest_batch`. Only the small envelope (`type`, `batch`, `cursor`)
is parsed here. That envelope parse is a real cost on a large frame (~11 ms for
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
        "since",
    )

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.batches = self.rows = self.dropped = 0
        self.bad = self.transient = self.duplicate = 0
        self.commit_ms_total = 0.0
        self.commit_ms_max = 0.0
        self.since = time.monotonic()

    def due(self) -> bool:
        return time.monotonic() - self.since >= SUMMARY_INTERVAL_S

    def summarise(self, tap_id: str) -> None:
        window = max(time.monotonic() - self.since, 1e-9)
        log.info(
            "ingest: tap %s | %d batches, %d rows (%.1f rows/s) | "
            "commit avg %.0fms max %.0fms | dropped=%d bad=%d transient=%d dup=%d",
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
        return self._store.commit_ingest_batch(
            tap_id, buffer_id, cursor, frame_text, conn=self._conn()
        )

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
    ws = web.WebSocketResponse(heartbeat=30.0, receive_timeout=HELLO_TIMEOUT * 4)
    await ws.prepare(request)

    store: Store = request.app["store"]
    writer: IngestWriter = request.app["ingest_writer"]
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
                    stats.summarise(identity[0])
                continue

            # `live`, `devices`, `command_result`, `pong` and anything a future
            # tap invents. Ignoring unknown frames is what lets either side add
            # one without a flag day (`tap/wire.py:97-99`).
    finally:
        if identity is not None:
            stats.summarise(identity[0])
            log.info("ingest: tap %s disconnected", identity[0])
    return ws


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
        await ws.send_json(wire.nack(batch, wire.NACK_BAD_BATCH, str(exc)))
        return

    if not rows:
        # An empty batch is a no-op, not an error -- but the cursor still has to
        # advance or tap would resend it forever.
        await ws.send_json(wire.ack(batch, cursor))
        return

    started = time.monotonic()
    try:
        result = await writer.commit(tap_id, buffer_id, cursor, raw)
    except Exception:
        stats.transient += 1
        # The batch is fine; we are not. `transient` asks tap to try again
        # rather than discarding rows over a full disk.
        log.warning("ingest: store write failed for batch %s", batch, exc_info=True)
        await ws.send_json(wire.nack(batch, wire.NACK_TRANSIENT, "store write failed"))
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
        await ws.send_json(wire.nack(batch, wire.NACK_BAD_BATCH, f"{result.bad} malformed rows"))
        return

    if result.dropped_ts:
        log.warning(
            "ingest: dropped %d row(s) of batch %s for an impossible timestamp",
            result.dropped_ts,
            batch,
        )
    await ws.send_json(wire.ack(batch, cursor))
