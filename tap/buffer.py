"""The local buffer: day-partitioned SQLite, written from a single thread.

**Why day files.** The requirement is a ~30-day rolling window at full 1 Hz —
roughly 4M rows a day. The hard part is not writing them, it is *expiring* them.
A single-file store makes retention a `DELETE` of a third of a billion rows, and
neither SQLite nor DuckDB gives that back as free disk without rewriting the
whole file. One file per UTC day makes retention `os.unlink`: constant time,
complete reclamation, and it still works on a file too corrupt to open, because
the expiry decision is made from the filename.

**Why SQLite.** It is a crash-safe append log with a query engine already
attached. WAL rolls back to the last commit, so the worst case is losing the
in-flight second — there is no torn-record recovery to write. It is in the
standard library, which matters for a daemon whose job is to survive. And the
columns are integers in the units the device reports, which SQLite varint-encodes:
a zero costs one byte, where a float watt would cost eight.

**Why a thread.** juice's `Store` calls DuckDB straight from the event loop, so
a slow write stalls the recorder and the web server together. Here every
database call is handed to a `ThreadPoolExecutor(max_workers=1)`. One worker
means one thread, which means one connection per file touched by exactly one
thread, which means no locking anywhere. The pollers never wait for a disk.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from tap.device import Sweep
from tap.errors import EXIT_INTERNAL, FatalError
from tap.health import BufferHealth
from tap.logmod import RateLimited

log = logging.getLogger(__name__)
_overflow_log = RateLimited(log)

# ~5s of sweeps for a 12-device fleet. If the disk is stalled longer than that,
# dropping the oldest is the correct answer and the counter makes it visible.
QUEUE_MAXSIZE = 600
# Rows per read batch handed to the uplink.
DEFAULT_READ_LIMIT = 5000

# A host with no RTC can boot before NTP syncs and stamp readings in 1970, which
# would then be faithfully uploaded and permanently poison the server's history.
# Refuse anything implausibly old. Any date before this project existed will do;
# the point is to reject an unsynced clock, not to be precise.
CLOCK_FLOOR = datetime(2025, 1, 1, tzinfo=UTC)

_SCHEMA_DAY = """
CREATE TABLE IF NOT EXISTS readings (
    seq        INTEGER PRIMARY KEY,   -- rowid alias: monotonic, and IS the cursor
    ts_ms      INTEGER NOT NULL,      -- epoch milliseconds, UTC
    device_id  TEXT    NOT NULL,
    child_id   TEXT    NOT NULL,      -- '' for a single-outlet device
    relay_on   INTEGER NOT NULL,
    power_mw   INTEGER,
    voltage_mv INTEGER,
    current_ma INTEGER,
    energy_wh  INTEGER
);
-- No secondary index. We only ever append, so seq is already ts order, which is
-- the only order anything reads in. An index on ts would cost disk and buy
-- nothing.
"""

_SCHEMA_META = """
CREATE TABLE IF NOT EXISTS counters (
    name  TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS cursor_state (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);
-- The alias roster. Aliases deliberately do not ride on every reading row (the
-- server would re-upsert a plug per row), so they live here and travel in their
-- own message.
CREATE TABLE IF NOT EXISTS devices (
    device_id TEXT NOT NULL,
    child_id  TEXT NOT NULL,
    alias     TEXT NOT NULL DEFAULT '',
    model     TEXT NOT NULL DEFAULT '',
    family    TEXT NOT NULL DEFAULT '',
    host      TEXT NOT NULL DEFAULT '',
    last_seen INTEGER NOT NULL,
    PRIMARY KEY (device_id, child_id)
);
-- Why a hole is a hole. Without this, a reading lost to an offline device and a
-- reading lost to a full queue are both just "fewer rows", and nobody can tell
-- a gap in the data from an absence of data.
CREATE TABLE IF NOT EXISTS gaps (
    device_id TEXT    NOT NULL,
    from_ms   INTEGER NOT NULL,
    to_ms     INTEGER,
    reason    TEXT    NOT NULL,
    PRIMARY KEY (device_id, from_ms, reason)
);
"""


def make_cursor(day: str, seq: int) -> str:
    """`YYYYMMDD:0000000000000123` — zero-padded so string order is seq order."""
    return f"{day}:{seq:016d}"


def parse_cursor(cursor: str) -> tuple[str, int]:
    day, _, seq = cursor.partition(":")
    return day, int(seq)


def _day_of(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, UTC).strftime("%Y%m%d")


@dataclass(frozen=True, slots=True)
class Row:
    """One buffered outlet reading, as it goes over the wire."""

    seq: int
    ts_ms: int
    device_id: str
    child_id: str
    relay_on: bool
    power_mw: int | None
    voltage_mv: int | None
    current_ma: int | None
    energy_wh: int | None

    def as_wire(self) -> list:
        """Positional, because at 5000 rows a batch the keys cost more than the data."""
        return [
            self.ts_ms,
            self.device_id,
            self.child_id,
            1 if self.relay_on else 0,
            self.power_mw,
            self.voltage_mv,
            self.current_ma,
            self.energy_wh,
        ]


class Buffer:
    """Durable local storage for readings, plus the cursor the uplink tails."""

    def __init__(
        self,
        directory: Path,
        *,
        retention_days: int = 30,
        health: BufferHealth | None = None,
        queue_maxsize: int = QUEUE_MAXSIZE,
    ) -> None:
        self._dir = Path(directory)
        self._retention_days = retention_days
        self._health = health if health is not None else BufferHealth()
        self._health.retention_days = retention_days
        self._queue: asyncio.Queue[Sweep] = asyncio.Queue(maxsize=queue_maxsize)
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="tap-buffer")
        self._conns: dict[str, sqlite3.Connection] = {}
        self._meta: sqlite3.Connection | None = None
        self._closed = False
        self._pending_devices: dict[tuple[str, str], str] = {}

    # ---- lifecycle ----------------------------------------------------------

    async def open(self) -> None:
        await self._run(self._open_sync)
        await self.prune()
        await self.refresh_stats()

    def _open_sync(self) -> None:
        try:
            self._dir.mkdir(parents=True, exist_ok=True)
            probe = self._dir / ".writable"
            probe.write_text("")
            probe.unlink()
        except OSError as e:
            raise FatalError(
                f"buffer directory {self._dir} is not writable: {e}", EXIT_INTERNAL
            ) from None
        self._meta = self._connect(self._dir / "meta.sqlite", _SCHEMA_META)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._run(self._close_sync)
        self._executor.shutdown(wait=True)

    def _close_sync(self) -> None:
        for conn in self._conns.values():
            conn.close()
        self._conns.clear()
        if self._meta is not None:
            self._meta.close()
            self._meta = None

    def _connect(self, path: Path, schema: str) -> sqlite3.Connection:
        conn = sqlite3.connect(path, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")
        # fsync at checkpoint, not per commit. A crash can lose the last second
        # of readings; a lost second is not worth an fsync every second on flash
        # that has to survive a month of this.
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA page_size=8192")
        conn.execute("PRAGMA wal_autocheckpoint=4000")  # ~32MB
        conn.executescript(schema)
        return conn

    def _day_conn(self, day: str) -> sqlite3.Connection:
        conn = self._conns.get(day)
        if conn is None:
            conn = self._connect(self._dir / f"{day}.sqlite", _SCHEMA_DAY)
            self._conns[day] = conn
        return conn

    async def _run(self, fn, *args):
        """Run a database call on the single writer thread."""
        return await asyncio.get_running_loop().run_in_executor(self._executor, fn, *args)

    # ---- the write path -----------------------------------------------------

    def submit(self, sweep: Sweep) -> None:
        """Hand a sweep to the writer. Never blocks, never raises.

        On overflow, drop the oldest and count it — the drain-and-count
        discipline juice uses for SSE subscribers. Silent loss is the one
        outcome worth ruling out; a poll task stalling on a disk is the other.
        """
        if sweep.ts < CLOCK_FLOOR:
            _overflow_log.warning(
                "clock is before %s (%s) — refusing to buffer; is NTP synced?",
                CLOCK_FLOOR.date(),
                sweep.ts.isoformat(),
            )
            self._health.rows_dropped += len(sweep.outlets)
            return
        try:
            self._queue.put_nowait(sweep)
        except asyncio.QueueFull:
            try:
                dropped = self._queue.get_nowait()
                self._health.rows_dropped += len(dropped.outlets)
            except asyncio.QueueEmpty:  # pragma: no cover - raced with the writer
                pass
            _overflow_log.warning(
                "buffer queue full; dropping oldest sweeps (%d rows dropped so far)",
                self._health.rows_dropped,
            )
            try:
                self._queue.put_nowait(sweep)
            except asyncio.QueueFull:  # pragma: no cover - raced with a producer
                self._health.rows_dropped += len(sweep.outlets)
        self._health.queue_depth = self._queue.qsize()

    async def run(self) -> None:
        """The writer loop. Drains greedily so a burst becomes one transaction."""
        last_prune_day = datetime.now(UTC).strftime("%Y%m%d")
        while True:
            sweep = await self._queue.get()
            batch = [sweep]
            while True:
                try:
                    batch.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            self._health.queue_depth = self._queue.qsize()
            await self._run(self._commit, batch)

            today = datetime.now(UTC).strftime("%Y%m%d")
            if today != last_prune_day:
                last_prune_day = today
                log.info("buffer: rolled to day file %s", today)
                await self.prune()
            await self.refresh_stats()

    async def flush(self) -> None:
        """Commit everything queued. Used on clean shutdown — SIGTERM must not lose data."""
        batch: list[Sweep] = []
        while True:
            try:
                batch.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        if batch:
            await self._run(self._commit, batch)
            await self.refresh_stats()

    def _commit(self, sweeps: list[Sweep]) -> None:
        started = time.monotonic()
        by_day: dict[str, list[tuple]] = {}
        seen_ms = 0
        for sweep in sweeps:
            ts_ms = int(sweep.ts.timestamp() * 1000)
            seen_ms = max(seen_ms, ts_ms)
            day = _day_of(ts_ms)
            rows = by_day.setdefault(day, [])
            for outlet in sweep.outlets:
                rows.append(
                    (
                        ts_ms,
                        sweep.device_id,
                        outlet.child_id,
                        1 if outlet.relay_on else 0,
                        outlet.power_mw,
                        outlet.voltage_mv,
                        outlet.current_ma,
                        outlet.energy_wh,
                    )
                )
                self._pending_devices[(sweep.device_id, outlet.child_id)] = outlet.alias

        written = 0
        for day, rows in by_day.items():
            conn = self._day_conn(day)
            try:
                conn.execute("BEGIN")
                conn.executemany(
                    "INSERT INTO readings "
                    "(ts_ms, device_id, child_id, relay_on, power_mw, voltage_mv, current_ma, energy_wh) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    rows,
                )
                conn.execute("COMMIT")
            except sqlite3.DatabaseError as e:
                conn.execute("ROLLBACK")
                raise FatalError(f"buffer write failed on {day}: {e}", EXIT_INTERNAL) from e
            written += len(rows)

        self._flush_devices(seen_ms)
        self._health.rows_written += written
        self._health.batches_committed += 1
        self._health.last_write = datetime.now(UTC)
        self._health.last_commit_ms = round((time.monotonic() - started) * 1000, 2)

    def _flush_devices(self, ts_ms: int) -> None:
        if not self._pending_devices or self._meta is None:
            return
        rows = [
            (device_id, child_id, alias, ts_ms)
            for (device_id, child_id), alias in self._pending_devices.items()
        ]
        self._pending_devices.clear()
        self._meta.executemany(
            "INSERT INTO devices (device_id, child_id, alias, last_seen) VALUES (?, ?, ?, ?) "
            "ON CONFLICT (device_id, child_id) DO UPDATE SET alias = excluded.alias, "
            "last_seen = excluded.last_seen",
            rows,
        )

    # ---- gaps ---------------------------------------------------------------

    async def record_gap(self, device_id: str, reason: str, from_ts: datetime) -> None:
        await self._run(self._record_gap, device_id, reason, int(from_ts.timestamp() * 1000))

    def _record_gap(self, device_id: str, reason: str, from_ms: int) -> None:
        if self._meta is None:  # pragma: no cover - open() always runs first
            return
        self._meta.execute(
            "INSERT INTO gaps (device_id, from_ms, to_ms, reason) VALUES (?, ?, NULL, ?) "
            "ON CONFLICT (device_id, from_ms, reason) DO NOTHING",
            (device_id, from_ms, reason),
        )

    async def close_gap(self, device_id: str, reason: str, to_ts: datetime) -> None:
        await self._run(self._close_gap, device_id, reason, int(to_ts.timestamp() * 1000))

    def _close_gap(self, device_id: str, reason: str, to_ms: int) -> None:
        if self._meta is None:  # pragma: no cover - open() always runs first
            return
        self._meta.execute(
            "UPDATE gaps SET to_ms = ? WHERE device_id = ? AND reason = ? AND to_ms IS NULL",
            (to_ms, device_id, reason),
        )

    # ---- reading ------------------------------------------------------------

    def _day_files(self) -> list[str]:
        days = []
        for path in self._dir.glob("[0-9]" * 8 + ".sqlite"):
            days.append(path.stem)
        return sorted(days)

    async def read_after(self, cursor: str | None, limit: int = DEFAULT_READ_LIMIT) -> list[Row]:
        """Rows strictly after `cursor`, in cursor order, across day boundaries."""
        return await self._run(self._read_after, cursor, limit)

    def _read_after(self, cursor: str | None, limit: int) -> list[Row]:
        start_day, start_seq = parse_cursor(cursor) if cursor else ("", 0)
        out: list[Row] = []
        for day in self._day_files():
            if start_day and day < start_day:
                continue
            floor = start_seq if day == start_day else 0
            remaining = limit - len(out)
            if remaining <= 0:
                break
            cur = self._day_conn(day).execute(
                "SELECT seq, ts_ms, device_id, child_id, relay_on, power_mw, voltage_mv, "
                "current_ma, energy_wh FROM readings WHERE seq > ? ORDER BY seq LIMIT ?",
                (floor, remaining),
            )
            for r in cur:
                out.append(
                    Row(
                        seq=r[0],
                        ts_ms=r[1],
                        device_id=r[2],
                        child_id=r[3],
                        relay_on=bool(r[4]),
                        power_mw=r[5],
                        voltage_mv=r[6],
                        current_ma=r[7],
                        energy_wh=r[8],
                    )
                )
        return out

    def cursor_of(self, row: Row) -> str:
        return make_cursor(_day_of(row.ts_ms), row.seq)

    async def extent(self) -> tuple[str | None, str | None]:
        """(oldest, newest) cursors, or (None, None) when the buffer is empty."""
        return await self._run(self._extent)

    def _extent(self) -> tuple[str | None, str | None]:
        days = self._day_files()
        oldest = newest = None
        for day in days:
            row = self._day_conn(day).execute("SELECT MIN(seq), MAX(seq) FROM readings").fetchone()
            if row is None or row[0] is None:
                continue
            if oldest is None:
                oldest = make_cursor(day, row[0])
            newest = make_cursor(day, row[1])
        return oldest, newest

    async def aliases(self) -> list[dict]:
        """The alias roster, for the uplink's `devices` message."""
        return await self._run(self._aliases)

    def _aliases(self) -> list[dict]:
        if self._meta is None:  # pragma: no cover - open() always runs first
            return []
        cur = self._meta.execute(
            "SELECT device_id, child_id, alias FROM devices ORDER BY device_id, child_id"
        )
        return [{"device_id": r[0], "child_id": r[1], "alias": r[2]} for r in cur]

    # ---- cursor state -------------------------------------------------------

    async def get_state(self, key: str) -> str | None:
        return await self._run(self._get_state, key)

    def _get_state(self, key: str) -> str | None:
        if self._meta is None:  # pragma: no cover - open() always runs first
            return None
        row = self._meta.execute("SELECT v FROM cursor_state WHERE k = ?", (key,)).fetchone()
        return None if row is None else row[0]

    async def set_state(self, key: str, value: str) -> None:
        await self._run(self._set_state, key, value)

    def _set_state(self, key: str, value: str) -> None:
        if self._meta is None:  # pragma: no cover - open() always runs first
            return
        self._meta.execute(
            "INSERT INTO cursor_state (k, v) VALUES (?, ?) "
            "ON CONFLICT (k) DO UPDATE SET v = excluded.v",
            (key, value),
        )

    # ---- retention and stats ------------------------------------------------

    async def prune(self) -> list[str]:
        """Drop day files older than the retention window. Returns the days dropped."""
        return await self._run(self._prune)

    def _prune(self) -> list[str]:
        cutoff = (datetime.now(UTC).date() - timedelta(days=self._retention_days)).strftime(
            "%Y%m%d"
        )
        dropped = []
        for day in self._day_files():
            if day >= cutoff:
                continue
            conn = self._conns.pop(day, None)
            if conn is not None:
                conn.close()
            for suffix in ("", "-wal", "-shm"):
                path = self._dir / f"{day}.sqlite{suffix}"
                try:
                    path.unlink(missing_ok=True)
                except OSError as e:  # pragma: no cover - unlikely, and not fatal
                    log.warning("buffer: could not unlink %s: %s", path, e)
            dropped.append(day)
            log.info("buffer: pruned day file %s (older than %d days)", day, self._retention_days)
        if dropped and self._meta is not None:
            cutoff_ms = int(
                datetime.strptime(cutoff, "%Y%m%d").replace(tzinfo=UTC).timestamp() * 1000
            )
            self._meta.execute("DELETE FROM gaps WHERE from_ms < ?", (cutoff_ms,))
        return dropped

    async def refresh_stats(self) -> None:
        await self._run(self._refresh_stats)

    def _refresh_stats(self) -> None:
        days: list[dict] = []
        total = 0
        oldest_ms = newest_ms = None
        for day in self._day_files():
            size = 0
            for suffix in ("", "-wal", "-shm"):
                path = self._dir / f"{day}.sqlite{suffix}"
                try:
                    size += path.stat().st_size
                except OSError:  # pragma: no cover - file vanished between glob and stat
                    pass
            row = (
                self._day_conn(day)
                .execute("SELECT COUNT(*), MIN(ts_ms), MAX(ts_ms) FROM readings")
                .fetchone()
            )
            count = row[0] if row else 0
            if row and row[1] is not None:
                oldest_ms = row[1] if oldest_ms is None else min(oldest_ms, row[1])
                newest_ms = row[2] if newest_ms is None else max(newest_ms, row[2])
            days.append({"day": day, "rows": count, "bytes": size})
            total += size
        self._health.days = days
        self._health.total_bytes = total
        self._health.oldest_ts = (
            None if oldest_ms is None else datetime.fromtimestamp(oldest_ms / 1000, UTC)
        )
        self._health.newest_ts = (
            None if newest_ms is None else datetime.fromtimestamp(newest_ms / 1000, UTC)
        )

    # ---- counters -----------------------------------------------------------

    async def bump(self, name: str, delta: int = 1) -> None:
        await self._run(self._bump, name, delta)

    def _bump(self, name: str, delta: int) -> None:
        if self._meta is None:  # pragma: no cover - open() always runs first
            return
        self._meta.execute(
            "INSERT INTO counters (name, value) VALUES (?, ?) "
            "ON CONFLICT (name) DO UPDATE SET value = value + excluded.value",
            (name, delta),
        )

    async def counters(self) -> dict[str, int]:
        return await self._run(self._counters)

    def _counters(self) -> dict[str, int]:
        if self._meta is None:  # pragma: no cover - open() always runs first
            return {}
        return dict(self._meta.execute("SELECT name, value FROM counters"))


def rows_to_wire(rows: Iterable[Row]) -> list[list]:
    return [r.as_wire() for r in rows]


__all__ = [
    "Buffer",
    "Row",
    "make_cursor",
    "parse_cursor",
    "rows_to_wire",
    "CLOCK_FLOOR",
    "QUEUE_MAXSIZE",
    "DEFAULT_READ_LIMIT",
]
