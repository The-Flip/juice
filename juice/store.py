"""DuckDB storage layer for power readings."""

from __future__ import annotations

import contextlib
import logging
import os
import tempfile
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

from juice.collector import StripReading
from juice.state import Activity, Calibration, classify

log = logging.getLogger(__name__)


class DuplicateCircuitError(Exception):
    """Raised when a (panel, breaker) already identifies another circuit."""


_SCHEMA = """
CREATE SEQUENCE IF NOT EXISTS plug_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS machine_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS circuit_id_seq START 1;

CREATE TABLE IF NOT EXISTS plugs (
    plug_id    SMALLINT PRIMARY KEY,
    device_id  VARCHAR NOT NULL,
    child_id   VARCHAR NOT NULL,
    alias      VARCHAR NOT NULL,
    has_emeter BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE (device_id, child_id)
);

CREATE TABLE IF NOT EXISTS readings (
    ts        TIMESTAMP NOT NULL,
    plug_id   SMALLINT  NOT NULL,
    watts     FLOAT,
    voltage   FLOAT,
    amps      FLOAT,
    total_kwh FLOAT,
    -- The relay as the device reports it, from tap. NULL means nobody told us:
    -- true for every row the cloud recorder wrote, which could only ever infer
    -- on-ness from watts. `status_vocabulary.md` is explicit that "on" (the
    -- relay) and "drawing" (watts > 0) are different facts; this is the first
    -- column that records the first one.
    relay_on  BOOLEAN
);

CREATE TABLE IF NOT EXISTS machines (
    machine_id SMALLINT PRIMARY KEY,
    asset_id   VARCHAR NOT NULL UNIQUE,
    name       VARCHAR NOT NULL,
    locked     BOOLEAN NOT NULL DEFAULT FALSE,  -- legacy, superseded by lock_mode
    lock_mode  VARCHAR                          -- NULL=unlocked, 'on'=locked-on, 'off'=locked-off
);

CREATE TABLE IF NOT EXISTS assignments (
    plug_id        SMALLINT  NOT NULL,
    machine_id     SMALLINT  NOT NULL,
    assigned_from  TIMESTAMP NOT NULL,
    assigned_until TIMESTAMP
);

CREATE TABLE IF NOT EXISTS calibrations (
    machine_id   SMALLINT PRIMARY KEY,
    idle_max_rsd FLOAT,
    play_min_rsd FLOAT NOT NULL
);

-- Per-machine "normal" sustained power, used by overload detection. Recomputed
-- periodically from recent readings (p99 of per-minute average watts).
CREATE TABLE IF NOT EXISTS power_baselines (
    machine_id     SMALLINT PRIMARY KEY,
    baseline_watts FLOAT NOT NULL,
    computed_at    TIMESTAMP NOT NULL
);

CREATE SEQUENCE IF NOT EXISTS power_event_id_seq START 1;

CREATE TABLE IF NOT EXISTS power_events (
    event_id     BIGINT    PRIMARY KEY,
    ts           TIMESTAMP NOT NULL,
    plug_id      SMALLINT  NOT NULL,
    action       VARCHAR   NOT NULL,
    source       VARCHAR   NOT NULL,
    operation_id VARCHAR,
    actor        VARCHAR   NOT NULL,
    result       VARCHAR   NOT NULL,
    error        VARCHAR
);

CREATE TABLE IF NOT EXISTS hourly_usage (
    plug_id        SMALLINT  NOT NULL,
    hour_ts        TIMESTAMP NOT NULL,
    kwh            FLOAT     NOT NULL,
    samples        INTEGER   NOT NULL,
    peak_watts     FLOAT,
    peak_watts_p99 FLOAT,
    PRIMARY KEY (plug_id, hour_ts)
);

CREATE TABLE IF NOT EXISTS hourly_strip_peak (
    device_id      VARCHAR   NOT NULL,
    hour_ts        TIMESTAMP NOT NULL,
    peak_watts     FLOAT     NOT NULL,
    peak_watts_p99 FLOAT,
    PRIMARY KEY (device_id, hour_ts)
);

CREATE TABLE IF NOT EXISTS strips (
    device_id  VARCHAR PRIMARY KEY,
    name       VARCHAR NOT NULL,
    sort_order INTEGER
);

-- An electrical circuit = one breaker (panel + breaker number) plus a
-- friendly description and breaker amperage. Strips assign to circuits.
CREATE TABLE IF NOT EXISTS circuits (
    circuit_id  INTEGER PRIMARY KEY,
    panel       VARCHAR NOT NULL,
    breaker     VARCHAR NOT NULL,
    description VARCHAR NOT NULL DEFAULT '',
    amps        FLOAT,
    UNIQUE (panel, breaker)
);

-- Strip → circuit membership (many strips to one circuit). PK on device_id
-- enforces one circuit per strip.
CREATE TABLE IF NOT EXISTS circuit_devices (
    device_id  VARCHAR PRIMARY KEY,
    circuit_id INTEGER NOT NULL
);

-- Per-circuit, per-hour peak of the summed simultaneous draw across all the
-- circuit's strips (the breaker-trip-relevant number). p99 discards inrush.
CREATE TABLE IF NOT EXISTS hourly_circuit_peak (
    circuit_id     INTEGER   NOT NULL,
    hour_ts        TIMESTAMP NOT NULL,
    peak_watts     FLOAT     NOT NULL,
    peak_watts_p99 FLOAT,
    PRIMARY KEY (circuit_id, hour_ts)
);

-- PLAYING time and on-time (non-OFF) per machine per local-Central hour, for the
-- "when we're busy" bubble grid. on_seconds is the denominator (time the machine
-- was powered and play was measurable); play_seconds the numerator.
CREATE TABLE IF NOT EXISTS hourly_play_seconds (
    machine_id   SMALLINT  NOT NULL,
    hour_local   TIMESTAMP NOT NULL,
    play_seconds FLOAT     NOT NULL,
    on_seconds   FLOAT     NOT NULL,
    PRIMARY KEY (machine_id, hour_local)
);

-- Qingping air-quality monitors. Room/zone-scoped (no FlipFix asset tag, no
-- power control), so deliberately parallel to the power tables rather than
-- routed through plugs/machines. Identified by MAC; `name` is the label set in
-- the Qingping+ app.
CREATE TABLE IF NOT EXISTS air_sensors (
    mac        VARCHAR   PRIMARY KEY,
    name       VARCHAR   NOT NULL DEFAULT '',
    first_seen TIMESTAMP NOT NULL,
    last_seen  TIMESTAMP NOT NULL,
    online     BOOLEAN   NOT NULL DEFAULT TRUE
);

-- One snapshot of a monitor's metrics. Devices report ~every 15 min, so volume
-- is tiny and charts query this raw (no hourly rollup). PK dedupes repeated
-- polls of the same device-side timestamp.
CREATE TABLE IF NOT EXISTS air_readings (
    ts          TIMESTAMP NOT NULL,
    mac         VARCHAR   NOT NULL,
    temperature FLOAT,
    humidity    FLOAT,
    co2         FLOAT,
    pm25        FLOAT,
    pm10        FLOAT,
    tvoc        FLOAT,
    noise       FLOAT,
    battery     FLOAT,
    PRIMARY KEY (ts, mac)
);

-- One row per applied one-off data migration (name = a stable identifier).
-- Guards run-once backfills that have no structural (column) signal to key off.
-- How far each tap collector has been durably stored. This is the entire
-- deduplication mechanism: the rows of a batch and its cursor commit in one
-- transaction, so they can never disagree, and `hello` hands the stored cursor
-- back as `resume_from`. A duplicate is therefore not filtered out on arrival
-- -- it is never sent. Scoped to (tap_id, buffer_id) because a cursor only
-- orders within one buffer's sequence space (`tap/wire.py:57-62`).
CREATE TABLE IF NOT EXISTS ingest_cursors (
    tap_id     VARCHAR   NOT NULL,
    buffer_id  VARCHAR   NOT NULL,
    cursor     VARCHAR   NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (tap_id, buffer_id)
);

-- The oldest reading ingest has written that the rollups have not yet seen.
-- One row, id 1, or empty.
--
-- The hourly rollups refresh a trailing window anchored at the *older* of
-- "latest reading" and "latest rollup". Ingest routinely writes rows older than
-- both -- that is what a collector catching up after a day offline does -- and
-- such rows move neither anchor, so the window never reaches them. Nothing
-- errors; the refresh reports success and those hours stay blank for good.
-- This records how far back the next refresh has to reach.
CREATE TABLE IF NOT EXISTS ingest_backfill (
    id        INTEGER   PRIMARY KEY,
    oldest_ts TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS applied_migrations (
    name       VARCHAR   PRIMARY KEY,
    applied_at TIMESTAMP NOT NULL
);

-- Where the last prune cut raw readings. Without it "the oldest surviving
-- reading" is ambiguous: it is the start of history on a database that has
-- never been pruned, and a deletion boundary on one that has. The rebuild
-- paths need to tell those apart -- the first hour of real history must be
-- rolled up normally, while the hour a prune cut through must be left alone.
CREATE TABLE IF NOT EXISTS raw_prune_mark (
    id            INTEGER   PRIMARY KEY,
    pruned_before TIMESTAMP NOT NULL
);
"""

# Hardcoded to the museum's timezone. Day buckets on the play-hours chart
# are local-Central dates so the bar at "Saturday" lines up with a
# Saturday a human in the museum would recognise.
_LOCAL_TZ_NAME = "America/Chicago"


def _local_hour(ts: datetime, local_tz: ZoneInfo) -> datetime:
    """Truncate an aware timestamp to its naive local wall-clock hour."""
    return ts.astimezone(local_tz).replace(minute=0, second=0, microsecond=0, tzinfo=None)


# How much pre-window readings to pull so the rolling classifier is fully
# primed at the inner-window boundary. The classifier uses a 30-sample
# rolling window of non-zero readings; an hour of warmup is generous.
_PLAY_HOURS_WARMUP = timedelta(hours=1)

# Max gap between consecutive readings to attribute energy across.
# Matches juice.recorder.IDLE_RECHECK_SECONDS — a longer gap means the
# recorder was down or the plug fell offline, so the energy from the
# previous reading isn't trustworthy beyond this window.
_USAGE_DT_CAP_SECONDS = 60.0

# A "busy grid" cell is only shown once the measurable machines were collectively
# powered for at least this much that hour — so a single machine on for a couple
# of minutes can't produce an extreme play/on ratio. 10 powered machine-hours.
_BUSY_MIN_ON_SECONDS = 10 * 3600.0


def _migrate(conn: duckdb.DuckDBPyConnection) -> None:
    """Apply idempotent schema migrations to an existing DB."""
    plug_cols = {row[1] for row in conn.execute("PRAGMA table_info('plugs')").fetchall()}
    if "has_emeter" not in plug_cols:
        # DuckDB does not support NOT NULL on ADD COLUMN, so add nullable
        # with a DEFAULT — existing rows backfill to TRUE.
        conn.execute("ALTER TABLE plugs ADD COLUMN has_emeter BOOLEAN DEFAULT TRUE")
        conn.execute("UPDATE plugs SET has_emeter = TRUE WHERE has_emeter IS NULL")

    machine_cols = {row[1] for row in conn.execute("PRAGMA table_info('machines')").fetchall()}
    if "locked" not in machine_cols:
        conn.execute("ALTER TABLE machines ADD COLUMN locked BOOLEAN DEFAULT FALSE")
        conn.execute("UPDATE machines SET locked = FALSE WHERE locked IS NULL")
        machine_cols.add("locked")
    if "lock_mode" not in machine_cols:
        # Tri-state lock: NULL=unlocked, 'on'=locked-on, 'off'=locked-off.
        # Legacy locked rows meant locked-ON, so backfill them to 'on'.
        conn.execute("ALTER TABLE machines ADD COLUMN lock_mode VARCHAR")
        conn.execute(
            "UPDATE machines SET lock_mode = 'on' WHERE locked = TRUE AND lock_mode IS NULL"
        )

    strip_cols = {row[1] for row in conn.execute("PRAGMA table_info('strips')").fetchall()}
    if strip_cols and "sort_order" not in strip_cols:
        # Nullable — existing strips have no position and sort after the rest.
        conn.execute("ALTER TABLE strips ADD COLUMN sort_order INTEGER")

    hourly_cols = {row[1] for row in conn.execute("PRAGMA table_info('hourly_usage')").fetchall()}
    if hourly_cols and "peak_watts" not in hourly_cols:
        conn.execute("ALTER TABLE hourly_usage ADD COLUMN peak_watts FLOAT")
        # One-time backfill from readings (they're never pruned). Full-table
        # columnar aggregate — seconds even on months of 1Hz data.
        conn.execute(
            """
            UPDATE hourly_usage
            SET peak_watts = src.peak_watts
            FROM (
                SELECT plug_id,
                       date_trunc('hour', ts) AS hour_ts,
                       MAX(COALESCE(watts, 0)) AS peak_watts
                FROM readings
                GROUP BY plug_id, date_trunc('hour', ts)
            ) src
            WHERE hourly_usage.plug_id = src.plug_id
              AND hourly_usage.hour_ts = src.hour_ts
            """
        )

    # Robust (99th-percentile) peaks, added after the raw peak_watts block so
    # a pre-#20 DB picks up both columns in order. A pre-#20 DB's freshly
    # created hourly_strip_peak already has p99 from _SCHEMA (the PRAGMA check
    # is then a no-op); its first refresh full-backfills the empty table.
    hourly_cols = {row[1] for row in conn.execute("PRAGMA table_info('hourly_usage')").fetchall()}
    if hourly_cols and "peak_watts_p99" not in hourly_cols:
        conn.execute("ALTER TABLE hourly_usage ADD COLUMN peak_watts_p99 FLOAT")
        conn.execute(
            """
            UPDATE hourly_usage
            SET peak_watts_p99 = src.p99
            FROM (
                SELECT plug_id,
                       date_trunc('hour', ts) AS hour_ts,
                       quantile_cont(watts, 0.99) FILTER (WHERE watts > 0) AS p99
                FROM readings
                GROUP BY plug_id, date_trunc('hour', ts)
            ) src
            WHERE hourly_usage.plug_id = src.plug_id
              AND hourly_usage.hour_ts = src.hour_ts
            """
        )

    strip_cols = {
        row[1] for row in conn.execute("PRAGMA table_info('hourly_strip_peak')").fetchall()
    }
    if strip_cols and "peak_watts_p99" not in strip_cols:
        conn.execute("ALTER TABLE hourly_strip_peak ADD COLUMN peak_watts_p99 FLOAT")
        conn.execute(
            """
            UPDATE hourly_strip_peak
            SET peak_watts_p99 = src.p99
            FROM (
                SELECT device_id, hour_ts,
                       quantile_cont(ts_watts, 0.99) FILTER (WHERE ts_watts > 0) AS p99
                FROM (
                    SELECT p.device_id,
                           date_trunc('hour', r.ts) AS hour_ts,
                           r.ts,
                           SUM(COALESCE(r.watts, 0)) AS ts_watts
                    FROM readings r
                    JOIN plugs p ON p.plug_id = r.plug_id AND p.has_emeter
                    GROUP BY p.device_id, date_trunc('hour', r.ts), r.ts
                ) per_ts
                GROUP BY device_id, hour_ts
            ) src
            WHERE hourly_strip_peak.device_id = src.device_id
              AND hourly_strip_peak.hour_ts = src.hour_ts
            """
        )

    # tap reports the relay directly; every pre-tap row leaves it NULL.
    # Metadata-only for a nullable column with no default -- measured at 0.36s
    # and +256KB against the 20.5M-row production database.
    reading_cols = {row[1] for row in conn.execute("PRAGMA table_info('readings')").fetchall()}
    if "relay_on" not in reading_cols:
        conn.execute("ALTER TABLE readings ADD COLUMN relay_on BOOLEAN")

    # Drop NOT NULL on readings power columns so EP10-style outlets can record
    # ON state with NULL power fields.
    reading_info = conn.execute("PRAGMA table_info('readings')").fetchall()
    notnull_by_name = {row[1]: row[3] for row in reading_info}
    for col in ("watts", "voltage", "amps", "total_kwh"):
        if notnull_by_name.get(col):
            conn.execute(f"ALTER TABLE readings ALTER COLUMN {col} DROP NOT NULL")


@dataclass(frozen=True, slots=True)
class IngestResult:
    """What happened to one `readings` frame.

    `verdict` is `ok`, `duplicate` (already stored, ack it again) or
    `bad_batch` (provably broken; tap must skip it). `dropped_ts` counts rows
    discarded for an impossible timestamp -- an ok batch can still have them.
    """

    verdict: str
    total: int = 0
    bad: int = 0
    dropped_ts: int = 0
    stored: int = 0


# The wire row layout, mirroring `tap.wire.ROW_FIELDS` and juice's own copy in
# `juice/api/v2/tap_wire.py`. Spelled out here rather than imported because
# `juice.store` must not depend on the API layer -- the dependency runs
# server -> api.v2, never back. `tests/test_ingest_isolation.py` asserts all
# three copies agree, which is the only thing that could catch a reordering:
# protocol negotiation cannot, since both sides would still say "version 1"
# while every reading landed in the wrong column.
_WIRE_ROW_FIELDS = (
    "ts_ms",
    "device_id",
    "child_id",
    "relay_on",
    "power_mw",
    "voltage_mv",
    "current_ma",
    "energy_wh",
)
_I = {name: i + 1 for i, name in enumerate(_WIRE_ROW_FIELDS)}  # DuckDB lists are 1-based

# Rows before this are not late data, they are a broken clock. Mirrors
# `tap/buffer.py`'s own floor.
_INGEST_TS_FLOOR_MS = 1_735_689_600_000  # 2025-01-01T00:00:00Z

# The shortest raw retention that is safe to configure.
# `refresh_power_baselines` reads 30 days of raw readings to arm overload
# protection, so a shorter window would quietly disarm it rather than fail in
# any way an operator would notice.
MIN_RETENTION_DAYS = 31
# The forward slack is deliberately wider than tap's own 5 minutes: that guard
# compares against tap's clock, this one against ours, and a tap a few minutes
# fast is a misconfiguration rather than corruption. Dropping its readings would
# be permanent loss over a solvable problem.
_INGEST_TS_SLACK_MS = 3_600_000

# `relay_on` is 0/1 on the wire and never a JSON boolean (`tap/wire.py:78`), but
# accept `true`/`false` anyway: answering a cosmetic encoding difference with
# `bad_batch` would discard real readings permanently.
_RELAY_OK = (
    f"(TRY_CAST(r[{_I['relay_on']}] AS TINYINT) IN (0, 1)"
    f" OR lower(r[{_I['relay_on']}]) IN ('true', 'false'))"
)
_RELAY_VALUE = (
    f"COALESCE(TRY_CAST(r[{_I['relay_on']}] AS TINYINT) <> 0, lower(r[{_I['relay_on']}]) = 'true')"
)


def _meter(field: str) -> str:
    """A nullable milli-unit field. NULL means unmeasured, never zero."""
    return f"TRY_CAST(r[{_I[field]}] AS DOUBLE) / 1000"


def _meter_ok(field: str) -> str:
    return f"(r[{_I[field]}] IS NULL OR TRY_CAST(r[{_I[field]}] AS DOUBLE) IS NOT NULL)"


# One JSON parse into typed columns. Everything after this is a cheap scan of a
# temp table, which is why validation is nearly free: re-reading the JSON for
# the verdict cost more than the insert itself.
_STAGE_SQL = f"""
CREATE OR REPLACE TEMP TABLE _ingest_stg AS
SELECT
    len(r) = {len(_WIRE_ROW_FIELDS)}
        AND TRY_CAST(r[{_I["ts_ms"]}] AS BIGINT) IS NOT NULL
        AND r[{_I["device_id"]}] IS NOT NULL
        AND r[{_I["child_id"]}] IS NOT NULL
        AND {_RELAY_OK}
        AND {_meter_ok("power_mw")}
        AND {_meter_ok("voltage_mv")}
        AND {_meter_ok("current_ma")}
        AND {_meter_ok("energy_wh")}                        AS ok,
    TRY_CAST(r[{_I["ts_ms"]}] AS BIGINT)                    AS ts_ms,
    r[{_I["device_id"]}]                                    AS device_id,
    r[{_I["child_id"]}]                                     AS child_id,
    {_RELAY_VALUE}                                          AS relay_on,
    {_meter("power_mw")}                                    AS watts,
    {_meter("voltage_mv")}                                  AS voltage,
    {_meter("current_ma")}                                  AS amps,
    {_meter("energy_wh")}                                   AS total_kwh
FROM (SELECT unnest(rows) AS r FROM read_json(?, columns = {{'rows': 'VARCHAR[][]'}}))
"""  # noqa: S608 - interpolates only integer constants, never input

# `epoch_ms(now())` and not `epoch_ms(CAST(now() AS TIMESTAMP))`: the former
# reads the absolute instant off a TIMESTAMPTZ, the latter first flattens it to
# wall-clock time in the *session's* zone. Those differ by the UTC offset, which
# is enough to make every recent reading look hours in the future.
_IN_RANGE = f"ts_ms BETWEEN {_INGEST_TS_FLOOR_MS} AND (epoch_ms(now()) + {_INGEST_TS_SLACK_MS})"

_COUNT_SQL = f"""
SELECT count(*), count(*) FILTER (ok), count(*) FILTER (ok AND {_IN_RANGE})
FROM _ingest_stg
"""  # noqa: S608 - interpolates only integer constants, never input

# Create plugs for outlets juice has never seen, with an EMPTY alias -- never
# `ensure_plug`, which overwrites it. tap does not know aliases exist, and
# machine assignment is driven entirely by the Kasa alias, so writing one here
# would unassign every machine on the floor. `has_emeter` defaults TRUE because
# that is the safe error: `refresh_hourly_usage` filters on it, so a metered
# plug wrongly marked FALSE would vanish from every energy chart.
_NEW_PLUGS_SQL = """
INSERT INTO plugs (plug_id, device_id, child_id, alias, has_emeter)
SELECT nextval('plug_id_seq'), d, c, '', TRUE
FROM (SELECT DISTINCT device_id AS d, child_id AS c FROM _ingest_stg WHERE ok) x
WHERE NOT EXISTS (
    SELECT 1 FROM plugs p WHERE p.device_id = x.d AND p.child_id = x.c
)
"""

_INSERT_SQL = f"""
INSERT INTO readings (ts, plug_id, watts, voltage, amps, total_kwh, relay_on)
SELECT epoch_ms(s.ts_ms), p.plug_id, s.watts, s.voltage, s.amps, s.total_kwh, s.relay_on
FROM _ingest_stg s
JOIN plugs p ON p.device_id = s.device_id AND p.child_id = s.child_id
WHERE s.ok AND s.{_IN_RANGE}
"""  # noqa: S608 - interpolates only integer constants, never input

# Remember the oldest row this batch actually stored, so the next rollup pass
# reaches back far enough to see it. `LEAST` because a later batch of newer rows
# must not move the mark forward past hours still waiting to be rolled up. Only
# rows that were really written count -- a discarded 1970 timestamp must not
# drag the window back through fifty years of empty hours.
_BACKFILL_SQL = f"""
INSERT INTO ingest_backfill (id, oldest_ts)
SELECT 1, MIN(epoch_ms(ts_ms)) FROM _ingest_stg
WHERE ok AND {_IN_RANGE}
HAVING MIN(ts_ms) IS NOT NULL
ON CONFLICT (id) DO UPDATE SET
    oldest_ts = LEAST(ingest_backfill.oldest_ts, excluded.oldest_ts)
"""  # noqa: S608 - interpolates only integer constants, never input

_CURSOR_SQL = """
INSERT INTO ingest_cursors (tap_id, buffer_id, cursor, updated_at)
VALUES (?, ?, ?, ?)
ON CONFLICT (tap_id, buffer_id) DO UPDATE SET
    cursor = excluded.cursor,
    updated_at = excluded.updated_at
WHERE excluded.cursor > ingest_cursors.cursor
"""


class Store:
    def __init__(self, path: str | Path) -> None:
        self._path = str(path)
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._plug_cache: dict[tuple[str, str], tuple[int, str]] = {}  # key -> (plug_id, alias)
        self._machine_cache: dict[str, tuple[int, str]] = {}  # asset_id -> (machine_id, name)
        self._assignment_cache: dict[int, int | None] = {}  # plug_id -> current machine_id

    @staticmethod
    def _configure(conn: duckdb.DuckDBPyConnection) -> None:
        """Session settings every connection to this database needs.

        Pin the session timezone so tz-aware datetimes round-trip cleanly --
        DuckDB otherwise converts aware values to the host's local zone before
        storing into a naive TIMESTAMP column.

        This is a separate method because `DuckDBPyConnection.cursor()` does
        **not** inherit session settings: a cursor comes up in the host's local
        zone regardless of what the connection that made it was set to. The
        ingest writer thread runs on such a cursor, and the first version of it
        compared incoming timestamps against a `now()` five hours in the past,
        which made every fresh reading look like it came from the future.
        """
        conn.execute("SET TimeZone='UTC'")

    def open(self) -> Store:
        self._conn = duckdb.connect(self._path)
        self._configure(self._conn)
        self._conn.execute(_SCHEMA)
        _migrate(self._conn)
        # Seed assignment cache from existing open assignments
        rows = self._conn.execute(
            "SELECT plug_id, machine_id FROM assignments WHERE assigned_until IS NULL"
        ).fetchall()
        for plug_id, machine_id in rows:
            self._assignment_cache[plug_id] = machine_id
        return self

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @property
    def path(self) -> str:
        """The DB file path (or ':memory:'). Used e.g. to stage a snapshot on
        the same filesystem as the database."""
        return self._path

    def __enter__(self) -> Store:
        return self.open()

    def __exit__(self, *exc: object) -> None:
        self.close()

    def snapshot_to(self, dest_path: str) -> None:
        """Write a consistent point-in-time copy of the DB to dest_path.

        Uses DuckDB's transactional `COPY FROM DATABASE`, so it's safe while
        the recorder keeps writing — no need to stop the daemon or quiesce
        the WAL. The destination is a clean standalone .duckdb (no WAL
        sidecar). dest_path must NOT already exist (COPY into a populated DB
        errors); callers pass a fresh temp path. Runs inline on the shared
        connection — a brief (~0.1s) blocking copy; the asyncio loop
        serialises it against the recorder so there's no concurrent use.
        """
        name = self._conn.execute("SELECT current_database()").fetchone()[0]
        # ATTACH/COPY take literal SQL (no bind params). dest_path is caller-
        # generated, but escape defensively; quote the catalog identifier.
        dest_lit = dest_path.replace("'", "''")
        name_ident = name.replace('"', '""')
        self._conn.execute(f"ATTACH '{dest_lit}' AS _backup")
        try:
            self._conn.execute(f'COPY FROM DATABASE "{name_ident}" TO _backup')
        finally:
            self._conn.execute("DETACH _backup")

    def ensure_plug(
        self,
        device_id: str,
        child_id: str,
        alias: str,
        has_emeter: bool = True,
    ) -> int:
        """Upsert a plug, returning its plug_id. Caches for repeated calls."""
        key = (device_id, child_id)
        cached = self._plug_cache.get(key)
        if cached is not None and cached[1] == alias:
            return cached[0]
        row = self._conn.execute(
            """
            INSERT INTO plugs (plug_id, device_id, child_id, alias, has_emeter)
            VALUES (nextval('plug_id_seq'), ?, ?, ?, ?)
            ON CONFLICT (device_id, child_id) DO UPDATE SET
                alias = excluded.alias,
                has_emeter = excluded.has_emeter
            RETURNING plug_id
            """,
            [device_id, child_id, alias, has_emeter],
        ).fetchone()
        plug_id = row[0]
        self._plug_cache[key] = (plug_id, alias)
        return plug_id

    def insert_readings(self, rows: list[tuple]) -> None:
        """Batch insert reading rows: (ts, plug_id, watts, voltage, amps, total_kwh)."""
        self._conn.executemany(
            "INSERT INTO readings (ts, plug_id, watts, voltage, amps, total_kwh) VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )

    # --- tap ingest --------------------------------------------------------

    def new_connection(self) -> duckdb.DuckDBPyConnection:
        """A second connection to the same database, safe to use from another
        thread. `Store._conn` is not: the recorder, the rollups and the backup
        snapshot all share it on the event loop thread."""
        conn = self._conn.cursor()
        self._configure(conn)
        return conn

    def commit_ingest_batch(
        self,
        tap_id: str,
        buffer_id: str,
        cursor: str,
        frame_text: str,
        conn: duckdb.DuckDBPyConnection | None = None,
    ) -> IngestResult:
        """Store one `readings` frame, and its cursor, in one transaction.

        `frame_text` is the raw socket payload. The rows are never parsed into
        Python: DuckDB reads the JSON, validates every field, converts units and
        resolves `(device_id, child_id)` to a plug in one pass. That is ~2x
        faster than doing any of it here, and it means the device-controlled
        strings never touch anything but a JSON parser.

        Returns without writing when the batch is a replay -- tap resends on the
        same socket if an ack goes missing (`tap/uplink.py:61`) -- or when a row
        is malformed in a way no retry can fix.

        The single transaction around the rows and the cursor is the whole
        deduplication design: because they cannot disagree, tap always resumes
        exactly where we committed, so a duplicate is never *sent* rather than
        being filtered on arrival. That matters because `readings` has no unique
        index and cannot affordably be given one at 20M+ rows.
        """
        target = conn if conn is not None else self._conn
        if target is None:
            raise RuntimeError("store is not open")

        stored = self._ingest_cursor(target, tap_id, buffer_id)
        if stored is not None and cursor <= stored:
            return IngestResult("duplicate")

        path = self._write_frame(frame_text)
        try:
            target.execute(_STAGE_SQL, [path])
            counts = target.execute(_COUNT_SQL).fetchone()
            assert counts is not None  # a bare count query always yields one row
            total, ok, in_range = counts
            if ok != total:
                # Provably broken bytes. Nothing is stored and the cursor does
                # not move, so tap skips exactly this batch and no more.
                return IngestResult("bad_batch", total=total, bad=total - ok)

            target.execute("BEGIN TRANSACTION")
            try:
                if in_range:
                    target.execute(_NEW_PLUGS_SQL)
                    target.execute(_INSERT_SQL)
                    target.execute(_BACKFILL_SQL)
                target.execute(_CURSOR_SQL, [tap_id, buffer_id, cursor, datetime.now(UTC)])
                target.execute("COMMIT")
            except Exception:
                target.execute("ROLLBACK")
                raise
        finally:
            with contextlib.suppress(OSError):
                os.unlink(path)

        return IngestResult("ok", total=total, dropped_ts=ok - in_range, stored=in_range)

    def _write_frame(self, frame_text: str) -> str:
        """Stage the raw frame beside the database.

        Beside it, not in /tmp: in production the database is on a mounted
        volume while /tmp may be a small tmpfs. Same reasoning as
        `handle_backup`. Unlike that one we cannot unlink before reading --
        `read_json` needs the path to exist -- so the caller unlinks in a
        `finally`. A SIGKILL mid-batch leaks one file, which the fixed prefix
        makes greppable.
        """
        db_dir = os.path.dirname(self._path) or None if self._path != ":memory:" else None
        fd, path = tempfile.mkstemp(prefix="juice-ingest-", suffix=".json", dir=db_dir)
        try:
            with os.fdopen(fd, "w") as handle:
                handle.write(frame_text)
        except Exception:
            with contextlib.suppress(OSError):
                os.unlink(path)
            raise
        return path

    def _ingest_cursor(
        self, conn: duckdb.DuckDBPyConnection, tap_id: str, buffer_id: str
    ) -> str | None:
        row = conn.execute(
            "SELECT cursor FROM ingest_cursors WHERE tap_id = ? AND buffer_id = ?",
            [tap_id, buffer_id],
        ).fetchone()
        return row[0] if row else None

    def ingest_cursor(self, tap_id: str, buffer_id: str) -> str | None:
        """How far this collector has been durably stored, or None if unseen.

        None is the honest answer for an unseen `buffer_id` even when we hold a
        cursor for the same `tap_id`: a new buffer id means tap's storage was
        replaced and its sequence restarted from zero, so our cursor names a
        row that no longer exists (`tap/wire.py:57-62`).
        """
        row = self._conn.execute(
            "SELECT cursor FROM ingest_cursors WHERE tap_id = ? AND buffer_id = ?",
            [tap_id, buffer_id],
        ).fetchone()
        return row[0] if row else None

    def set_ingest_cursor(self, tap_id: str, buffer_id: str, cursor: str) -> None:
        """Advance the durable cursor. Never retreats.

        The guard is not paranoia. Two live sockets for one tap -- a reconnect
        where the server has not yet reaped the half-open one -- can deliver an
        older cursor after a newer one. Storing it would hand the next `hello` a
        stale resume point and re-deliver every row in between, which is exactly
        the duplicate this table exists to prevent. Cursors are fixed-width
        zero-padded decimal, so `>` on the string is `>` on the sequence.
        """
        self._conn.execute(
            """
            INSERT INTO ingest_cursors (tap_id, buffer_id, cursor, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (tap_id, buffer_id) DO UPDATE SET
                cursor = excluded.cursor,
                updated_at = excluded.updated_at
            WHERE excluded.cursor > ingest_cursors.cursor
            """,
            [tap_id, buffer_id, cursor, datetime.now(UTC)],
        )

    def pending_backfill_start(
        self, conn: duckdb.DuckDBPyConnection | None = None
    ) -> datetime | None:
        """The oldest ingested reading the rollups have not yet covered.

        `conn` lets the retention worker read this off its own connection,
        like the other guards `prunable_before` consults.
        """
        c = conn or self._conn
        row = c.execute("SELECT oldest_ts FROM ingest_backfill WHERE id = 1").fetchone()
        return row[0] if row else None

    def clear_pending_backfill(self, covered_from: datetime | None) -> None:
        """Retire the mark a rollup pass actually covered.

        `covered_from` is `pending_backfill_start()` as it stood when that pass
        began. The ingest writer commits on its own connection while the rollups
        run, so a batch landing mid-pass can lower `oldest_ts` below the range
        the pass walked; deleting unconditionally would drop a mark nothing has
        covered, and those hours would be skipped for good with nothing
        reporting a problem. `None` means the pass began with nothing pending,
        which entitles it to clear nothing.
        """
        if covered_from is None:
            return
        self._conn.execute(
            "DELETE FROM ingest_backfill WHERE id = 1 AND oldest_ts >= ?", [covered_from]
        )

    def rollup_lookback_hours(self, default: int) -> int:
        """How far back a rollup refresh must reach this time.

        Normally `default`. After ingest has backfilled older rows, far enough
        to cover them -- otherwise the refresh silently skips those hours and
        the charts stay blank with nothing reporting a problem.
        """
        pending = self.pending_backfill_start()
        if pending is None:
            return default
        latest = self._conn.execute("SELECT MAX(ts) FROM readings").fetchone()[0]
        if latest is None:
            return default
        # +1 hour so the window covers the whole hour the oldest row sits in
        # rather than starting partway through it.
        span = latest - pending
        hours = int(span.total_seconds() // 3600) + 1
        return max(default, hours)

    # --- retention ---------------------------------------------------------

    def rollup_high_water(self, conn: duckdb.DuckDBPyConnection | None = None) -> datetime | None:
        """The newest hour every rollup has covered, or None if any is empty.

        The *minimum* of the four maxima, because raw is only safe to delete
        once the slowest of them has read it. None means "do not prune at all":
        an empty rollup table triggers a full backfill from raw on its next
        refresh, so pruning first would make that backfill quietly produce a
        truncated history and then report success.
        """
        c = conn or self._conn
        oldest: datetime | None = None
        for table, column in (
            ("hourly_usage", "hour_ts"),
            ("hourly_strip_peak", "hour_ts"),
            ("hourly_circuit_peak", "hour_ts"),
            ("hourly_play_seconds", "hour_local"),
        ):
            row = c.execute(f"SELECT MAX({column}) FROM {table}").fetchone()  # noqa: S608
            if row is None or row[0] is None:
                return None
            oldest = row[0] if oldest is None else min(oldest, row[0])
        return oldest

    def prunable_before(
        self,
        retention_days: int,
        *,
        now: datetime | None = None,
        conn: duckdb.DuckDBPyConnection | None = None,
    ) -> datetime | None:
        """The cutoff it is currently safe to prune to, or None to not prune.

        Every branch that returns None is a case where deleting would destroy
        something unrecoverable, so the default answer is "don't".

        `conn` lets the retention worker run every guard on its own connection,
        so none of this touches `Store._conn` from off the event loop thread.
        """
        c = conn or self._conn
        if retention_days <= 0:
            return None
        if retention_days < MIN_RETENTION_DAYS:
            log.warning(
                "raw retention of %d days is below the %d-day minimum "
                "(power baselines read 30 days of raw); not pruning",
                retention_days,
                MIN_RETENTION_DAYS,
            )
            return None

        # The one-shot migration that backfills play-hours across all of
        # history. Prune first and the pruned span is simply missing from it.
        if not self.has_migration("retro_play_hours_v1", conn):
            return None

        high_water = self.rollup_high_water(conn)
        if high_water is None:
            return None

        oldest = c.execute("SELECT MIN(ts) FROM readings").fetchone()[0]
        if oldest is None:
            return None

        moment = (now or datetime.now(UTC)).replace(tzinfo=None)
        cutoff = min(moment - timedelta(days=retention_days), high_water)

        # A tap catching up after an outage writes rows *older* than the
        # high-water mark -- that is what backfill is -- so neither bound above
        # holds the cutoff back from them, and the delete would take rows no
        # rollup has read. `ingest_backfill.oldest_ts` is already the mark that
        # tells the next refresh how far to reach back (`rollup_lookback_hours`);
        # pruning stops at the same place, or the two halves disagree and the
        # rows are gone from raw and rollups both.
        #
        # Truncated to the hour, because a refresh recomputes whole hours from
        # raw: keeping the backfilled row but deleting its earlier neighbours
        # would leave the hour intact-looking and quietly short.
        pending = self.pending_backfill_start(conn)
        if pending is not None:
            cutoff = min(cutoff, pending.replace(minute=0, second=0, microsecond=0))

        return cutoff if cutoff > oldest else None

    def prune_readings(
        self, before: datetime, conn: duckdb.DuckDBPyConnection | None = None
    ) -> int:
        """Delete raw readings older than `before`. Returns rows removed.

        Call `prunable_before` for the cutoff rather than computing one: the
        guards it applies are the whole safety story.

        DuckDB does not shrink the file on delete. The CHECKPOINT afterwards
        lets the freed blocks be reused, so the database stops *growing* even
        though it does not get smaller.
        """
        c = conn or self._conn
        # `prunable_before` already floors the cutoff at the pending backfill
        # mark, but it ran earlier and on another connection: ingest commits
        # while retention is deciding, so a tap catching up can lower the mark
        # below `before` in between. Re-read it here and refuse rather than
        # delete rows the next rollup refresh is on its way to read. Refusing
        # costs one skipped pass -- the next one recomputes a cutoff that
        # respects the new mark.
        pending = self.pending_backfill_start(conn)
        if pending is not None and pending < before:
            log.info(
                "not pruning to %s: ingest backfilled to %s and the rollups have "
                "not covered it yet",
                before,
                pending,
            )
            return 0
        deleted = c.execute("SELECT count(*) FROM readings WHERE ts < ?", [before]).fetchone()[0]
        if not deleted:
            return 0
        # One transaction, because a delete that lands without its mark is
        # worse than either alone: the raw is gone and nothing records that it
        # went, so `_unrecomputable_before()` reports a cutoff older than what
        # actually survives and the next rebuild deletes rollup history it
        # cannot regenerate -- reporting success as it does. DuckDB
        # auto-commits each statement, so this has to be asked for.
        c.execute("BEGIN TRANSACTION")
        try:
            c.execute("DELETE FROM readings WHERE ts < ?", [before])
            # The mark is what later tells a rebuild that the hour containing
            # `before` is a cut rather than the start of history.
            c.execute(
                "INSERT INTO raw_prune_mark (id, pruned_before) VALUES (1, ?) "
                "ON CONFLICT (id) DO UPDATE SET pruned_before = excluded.pruned_before",
                [before],
            )
            # Inside the try: a COMMIT that raises must roll back like any
            # other failure, not leave the transaction open behind it.
            c.execute("COMMIT")
        except Exception:
            # Suppressed, for two reasons. The exception worth reporting is the
            # one that broke the prune, not whatever the cleanup then hit --
            # `retention_loop` logs it and moves on, so it is the only record
            # anyone gets. And the retention worker holds this connection for
            # the life of the process: a transaction left open on it freezes
            # every later read on a stale snapshot and makes every later BEGIN
            # raise "cannot start a transaction within a transaction", with
            # nothing but a six-hourly "retention pass failed" to show for it.
            with contextlib.suppress(Exception):
                c.execute("ROLLBACK")
            raise
        # Outside the transaction, and after it: the delete and the mark have
        # already committed, so a CHECKPOINT that cannot run --
        # another connection holding a write transaction is the usual reason --
        # is a missed opportunity to reuse blocks, not a failed prune. Reporting
        # it as one would roll the caller back to "nothing was deleted", which
        # is the one thing that is definitely untrue.
        try:
            c.execute("CHECKPOINT")
        except Exception:
            log.warning("prune: CHECKPOINT failed; freed blocks stay unreclaimed", exc_info=True)
        log.info("pruned %d raw readings older than %s", deleted, before)
        return deleted

    def _unrecomputable_before(self) -> datetime | None:
        """The instant before which no rollup can be regenerated from raw.

        Rebuild paths truncate a rollup and recompute it. Once raw has been
        pruned, the hours before the cut cannot be recomputed -- so a rebuild
        must leave them alone rather than delete them.

        The recorded prune cutoff, not `MIN(ts)`: on a database that has never
        been pruned those hours are not unrecomputable at all, they are simply
        the start of history, and every one of them must still be rolled up.
        Returns None in that case, where the distinction does not arise.
        """
        row = self._conn.execute("SELECT pruned_before FROM raw_prune_mark WHERE id = 1").fetchone()
        return row[0] if row else None

    def _first_recomputable_hour(self) -> datetime | None:
        """The first UTC hour a refresh or rebuild may recompute from raw.

        A prune cuts raw at an instant, not on an hour boundary, so the bucket
        *containing* that instant still holds a value derived from the whole
        hour while raw keeps only its tail. Recomputing it would quietly
        replace a correct number with a smaller one -- and unlike a deleted
        bucket, nothing about the result looks wrong. So the first hour that
        may be touched is the first one starting at or after the cut.

        None when nothing has been pruned: there is no partial bucket to
        protect, and clamping to the start of history would exclude the first
        hour of it from every refresh -- including the backfilled hours ingest
        exists to deliver.
        """
        floor = self._unrecomputable_before()
        if floor is None:
            return None
        hour = floor.replace(minute=0, second=0, microsecond=0)
        return hour if hour == floor else hour + timedelta(hours=1)

    # --- Air-quality monitors (Qingping) -----------------------------------
    # Parallel to the power path: upsert a sensor, idempotently append readings,
    # query latest + history. No rollups — at ~15-min cadence the raw table is
    # small enough to chart directly.

    _AIR_METRIC_COLS = (
        "temperature",
        "humidity",
        "co2",
        "pm25",
        "pm10",
        "tvoc",
        "noise",
        "battery",
    )

    def ensure_air_sensor(self, mac: str, name: str, online: bool, seen_ts: datetime) -> None:
        """Upsert an air monitor. first_seen is set once; name/online/last_seen track."""
        self._conn.execute(
            """
            INSERT INTO air_sensors (mac, name, first_seen, last_seen, online)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (mac) DO UPDATE SET
                name = excluded.name,
                last_seen = excluded.last_seen,
                online = excluded.online
            """,
            [mac, name, seen_ts, seen_ts, online],
        )

    def insert_air_readings(self, rows: list[tuple]) -> None:
        """Batch insert air readings, deduped on (ts, mac).

        Each row is (ts, mac, temperature, humidity, co2, pm25, pm10, tvoc,
        noise, battery). Polling faster than the device's report interval
        re-sees the same device-side timestamp; ON CONFLICT DO NOTHING drops
        those repeats instead of erroring.
        """
        self._conn.executemany(
            """
            INSERT INTO air_readings
                (ts, mac, temperature, humidity, co2, pm25, pm10, tvoc, noise, battery)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING
            """,
            rows,
        )

    def air_last_ts(self, mac: str) -> datetime | None:
        """Timestamp of the most recent stored reading for a monitor, or None.

        Used to gap-fill: backfill history from just after this point so a
        restart or device outage doesn't leave a hole.
        """
        row = self._conn.execute("SELECT max(ts) FROM air_readings WHERE mac = ?", [mac]).fetchone()
        return row[0] if row and row[0] is not None else None

    def list_air_sensors(self) -> list[dict]:
        """All known monitors: mac, name, online, first_seen, last_seen."""
        rows = self._conn.execute(
            "SELECT mac, name, online, first_seen, last_seen FROM air_sensors ORDER BY name, mac"
        ).fetchall()
        return [
            {
                "mac": mac,
                "name": name,
                "online": bool(online),
                "first_seen": first_seen,
                "last_seen": last_seen,
            }
            for mac, name, online, first_seen, last_seen in rows
        ]

    def air_latest(self) -> dict[str, dict]:
        """Latest reading per monitor, keyed by mac (metric fields + ts)."""
        cols = ", ".join(self._AIR_METRIC_COLS)
        rows = self._conn.execute(
            f"""
            SELECT mac, ts, {cols}
            FROM air_readings
            QUALIFY row_number() OVER (PARTITION BY mac ORDER BY ts DESC) = 1
            """  # noqa: S608 — cols is a fixed internal constant, not user input
        ).fetchall()
        out: dict[str, dict] = {}
        for row in rows:
            mac, ts = row[0], row[1]
            metrics = dict(zip(self._AIR_METRIC_COLS, row[2:], strict=True))
            out[mac] = {"ts": ts, **metrics}
        return out

    def air_history(self, mac: str, start: datetime, end: datetime) -> list[dict]:
        """Readings for one monitor in the half-open window [start, end), by ts."""
        cols = ", ".join(self._AIR_METRIC_COLS)
        rows = self._conn.execute(
            f"""
            SELECT ts, {cols}
            FROM air_readings
            WHERE mac = ? AND ts >= ? AND ts < ?
            ORDER BY ts
            """,  # noqa: S608 — cols is a fixed internal constant, not user input
            [mac, start, end],
        ).fetchall()
        return [
            {"ts": row[0], **dict(zip(self._AIR_METRIC_COLS, row[1:], strict=True))} for row in rows
        ]

    def ensure_machine(self, asset_id: str, name: str) -> int:
        """Upsert a machine, returning its machine_id. Caches for repeated calls."""
        cached = self._machine_cache.get(asset_id)
        if cached is not None and cached[1] == name:
            return cached[0]
        row = self._conn.execute(
            """
            INSERT INTO machines (machine_id, asset_id, name)
            VALUES (nextval('machine_id_seq'), ?, ?)
            ON CONFLICT (asset_id) DO UPDATE SET name = excluded.name
            RETURNING machine_id
            """,
            [asset_id, name],
        ).fetchone()
        machine_id = row[0]
        self._machine_cache[asset_id] = (machine_id, name)
        return machine_id

    def get_machine_id(self, asset_id: str) -> int | None:
        """Return an asset tag's machine_id, or None if unknown. Read-only.

        For read paths (e.g. the detail-page cost endpoint) that must not write —
        unlike ensure_machine, which upserts. Uses the same cache when warm.
        """
        cached = self._machine_cache.get(asset_id)
        if cached is not None:
            return cached[0]
        row = self._conn.execute(
            "SELECT machine_id FROM machines WHERE asset_id = ?", [asset_id]
        ).fetchone()
        return row[0] if row else None

    def list_plugs(self) -> list[tuple[int, str, str, str, bool]]:
        """All known plugs: (plug_id, device_id, child_id, alias, has_emeter).

        Includes unassigned outlets, so the strip outlet map stays complete
        even for devices that are offline at startup.
        """
        rows = self._conn.execute(
            "SELECT plug_id, device_id, child_id, alias, has_emeter FROM plugs ORDER BY plug_id"
        ).fetchall()
        return [(int(pid), did, cid, alias, bool(em)) for pid, did, cid, alias, em in rows]

    def set_strip_name(self, device_id: str, name: str) -> None:
        """Set a human-friendly strip name; empty/whitespace clears the override.

        The strips row also carries sort_order, so clearing the name keeps the
        row (with name='') when a position is set; only a row with neither a
        name nor an order is removed.
        """
        name = name.strip()
        self._conn.execute(
            """
            INSERT INTO strips (device_id, name) VALUES (?, ?)
            ON CONFLICT (device_id) DO UPDATE SET name = excluded.name
            """,
            [device_id, name],
        )
        self._conn.execute(
            "DELETE FROM strips WHERE device_id = ? AND name = '' AND sort_order IS NULL",
            [device_id],
        )

    def get_strip_names(self) -> dict[str, str]:
        """All operator-set strip names, keyed by device_id."""
        rows = self._conn.execute("SELECT device_id, name FROM strips WHERE name <> ''").fetchall()
        return {row[0]: row[1] for row in rows}

    def set_strip_orders(self, device_ids: Sequence[str]) -> None:
        """Replace the dashboard order of strips: each gets sort_order = index.

        The dashboard sends the whole new order on a drag, so this is a full
        replace — any strip not in the list loses its position (no stale rows)
        and falls back to by-name order. Positions are gap-free. Rows are
        created with an empty name when the strip had none; rows left with
        neither a name nor an order are removed.
        """
        self._conn.execute("UPDATE strips SET sort_order = NULL WHERE sort_order IS NOT NULL")
        for i, device_id in enumerate(device_ids):
            self._conn.execute(
                """
                INSERT INTO strips (device_id, name, sort_order) VALUES (?, '', ?)
                ON CONFLICT (device_id) DO UPDATE SET sort_order = excluded.sort_order
                """,
                [device_id, i],
            )
        self._conn.execute("DELETE FROM strips WHERE name = '' AND sort_order IS NULL")

    def get_strip_orders(self) -> dict[str, int]:
        """Operator-set strip positions, keyed by device_id (NULL omitted)."""
        rows = self._conn.execute(
            "SELECT device_id, sort_order FROM strips WHERE sort_order IS NOT NULL"
        ).fetchall()
        return {row[0]: int(row[1]) for row in rows}

    # --- Circuits -------------------------------------------------------

    def create_circuit(
        self, panel: str, breaker: str, description: str = "", amps: float | None = None
    ) -> int:
        """Create a circuit (one breaker), returning its circuit_id.

        Raises DuplicateCircuitError if (panel, breaker) already exists — a physical
        breaker maps to exactly one circuit.
        """
        try:
            row = self._conn.execute(
                """
                INSERT INTO circuits (circuit_id, panel, breaker, description, amps)
                VALUES (nextval('circuit_id_seq'), ?, ?, ?, ?)
                RETURNING circuit_id
                """,
                [panel, breaker, description, amps],
            ).fetchone()
        except duckdb.ConstraintException as e:
            raise DuplicateCircuitError(f"{panel} {breaker} already exists") from e
        return int(row[0])

    def update_circuit(
        self, circuit_id: int, panel: str, breaker: str, description: str, amps: float | None
    ) -> None:
        """Overwrite a circuit's fields.

        Raises DuplicateCircuitError if (panel, breaker) collides with another row.
        """
        try:
            self._conn.execute(
                "UPDATE circuits SET panel = ?, breaker = ?, description = ?, amps = ? "
                "WHERE circuit_id = ?",
                [panel, breaker, description, amps, circuit_id],
            )
        except duckdb.ConstraintException as e:
            raise DuplicateCircuitError(f"{panel} {breaker} already exists") from e

    def delete_circuit(self, circuit_id: int) -> None:
        """Delete a circuit and its strip memberships + rollup rows."""
        self._conn.execute("DELETE FROM circuit_devices WHERE circuit_id = ?", [circuit_id])
        self._conn.execute("DELETE FROM hourly_circuit_peak WHERE circuit_id = ?", [circuit_id])
        self._conn.execute("DELETE FROM circuits WHERE circuit_id = ?", [circuit_id])

    @staticmethod
    def _circuit_row(row: tuple) -> dict:
        return {
            "circuit_id": int(row[0]),
            "panel": row[1],
            "breaker": row[2],
            "description": row[3],
            "amps": float(row[4]) if row[4] is not None else None,
        }

    def get_circuit(self, circuit_id: int) -> dict | None:
        """One circuit's fields, or None if unknown."""
        row = self._conn.execute(
            "SELECT circuit_id, panel, breaker, description, amps FROM circuits "
            "WHERE circuit_id = ?",
            [circuit_id],
        ).fetchone()
        return self._circuit_row(row) if row is not None else None

    def list_circuits(self) -> list[dict]:
        """All circuits, sorted by panel then breaker."""
        rows = self._conn.execute(
            "SELECT circuit_id, panel, breaker, description, amps FROM circuits "
            "ORDER BY panel, breaker"
        ).fetchall()
        return [self._circuit_row(r) for r in rows]

    def set_device_circuit(self, device_id: str, circuit_id: int | None) -> None:
        """Assign a strip to a circuit, or clear it (circuit_id=None).

        Raises ValueError for an unknown circuit, so membership can't point at
        a non-existent circuit. Callers that change membership should then run
        rebuild_hourly_circuit_peak() to recompute history (the API handlers
        do — kept there rather than here to avoid a full rebuild per call in
        bulk/test setup).
        """
        if circuit_id is None:
            self._conn.execute("DELETE FROM circuit_devices WHERE device_id = ?", [device_id])
            return
        if self.get_circuit(circuit_id) is None:
            raise ValueError(f"Unknown circuit: {circuit_id}")
        self._conn.execute(
            """
            INSERT INTO circuit_devices (device_id, circuit_id) VALUES (?, ?)
            ON CONFLICT (device_id) DO UPDATE SET circuit_id = excluded.circuit_id
            """,
            [device_id, circuit_id],
        )

    def get_circuit_devices(self) -> dict[str, int]:
        """Strip → circuit membership, keyed by device_id."""
        rows = self._conn.execute("SELECT device_id, circuit_id FROM circuit_devices").fetchall()
        return {row[0]: int(row[1]) for row in rows}

    def set_machine_lock_mode(self, machine_id: int, mode: str | None) -> None:
        """Set the lock mode: 'on' (locked-on), 'off' (locked-off), or None (unlocked)."""
        assert mode in (None, "on", "off")
        self._conn.execute(
            "UPDATE machines SET lock_mode = ? WHERE machine_id = ?",
            [mode, machine_id],
        )

    def get_lock_modes(self) -> dict[str, str]:
        """asset_id -> 'on'|'off' for every locked machine (unlocked machines omitted)."""
        rows = self._conn.execute(
            "SELECT asset_id, lock_mode FROM machines WHERE lock_mode IS NOT NULL"
        ).fetchall()
        return {row[0]: row[1] for row in rows}

    def update_assignment(self, plug_id: int, machine_id: int | None, ts: datetime) -> None:
        """Update plug-to-machine assignment. Closes old if changed, opens new if not None."""
        current = self._assignment_cache.get(plug_id)
        if current == machine_id:
            return
        # Close any open assignment for this plug
        if current is not None:
            self._conn.execute(
                "UPDATE assignments SET assigned_until = ? WHERE plug_id = ? AND assigned_until IS NULL",
                [ts, plug_id],
            )
        # Open new assignment
        if machine_id is not None:
            self._conn.execute(
                "INSERT INTO assignments (plug_id, machine_id, assigned_from) VALUES (?, ?, ?)",
                [plug_id, machine_id, ts],
            )
        self._assignment_cache[plug_id] = machine_id

    def get_calibration(self, machine_id: int) -> Calibration | None:
        """Return calibration for a machine, or None if not set."""
        row = self._conn.execute(
            "SELECT idle_max_rsd, play_min_rsd FROM calibrations WHERE machine_id = ?",
            [machine_id],
        ).fetchone()
        if row is None:
            return None
        return Calibration(idle_max_rsd=row[0], play_min_rsd=row[1])

    def set_calibration(self, machine_id: int, calibration: Calibration) -> None:
        """Upsert calibration for a machine."""
        self._conn.execute(
            """
            INSERT INTO calibrations (machine_id, idle_max_rsd, play_min_rsd)
            VALUES (?, ?, ?)
            ON CONFLICT (machine_id) DO UPDATE SET
                idle_max_rsd = excluded.idle_max_rsd,
                play_min_rsd = excluded.play_min_rsd
            """,
            [machine_id, calibration.idle_max_rsd, calibration.play_min_rsd],
        )

    def seed_calibrations(self, calibrations: dict[str, Calibration]) -> None:
        """Seed calibrations for machines that exist in the DB, keyed by machine name."""
        for name, cal in calibrations.items():
            row = self._conn.execute(
                "SELECT machine_id FROM machines WHERE name = ?", [name]
            ).fetchone()
            if row:
                self.set_calibration(row[0], cal)

    def refresh_power_baselines(
        self,
        days: int = 30,
        min_minutes: int = 500,
        now: datetime | None = None,
    ) -> dict[int, float]:
        """Recompute per-machine power baselines from recent readings.

        Baseline = the `BASELINE_QUANTILE` of per-minute *average* watts over the
        trailing `days`, attributing each reading to the machine assigned to its
        plug at that time. Minute-averaging strips transient solenoid spikes; the
        high quantile absorbs brief past incidents. Machines with fewer than
        `min_minutes` of "on" history are left out (not armed). Upserts the result
        and returns the machine_id -> baseline_watts map.
        """
        from juice.overload import BASELINE_QUANTILE

        upper = now if now is not None else datetime.now(UTC)
        lower = upper - timedelta(days=days)
        rows = self._conn.execute(
            """
            WITH minute_avg AS (
                SELECT a.machine_id,
                       date_trunc('minute', r.ts) AS minute,
                       avg(r.watts) AS avg_w
                FROM readings r
                JOIN assignments a
                  ON a.plug_id = r.plug_id
                 AND r.ts >= a.assigned_from
                 AND (a.assigned_until IS NULL OR r.ts < a.assigned_until)
                -- watts > 5 is an overload-baseline arming floor (ignore near-off
                -- minutes), NOT the OFF_WATTS no-draw threshold (state.py) — keep
                -- the two distinct.
                WHERE r.ts >= ? AND r.ts < ? AND r.watts > 5
                GROUP BY 1, 2
            )
            SELECT machine_id,
                   quantile_cont(avg_w, ?) AS baseline,
                   count(*) AS mins
            FROM minute_avg
            GROUP BY 1
            HAVING count(*) >= ?
            """,
            [lower, upper, BASELINE_QUANTILE, min_minutes],
        ).fetchall()
        result = {int(mid): float(b) for mid, b, _mins in rows}
        for machine_id, baseline in result.items():
            self._conn.execute(
                """
                INSERT INTO power_baselines (machine_id, baseline_watts, computed_at)
                VALUES (?, ?, ?)
                ON CONFLICT (machine_id) DO UPDATE SET
                    baseline_watts = excluded.baseline_watts,
                    computed_at = excluded.computed_at
                """,
                [machine_id, baseline, upper],
            )
        # Drop machines that no longer qualify (e.g. now idle for the whole
        # window), so get_power_baselines() stops arming them.
        if result:
            placeholders = ",".join("?" for _ in result)
            self._conn.execute(
                f"DELETE FROM power_baselines WHERE machine_id NOT IN ({placeholders})",  # noqa: S608
                list(result.keys()),
            )
        else:
            self._conn.execute("DELETE FROM power_baselines")
        return result

    def get_power_baselines(self) -> dict[str, float]:
        """asset_id -> baseline watts for every machine with a computed baseline.

        Keyed by asset_id so a baseline follows the machine across outlet moves,
        like lock state.
        """
        rows = self._conn.execute(
            """
            SELECT m.asset_id, b.baseline_watts
            FROM power_baselines b
            JOIN machines m ON m.machine_id = b.machine_id
            """
        ).fetchall()
        return {row[0]: float(row[1]) for row in rows}

    def get_recent_watts(self, plug_id: int, seconds: int = 3600) -> list[float | None]:
        """Fetch the last N seconds of watt readings for a plug.

        `None` where the meter did not report -- a meterless plug, or an outlet
        whose read failed inside an otherwise good sweep. Not coalesced to zero:
        the callers classify these, and a zero would read as "not drawing".
        """
        rows = self._conn.execute(
            """
            SELECT watts FROM readings
            WHERE plug_id = ? AND ts >= (now() - INTERVAL (?) SECOND)
            ORDER BY ts
            """,
            [plug_id, seconds],
        ).fetchall()
        return [r[0] for r in rows]

    def get_readings_since(self, plug_id: int, since: datetime) -> list[tuple[str, float | None]]:
        """Fetch (iso_timestamp, watts) pairs for a plug since a given time.

        Watts is `None` where the meter did not report; see `get_recent_watts`.
        """
        rows = self._conn.execute(
            "SELECT ts, watts FROM readings WHERE plug_id = ? AND ts >= ? ORDER BY ts",
            [plug_id, since],
        ).fetchall()
        return [(ts.isoformat() + "Z", watts) for ts, watts in rows]

    def list_unassigned_outlets(
        self, recent_seconds: int = 24 * 3600
    ) -> list[tuple[int, str, str, bool | None]]:
        """List plugs that recently drew power but aren't assigned to a machine.

        These are non-machine devices (signs, snack machines, lights) on either
        emeter or no-emeter outlets. A plug qualifies if it has no open machine
        assignment and drew power within the last `recent_seconds` — i.e. a
        reading with watts > 0 (emeter on) or watts IS NULL (no-emeter on).

        Each row: (plug_id, device_id, alias, is_drawing_latest). This last field
        is **strictly draw** — True iff the most recent reading measured watts > 0,
        False if watts = 0, and None when draw is unknown (a no-emeter plug, which
        has no watt measurement, or a plug with no readings). It is deliberately
        NOT an on/relay signal: relay state isn't persisted, so on-ness must come
        from a live reading, not history.
        """
        rows = self._conn.execute(
            """
            WITH recent_power AS (
                SELECT DISTINCT plug_id
                FROM readings
                WHERE ts >= (now() - INTERVAL (?) SECOND)
                  AND (watts IS NULL OR watts > 0)
            ), latest AS (
                SELECT plug_id, MAX(ts) AS max_ts
                FROM readings
                GROUP BY plug_id
            )
            SELECT
                p.plug_id,
                p.device_id,
                p.alias,
                CASE
                    WHEN r.watts IS NOT NULL THEN r.watts > 0
                    ELSE NULL
                END AS is_drawing_latest
            FROM plugs p
            LEFT JOIN latest l ON l.plug_id = p.plug_id
            LEFT JOIN readings r ON r.plug_id = l.plug_id AND r.ts = l.max_ts
            WHERE EXISTS (
                SELECT 1 FROM recent_power rp WHERE rp.plug_id = p.plug_id
              )
              AND NOT EXISTS (
                SELECT 1 FROM assignments a
                WHERE a.plug_id = p.plug_id AND a.assigned_until IS NULL
              )
            ORDER BY p.plug_id
            """,
            [recent_seconds],
        ).fetchall()
        return [(int(pid), did, alias, drawing) for pid, did, alias, drawing in rows]

    def list_open_assignments(self) -> list[tuple[int, str, str, str, bool, str, str]]:
        """List currently-assigned plugs joined with their machine.

        Each row: (plug_id, device_id, child_id, alias, has_emeter, asset_id,
        machine_name) for assignments with assigned_until IS NULL. Used to
        hydrate in-memory recorder state on startup so a machine whose plug is
        offline (and therefore skipped by metadata refresh) still appears.
        """
        rows = self._conn.execute(
            """
            SELECT p.plug_id, p.device_id, p.child_id, p.alias, p.has_emeter,
                   m.asset_id, m.name
            FROM assignments a
            JOIN plugs p ON p.plug_id = a.plug_id
            JOIN machines m ON m.machine_id = a.machine_id
            WHERE a.assigned_until IS NULL
            ORDER BY p.plug_id
            """
        ).fetchall()
        return [
            (int(pid), did, cid, alias, bool(em), asset, name)
            for pid, did, cid, alias, em, asset, name in rows
        ]

    def record_power_event(
        self,
        ts: datetime,
        plug_id: int,
        action: str,
        source: str,
        actor: str,
        result: str,
        operation_id: str | None = None,
        error: str | None = None,
    ) -> int:
        """Insert a power audit-log row and return its event_id."""
        row = self._conn.execute(
            """
            INSERT INTO power_events
                (event_id, ts, plug_id, action, source, operation_id, actor, result, error)
            VALUES (nextval('power_event_id_seq'), ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING event_id
            """,
            [ts, plug_id, action, source, operation_id, actor, result, error],
        ).fetchone()
        return int(row[0])

    def recent_power_events(
        self, limit: int = 50, before: int | None = None, plug_id: int | None = None
    ) -> list[dict]:
        """Return recent power events (newest first), joined with machine + plug alias.

        `before`: if given, only events with event_id strictly less than this are returned —
        used for cursor-style pagination back through history.
        `plug_id`: if given, restrict to events for that outlet (the per-machine detail view).
        """
        conditions: list[str] = []
        params: list[object] = []
        if before is not None:
            conditions.append("pe.event_id < ?")
            params.append(before)
        if plug_id is not None:
            conditions.append("pe.plug_id = ?")
            params.append(plug_id)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)
        rows = self._conn.execute(
            f"""
            SELECT pe.event_id, pe.ts, pe.plug_id, pe.action, pe.source,
                   pe.operation_id, pe.actor, pe.result, pe.error,
                   p.alias AS plug_alias,
                   m.name  AS machine_name
            FROM power_events pe
            LEFT JOIN plugs p ON p.plug_id = pe.plug_id
            LEFT JOIN assignments a
                   ON a.plug_id = pe.plug_id
                  AND a.assigned_from <= pe.ts
                  AND (a.assigned_until IS NULL OR a.assigned_until > pe.ts)
            LEFT JOIN machines m ON m.machine_id = a.machine_id
            {where}
            ORDER BY pe.event_id DESC
            LIMIT ?
            """,  # noqa: S608 — `where` is built from fixed clauses; values are bound "?" markers
            params,
        ).fetchall()
        return [
            {
                "event_id": int(r[0]),
                "ts": r[1],
                "plug_id": int(r[2]),
                "action": r[3],
                "source": r[4],
                "operation_id": r[5],
                "actor": r[6],
                "result": r[7],
                "error": r[8],
                "plug_alias": r[9],
                "machine_name": r[10],
            }
            for r in rows
        ]

    def refresh_hourly_usage(self, *, lookback_hours: int = 2) -> int:
        """Idempotently upsert recent (plug_id, hour) buckets in hourly_usage.

        Per-hour kWh = SUM(watts × min(dt, 60s)) ÷ 3600 ÷ 1000, where dt is the
        gap since the previous reading for the same plug. The 60s cap maps to
        the recorder's OFF-rate-limit; a longer gap means the recorder was
        down, so the prior reading's watts aren't trustworthy beyond that.

        Also records peak_watts = MAX(watts) per hour and
        peak_watts_p99 = 99th-percentile of the hour's powered-on (watts > 0)
        readings — a robust peak that discards single-reading power-on inrush
        spikes (NULL when the hour was all-off). The prev_dt filter means each
        plug's first-ever reading is excluded from its hour's peak — one
        reading per plug lifetime, negligible.

        Each call recomputes hours back to `max(latest reading - lookback,
        latest rollup - lookback)` — so on first call against a fresh table
        the whole history backfills, and the most recent (still-filling) hour
        gets refreshed on every subsequent call.

        Skips no-emeter plugs. Returns the count of upserted rows.
        """
        latest_reading = self._conn.execute("SELECT MAX(ts) FROM readings").fetchone()[0]
        if latest_reading is None:
            return 0
        latest_rollup = self._conn.execute("SELECT MAX(hour_ts) FROM hourly_usage").fetchone()[0]
        # Window starts at lookback_hours before the older of "latest reading"
        # and "latest rollup". If the table is fresh, latest_rollup is None and
        # we go back to the earliest reading — full backfill.
        if latest_rollup is None:
            window_start = self._conn.execute("SELECT MIN(ts) FROM readings").fetchone()[0]
        else:
            anchor = min(latest_reading, latest_rollup)
            window_start = anchor - timedelta(hours=lookback_hours)
            # Never let the window reach the partially-pruned boundary bucket:
            # its raw is only half present, so recomputing it would overwrite a
            # complete stored value with one derived from the surviving tail.
            # Only on this branch -- an empty table is a full backfill, where
            # there is no stored value to protect.
            first_full = self._first_recomputable_hour()
            if first_full is not None and window_start < first_full:
                window_start = first_full

        # Include the most recent reading strictly BEFORE the window as a
        # one-row-per-plug "anchor" so LAG has a predecessor for the boundary
        # row inside the window. Without this, the boundary row's prev_dt
        # is NULL, its energy contribution to the boundary hour is dropped,
        # and the destructive upsert overwrites the prior correct value.
        # We can't just widen the window by dt_cap — when polling is sparse
        # (or there's been a recorder gap > dt_cap), the actual predecessor
        # may be much further back.
        self._conn.execute(
            """
            INSERT INTO hourly_usage (plug_id, hour_ts, kwh, samples, peak_watts, peak_watts_p99)
            WITH eligible AS (
                SELECT plug_id FROM plugs WHERE has_emeter = TRUE
            ),
            in_window AS (
                SELECT ts, plug_id, COALESCE(watts, 0) AS watts
                FROM readings
                WHERE plug_id IN (SELECT plug_id FROM eligible)
                  AND ts >= ?
            ),
            pre_window AS (
                SELECT ts, plug_id, watts FROM (
                    SELECT ts, plug_id, COALESCE(watts, 0) AS watts,
                           ROW_NUMBER() OVER (
                               PARTITION BY plug_id ORDER BY ts DESC
                           ) AS rn
                    FROM readings
                    WHERE plug_id IN (SELECT plug_id FROM eligible)
                      AND ts < ?
                ) ranked WHERE rn = 1
            ),
            relevant AS (
                SELECT ts, plug_id, watts FROM in_window
                UNION ALL
                SELECT ts, plug_id, watts FROM pre_window
            ),
            with_lag AS (
                SELECT ts, plug_id, watts,
                       date_trunc('hour', ts) AS hour_ts,
                       EXTRACT(EPOCH FROM ts - LAG(ts) OVER (
                           PARTITION BY plug_id ORDER BY ts
                       )) AS prev_dt
                FROM relevant
            )
            SELECT plug_id, hour_ts,
                   SUM(watts * LEAST(prev_dt, ?)) / 3600.0 / 1000.0 AS kwh,
                   COUNT(*) AS samples,
                   MAX(watts) AS peak_watts,
                   quantile_cont(watts, 0.99) FILTER (WHERE watts > 0) AS peak_watts_p99
            FROM with_lag
            WHERE ts >= ?
              AND prev_dt IS NOT NULL
            GROUP BY plug_id, hour_ts
            ON CONFLICT (plug_id, hour_ts) DO UPDATE SET
                kwh = excluded.kwh,
                samples = excluded.samples,
                peak_watts = excluded.peak_watts,
                peak_watts_p99 = excluded.peak_watts_p99
            """,
            [window_start, window_start, _USAGE_DT_CAP_SECONDS, window_start],
        )
        # DuckDB's execute() doesn't reliably return a rowcount for INSERT
        # ... ON CONFLICT; just report the size of the affected window.
        affected = self._conn.execute(
            "SELECT COUNT(*) FROM hourly_usage WHERE hour_ts >= ?",
            [window_start],
        ).fetchone()[0]
        return int(affected)

    def refresh_hourly_strip_peak(self, *, lookback_hours: int = 2) -> int:
        """Idempotently upsert recent (device, hour) peaks in hourly_strip_peak.

        Per-hour peak = MAX over the hour's poll instants of the summed watts
        across the device's emeter plugs at that instant. The recorder stamps
        every insert in one poll loop with the same ts, so grouping readings
        by exact ts reconstructs simultaneous draw. OFF plugs that skipped a
        poll (rate-limited) would have contributed 0 anyway.

        Also records peak_watts_p99 = 99th-percentile of the hour's non-zero
        per-instant sums — a robust peak discarding inrush spikes (NULL when
        the hour had no draw).

        Same windowing as refresh_hourly_usage: full backfill on an empty
        table, otherwise recompute from lookback_hours before the older of
        latest-reading / latest-rollup. No LAG involved, so no pre-window
        anchor row is needed. Returns the count of rows in the window.
        """
        latest_reading = self._conn.execute("SELECT MAX(ts) FROM readings").fetchone()[0]
        if latest_reading is None:
            return 0
        latest_rollup = self._conn.execute("SELECT MAX(hour_ts) FROM hourly_strip_peak").fetchone()[
            0
        ]
        if latest_rollup is None:
            window_start = self._conn.execute("SELECT MIN(ts) FROM readings").fetchone()[0]
        else:
            anchor = min(latest_reading, latest_rollup)
            window_start = anchor - timedelta(hours=lookback_hours)
            # Never let the window reach the partially-pruned boundary bucket:
            # its raw is only half present, so recomputing it would overwrite a
            # complete stored value with one derived from the surviving tail.
            # Only on this branch -- an empty table is a full backfill, where
            # there is no stored value to protect.
            first_full = self._first_recomputable_hour()
            if first_full is not None and window_start < first_full:
                window_start = first_full

        self._conn.execute(
            """
            INSERT INTO hourly_strip_peak (device_id, hour_ts, peak_watts, peak_watts_p99)
            SELECT device_id, hour_ts,
                   MAX(ts_watts) AS peak_watts,
                   quantile_cont(ts_watts, 0.99) FILTER (WHERE ts_watts > 0) AS peak_watts_p99
            FROM (
                SELECT p.device_id,
                       date_trunc('hour', r.ts) AS hour_ts,
                       r.ts,
                       SUM(COALESCE(r.watts, 0)) AS ts_watts
                FROM readings r
                JOIN plugs p ON p.plug_id = r.plug_id AND p.has_emeter
                WHERE r.ts >= ?
                GROUP BY p.device_id, date_trunc('hour', r.ts), r.ts
            ) per_ts
            GROUP BY device_id, hour_ts
            ON CONFLICT (device_id, hour_ts) DO UPDATE SET
                peak_watts = excluded.peak_watts,
                peak_watts_p99 = excluded.peak_watts_p99
            """,
            [window_start],
        )
        affected = self._conn.execute(
            "SELECT COUNT(*) FROM hourly_strip_peak WHERE hour_ts >= ?",
            [window_start],
        ).fetchone()[0]
        return int(affected)

    def refresh_hourly_circuit_peak(self, *, lookback_hours: int = 2) -> int:
        """Idempotently upsert recent (circuit, hour) peaks in hourly_circuit_peak.

        Per-hour peak = MAX over the hour's poll instants of the summed watts
        across ALL emeter plugs on ALL strips currently assigned to the
        circuit — the breaker-trip-relevant number. Same per-ts grouping and
        p99 logic as refresh_hourly_strip_peak, but joined through
        circuit_devices (CURRENT membership).

        Same windowing: full backfill on an empty table, else recompute from
        lookback_hours before the older of latest-reading / latest-rollup.
        Because membership is mutable, callers that change assignments should
        run rebuild_hourly_circuit_peak() to recompute history.
        """
        latest_reading = self._conn.execute("SELECT MAX(ts) FROM readings").fetchone()[0]
        if latest_reading is None:
            return 0
        latest_rollup = self._conn.execute(
            "SELECT MAX(hour_ts) FROM hourly_circuit_peak"
        ).fetchone()[0]
        if latest_rollup is None:
            window_start = self._conn.execute("SELECT MIN(ts) FROM readings").fetchone()[0]
        else:
            anchor = min(latest_reading, latest_rollup)
            window_start = anchor - timedelta(hours=lookback_hours)
            # Never let the window reach the partially-pruned boundary bucket:
            # its raw is only half present, so recomputing it would overwrite a
            # complete stored value with one derived from the surviving tail.
            # Only on this branch -- an empty table is a full backfill, where
            # there is no stored value to protect.
            first_full = self._first_recomputable_hour()
            if first_full is not None and window_start < first_full:
                window_start = first_full

        self._conn.execute(
            """
            INSERT INTO hourly_circuit_peak (circuit_id, hour_ts, peak_watts, peak_watts_p99)
            SELECT circuit_id, hour_ts,
                   MAX(ts_watts) AS peak_watts,
                   quantile_cont(ts_watts, 0.99) FILTER (WHERE ts_watts > 0) AS peak_watts_p99
            FROM (
                SELECT cd.circuit_id,
                       date_trunc('hour', r.ts) AS hour_ts,
                       r.ts,
                       SUM(COALESCE(r.watts, 0)) AS ts_watts
                FROM readings r
                JOIN plugs p ON p.plug_id = r.plug_id AND p.has_emeter
                JOIN circuit_devices cd ON cd.device_id = p.device_id
                WHERE r.ts >= ?
                GROUP BY cd.circuit_id, date_trunc('hour', r.ts), r.ts
            ) per_ts
            GROUP BY circuit_id, hour_ts
            ON CONFLICT (circuit_id, hour_ts) DO UPDATE SET
                peak_watts = excluded.peak_watts,
                peak_watts_p99 = excluded.peak_watts_p99
            """,
            [window_start],
        )
        affected = self._conn.execute(
            "SELECT COUNT(*) FROM hourly_circuit_peak WHERE hour_ts >= ?",
            [window_start],
        ).fetchone()[0]
        return int(affected)

    def rebuild_hourly_circuit_peak(self) -> int:
        """Recompute hourly_circuit_peak from scratch under current membership.

        Run after a strip's circuit assignment changes (or a circuit is
        deleted): a windowed refresh would leave historical rows reflecting
        stale membership. Truncate + full backfill — a bounded full scan
        (~0.1s on the dev DB; seconds at production scale).
        """
        # Keep hours below the recorded prune cut: they cannot be recomputed,
        # so deleting them would destroy history outright rather than refresh
        # it. Nothing pruned means no cut, and this deletes the whole table
        # exactly as before.
        floor = self._first_recomputable_hour()
        if floor is None:
            self._conn.execute("DELETE FROM hourly_circuit_peak")
        else:
            self._conn.execute("DELETE FROM hourly_circuit_peak WHERE hour_ts >= ?", [floor])
        return self.refresh_hourly_circuit_peak()

    def circuit_peaks(self, start: datetime, end: datetime) -> dict[int, float]:
        """Per-circuit robust peak (MAX of hourly p99 of simultaneous draw).

        Uses peak_watts_p99 from hourly_circuit_peak; circuits with only NULL
        p99 (all-off hours) are omitted.
        """
        rows = self._conn.execute(
            """
            SELECT circuit_id, MAX(peak_watts_p99)
            FROM hourly_circuit_peak
            WHERE hour_ts >= ? AND hour_ts < ?
              AND peak_watts_p99 IS NOT NULL
            GROUP BY circuit_id
            """,
            [start, end],
        ).fetchall()
        return {int(r[0]): float(r[1]) for r in rows}

    def usage_by_machine(self, start: datetime, end: datetime) -> list[dict]:
        """Return per-hour kWh aggregated by machine in [start, end).

        Plug-hours with no active assignment surface as machine_id=None and
        machine_name='Unassigned'. Attribution rule for plugs reassigned
        mid-hour: the assignment active at the START of the hour gets credit.
        """
        rows = self._conn.execute(
            """
            SELECT
                hu.hour_ts,
                m.machine_id,
                COALESCE(m.name, 'Unassigned') AS machine_name,
                SUM(hu.kwh) AS kwh
            FROM hourly_usage hu
            LEFT JOIN assignments a
              ON a.plug_id = hu.plug_id
             AND a.assigned_from <= hu.hour_ts
             AND (a.assigned_until IS NULL OR a.assigned_until > hu.hour_ts)
            LEFT JOIN machines m ON m.machine_id = a.machine_id
            WHERE hu.hour_ts >= ? AND hu.hour_ts < ?
            GROUP BY hu.hour_ts, m.machine_id, m.name
            ORDER BY hu.hour_ts, machine_name
            """,
            [start, end],
        ).fetchall()
        return [
            {
                "hour_ts": r[0],
                "machine_id": r[1],
                "machine_name": r[2],
                "kwh": float(r[3]) if r[3] is not None else 0.0,
            }
            for r in rows
        ]

    def kwh_by_machine_and_local_day(self, start_day: date, end_day: date) -> list[dict]:
        """Per-machine kWh summed by **local-Central day** for [start_day, end_day).

        Backs the energy-cost section: hourly_usage.hour_ts is UTC, so each hour is
        bucketed to the Chicago local day it falls in (DST-correct via the tz db)
        before summing. Same machine attribution as usage_by_machine (assignment
        active at the start of the hour; unassigned plug-hours surface as
        machine_id=None / 'Unassigned'). Each row: {day_local (date),
        machine_id (int|None), machine_name (str), kwh (float)}.
        """
        rows = self._conn.execute(
            """
            WITH per_hour AS (
                SELECT
                    CAST(hu.hour_ts AT TIME ZONE 'UTC' AT TIME ZONE ? AS DATE) AS day_local,
                    a.machine_id AS machine_id,
                    COALESCE(m.name, 'Unassigned') AS machine_name,
                    hu.kwh AS kwh
                FROM hourly_usage hu
                LEFT JOIN assignments a
                  ON a.plug_id = hu.plug_id
                 AND a.assigned_from <= hu.hour_ts
                 AND (a.assigned_until IS NULL OR a.assigned_until > hu.hour_ts)
                LEFT JOIN machines m ON m.machine_id = a.machine_id
            )
            SELECT day_local, machine_id, machine_name, SUM(kwh) AS kwh
            FROM per_hour
            WHERE day_local >= ? AND day_local < ?
            GROUP BY 1, 2, 3
            ORDER BY 1, machine_name
            """,
            [_LOCAL_TZ_NAME, start_day, end_day],
        ).fetchall()
        return [
            {
                "day_local": r[0],
                "machine_id": r[1],
                "machine_name": r[2],
                "kwh": float(r[3]) if r[3] is not None else 0.0,
            }
            for r in rows
        ]

    def usage_for_plugs(
        self, plug_ids: Sequence[int], start: datetime, end: datetime
    ) -> list[tuple[datetime, float]]:
        """Per-hour kWh summed across the given plugs in [start, end).

        Rows are (hour_ts, kwh) ordered by hour; hour_ts is naive UTC as
        DuckDB yields it (the session timezone is pinned to UTC).
        """
        if not plug_ids:
            return []
        placeholders = ", ".join("?" for _ in plug_ids)
        rows = self._conn.execute(
            f"""
            SELECT hour_ts, SUM(kwh)
            FROM hourly_usage
            WHERE plug_id IN ({placeholders}) AND hour_ts >= ? AND hour_ts < ?
            GROUP BY hour_ts
            ORDER BY hour_ts
            """,  # noqa: S608 — placeholders are "?" markers, values are bound
            [*plug_ids, start, end],
        ).fetchall()
        return [(r[0], float(r[1]) if r[1] is not None else 0.0) for r in rows]

    def plug_peaks(
        self, plug_ids: Sequence[int], start: datetime, end: datetime
    ) -> dict[int, float]:
        """Per-plug robust peak (MAX of hourly p99s) over [start, end).

        Uses peak_watts_p99 — the 99th percentile of each hour's powered-on
        readings — so single-reading power-on inrush spikes don't dominate.
        Caveat: in a sparse hour (few on-readings) the p99 nears that hour's
        max, so a spike can survive there; on real data this is rare. Plugs
        with no rows or only NULL p99 (all-off hours) are omitted, not 0.
        """
        if not plug_ids:
            return {}
        placeholders = ", ".join("?" for _ in plug_ids)
        rows = self._conn.execute(
            f"""
            SELECT plug_id, MAX(peak_watts_p99)
            FROM hourly_usage
            WHERE plug_id IN ({placeholders})
              AND hour_ts >= ? AND hour_ts < ?
              AND peak_watts_p99 IS NOT NULL
            GROUP BY plug_id
            """,  # noqa: S608 — placeholders are "?" markers, values are bound
            [*plug_ids, start, end],
        ).fetchall()
        return {int(r[0]): float(r[1]) for r in rows}

    def strip_peaks(self, start: datetime, end: datetime) -> dict[str, float]:
        """Per-device robust peak (MAX of hourly p99 of simultaneous draw).

        Uses peak_watts_p99 from hourly_strip_peak; devices with only NULL
        p99 (all-off hours) are omitted.
        """
        rows = self._conn.execute(
            """
            SELECT device_id, MAX(peak_watts_p99)
            FROM hourly_strip_peak
            WHERE hour_ts >= ? AND hour_ts < ?
              AND peak_watts_p99 IS NOT NULL
            GROUP BY device_id
            """,
            [start, end],
        ).fetchall()
        return {r[0]: float(r[1]) for r in rows}

    def play_hours_by_machine(self, start_day: date, end_day: date) -> list[dict]:
        """Per-machine play hours for the half-open local-day window.

        Derived from `hourly_play_seconds` (the single source of truth for play
        time) by summing per local date — so this and the busy grid can't
        disagree. Each row: {day_local (date), machine_id (int),
        machine_name (str), hours (float)}.
        """
        rows = self._conn.execute(
            """
            SELECT CAST(h.hour_local AS DATE) AS day_local, m.machine_id, m.name,
                   SUM(h.play_seconds) AS seconds
            FROM hourly_play_seconds h
            JOIN machines m ON m.machine_id = h.machine_id
            WHERE CAST(h.hour_local AS DATE) >= ? AND CAST(h.hour_local AS DATE) < ?
            GROUP BY 1, 2, 3
            ORDER BY 1, m.name
            """,
            [start_day, end_day],
        ).fetchall()
        return [
            {
                "day_local": r[0],
                "machine_id": int(r[1]),
                "machine_name": r[2],
                "hours": float(r[3]) / 3600.0,
            }
            for r in rows
        ]

    def refresh_hourly_play_seconds(self, *, lookback_hours: int = 49) -> int:
        """Roll up PLAYING time and on-time per (machine, local-Central hour).

        The single source of truth for play time, summed per local date for the
        play-hours chart and per (date, hour) for the busy grid. Only machines
        with a calibration row and an open assignment contribute; runs the same
        `classify()` with a warmup and 60s gap cap, adding `dt` to `on_seconds`
        when the machine is non-OFF and to `play_seconds` when PLAYING, bucketed
        by local-Central wall-clock hour. The window aligns to local-hour
        boundaries (so no truncation at the trailing edge). Idempotent via UPSERT
        on (machine_id, hour_local).
        """
        plug_cals = self._conn.execute(
            """
            SELECT a.plug_id, a.machine_id, c.idle_max_rsd, c.play_min_rsd
            FROM assignments a
            JOIN calibrations c ON c.machine_id = a.machine_id
            WHERE a.assigned_until IS NULL
            """
        ).fetchall()
        if not plug_cals:
            return 0

        latest_reading = self._conn.execute("SELECT MAX(ts) FROM readings").fetchone()[0]
        if latest_reading is None:
            return 0
        if latest_reading.tzinfo is None:
            latest_reading = latest_reading.replace(tzinfo=UTC)

        local_tz = ZoneInfo(_LOCAL_TZ_NAME)
        latest_rollup = self._conn.execute(
            "SELECT MAX(hour_local) FROM hourly_play_seconds"
        ).fetchone()[0]

        # Window anchor: the older of the latest reading vs. the latest rolled-up
        # hour. Fresh table → backfill from the oldest reading.
        if latest_rollup is None:
            window_start = self._conn.execute("SELECT MIN(ts) FROM readings").fetchone()[0]
            if window_start is None:
                return 0
            if window_start.tzinfo is None:
                window_start = window_start.replace(tzinfo=UTC)
        else:
            # hour_local is a naive local wall-clock hour; read it back as local.
            rollup_anchor = latest_rollup.replace(tzinfo=local_tz).astimezone(UTC)
            anchor = min(latest_reading, rollup_anchor)
            window_start = anchor - timedelta(hours=lookback_hours)
            # Never let the window reach past the prune cut. The stakes here are
            # higher than in the peak refreshes: this one DELETEs its window
            # before reinserting, so an hour it cannot recompute is not
            # overwritten with a smaller number, it is simply gone. The window
            # is wide enough for that to matter -- `rollup_lookback_hours`
            # stretches it to cover whatever ingest last backfilled.
            first_full = self._first_recomputable_hour()
            if first_full is not None:
                floor = first_full.replace(tzinfo=UTC)
                if window_start < floor:
                    window_start = floor
        warmup_start = window_start - _PLAY_HOURS_WARMUP

        play_seconds: dict[tuple[int, datetime], float] = defaultdict(float)
        on_seconds: dict[tuple[int, datetime], float] = defaultdict(float)

        for plug_id, machine_id, idle_max, play_min in plug_cals:
            rows = self._conn.execute(
                "SELECT ts, COALESCE(watts, 0) FROM readings "
                "WHERE plug_id = ? AND ts >= ? ORDER BY ts",
                [plug_id, warmup_start],
            ).fetchall()
            self._bucket_play_on(
                rows,
                int(machine_id),
                Calibration(idle_max_rsd=idle_max, play_min_rsd=play_min),
                window_start,
                play_seconds,
                on_seconds,
            )

        # Wipe the recompute window for eligible machines so hours that no longer
        # qualify (e.g. after recalibration) don't keep stale rows.
        window_start_hour = _local_hour(window_start, local_tz)
        eligible_machine_ids = sorted({int(mid) for _, mid, _, _ in plug_cals})
        if eligible_machine_ids:
            placeholders = ",".join(["?"] * len(eligible_machine_ids))
            self._conn.execute(
                f"DELETE FROM hourly_play_seconds "  # noqa: S608
                f"WHERE machine_id IN ({placeholders}) AND hour_local >= ?",
                [*eligible_machine_ids, window_start_hour],
            )

        for bucket, on_s in on_seconds.items():
            machine_id, hour_local = bucket
            self._conn.execute(
                """
                INSERT INTO hourly_play_seconds (machine_id, hour_local, play_seconds, on_seconds)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (machine_id, hour_local) DO UPDATE SET
                    play_seconds = excluded.play_seconds,
                    on_seconds = excluded.on_seconds
                """,
                [machine_id, hour_local, play_seconds.get(bucket, 0.0), on_s],
            )
        return len(on_seconds)

    def _bucket_play_on(
        self,
        rows: Sequence[tuple[datetime, float]],
        machine_id: int,
        cal: Calibration,
        window_start: datetime,
        play_seconds: dict[tuple[int, datetime], float],
        on_seconds: dict[tuple[int, datetime], float],
    ) -> None:
        """Classify `rows` (ts, watts, time-ordered) and accumulate on-time and
        PLAYING-time into the bucket dicts keyed by (machine_id, local hour).

        Shared by the incremental refresh and the full retroactive rebuild so
        both classify identically. Rows before `window_start` only prime the
        rolling classifier — they aren't attributed (pass a very old
        `window_start` to attribute the whole series).
        """
        if len(rows) < 2:
            return
        local_tz = ZoneInfo(_LOCAL_TZ_NAME)
        states = classify([float(r[1]) for r in rows], cal)
        for i in range(len(rows) - 1):
            if states[i] is None:  # no draw ⇒ no activity, nothing to attribute
                continue
            ts_i = rows[i][0]
            if ts_i.tzinfo is None:
                ts_i = ts_i.replace(tzinfo=UTC)
            # Warmup rows only prime the classifier — don't attribute them.
            if ts_i < window_start:
                continue
            ts_next = rows[i + 1][0]
            if ts_next.tzinfo is None:
                ts_next = ts_next.replace(tzinfo=UTC)
            dt = min((ts_next - ts_i).total_seconds(), _USAGE_DT_CAP_SECONDS)
            if dt <= 0:
                continue
            bucket = (machine_id, _local_hour(ts_i, local_tz))
            on_seconds[bucket] += dt
            if states[i] is Activity.PLAYING:
                play_seconds[bucket] += dt

    def calibrated_assigned_machine_ids(self) -> list[int]:
        """Machine ids that contribute to `hourly_play_seconds` — i.e. have both
        an open assignment and a calibration row. The eligible set for a
        retroactive rebuild."""
        rows = self._conn.execute(
            """
            SELECT DISTINCT a.machine_id
            FROM assignments a
            JOIN calibrations c ON c.machine_id = a.machine_id
            WHERE a.assigned_until IS NULL
            ORDER BY a.machine_id
            """
        ).fetchall()
        return [int(r[0]) for r in rows]

    def rebuild_play_hours(self, machine_id: int) -> int:
        """Recompute ALL of one machine's `hourly_play_seconds` from raw readings
        using its current stored calibration — making recalibration retroactive.

        Replays `classify()` over the machine's full history, one assignment
        interval at a time: each plug the machine was assigned to contributes only
        its `[assigned_from, assigned_until)` readings, so a machine that was moved
        between outlets keeps its prior-plug history and never picks up readings
        that belonged to another machine on the same plug. (The incremental
        refresh sidesteps this by only ever revisiting a trailing window; a full
        rebuild has to be interval-aware.) Wipes the machine's rows and reinserts.
        Returns the number of hour-buckets written. No-op (0) if the machine has
        no calibration or no assignment history.
        """
        cal_row = self._conn.execute(
            "SELECT idle_max_rsd, play_min_rsd FROM calibrations WHERE machine_id = ?",
            [machine_id],
        ).fetchone()
        if cal_row is None:
            return 0
        cal = Calibration(idle_max_rsd=cal_row[0], play_min_rsd=cal_row[1])

        intervals = self._conn.execute(
            "SELECT plug_id, assigned_from, assigned_until FROM assignments "
            "WHERE machine_id = ? ORDER BY assigned_from",
            [machine_id],
        ).fetchall()
        if not intervals:
            return 0

        play_seconds: dict[tuple[int, datetime], float] = defaultdict(float)
        on_seconds: dict[tuple[int, datetime], float] = defaultdict(float)
        for plug_id, assigned_from, assigned_until in intervals:
            if assigned_from.tzinfo is None:
                assigned_from = assigned_from.replace(tzinfo=UTC)
            if assigned_until is not None and assigned_until.tzinfo is None:
                assigned_until = assigned_until.replace(tzinfo=UTC)
            # Pull a warmup lead-in to prime the classifier; attribution is capped
            # to the interval by `_bucket_play_on` (>= assigned_from) and the query
            # (< assigned_until).
            warmup_start = assigned_from - _PLAY_HOURS_WARMUP
            if assigned_until is None:
                rows = self._conn.execute(
                    "SELECT ts, COALESCE(watts, 0) FROM readings "
                    "WHERE plug_id = ? AND ts >= ? ORDER BY ts",
                    [plug_id, warmup_start],
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT ts, COALESCE(watts, 0) FROM readings "
                    "WHERE plug_id = ? AND ts >= ? AND ts < ? ORDER BY ts",
                    [plug_id, warmup_start, assigned_until],
                ).fetchall()
            self._bucket_play_on(
                rows, int(machine_id), cal, assigned_from, play_seconds, on_seconds
            )

        # Keep buckets below the recorded prune cut: that raw is gone and they
        # cannot be recomputed, so deleting them would erase history rather
        # than rebuild it. This path runs on *recalibration*, a routine
        # operator action, which is what makes it worth guarding. Nothing
        # pruned means no cut, and this deletes everything, exactly as
        # before.
        # The floor is converted into the same local-hour space the buckets use;
        # comparing a UTC instant against a Chicago hour would be off by the
        # offset and would keep or drop the wrong hours.
        # The first fully surviving *local* hour, not the one containing the
        # floor: that bucket's raw is only partly present, so recomputing it
        # would overwrite a complete value with one derived from its tail.
        raw_floor = self._unrecomputable_before()
        floor_local: datetime | None = None
        if raw_floor is not None:
            local_tz = ZoneInfo(_LOCAL_TZ_NAME)
            floor_utc = raw_floor.replace(tzinfo=UTC)
            truncated = _local_hour(floor_utc, local_tz)
            exact = floor_utc.astimezone(local_tz).replace(tzinfo=None)
            floor_local = truncated if truncated == exact else truncated + timedelta(hours=1)
        if floor_local is None:
            self._conn.execute("DELETE FROM hourly_play_seconds WHERE machine_id = ?", [machine_id])
        else:
            self._conn.execute(
                "DELETE FROM hourly_play_seconds WHERE machine_id = ? AND hour_local >= ?",
                [machine_id, floor_local],
            )
        for bucket, on_s in on_seconds.items():
            _mid, hour_local = bucket
            # The hour containing the floor would be recomputed from only the
            # surviving tail of its readings -- worse than the complete value
            # already stored, and a duplicate-key collision with it.
            if floor_local is not None and hour_local < floor_local:
                continue
            self._conn.execute(
                """
                INSERT INTO hourly_play_seconds (machine_id, hour_local, play_seconds, on_seconds)
                VALUES (?, ?, ?, ?)
                """,
                [machine_id, hour_local, play_seconds.get(bucket, 0.0), on_s],
            )
        return len(on_seconds)

    def has_migration(self, name: str, conn: duckdb.DuckDBPyConnection | None = None) -> bool:
        """Whether the one-off data migration `name` has been applied to this DB."""
        row = (
            (conn or self._conn)
            .execute("SELECT 1 FROM applied_migrations WHERE name = ?", [name])
            .fetchone()
        )
        return row is not None

    def mark_migration(self, name: str) -> None:
        """Record a one-off data migration as applied (idempotent)."""
        self._conn.execute(
            "INSERT INTO applied_migrations (name, applied_at) VALUES (?, current_timestamp) "
            "ON CONFLICT (name) DO NOTHING",
            [name],
        )

    def play_utilization_grid(
        self,
        start_local: datetime,
        end_local: datetime,
        *,
        min_on_seconds: float = _BUSY_MIN_ON_SECONDS,
    ) -> list[dict]:
        """Per (local date, hour-of-day) play utilization for the bubble grid.

        Aggregates `hourly_play_seconds` across measurable machines over the
        half-open naive-local window. Only cells where the collective on-time
        clears `min_on_seconds` are returned (so the grid reflects hours we were
        actually open, not a lone machine briefly powered). Each row:
        {date_local (date), hour (int 0-23), play_hours, on_hours,
         ratio = play/on in [0, 1]}.
        """
        rows = self._conn.execute(
            """
            SELECT CAST(hour_local AS DATE)      AS date_local,
                   EXTRACT(HOUR FROM hour_local) AS hod,
                   SUM(play_seconds)             AS play_s,
                   SUM(on_seconds)               AS on_s
            FROM hourly_play_seconds
            WHERE hour_local >= ? AND hour_local < ?
            GROUP BY 1, 2
            HAVING SUM(on_seconds) >= ?
            ORDER BY 1, 2
            """,
            [start_local, end_local, min_on_seconds],
        ).fetchall()
        return [
            {
                "date_local": r[0],
                "hour": int(r[1]),
                "play_hours": float(r[2]) / 3600.0,
                "on_hours": float(r[3]) / 3600.0,
                "ratio": float(r[2]) / float(r[3]),
            }
            for r in rows
        ]

    def record_strip(self, strip_reading: StripReading, ts: datetime) -> None:
        """Record all plug readings from a strip."""
        rows = []
        for plug in strip_reading.plugs:
            plug_id = self.ensure_plug(strip_reading.device_id, plug.child_id, plug.alias)
            rows.append((ts, plug_id, plug.watts, plug.voltage, plug.amps, plug.total_kwh))
        self.insert_readings(rows)
