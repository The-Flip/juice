"""DuckDB storage layer for power readings."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

from juice.collector import StripReading
from juice.state import Calibration, State, classify


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
    total_kwh FLOAT
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

CREATE TABLE IF NOT EXISTS daily_play_seconds (
    machine_id SMALLINT NOT NULL,
    day_local  DATE     NOT NULL,
    seconds    FLOAT    NOT NULL,
    PRIMARY KEY (machine_id, day_local)
);
"""

# Hardcoded to the museum's timezone. Day buckets on the play-hours chart
# are local-Central dates so the bar at "Saturday" lines up with a
# Saturday a human in the museum would recognise.
_LOCAL_TZ_NAME = "America/Chicago"

# How much pre-window readings to pull so the rolling classifier is fully
# primed at the inner-window boundary. The classifier uses a 30-sample
# rolling window of non-zero readings; an hour of warmup is generous.
_PLAY_HOURS_WARMUP = timedelta(hours=1)

# Max gap between consecutive readings to attribute energy across.
# Matches juice.recorder.IDLE_RECHECK_SECONDS — a longer gap means the
# recorder was down or the plug fell offline, so the energy from the
# previous reading isn't trustworthy beyond this window.
_USAGE_DT_CAP_SECONDS = 60.0


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

    # Drop NOT NULL on readings power columns so EP10-style outlets can record
    # ON state with NULL power fields.
    reading_info = conn.execute("PRAGMA table_info('readings')").fetchall()
    notnull_by_name = {row[1]: row[3] for row in reading_info}
    for col in ("watts", "voltage", "amps", "total_kwh"):
        if notnull_by_name.get(col):
            conn.execute(f"ALTER TABLE readings ALTER COLUMN {col} DROP NOT NULL")


class Store:
    def __init__(self, path: str | Path) -> None:
        self._path = str(path)
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._plug_cache: dict[tuple[str, str], tuple[int, str]] = {}  # key -> (plug_id, alias)
        self._machine_cache: dict[str, tuple[int, str]] = {}  # asset_id -> (machine_id, name)
        self._assignment_cache: dict[int, int | None] = {}  # plug_id -> current machine_id

    def open(self) -> Store:
        self._conn = duckdb.connect(self._path)
        # Pin the session timezone so tz-aware datetimes round-trip cleanly —
        # DuckDB otherwise converts aware values to the host's local zone
        # before storing into a naive TIMESTAMP column.
        self._conn.execute("SET TimeZone='UTC'")
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

    def get_recent_watts(self, plug_id: int, seconds: int = 3600) -> list[float]:
        """Fetch the last N seconds of watt readings for a plug."""
        rows = self._conn.execute(
            """
            SELECT watts FROM readings
            WHERE plug_id = ? AND ts >= (now() - INTERVAL (?) SECOND)
            ORDER BY ts
            """,
            [plug_id, seconds],
        ).fetchall()
        return [r[0] for r in rows]

    def get_readings_since(self, plug_id: int, since: datetime) -> list[tuple[str, float]]:
        """Fetch (iso_timestamp, watts) pairs for a plug since a given time."""
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

        Each row: (plug_id, device_id, alias, is_on_latest). `is_on_latest`
        is True if the most recent reading has watts IS NULL (on-without-emeter
        signal) or watts > 0, False if watts = 0, or None if it has no readings.
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
                    WHEN r.watts IS NULL AND r.ts IS NOT NULL THEN TRUE
                    WHEN r.watts IS NOT NULL THEN r.watts > 0
                    ELSE NULL
                END AS is_on_latest
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
        return [(int(pid), did, alias, on) for pid, did, alias, on in rows]

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

    def recent_power_events(self, limit: int = 50, before: int | None = None) -> list[dict]:
        """Return recent power events (newest first), joined with machine + plug alias.

        `before`: if given, only events with event_id strictly less than this are returned —
        used for cursor-style pagination back through history.
        """
        if before is None:
            rows = self._conn.execute(
                """
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
                ORDER BY pe.event_id DESC
                LIMIT ?
                """,
                [limit],
            ).fetchall()
        else:
            rows = self._conn.execute(
                """
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
                WHERE pe.event_id < ?
                ORDER BY pe.event_id DESC
                LIMIT ?
                """,
                [before, limit],
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
        self._conn.execute("DELETE FROM hourly_circuit_peak")
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

    def refresh_daily_play_seconds(self, *, lookback_days: int = 2) -> int:
        """Roll up PLAYING-state time per (machine, local-Central day).

        Only machines that have a calibration row AND a currently-open
        assignment contribute — "we can detect when this machine was
        played" hinges on the calibration existing. For each such plug,
        pull readings since `window_start - warmup`, run `classify()`,
        and sum `dt` (capped at the same 60s the kWh rollup uses) for
        consecutive readings whose state == PLAYING. Bucket by local
        date. Idempotent via UPSERT on (machine_id, day_local).
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
        latest_rollup_day = self._conn.execute(
            "SELECT MAX(day_local) FROM daily_play_seconds"
        ).fetchone()[0]

        # Window starts `lookback_days` before the anchor — older of latest
        # reading vs. latest rollup day. On a fresh table, latest_rollup_day
        # is None and we backfill from the oldest reading.
        # DuckDB returns naive TIMESTAMPs (session is pinned to UTC) — mark
        # them aware so timedelta math stays consistent across the function.
        if latest_reading.tzinfo is None:
            latest_reading = latest_reading.replace(tzinfo=UTC)

        if latest_rollup_day is None:
            window_start = self._conn.execute("SELECT MIN(ts) FROM readings").fetchone()[0]
            if window_start is None:
                return 0
            if window_start.tzinfo is None:
                window_start = window_start.replace(tzinfo=UTC)
        else:
            rollup_anchor = datetime.combine(latest_rollup_day, datetime.min.time(), tzinfo=UTC)
            anchor = min(latest_reading, rollup_anchor)
            window_start = anchor - timedelta(days=lookback_days)
        warmup_start = window_start - _PLAY_HOURS_WARMUP

        local_tz = ZoneInfo(_LOCAL_TZ_NAME)
        play_seconds: dict[tuple[int, date], float] = defaultdict(float)

        for plug_id, machine_id, idle_max, play_min in plug_cals:
            rows = self._conn.execute(
                "SELECT ts, COALESCE(watts, 0) FROM readings "
                "WHERE plug_id = ? AND ts >= ? ORDER BY ts",
                [plug_id, warmup_start],
            ).fetchall()
            if len(rows) < 2:
                continue
            cal = Calibration(idle_max_rsd=idle_max, play_min_rsd=play_min)
            states = classify([float(r[1]) for r in rows], cal)
            for i in range(len(rows) - 1):
                if states[i] is not State.PLAYING:
                    continue
                ts_i = rows[i][0]
                if ts_i.tzinfo is None:
                    ts_i = ts_i.replace(tzinfo=UTC)
                # Skip warmup-region rows when attributing time — they
                # exist only to prime the classifier for the boundary.
                if ts_i < window_start:
                    continue
                ts_next = rows[i + 1][0]
                if ts_next.tzinfo is None:
                    ts_next = ts_next.replace(tzinfo=UTC)
                dt = min((ts_next - ts_i).total_seconds(), _USAGE_DT_CAP_SECONDS)
                if dt <= 0:
                    continue
                day_local = ts_i.astimezone(local_tz).date()
                play_seconds[(machine_id, day_local)] += dt

        # Wipe eligible machines' rows in the recompute window before
        # re-inserting — without this, a day that used to have PLAYING
        # but no longer does (e.g. after recalibration) keeps its stale
        # non-zero row forever and /api/play-hours returns wrong totals.
        window_start_local = window_start.astimezone(local_tz).date()
        eligible_machine_ids = sorted({int(mid) for _, mid, _, _ in plug_cals})
        if eligible_machine_ids:
            # `placeholders` is just "?,?,..." — no untrusted input is
            # interpolated into the SQL. Counts: one ? per machine_id plus
            # one for the day_local bound.
            placeholders = ",".join(["?"] * len(eligible_machine_ids))
            self._conn.execute(
                f"DELETE FROM daily_play_seconds "  # noqa: S608
                f"WHERE machine_id IN ({placeholders}) AND day_local >= ?",
                [*eligible_machine_ids, window_start_local],
            )

        for (machine_id, day), seconds in play_seconds.items():
            self._conn.execute(
                """
                INSERT INTO daily_play_seconds (machine_id, day_local, seconds)
                VALUES (?, ?, ?)
                ON CONFLICT (machine_id, day_local) DO UPDATE SET
                    seconds = excluded.seconds
                """,
                [machine_id, day, seconds],
            )
        return len(play_seconds)

    def play_hours_by_machine(self, start_day: date, end_day: date) -> list[dict]:
        """Per-machine play hours for the half-open local-day window.

        Each row: {day_local (date), machine_id (int), machine_name (str),
                   hours (float)}.
        """
        rows = self._conn.execute(
            """
            SELECT d.day_local, m.machine_id, m.name, d.seconds
            FROM daily_play_seconds d
            JOIN machines m ON m.machine_id = d.machine_id
            WHERE d.day_local >= ? AND d.day_local < ?
            ORDER BY d.day_local, m.name
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

    def record_strip(self, strip_reading: StripReading, ts: datetime) -> None:
        """Record all plug readings from a strip."""
        rows = []
        for plug in strip_reading.plugs:
            plug_id = self.ensure_plug(strip_reading.device_id, plug.child_id, plug.alias)
            rows.append((ts, plug_id, plug.watts, plug.voltage, plug.amps, plug.total_kwh))
        self.insert_readings(rows)
