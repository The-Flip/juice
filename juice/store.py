"""DuckDB storage layer for power readings."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb

from juice.collector import StripReading
from juice.state import Calibration

_SCHEMA = """
CREATE SEQUENCE IF NOT EXISTS plug_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS machine_id_seq START 1;

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
    name       VARCHAR NOT NULL
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
"""


def _migrate(conn: duckdb.DuckDBPyConnection) -> None:
    """Apply idempotent schema migrations to an existing DB."""
    plug_cols = {row[1] for row in conn.execute("PRAGMA table_info('plugs')").fetchall()}
    if "has_emeter" not in plug_cols:
        # DuckDB does not support NOT NULL on ADD COLUMN, so add nullable
        # with a DEFAULT — existing rows backfill to TRUE.
        conn.execute("ALTER TABLE plugs ADD COLUMN has_emeter BOOLEAN DEFAULT TRUE")
        conn.execute("UPDATE plugs SET has_emeter = TRUE WHERE has_emeter IS NULL")

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

    def __enter__(self) -> Store:
        return self.open()

    def __exit__(self, *exc: object) -> None:
        self.close()

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

    def list_unassigned_outlets(self) -> list[tuple[int, str, str, bool | None]]:
        """List no-emeter plugs without an open machine assignment.

        Each row: (plug_id, device_id, alias, is_on_latest). `is_on_latest`
        is True if the most recent reading has watts IS NULL (on-without-emeter
        signal), False if watts = 0, or None if the plug has no readings yet.
        """
        rows = self._conn.execute(
            """
            WITH latest AS (
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
            WHERE p.has_emeter = FALSE
              AND NOT EXISTS (
                SELECT 1 FROM assignments a
                WHERE a.plug_id = p.plug_id AND a.assigned_until IS NULL
              )
            ORDER BY p.plug_id
            """
        ).fetchall()
        return [(int(pid), did, alias, on) for pid, did, alias, on in rows]

    def record_strip(self, strip_reading: StripReading, ts: datetime) -> None:
        """Record all plug readings from a strip."""
        rows = []
        for plug in strip_reading.plugs:
            plug_id = self.ensure_plug(strip_reading.device_id, plug.child_id, plug.alias)
            rows.append((ts, plug_id, plug.watts, plug.voltage, plug.amps, plug.total_kwh))
        self.insert_readings(rows)
