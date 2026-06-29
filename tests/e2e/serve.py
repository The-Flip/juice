"""Cloud-free juice server for the Playwright e2e harness.

Reuses the real app (``create_app``/``start_server``) against a seeded fixture
DuckDB, with ``RecorderState`` hydrated from the DB — no Kasa cloud, no recorder.
The dev-auth shim gives the real logged-out → one-click ``/login`` → ``/logout``
flow. Playwright's ``webServer`` launches this and waits for the port.

    uv run python -m tests.e2e.serve --port 8099 [--db path.duckdb]

With no ``--db`` it seeds a fresh fixture into a temp file. This is read-only:
power-control POSTs have no plug objects to act on (that's the Phase 2 work);
charts, tooltips, nav, and auth all render from the fixture.
"""

from __future__ import annotations

import argparse
import asyncio
import tempfile
from pathlib import Path

from juice.collector import PlugReading
from juice.recorder import hydrate_assignments
from juice.server import RecorderState, seed_buffers, start_server
from juice.state import OFF_WATTS, Calibration
from juice.store import Store
from tests.e2e.seed import seed_fixture_db


def _load_calibrations(state: RecorderState, store: Store) -> None:
    """Populate ``state.calibrations`` (plug_id → Calibration) from the DB.

    ``hydrate_assignments`` doesn't carry calibrations (the cloud refresh path
    normally does), but the dashboard's live state band (OFF/ATTRACT/PLAYING)
    needs them per plug — without this every tile shows an unclassified state.
    """
    rows = store._conn.execute(  # noqa: SLF001 — fixture harness, DB read only
        "SELECT a.plug_id, c.idle_max_rsd, c.play_min_rsd "
        "FROM assignments a JOIN calibrations c ON c.machine_id = a.machine_id "
        "WHERE a.assigned_until IS NULL"
    ).fetchall()
    for plug_id, idle_max, play_min in rows:
        state.calibrations[plug_id] = Calibration(idle_max_rsd=idle_max, play_min_rsd=play_min)


def _snapshot_plug_readings(state: RecorderState, store: Store) -> None:
    """Fill ``state.plug_readings`` with each plug's latest stored reading.

    Without the recorder there's no live poll, so the dashboard's current-power
    numbers and on/off dots would be blank. This one-shot snapshot from the DB
    makes the tiles look alive (static); live ticking is the Phase 2 work.
    """
    for plug_id in state.plugs:
        row = store._conn.execute(  # noqa: SLF001 — fixture harness, DB read only
            "SELECT watts, voltage, amps, total_kwh, child_id, alias "
            "FROM readings JOIN plugs USING (plug_id) "
            "WHERE plug_id = ? ORDER BY ts DESC LIMIT 1",
            [plug_id],
        ).fetchone()
        if row is None:
            continue
        watts, voltage, amps, total_kwh, child_id, alias = row
        # A static fixture has no real relay state, so we proxy is_on from draw:
        # NULL watts (a no-emeter plug's "on" row) → on, 0 → off, else drawing → on.
        # This is the drawing-as-relay fallback (cf. the "drawing != on" convention);
        # fine here because there is no live relay to read.
        is_on = watts is None or watts >= OFF_WATTS
        state.plug_readings[plug_id] = PlugReading(
            child_id=child_id,
            alias=alias,
            is_on=is_on,
            watts=watts,
            voltage=voltage,
            amps=amps,
            total_kwh=total_kwh,
        )


async def _run(db_path: str, host: str, port: int) -> None:
    with Store(db_path) as store:
        state = RecorderState()
        hydrate_assignments(state, store)  # plugs/assignments/strips/circuits/locks from DB
        _load_calibrations(state, store)  # per-plug calibrations for the live state band
        seed_buffers(state, store)  # sparkline ring buffers from recent readings
        _snapshot_plug_readings(state, store)  # current power/on-off for the tiles
        runner = await start_server(state, store, host, port, dev_auth=True)
        print(f"e2e server ready at http://{host}:{port}/  (db={db_path})", flush=True)
        try:
            await asyncio.Event().wait()  # serve until cancelled / killed
        finally:
            await runner.cleanup()


def main() -> None:
    ap = argparse.ArgumentParser(description="Cloud-free juice server for e2e tests")
    ap.add_argument("--port", type=int, default=8099)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--db", default=None, help="Fixture DB; seeded into a temp file if omitted.")
    args = ap.parse_args()

    db_path = args.db
    if db_path is None:
        db_path = str(Path(tempfile.gettempdir()) / "juice-e2e-fixture.duckdb")
        print(f"seeding fixture → {db_path}", flush=True)
        seed_fixture_db(db_path)  # idempotent: removes any existing file first

    try:
        asyncio.run(_run(db_path, args.host, args.port))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
