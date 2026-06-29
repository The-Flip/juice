"""Cloud-free juice server for the Playwright e2e harness.

Reuses the real app (``create_app``/``start_server``) against a seeded fixture
DuckDB, with ``RecorderState`` hydrated from the DB — no Kasa cloud, no recorder.
The dev-auth shim gives the real logged-out → one-click ``/login`` → ``/logout``
flow. Playwright's ``webServer`` launches this and waits for the port.

    uv run python -m tests.e2e.serve --port 8099 [--db path.duckdb] [--interactive]

With no ``--db`` it seeds a fresh fixture into a temp file. By default it's
read-only (charts, tooltips, nav, auth). ``--interactive`` adds fake plug objects
and a live readings tick so the power-control and SSE flows work without a cloud.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import tempfile
from pathlib import Path

from juice.collector import PlugReading
from juice.recorder import _update_buffer, hydrate_assignments
from juice.server import RecorderState, seed_buffers, start_server
from juice.state import OFF_WATTS, Calibration
from juice.store import Store
from tests.e2e.seed import seed_fixture_db

log = logging.getLogger(__name__)


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


class _FakePlug:
    """A duck-typed `Controllable` (Plug | _SelfPlug) for interactive e2e mode.

    `turn_on`/`turn_off` flip an in-memory relay and update `state.plug_readings`
    immediately, so the next readings tick (and any `/api/machines` fetch) reflects
    the change — standing in for a real Kasa plug with no cloud. The power-control
    handlers only ever call `turn_on`/`turn_off` and read `.alias`, so this is a
    faithful stand-in.

    Fidelity caveat: this bypasses the real watch_until → recorder-repoll path
    (the recorder normally refreshes plug_readings on its next poll). So the e2e
    exercises the handler + UI-settle flow, but NOT that reconciliation — keep
    that covered by the Python recorder tests.
    """

    def __init__(
        self,
        state: RecorderState,
        plug_id: int,
        child_id: str,
        alias: str,
        has_emeter: bool,
        on_watts: float | None,
    ) -> None:
        self._state = state
        self._plug_id = plug_id
        self._child_id = child_id
        self.alias = alias
        self._has_emeter = has_emeter
        self._on_watts = on_watts  # nominal draw when on (None for no-emeter)

    async def turn_on(self) -> None:
        self._set(on=True)

    async def turn_off(self) -> None:
        self._set(on=False)

    def _set(self, *, on: bool) -> None:
        watts = (self._on_watts if on else 0.0) if self._has_emeter else None
        self._state.plug_readings[self._plug_id] = PlugReading(
            child_id=self._child_id,
            alias=self.alias,
            is_on=on,
            watts=watts,
            voltage=120.0 if self._has_emeter else None,
            amps=(watts / 120.0) if (self._has_emeter and watts) else None,
            total_kwh=0.0 if self._has_emeter else None,
        )
        if self._has_emeter and watts is not None:
            _update_buffer(self._state, self._plug_id, watts)


def _install_fake_devices(state: RecorderState) -> None:
    """Register a `_FakePlug` per plug so power-control handlers can act on them.

    The nominal "on" draw is the plug's current snapshot watts (or a default), so
    toggling off→on restores a realistic load.
    """
    for plug_id, (_device_id, child_id, alias) in state.plugs.items():
        has_emeter = state.plug_has_emeter.get(plug_id, True)
        reading = state.plug_readings.get(plug_id)
        on_watts: float | None = None
        if has_emeter:
            on_watts = (
                reading.watts
                if reading is not None and reading.watts and reading.watts > OFF_WATTS
                else 120.0
            )
        state.plug_objects[plug_id] = _FakePlug(
            state, plug_id, child_id, alias, has_emeter, on_watts
        )


async def _readings_ticker(state: RecorderState) -> None:
    """Stand in for the recorder's 1 Hz SSE publish so live tiles/sparklines update
    and the power-control pending state reconciles. Reuses the real snapshot+publish
    so the event shape can't drift from production."""
    from juice.server import _publish, _readings_snapshot

    while True:
        await asyncio.sleep(1.0)
        if not state.event_subscribers:
            continue
        try:
            _publish(state, {"type": "readings", "machines": _readings_snapshot(state)})
        except Exception:  # noqa: BLE001 — keep the tick loop alive; a dead loop
            # would surface only as a confusing spec timeout, so log loudly instead.
            log.exception("e2e readings tick failed")


async def _run(db_path: str, host: str, port: int, interactive: bool) -> None:
    with Store(db_path) as store:
        state = RecorderState()
        hydrate_assignments(state, store)  # plugs/assignments/strips/circuits/locks from DB
        _load_calibrations(state, store)  # per-plug calibrations for the live state band
        seed_buffers(state, store)  # sparkline ring buffers from recent readings
        _snapshot_plug_readings(state, store)  # current power/on-off for the tiles
        ticker: asyncio.Task | None = None
        if interactive:
            _install_fake_devices(state)  # fake plug objects for power control
            ticker = asyncio.create_task(_readings_ticker(state))  # live SSE ticks
        runner = await start_server(state, store, host, port, dev_auth=True)
        mode = "interactive" if interactive else "read-only"
        print(f"e2e server ready ({mode}) at http://{host}:{port}/  (db={db_path})", flush=True)
        try:
            await asyncio.Event().wait()  # serve until cancelled / killed
        finally:
            if ticker is not None:
                ticker.cancel()
            await runner.cleanup()


def main() -> None:
    ap = argparse.ArgumentParser(description="Cloud-free juice server for e2e tests")
    ap.add_argument("--port", type=int, default=8099)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--db", default=None, help="Fixture DB; seeded into a temp file if omitted.")
    ap.add_argument(
        "--interactive",
        action="store_true",
        help="Install fake plug objects + a live readings tick so power-control "
        "and SSE flows work (Phase 2). Default is read-only.",
    )
    args = ap.parse_args()

    db_path = args.db
    if db_path is None:
        db_path = str(Path(tempfile.gettempdir()) / "juice-e2e-fixture.duckdb")
        print(f"seeding fixture → {db_path}", flush=True)
        seed_fixture_db(db_path)  # idempotent: removes any existing file first

    try:
        asyncio.run(_run(db_path, args.host, args.port, args.interactive))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
