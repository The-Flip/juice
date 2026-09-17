"""Click CLI for juice power monitoring."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

import click


async def _air_loop(app_key: str, app_secret: str, store: object) -> None:
    """Open a Qingping session and poll air monitors forever.

    Run beside the collector via asyncio.gather, so air is purely additive.
    """
    from juice.air_collector import air_record
    from juice.air_collector import connect as air_connect

    async with air_connect(app_key, app_secret) as air_account:
        await air_record(air_account, store)  # type: ignore[arg-type]


@click.group()
def cli() -> None:
    """Juice — pinball machine power monitoring."""


@cli.command(name="overload-report")
@click.option("--db", default="juice.duckdb", type=click.Path(), help="DuckDB file path.")
@click.option("--days", default=35, help="How many days of readings to scan for episodes.")
@click.option(
    "--max-gap",
    default=None,
    type=click.FloatRange(min=0, min_open=True),
    help="Widest hole (seconds) a window may span and still be believed. Defaults to "
    "the live detector's bound; readings from before the tap cutover (2026-09-16) "
    "arrived 6-9 s apart and want 30.",
)
def overload_report(db: str, days: int, max_gap: float | None) -> None:
    """Backtest overload detection over stored readings.

    Replays history through the SAME detector the recorder runs live and prints
    every episode it would have flagged (machine, start, duration, peak sustained
    watts, baseline). Use it to validate thresholds against real data before
    trusting auto-shutdown — it never touches a device.

    `--days` scopes only the readings scanned; baselines always use the
    production window (BASELINE_DAYS) so the thresholds match the live detector.
    """
    from zoneinfo import ZoneInfo

    from juice.overload import (
        FLOOR_WATTS,
        REL_MULTIPLIER,
        SUSTAIN_SECONDS,
        OverloadWindow,
        threshold_for,
    )
    from juice.store import Store

    central = ZoneInfo("America/Chicago")
    rule = f"{REL_MULTIPLIER}x baseline (floor {FLOOR_WATTS:.0f}W) sustained {SUSTAIN_SECONDS}s"

    with Store(db) as store:
        # Baseline uses the production window (default BASELINE_DAYS), independent
        # of --days, so the replay matches the live detector's thresholds.
        baselines = store.refresh_power_baselines()  # machine_id -> baseline
        names = {mid: (asset, name) for mid, asset, name in _machine_index(store)}
        click.echo(f"Armed machines (>= baseline history): {len(baselines)}")

        episodes: list[dict] = []
        for machine_id, baseline in baselines.items():
            rows = store._conn.execute(
                """
                SELECT r.ts, r.watts
                FROM readings r
                JOIN assignments a
                  ON a.plug_id = r.plug_id
                 AND r.ts >= a.assigned_from
                 AND (a.assigned_until IS NULL OR r.ts < a.assigned_until)
                WHERE a.machine_id = ? AND r.watts IS NOT NULL
                  AND r.ts >= (now() - INTERVAL (?) DAY)
                ORDER BY r.ts
                """,
                [machine_id, days],
            ).fetchall()

            win = OverloadWindow() if max_gap is None else OverloadWindow(max_gap_seconds=max_gap)
            cur: dict | None = None
            for ts, watts in rows:
                win.add(ts, float(watts))
                fire, mean_w = win.verdict(baseline)
                if fire:
                    if cur and (ts - cur["last"]).total_seconds() <= 180:
                        cur["last"] = ts
                        cur["peak"] = max(cur["peak"], mean_w)
                    else:
                        if cur:
                            episodes.append(cur)
                        cur = {"machine_id": machine_id, "first": ts, "last": ts, "peak": mean_w}
            if cur:
                episodes.append(cur)

        click.echo(f"\n=== Overload episodes (trigger: {rule}) ===")
        if not episodes:
            click.echo("  none")
        for e in sorted(episodes, key=lambda x: x["first"]):
            asset, name = names.get(e["machine_id"], ("?", f"machine {e['machine_id']}"))
            start = e["first"].replace(tzinfo=ZoneInfo("UTC")).astimezone(central)
            # The load began ~one sustain window before the first detection.
            onset = start - timedelta(seconds=SUSTAIN_SECONDS)
            dur_min = (e["last"] - e["first"]).total_seconds() / 60 + SUSTAIN_SECONDS / 60
            base = baselines[e["machine_id"]]
            click.echo(
                f"  {name[:26]:26s} ({asset})  {onset:%Y-%m-%d %H:%M} Central  "
                f"dur~{dur_min:4.0f}min  peak={e['peak']:4.0f}W  "
                f"baseline={base:3.0f}W  threshold={threshold_for(base):3.0f}W"
            )


def _machine_index(store) -> list[tuple[int, str, str]]:
    return [
        (row[0], row[1], row[2])
        for row in store._conn.execute("SELECT machine_id, asset_id, name FROM machines").fetchall()
    ]


@cli.command()
@click.option(
    "--db",
    default="juice.duckdb",
    # `exists=True`: `Store` would create a missing file, and a doctor that
    # reported "none" three times about a database that was never there is
    # the one failure mode worse than crashing.
    type=click.Path(exists=True, dir_okay=False),
    help="DuckDB file path.",
)
@click.option(
    "--days",
    default=7,
    show_default=True,
    help="An outlet that has not reported for this long is quiet.",
)
def doctor(db: str, days: int) -> None:
    """Diagnose outlet and assignment health from the store alone.

    Three things silently degrade the floor after a plug shuffle or a dead
    strip, and none of them needs a device probe to see: outlets that have
    gone quiet (and the machines the store still puts on them), outlets that
    are drawing power under a label with no asset tag (so the machine on them
    never gets assigned), and a machine the store has on two outlets at once
    (it moved, and the old assignment was never closed). Needs no tap and no
    credentials; the DB must not be held open by a running server.
    """
    from juice.identity import extract_asset_tag
    from juice.store import Store

    now = datetime.now(UTC)
    cutoff = now - timedelta(days=days)
    with Store(db) as store:
        plugs = store.list_plugs()
        last = store.plug_last_readings()
        assigned = store.list_open_assignments()

    # plug_id -> [(asset_id, machine_name), ...]; more than one is a finding.
    on_plug: dict[int, list[tuple[str, str]]] = {}
    by_machine: dict[str, list[tuple[str, int, str, str]]] = {}
    for plug_id, device_id, child_id, _alias, _em, asset, name in assigned:
        on_plug.setdefault(plug_id, []).append((asset, name))
        by_machine.setdefault(asset, []).append((name, plug_id, device_id, child_id))

    def outlet(device_id: str, child_id: str) -> str:
        # A single-outlet device (EP10) has an empty child id.
        return f"{device_id}/{child_id}" if child_id else device_id

    click.echo(f"=== Quiet outlets (no reading in {days} days) ===")
    quiet = 0
    for plug_id, device_id, child_id, alias, _em in plugs:
        seen = last.get(plug_id)
        if seen is not None and seen[0] >= cutoff:
            continue
        quiet += 1
        when = "never reported" if seen is None else f"last {(now - seen[0]).days} days ago"
        click.echo(f'  {outlet(device_id, child_id)}  "{alias}"  {when}')
        for asset, name in on_plug.get(plug_id, []):
            click.echo(f"      affects: {name} ({asset}) -- reassign or clear")
    if not quiet:
        click.echo("  none")

    click.echo("\n=== Relabel candidates (drawing power, no asset tag) ===")
    relabel = 0
    for plug_id, device_id, child_id, alias, _em in plugs:
        seen = last.get(plug_id)
        if seen is None or seen[0] < cutoff or extract_asset_tag(alias):
            continue
        _ts, watts, relay_on = seen
        # Metered: drawing means watts. Unmetered (NULL watts): the relay is
        # all there is to go on, and only tap-era rows carry it.
        if watts is not None:
            if watts <= 0:
                continue
            draw = f"{watts:.0f} W"
        elif relay_on:
            draw = "on, unmetered"
        else:
            continue
        relabel += 1
        click.echo(f'  {outlet(device_id, child_id)}  "{alias}"  {draw}')
    if relabel:
        click.echo("  Not every load is a machine (neon, display cases). For one that is,")
        click.echo("  rename the outlet in the Kasa app to include the machine's tag, e.g.")
        click.echo("  'Star Trip - M0009'; tap re-sends its roster on the change.")
    else:
        click.echo("  none")

    click.echo("\n=== Machines on more than one outlet ===")
    doubled = 0
    for asset, rows in sorted(by_machine.items()):
        if len(rows) < 2:
            continue
        doubled += 1
        click.echo(f"  {rows[0][0]} ({asset})")
        for _name, plug_id, device_id, child_id in rows:
            seen = last.get(plug_id)
            when = "never reported" if seen is None else f"last {(now - seen[0]).days} days ago"
            click.echo(f"      {outlet(device_id, child_id)}  {when}")
    if not doubled:
        click.echo("  none")


@cli.command("air-discover")
@click.option("--qingping-key", envvar="QINGPING_APP_KEY", default=None, help="Qingping App Key.")
@click.option(
    "--qingping-secret", envvar="QINGPING_APP_SECRET", default=None, help="Qingping App Secret."
)
def air_discover(qingping_key: str | None, qingping_secret: str | None) -> None:
    """List Qingping air monitors and their latest readings.

    Uses the Qingping cloud (App Key/Secret from developer.qingping.co), which
    is separate from the Kasa account — so it doesn't need the Kasa credentials.
    """
    from juice.air_collector import connect as air_connect

    if not (qingping_key and qingping_secret):
        raise click.UsageError(
            "Set QINGPING_APP_KEY and QINGPING_APP_SECRET (or pass --qingping-key/--qingping-secret)."
        )

    async def _run() -> None:
        async with air_connect(qingping_key, qingping_secret) as account:
            pairs = await account.devices()
            if not pairs:
                click.echo("No air monitors found.")
                return
            for sensor, r in pairs:
                status = "online" if sensor.online else "OFFLINE"
                parts = []
                if r.temperature is not None:
                    parts.append(f"{r.temperature:.1f}°C")
                if r.humidity is not None:
                    parts.append(f"{r.humidity:.0f}%RH")
                if r.co2 is not None:
                    parts.append(f"CO2 {r.co2:.0f}ppm")
                if r.pm25 is not None:
                    parts.append(f"PM2.5 {r.pm25:.0f}")
                metrics = "  ".join(parts) if parts else "(no data)"
                click.echo(f"[{status:>7}] {sensor.name or sensor.mac}  ({sensor.mac})  {metrics}")

    asyncio.run(_run())


@cli.command("serve")
@click.option("--db", default="juice.duckdb", type=click.Path(), help="DuckDB file path.")
@click.option("--host", default="0.0.0.0", help="Server bind address.")  # noqa: S104
@click.option("--port", default=8000, type=int, help="Server port.")
@click.option("--flipfix-url", envvar="FLIPFIX_API_URL", default=None, help="FlipFix API base URL.")
@click.option("--flipfix-key", envvar="FLIPFIX_API_KEY", default=None, help="FlipFix API key.")
@click.option("--oauth-client-id", envvar="OAUTH_CLIENT_ID", default=None, help="OAuth client ID.")
@click.option(
    "--oauth-client-secret", envvar="OAUTH_CLIENT_SECRET", default=None, help="OAuth client secret."
)
@click.option(
    "--oauth-provider-url", envvar="OAUTH_PROVIDER_URL", default=None, help="OAuth provider URL."
)
@click.option(
    "--oauth-redirect-uri",
    envvar="OAUTH_REDIRECT_URI",
    default=None,
    help="OAuth redirect URI (defaults to http://host:port/callback).",
)
@click.option(
    "--backup-token",
    envvar="JUICE_BACKUP_TOKEN",
    default=None,
    help="Secret token enabling GET /api/backup. Unset disables the endpoint.",
)
@click.option(
    "--public-url",
    envvar="JUICE_PUBLIC_URL",
    default=None,
    help="Juice's public base URL (e.g. https://juice.theflip.museum) for FlipFix deep links.",
)
@click.option("--qingping-key", envvar="QINGPING_APP_KEY", default=None, help="Qingping App Key.")
@click.option(
    "--qingping-secret", envvar="QINGPING_APP_SECRET", default=None, help="Qingping App Secret."
)
@click.option(
    "--ingest-token",
    envvar="JUICE_INGEST_TOKEN",
    default=None,
    help="Secret token for the tap collector's WebSocket at /api/v2/ingest. Required: "
    "every reading arrives over that route, which is not registered without one. Must "
    "match tap's TAP_UPLINK_TOKEN.",
)
@click.option(
    "--raw-retention-days",
    envvar="JUICE_RAW_RETENTION_DAYS",
    default=None,
    type=int,
    help="Days of raw readings to keep (default 90; 0 disables pruning).",
)
@click.option(
    "--dev-auth/--no-dev-auth",
    envvar="JUICE_DEV_AUTH",
    default=False,
    help="LOCAL DEV ONLY: when OAuth isn't configured, enable a one-click login shim "
    "(grants control_power). Without it, a no-OAuth serve refuses to start.",
)
def serve_cmd(
    db: str,
    host: str,
    port: int,
    flipfix_url: str | None,
    flipfix_key: str | None,
    oauth_client_id: str | None,
    oauth_client_secret: str | None,
    oauth_provider_url: str | None,
    oauth_redirect_uri: str | None,
    backup_token: str | None,
    public_url: str | None,
    qingping_key: str | None,
    qingping_secret: str | None,
    ingest_token: str | None,
    raw_retention_days: int | None,
    dev_auth: bool,
) -> None:
    """Record power readings and serve the web dashboard."""
    from juice.retention import DEFAULT_RETENTION_DAYS

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    log = logging.getLogger(__name__)

    oauth_config = None
    if oauth_client_id and oauth_client_secret and oauth_provider_url:
        oauth_config = {
            "client_id": oauth_client_id,
            "client_secret": oauth_client_secret,
            "provider_url": oauth_provider_url,
            "redirect_uri": oauth_redirect_uri or f"http://{host}:{port}/callback",
        }

    # Fail closed: never serve without authentication. Production must set the
    # OAUTH_* vars; local dev must opt in explicitly via --dev-auth so a
    # deployment with missing OAuth env can't silently fall back to one-click
    # operator login.
    if oauth_config is None:
        if not dev_auth:
            raise click.UsageError(
                "No OAuth configured. Set OAUTH_CLIENT_ID / OAUTH_CLIENT_SECRET / "
                "OAUTH_PROVIDER_URL for production, or pass --dev-auth "
                "(JUICE_DEV_AUTH=1) for a LOCAL one-click dev login. Refusing to "
                "serve without authentication."
            )
        log.warning(
            "OAuth not configured — enabling the DEV one-click login shim "
            "(local use only; do NOT expose this server)."
        )

    if not ingest_token:
        # Fail closed: the ingest route is only registered with a token, and a
        # server without it looks healthy and collects nothing, forever.
        # Checked before Store(db) so a refused start leaves no database file.
        raise click.UsageError(
            "serve needs --ingest-token (JUICE_INGEST_TOKEN): every reading arrives over "
            "/api/v2/ingest, which is not registered without one."
        )
    log.info(
        "Readings, roster, live state and power control come from the tap daemon over "
        "/api/v2/ingest. Overload protection is %s.",
        _overload_mode_for_log(),
    )

    retention_days = DEFAULT_RETENTION_DAYS if raw_retention_days is None else raw_retention_days
    server_kwargs = {
        "host": host,
        "port": port,
        "oauth_config": oauth_config,
        "backup_token": backup_token,
        "dev_auth": dev_auth,
        "ingest_token": ingest_token,
    }
    asyncio.run(
        _serve(
            db,
            server_kwargs,
            flipfix_url=flipfix_url,
            flipfix_key=flipfix_key,
            public_url=public_url,
            qingping=(qingping_key, qingping_secret),
            retention_days=retention_days,
        )
    )


def _overload_mode_for_log() -> str:
    import os

    from juice.overload import resolve_overload_mode

    return resolve_overload_mode(os.environ.get("JUICE_OVERLOAD_PROTECTION"))


async def _serve(
    db: str,
    server_kwargs: dict,
    *,
    flipfix_url: str | None,
    flipfix_key: str | None,
    public_url: str | None,
    qingping: tuple[str | None, str | None],
    retention_days: int,
) -> None:
    """The server. It has no poll loop of its own:

    The tap daemon's frames arrive on the ingest socket and are projected by
    the three seams `create_app` installs -- roster, live, control -- and
    `run_tap_collector` wraps them with the startup, the 1 Hz sweep and the
    minute-cadence housekeeping. Rollups, retention and air run beside it;
    they never depended on the collector.
    """
    from juice.collector_tap import (
        LiveProjector,
        TapControl,
        roster_projection,
        run_tap_collector,
    )
    from juice.loopwatch import stall_monitor
    from juice.retention import retention_loop
    from juice.rollups import RollupWorker, rollup_loop
    from juice.server import SEED_CALIBRATIONS, RecorderState, start_server
    from juice.store import Store

    log = logging.getLogger(__name__)
    with Store(db) as store:
        store.seed_calibrations(SEED_CALIBRATIONS)
        recorder_state = RecorderState()
        rollups = RollupWorker(store)
        control = TapControl()
        projector = LiveProjector(recorder_state, store)
        runner = await start_server(
            recorder_state,
            store,
            rollups=rollups,
            tap_devices=roster_projection(recorder_state, store, control),
            tap_live=projector,
            tap_control=control,
            **server_kwargs,
        )
        log.info("Dashboard at http://%s:%d/", server_kwargs["host"], server_kwargs["port"])
        try:
            tasks = [
                run_tap_collector(
                    recorder_state,
                    store,
                    rollups,
                    control,
                    projector,
                    flipfix_url=flipfix_url,
                    flipfix_key=flipfix_key,
                    public_url=public_url,
                ),
                rollup_loop(store, rollups, recorder_state),
                retention_loop(store, retention_days),
                stall_monitor(),
            ]
            if all(qingping):
                tasks.append(_air_loop(qingping[0], qingping[1], store))  # type: ignore[arg-type]
            await asyncio.gather(*tasks)
        finally:
            # Sockets first, so no incoming frame restarts an apply behind the
            # settle; then the in-flight apply; then the worker whose
            # connection the apply might still be writing through.
            await runner.cleanup()
            await projector.settle()
            projector.close()
            rollups.close()


@cli.command("prune")
@click.option("--db", default="juice.duckdb", type=click.Path(), help="DuckDB file path.")
@click.option(
    "--days",
    envvar="JUICE_RAW_RETENTION_DAYS",
    default=None,
    type=int,
    help="Days of raw readings to keep (default 90; 0 disables).",
)
@click.option("--dry-run", is_flag=True, help="Report what would be deleted, delete nothing.")
def prune_cmd(db: str, days: int | None, dry_run: bool) -> None:
    """Delete raw readings older than the retention window.

    Normally the server does this on its own schedule; this is for running it
    by hand, and for seeing *why* it declines to run.
    """
    from juice.retention import DEFAULT_RETENTION_DAYS
    from juice.store import Store

    retention = DEFAULT_RETENTION_DAYS if days is None else days
    with Store(db) as store:
        cutoff = store.prunable_before(retention)
        if cutoff is None:
            click.echo(
                f"Not pruning (retention {retention}d). Either it is disabled, below the "
                f"minimum, the rollups have not caught up, one of them is empty, or the "
                f"retro play-hours migration has not run. Nothing was deleted."
            )
            return
        pending = store._conn.execute(
            "SELECT count(*) FROM readings WHERE ts < ?", [cutoff]
        ).fetchone()[0]
        if dry_run:
            click.echo(
                f"Would delete {pending} readings older than {cutoff} (retention {retention}d)."
            )
            return
        deleted = store.prune_readings(cutoff)
        click.echo(f"Deleted {deleted} readings older than {cutoff}.")


@cli.command("tui")
@click.option(
    "--url",
    envvar="JUICE_TUI_URL",
    default="http://127.0.0.1:8000",
    help="Base URL of the juice server to inspect.",
)
@click.option(
    "--login/--no-login",
    default=False,
    help="Log in on startup via the dev-auth shim, so operator-only fields are visible.",
)
@click.option(
    "--cookie",
    multiple=True,
    metavar="NAME=VALUE",
    help=(
        "Session cookie to send (repeatable). Use against a server with real "
        "OAuth, where --login cannot work: copy AIOHTTP_SESSION out of a "
        "logged-in browser. A bare value is taken as AIOHTTP_SESSION."
    ),
)
def tui_cmd(url: str, login: bool, cookie: tuple[str, ...]) -> None:
    """Browse /api/v2 in a terminal UI.

    A read-only client built against api_v2.md alone, for evaluating the v2
    contract: a machine table plus a live view of the SSE stream. Point it at
    the e2e fixture server to exercise it without a cloud:

        uv run python -m tests.e2e.serve --port 8150 --interactive --with-problems
        uv run juice tui --url http://localhost:8150 --login
    """
    from juice.tui.__main__ import run

    raise SystemExit(run(url, login=login, cookies=list(cookie)))
