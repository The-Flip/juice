"""Click CLI for juice power monitoring."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta

import click

from juice.collector import connect


@click.group()
@click.option(
    "--username", "-u", envvar="KASA_USERNAME", required=True, help="TP-Link account email."
)
@click.option(
    "--password", "-p", envvar="KASA_PASSWORD", required=True, help="TP-Link account password."
)
@click.pass_context
def cli(ctx: click.Context, username: str, password: str) -> None:
    """Juice — pinball machine power monitoring."""
    ctx.ensure_object(dict)
    ctx.obj["username"] = username
    ctx.obj["password"] = password


@cli.command()
@click.pass_context
def discover(ctx: click.Context) -> None:
    """Discover Kasa devices on the account.

    Lists every device the cloud reports — including ones juice doesn't support
    (flagged) and offline ones — so a swapped-in plug that silently vanishes
    from the dashboard is visible here.
    """
    from juice.collector import _build_device, _decode_alias

    async def _run() -> None:
        async with connect(ctx.obj["username"], ctx.obj["password"]) as account:
            raw = await account.raw_devices()
            if not raw:
                click.echo("No devices found.")
                return
            for dev in raw:
                model = dev.get("deviceModel", "?")
                # Newer Kasa devices report alias base64-encoded; decode so the
                # operator can recognise the physical plug.
                alias = _decode_alias(dev.get("alias", "?"))
                dev_id = dev.get("deviceId", "")[:12]
                status = "online" if dev.get("status") else "OFFLINE"
                supported = _build_device(dev, account) is not None
                flag = "" if supported else "  [UNSUPPORTED MODEL]"
                click.echo(f"[{status:>7}] {alias}  {model}  {dev_id}...{flag}")

    asyncio.run(_run())


@cli.command()
@click.argument("device_id")
@click.pass_context
def status(ctx: click.Context, device_id: str) -> None:
    """Show current power readings for a device (strip or outlet)."""

    async def _run() -> None:
        async with connect(ctx.obj["username"], ctx.obj["password"]) as account:
            device = await account.device(device_id)
            reading = await device.read()
            click.echo(f"{reading.alias}")
            for p in reading.plugs:
                state = "ON" if p.is_on else "OFF"
                if p.watts is None:
                    click.echo(f"  {p.alias}: {state}  (no power data)")
                else:
                    click.echo(f"  {p.alias}: {state}  {p.watts:.1f}W")

    asyncio.run(_run())


@cli.command(name="overload-report")
@click.option("--db", default="juice.duckdb", type=click.Path(), help="DuckDB file path.")
@click.option("--days", default=35, help="How many days of readings to scan for episodes.")
@click.pass_context
def overload_report(ctx: click.Context, db: str, days: int) -> None:
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

            win = OverloadWindow()
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
@click.option("--db", default="juice.duckdb", type=click.Path(), help="DuckDB file path.")
@click.pass_context
def doctor(ctx: click.Context, db: str) -> None:
    """Diagnose device + assignment health after a plug shuffle.

    Probes every Kasa device for online/offline, cross-references the DB's
    current assignments, and flags the two things that silently break the
    dashboard: online outlets with no asset tag (so a moved machine never gets
    assigned) and assignments whose outlet has vanished.
    """
    from juice.recorder import extract_asset_tag
    from juice.store import Store

    async def _run() -> None:
        with Store(db) as store:
            open_assignments = store.list_open_assignments()
        # (device_id, child_id) -> (asset_id, machine_name)
        assigned = {
            (did, cid): (asset, name) for _, did, cid, _, _, asset, name in open_assignments
        }

        async with connect(ctx.obj["username"], ctx.obj["password"]) as account:
            devices = await account.devices()
            discovered_ids = {d.device_id for d in devices}

            relabel_candidates: list[str] = []
            click.echo("=== Devices ===")
            for d in devices:
                try:
                    children = await d.child_states()
                    online = True
                except Exception as e:  # offline / unreachable
                    online = False
                    err = e

                if not online:
                    click.echo(f"[OFFLINE] {d.alias}  {d.model}  {d.device_id[:12]}...  ({err})")
                    # List the machines this dead device is supposed to be running.
                    for (did, _cid), (asset, name) in assigned.items():
                        if did == d.device_id:
                            click.echo(f"            affects: {name} ({asset})")
                    continue

                click.echo(f"[online ] {d.alias}  {d.model}  {d.device_id[:12]}...")
                for c in children:
                    alias = c["alias"]
                    tag = extract_asset_tag(alias)
                    mapped = assigned.get((d.device_id, c["id"]))
                    powered = bool(c.get("state"))
                    if mapped:
                        click.echo(f"            {alias}  ->  {mapped[1]} ({mapped[0]})")
                    elif tag:
                        click.echo(f"            {alias}  ->  tag {tag} (not in assignments)")
                    else:
                        flag = "  <-- powered, no asset tag" if powered else "  (no tag)"
                        click.echo(f"            {alias}{flag}")
                        if powered:
                            relabel_candidates.append(f'{d.device_id[:12]}...  "{alias}"')

            click.echo("\n=== Relabel candidates (online + powered, no asset tag) ===")
            if relabel_candidates:
                click.echo("Rename the outlet in the Kasa app to include the machine's tag,")
                click.echo("e.g. 'Star Trip - M0009'. Auto-assigns within ~60s.")
                for line in relabel_candidates:
                    click.echo(f"  {line}")
            else:
                click.echo("  none")

            click.echo("\n=== Stale assignments (outlet no longer discovered) ===")
            stale = [
                (asset, name, did)
                for (did, _cid), (asset, name) in assigned.items()
                if did not in discovered_ids
            ]
            if stale:
                for asset, name, did in stale:
                    click.echo(f"  {name} ({asset}) on {did[:12]}...  — reassign or clear")
            else:
                click.echo("  none")

    asyncio.run(_run())


@cli.command()
@click.argument("device_id")
@click.option("--interval", "-i", default=5.0, help="Seconds between readings.")
@click.pass_context
def monitor(ctx: click.Context, device_id: str, interval: float) -> None:
    """Continuously poll and display readings for a device (strip or outlet)."""

    async def _run() -> None:
        async with connect(ctx.obj["username"], ctx.obj["password"]) as account:
            device = await account.device(device_id)
            click.echo(f"Monitoring {device.alias} every {interval}s (Ctrl+C to stop)\n")
            try:
                while True:
                    start = asyncio.get_running_loop().time()
                    reading = await device.read()
                    ts = datetime.now().strftime("%H:%M:%S")
                    lines = []
                    for p in reading.plugs:
                        if p.watts is None:
                            if p.is_on:
                                lines.append(f"  {p.alias}: ON  (no power data)")
                        elif p.watts > 0:
                            lines.append(f"  {p.alias}: {p.watts:.1f}W  {p.amps:.3f}A")
                    if lines:
                        click.echo(f"[{ts}]\n" + "\n".join(lines))
                    else:
                        click.echo(f"[{ts}]  (all idle)")
                    elapsed = asyncio.get_running_loop().time() - start
                    await asyncio.sleep(max(0, interval - elapsed))
            except KeyboardInterrupt:
                click.echo("\nStopped.")

    asyncio.run(_run())


@cli.command("record")
@click.option("--db", default="juice.duckdb", type=click.Path(), help="DuckDB file path.")
@click.option("--flipfix-url", envvar="FLIPFIX_API_URL", default=None, help="FlipFix API base URL.")
@click.option("--flipfix-key", envvar="FLIPFIX_API_KEY", default=None, help="FlipFix API key.")
@click.pass_context
def record_cmd(
    ctx: click.Context, db: str, flipfix_url: str | None, flipfix_key: str | None
) -> None:
    """Record power readings to DuckDB."""
    from juice.recorder import record
    from juice.store import Store

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    log = logging.getLogger(__name__)

    async def _run() -> None:
        with Store(db) as store:
            log.info("Connecting to TP-Link cloud...")
            async with connect(ctx.obj["username"], ctx.obj["password"]) as account:
                log.info("Connected. Starting recorder.")
                click.echo(f"Recording to {db} (Ctrl+C to stop)")
                await record(account, store, flipfix_url, flipfix_key)

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
@click.pass_context
def serve_cmd(
    ctx: click.Context,
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
) -> None:
    """Record power readings and serve the web dashboard."""
    from juice.recorder import record
    from juice.server import SEED_CALIBRATIONS, RecorderState, start_server
    from juice.store import Store

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

    async def _run() -> None:
        with Store(db) as store:
            store.seed_calibrations(SEED_CALIBRATIONS)
            recorder_state = RecorderState()

            log.info("Connecting to TP-Link cloud...")
            async with connect(ctx.obj["username"], ctx.obj["password"]) as account:
                runner = await start_server(
                    recorder_state,
                    store,
                    host,
                    port,
                    oauth_config=oauth_config,
                    backup_token=backup_token,
                )
                log.info("Dashboard at http://%s:%d/", host, port)
                try:
                    await record(account, store, flipfix_url, flipfix_key, recorder_state)
                finally:
                    await runner.cleanup()

    asyncio.run(_run())
