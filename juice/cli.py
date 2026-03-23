"""Click CLI for juice power monitoring."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime

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
    """Discover Kasa power strips on the account."""

    async def _run() -> None:
        async with connect(ctx.obj["username"], ctx.obj["password"]) as account:
            strips = await account.strips()
            if not strips:
                click.echo("No power strips found.")
                return
            for s in strips:
                click.echo(f"{s.alias}  {s.model}  {s.device_id[:12]}...")

    asyncio.run(_run())


@cli.command()
@click.argument("device_id")
@click.pass_context
def status(ctx: click.Context, device_id: str) -> None:
    """Show current power readings for a strip."""

    async def _run() -> None:
        async with connect(ctx.obj["username"], ctx.obj["password"]) as account:
            strip = await account.strip(device_id)
            reading = await strip.read()
            click.echo(f"{reading.alias}")
            for p in reading.plugs:
                state = "ON" if p.is_on else "OFF"
                click.echo(f"  {p.alias}: {state}  {p.watts:.1f}W")

    asyncio.run(_run())


@cli.command()
@click.argument("device_id")
@click.option("--interval", "-i", default=5.0, help="Seconds between readings.")
@click.pass_context
def monitor(ctx: click.Context, device_id: str, interval: float) -> None:
    """Continuously poll and display power readings."""

    async def _run() -> None:
        async with connect(ctx.obj["username"], ctx.obj["password"]) as account:
            strip = await account.strip(device_id)
            click.echo(f"Monitoring {strip.alias} every {interval}s (Ctrl+C to stop)\n")
            try:
                while True:
                    start = asyncio.get_running_loop().time()
                    reading = await strip.read()
                    ts = datetime.now().strftime("%H:%M:%S")
                    lines = []
                    for p in reading.plugs:
                        if p.watts > 0:
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
@click.pass_context
def serve_cmd(
    ctx: click.Context,
    db: str,
    host: str,
    port: int,
    flipfix_url: str | None,
    flipfix_key: str | None,
) -> None:
    """Record power readings and serve the web dashboard."""
    from juice.recorder import record
    from juice.server import SEED_CALIBRATIONS, RecorderState, start_server
    from juice.store import Store

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    log = logging.getLogger(__name__)

    async def _run() -> None:
        with Store(db) as store:
            store.seed_calibrations(SEED_CALIBRATIONS)
            recorder_state = RecorderState()

            log.info("Connecting to TP-Link cloud...")
            async with connect(ctx.obj["username"], ctx.obj["password"]) as account:
                runner = await start_server(recorder_state, store, host, port)
                log.info("Dashboard at http://%s:%d/", host, port)
                try:
                    await record(account, store, flipfix_url, flipfix_key, recorder_state)
                finally:
                    await runner.cleanup()

    asyncio.run(_run())
