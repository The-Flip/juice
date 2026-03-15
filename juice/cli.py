"""Click CLI for juice power monitoring."""

from __future__ import annotations

import asyncio
from datetime import datetime

import click

from juice.collector import connect


@click.group()
@click.option("--username", "-u", envvar="KASA_USERNAME", required=True, help="TP-Link account email.")
@click.option("--password", "-p", envvar="KASA_PASSWORD", required=True, help="TP-Link account password.")
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
