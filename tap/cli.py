"""The `tap` command line.

House conventions borrowed from juice's CLI: every heavy import happens inside a
command body so `--help` stays fast and a machine without a device library can
still run the commands that do not need one; options carry `envvar=`; long-
running commands define a nested `async def _run()` and finish with
`asyncio.run`.

This is the only module in `tap` allowed to print.
"""

from __future__ import annotations

import asyncio
import json
import sys

import click

from tap import __version__
from tap.errors import EXIT_CONFIG, FatalError
from tap.logmod import setup_logging

_KASA_HINT = (
    "tap needs python-kasa to talk to devices: uv sync --extra tap "
    "(or pip install 'python-kasa>=0.10.2')"
)


def _load(ctx: click.Context, **overrides):
    """Build the effective config, turning a bad one into a clean CLI error."""
    from tap.config import load_config

    try:
        return load_config(path=ctx.obj.get("config"), overrides=overrides)
    except FatalError as e:
        raise click.ClickException(str(e)) from None


def _credentials(config, host: str):
    from tap.config import DeviceSpec

    return config.credentials_for(DeviceSpec(host=host))


@click.group()
@click.version_option(__version__, prog_name="tap")
@click.option(
    "--config",
    "-c",
    envvar="TAP_CONFIG",
    default=None,
    type=click.Path(),
    help="Path to tap.toml. Defaults to ./tap.toml, then /etc/tap/tap.toml.",
)
@click.pass_context
def cli(ctx: click.Context, config: str | None) -> None:
    """tap — a local collector for smart-plug power data."""
    ctx.ensure_object(dict)
    ctx.obj["config"] = config


@cli.command("run")
@click.option("--buffer-dir", default=None, type=click.Path(), help="Where to keep the buffer.")
@click.option("--retention-days", default=None, type=int, help="Days of readings to keep.")
@click.option("--web-host", default=None, help="Status page bind address.")
@click.option("--web-port", default=None, type=int, help="Status page port.")
@click.option("--uplink-url", default=None, help="WebSocket URL of the server.")
@click.option(
    "--uplink-token", default=None, envvar="TAP_UPLINK_TOKEN", help="Bearer token for the server."
)
@click.option("--no-uplink", is_flag=True, default=None, help="Buffer locally, upload nothing.")
@click.option("--no-discovery", is_flag=True, default=None, help="Poll only pinned devices.")
@click.option("--tap-id", default=None, help="Identifies this collector to the server.")
@click.option("--log-level", default=None, help="DEBUG, INFO, WARNING, ERROR.")
@click.pass_context
def run_cmd(ctx: click.Context, **overrides) -> None:
    """Poll devices, buffer readings, and serve the status page."""
    config = _load(ctx, **overrides)
    setup_logging(config.log_level)

    from tap.supervise import run

    try:
        code = asyncio.run(run(config, config_path=ctx.obj.get("config"), overrides=overrides))
    except ModuleNotFoundError as e:  # pragma: no cover - depends on the env
        if e.name != "kasa":
            raise
        raise click.ClickException(_KASA_HINT) from None
    except FatalError as e:
        # A fatal raised before the supervisor's own handler is installed must
        # still exit with its code, so the supervisor's restart is deliberate.
        click.echo(f"fatal: {e}", err=True)
        code = e.code
    except KeyboardInterrupt:  # pragma: no cover - interactive only
        code = 0
    sys.exit(code)


@cli.command("probe")
@click.argument("host")
@click.option(
    "--family",
    type=click.Choice(["auto", "smart", "iot"]),
    default="auto",
    help="Protocol to use. 'auto' asks the device.",
)
@click.option("--count", default=1, type=int, help="How many sweeps to take.")
@click.pass_context
def probe_cmd(ctx: click.Context, host: str, family: str, count: int) -> None:
    """Read one device once and print the result, with timings."""
    config = _load(ctx)
    setup_logging(config.log_level)

    async def _run() -> None:
        from tap.config import DeviceSpec
        from tap.device import Family
        from tap.kasa_common import probe_family
        from tap.poller import build_device

        resolved = Family(family)
        if resolved is Family.AUTO:
            resolved, model = await probe_family(host, credentials=_credentials(config, host))
            click.echo(f"{host} speaks {resolved} ({model})")
        spec = DeviceSpec(host=host, family=resolved)
        device = build_device(spec, config)
        await device.open()
        try:
            click.echo(f"device_id {device.device_id}  model {device.model}")
            for _ in range(count):
                sweep = await device.sweep()
                # Bound to locals so the None checks narrow: `None not in (...)`
                # reads fine but tells a type checker nothing.
                listing = sweep.listing_ms
                outlets_ms = sweep.emeter_total_ms
                slowest = sweep.emeter_max_ms
                breakdown = ""
                if listing is not None and outlets_ms is not None and slowest is not None:
                    # One sweep, so this really is that sweep's share.
                    share = slowest / outlets_ms if outlets_ms else 0.0
                    breakdown = (
                        f"  (listing {listing:.0f} ms, "
                        f"outlets {outlets_ms:.0f} ms, "
                        f"slowest {slowest:.0f} ms = {share:.0%})"
                    )
                click.echo(f"--- sweep in {sweep.duration_ms:.0f} ms ---{breakdown}")
                for outlet in sweep.outlets:
                    watts = (
                        "     -" if outlet.power_mw is None else f"{outlet.power_mw / 1000:6.2f}"
                    )
                    volts = (
                        "    -" if outlet.voltage_mv is None else f"{outlet.voltage_mv / 1000:5.1f}"
                    )
                    click.echo(
                        f"  [{outlet.child_id or '(single)'}] "
                        f"{'on ' if outlet.relay_on else 'off'} "
                        f"{watts} W  {volts} V  {outlet.alias!r}"
                    )
        finally:
            await device.close()

    _dispatch(_run())


@cli.command("devices")
@click.pass_context
def devices_cmd(ctx: click.Context) -> None:
    """Discover devices on the LAN and show the resulting roster."""
    config = _load(ctx)
    setup_logging(config.log_level)

    async def _run() -> None:
        from tap.discovery import Roster, discover

        roster = Roster(config)
        found = await discover(config)
        roster.apply_discovery(found)
        specs = roster.specs
        if not specs:
            click.echo("no devices: discovery found nothing and no [[device]] is pinned")
            return
        click.echo(f"{'HOST':<18} {'FAMILY':<8} {'SOURCE':<12}")
        for host, spec in sorted(specs.items()):
            source = "config" if spec.pinned else "discovered"
            click.echo(f"{host:<18} {str(spec.family):<8} {source:<12}")

    _dispatch(_run())


@cli.command("relay")
@click.argument("target")
@click.option("--on/--off", "on", required=True, help="Which way to switch the outlet.")
@click.option(
    "--family", type=click.Choice(["auto", "smart", "iot"]), default="auto", help="Protocol to use."
)
@click.pass_context
def relay_cmd(ctx: click.Context, target: str, on: bool, family: str) -> None:
    """Switch one outlet: `tap relay HOST:CHILD_ID --off`.

    The escape hatch for when the server is unreachable. tap never initiates a
    command on its own, and the status page is read-only on purpose — local
    authorization is a problem tap should not have.
    """
    config = _load(ctx)
    setup_logging(config.log_level)
    host, _, child_id = target.partition(":")

    async def _run() -> None:
        from tap.config import DeviceSpec
        from tap.device import Family
        from tap.kasa_common import probe_family
        from tap.poller import build_device

        resolved = Family(family)
        if resolved is Family.AUTO:
            resolved, _model = await probe_family(host, credentials=_credentials(config, host))
        device = build_device(DeviceSpec(host=host, family=resolved), config)
        await device.open()
        try:
            await device.set_relay(child_id, on)
            click.echo(f"{host}:{child_id or '(single)'} -> {'on' if on else 'off'}")
        finally:
            await device.close()

    _dispatch(_run())


@cli.command("status")
@click.option("--url", default="http://127.0.0.1:8010", help="Base URL of a running tap.")
def status_cmd(url: str) -> None:
    """Fetch and print the status of a running tap."""

    async def _run() -> None:
        import aiohttp

        async with (
            aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session,
            session.get(f"{url.rstrip('/')}/api/status") as response,
        ):
            click.echo(json.dumps(await response.json(), indent=2))

    _dispatch(_run())


@cli.command("bench")
@click.option("--devices", "device_count", default=12, type=int, help="Simulated devices.")
@click.option("--outlets", default=6, type=int, help="Outlets per simulated device.")
@click.option("--ticks", default=2000, type=int, help="Simulated 1 Hz ticks.")
@click.option(
    "--buffer-dir", default=None, type=click.Path(), help="Where to write. A temp dir by default."
)
def bench_cmd(device_count: int, outlets: int, ticks: int, buffer_dir: str | None) -> None:
    """Write synthetic readings as fast as the buffer will take them.

    The buffer has to sustain roughly one row per plug per second forever. This
    measures how much headroom there actually is on this disk.
    """
    setup_logging("WARNING")

    async def _run() -> None:
        import shutil
        import statistics
        import tempfile
        import time
        from datetime import UTC, datetime, timedelta
        from pathlib import Path

        from tap.buffer import Buffer
        from tap.device import OutletReading, Sweep

        temp = buffer_dir is None
        directory = Path(buffer_dir) if buffer_dir else Path(tempfile.mkdtemp()) / "buffer"
        buffer = Buffer(directory, retention_days=30)
        await buffer.open()
        writer = asyncio.create_task(buffer.run())
        base = datetime.now(UTC)
        latencies: list[float] = []
        started = time.perf_counter()
        try:
            for tick in range(ticks):
                ts = base + timedelta(seconds=tick)
                for device in range(device_count):
                    buffer.submit(
                        Sweep(
                            device_id=f"BENCH{device:04d}",
                            ts=ts,
                            outlets=[
                                OutletReading(
                                    child_id=f"{device:04d}{i:02d}",
                                    alias=f"outlet {i}",
                                    relay_on=True,
                                    power_mw=0 if i % 3 else 84_000 + tick % 900,
                                    voltage_mv=119_000 + tick % 400,
                                    current_ma=0 if i % 3 else 700,
                                    energy_wh=tick // 3600,
                                )
                                for i in range(outlets)
                            ],
                        )
                    )
                while buffer._queue.qsize() > 24:  # noqa: SLF001 — a bench, not an API
                    await asyncio.sleep(0.001)
                if buffer._health.last_commit_ms is not None:  # noqa: SLF001
                    latencies.append(buffer._health.last_commit_ms)  # noqa: SLF001
            while buffer._queue.qsize():  # noqa: SLF001
                await asyncio.sleep(0.001)
            await buffer.flush()
        finally:
            writer.cancel()
            rows = buffer._health.rows_written  # noqa: SLF001
            dropped = buffer._health.rows_dropped  # noqa: SLF001
            await buffer.close()

        elapsed = time.perf_counter() - started
        size = sum(f.stat().st_size for f in directory.glob("*.sqlite"))
        plugs = device_count * outlets
        per_row = size / rows if rows else 0
        click.echo(f"rows written   : {rows:,} ({dropped:,} dropped)")
        click.echo(f"drain rate     : {rows / elapsed:,.0f} rows/s")
        click.echo(f"headroom       : {rows / elapsed / plugs:,.0f}x real time at {plugs} plugs")
        if latencies:
            ordered = sorted(latencies)
            click.echo(
                f"commit p50/p99 : {statistics.median(ordered):.2f} / "
                f"{ordered[int(len(ordered) * 0.99)]:.2f} ms"
            )
        click.echo(f"bytes per row  : {per_row:.1f}")
        click.echo(f"30 days at {plugs:>3} plugs : {per_row * plugs * 86400 * 30 / 1e9:.2f} GB")
        if temp:
            shutil.rmtree(directory.parent, ignore_errors=True)

    _dispatch(_run())


def _dispatch(coro) -> None:
    """Run a one-shot command, mapping tap's errors onto CLI errors."""
    try:
        asyncio.run(coro)
    except ModuleNotFoundError as e:
        if e.name != "kasa":
            raise
        raise click.ClickException(_KASA_HINT) from None
    except FatalError as e:
        raise click.ClickException(str(e)) from None
    except KeyboardInterrupt:  # pragma: no cover - interactive only
        sys.exit(EXIT_CONFIG)


if __name__ == "__main__":  # pragma: no cover
    cli()
