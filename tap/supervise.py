"""Process supervision: what recovers in place, and what exits.

**Two tiers, because `asyncio.TaskGroup` has exactly the right semantics for one
of them and exactly the wrong ones for the other.**

Structural tasks — the buffer writer, the uplink, discovery, the watchdog — go
in a TaskGroup. If one of those dies the process is not doing its job, and the
group's cancel-siblings-and-propagate behaviour is precisely what we want.

Device pollers go *outside* the group. There, the same behaviour would be a bug:
one unreachable strip must never take down the other eleven. Every poller
catches everything, forever.

Recovery philosophy matches juice's: there is no Python restart wrapper. A
condition that cannot be recovered by trying again exits non-zero and lets the
supervisor (Docker's `restart: unless-stopped`) restart a clean process, which
rebuilds its state from the buffer on disk. A daemon that stays up doing nothing
is worse than one that visibly crash-loops.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from datetime import UTC, datetime

from tap import __version__, webui
from tap.buffer import Buffer
from tap.config import Config, load_config
from tap.device import DeviceState
from tap.discovery import Roster, discover
from tap.errors import EXIT_INTERNAL, FatalError
from tap.health import Health
from tap.logmod import set_level
from tap.poller import PollerSet
from tap.uplink import Uplink

log = logging.getLogger(__name__)

WATCHDOG_INTERVAL = 5.0
# Long enough that a whole-fleet network blip recovers on its own; short enough
# that a wedged process is restarted before anyone notices the data stopped.
NO_SWEEP_FATAL_SECONDS = 120.0
# The write path is stuck: devices are reporting but nothing reaches the disk.
STALE_BUFFER_FATAL_SECONDS = 60.0
# The event loop itself has stopped turning.
LOOP_STALL_FATAL_SECONDS = 30.0
# Nothing is judged until the process has had a chance to connect to anything.
STARTUP_GRACE_SECONDS = 90.0

# Structural config keys that a SIGHUP cannot apply. Changing them is legitimate;
# silently ignoring the change is not.
STRUCTURAL_KEYS = ("buffer_dir", "retention_days", "web", "uplink")


class Supervisor:
    """Owns every task, and decides what is fatal."""

    def __init__(self, config: Config, *, config_path=None, overrides=None) -> None:
        self._config = config
        self._config_path = config_path
        # The CLI flags this process was started with. A reload has to replay
        # them, or SIGHUP would silently demote every --flag back to whatever
        # the file says — including --buffer-dir, which would then disagree with
        # the buffer we are actually writing to.
        self._overrides = dict(overrides or {})
        self.health = Health(
            tap_id=config.tap_id,
            version=__version__,
            config_path=str(config.source_path) if config.source_path else None,
        )
        self.buffer = Buffer(
            config.buffer_dir,
            retention_days=config.retention_days,
            health=self.health.buffer,
        )
        self.pollers = PollerSet(
            config,
            self.buffer,
            self.health,
            interval=config.polling.interval_seconds,
            sweep_budget=config.polling.sweep_budget_seconds,
        )
        self.roster = Roster(config)
        self.uplink = Uplink(config, self.buffer, self.health, self.pollers)
        self._stop = asyncio.Event()
        # Set by either signal, so a sleeping loop wakes promptly for both.
        self._wake = asyncio.Event()
        self._fatal: FatalError | None = None
        self._reload = False
        self._reconcile_after_reload = False

    # ---- entry point --------------------------------------------------------

    async def run(self) -> int:
        """Run until stopped or something fatal happens. Returns an exit code."""
        try:
            # Both of these can fail fatally — an unwritable or corrupt buffer,
            # a port already in use — and both must exit with their own code
            # rather than a traceback.
            await self.buffer.open()
            runner = await webui.serve(self.health, self._config.web.host, self._config.web.port)
        except FatalError as e:
            log.error("fatal: %s", e)
            await self.buffer.close()
            return e.code
        self._install_signal_handlers()
        log.info(
            "tap %s started (id=%s, buffer=%s, retention=%dd, %d pinned device(s), "
            "discovery %s, uplink %s)",
            __version__,
            self._config.tap_id,
            self._config.buffer_dir,
            self._config.retention_days,
            len(self._config.devices),
            "on" if self._config.discovery.enabled else "off",
            self._config.uplink.url if self._config.uplink.active else "standalone",
        )
        try:
            await self._run_tasks()
        except FatalError as e:
            self._fatal = e
        finally:
            await self._shutdown(runner)

        if self._fatal is not None:
            log.error("fatal: %s — exiting %d for a restart", self._fatal, self._fatal.code)
            return self._fatal.code
        return 0

    async def _run_tasks(self) -> None:
        """Run the structural tasks until one ends or a signal stops us.

        Deliberately `asyncio.wait(FIRST_COMPLETED)` rather than a TaskGroup: a
        TaskGroup child that raises `CancelledError` counts as *cancelled*, not
        failed, so it does not cancel its siblings — which would leave the
        process hanging on shutdown. Here the first task to finish, for any
        reason, ends the process, which is exactly the intended policy: if the
        buffer writer or the uplink has stopped, tap is not doing its job.
        """
        await self.pollers.reconcile(self.roster.specs)
        tasks = {
            asyncio.create_task(self._guard(self.buffer.run(), "buffer"), name="buffer"),
            asyncio.create_task(self._guard(self.uplink.run(), "uplink"), name="uplink"),
            asyncio.create_task(self._guard(self._discovery_loop(), "discovery"), name="discovery"),
            asyncio.create_task(self._guard(self._watchdog(), "watchdog"), name="watchdog"),
        }
        stopper = asyncio.create_task(self._stop.wait(), name="stopper")
        done, pending = await asyncio.wait({*tasks, stopper}, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

        for task in done:
            if task is stopper or task.cancelled():
                continue
            exc = task.exception()
            if exc is not None:
                raise exc
            # A structural task returning cleanly is itself a problem: none of
            # them is supposed to finish while tap is running.
            if not self._stop.is_set():
                raise FatalError(f"{task.get_name()} task exited unexpectedly", EXIT_INTERNAL)

    async def _guard(self, coro, name: str) -> None:
        """Let FatalError through; turn anything else from a structural task into one."""
        try:
            await coro
        except asyncio.CancelledError, FatalError:
            raise
        except Exception as e:
            log.error("structural task %r crashed", name, exc_info=True)
            raise FatalError(f"{name} task crashed: {e}", EXIT_INTERNAL) from e

    # ---- discovery ----------------------------------------------------------

    async def _discovery_loop(self) -> None:
        while not self._stop.is_set():
            if self._reload:
                self._apply_reload()
            if self._reconcile_after_reload:
                self._reconcile_after_reload = False
                await self.pollers.reconcile(self.roster.specs)
            cfg = self._config.discovery
            if cfg.enabled:
                found = await discover(self._config)
                self.health.discovery_last = datetime.now(UTC)
                self.health.discovery_found = len(found)
                if not found:
                    log.warning("discovery: found nothing; keeping the current roster")
                added, removed = self.roster.apply_discovery(found)
                for host in added:
                    log.info("discovery: found %s", host)
                for host in removed:
                    log.info("discovery: %s gone for good; dropping", host)
                if added or removed:
                    await self.pollers.reconcile(self.roster.specs)
            # Wake early for a signal. Sleeping the full discovery interval here
            # would mean a SIGHUP took up to five minutes to do anything, which
            # is not what anybody pressing it expects.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout=cfg.interval_seconds)
            self._wake.clear()

    def _apply_reload(self) -> None:
        """Apply a SIGHUP: roster and log level only, saying so when more changed."""
        self._reload = False
        try:
            new = load_config(path=self._config_path, overrides=self._overrides)
        except FatalError as e:
            log.warning("config reload failed, keeping the running config: %s", e)
            return
        for key in STRUCTURAL_KEYS:
            if getattr(new, key) != getattr(self._config, key):
                log.warning("config: %s changed on reload; restart required to apply", key)
        self._config = new
        self.roster.replace_config(new)
        self.pollers.replace_config(new)
        set_level(new.log_level)
        log.info("config reloaded from %s", new.source_path or "defaults")
        self._reconcile_after_reload = True

    # ---- watchdog -----------------------------------------------------------

    async def _watchdog(self) -> None:
        """Detect the conditions a restart fixes and nothing else does."""
        loop = asyncio.get_running_loop()
        started = loop.time()
        last_tick = loop.time()
        while not self._stop.is_set():
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=WATCHDOG_INTERVAL)
            if self._stop.is_set():
                return
            now = loop.time()
            stall = now - last_tick - WATCHDOG_INTERVAL
            last_tick = now

            warnings: list[str] = []
            if stall > LOOP_STALL_FATAL_SECONDS:
                raise FatalError(f"event loop stalled for {stall:.0f}s", EXIT_INTERNAL)
            if now - started < STARTUP_GRACE_SECONDS:
                self.health.warnings = warnings
                continue

            if len(self.pollers):
                last_sweep = self.health.last_successful_sweep()
                # No successful sweep at all, once past the grace period, counts
                # the same as one long ago: nothing is being collected.
                age = (
                    float("inf")
                    if last_sweep is None
                    else (datetime.now(UTC) - last_sweep).total_seconds()
                )
                if age > NO_SWEEP_FATAL_SECONDS:
                    raise FatalError(
                        f"no device has been read for {NO_SWEEP_FATAL_SECONDS:.0f}s "
                        f"across {len(self.pollers)} device(s)",
                        EXIT_INTERNAL,
                    )
                if age > NO_SWEEP_FATAL_SECONDS / 2:
                    warnings.append(f"no successful read for {age:.0f}s")

            if self.health.any_device_online():
                newest = self.health.buffer.newest_ts
                stale = None if newest is None else (datetime.now(UTC) - newest).total_seconds()
                if stale is not None and stale > STALE_BUFFER_FATAL_SECONDS:
                    raise FatalError(
                        f"devices are online but nothing has reached the buffer for "
                        f"{stale:.0f}s — the write path is wedged",
                        EXIT_INTERNAL,
                    )

            if not len(self.pollers):
                warnings.append(
                    "no devices: discovery has found nothing and no [[device]] is pinned"
                )

            unauthorized = [
                d.host for d in self.health.devices.values() if d.state is DeviceState.UNAUTHORIZED
            ]
            if unauthorized:
                warnings.append(f"credentials rejected by {', '.join(sorted(unauthorized))}")
            if self.health.buffer.rows_dropped:
                warnings.append(f"{self.health.buffer.rows_dropped} rows dropped")
            self.health.warnings = warnings

    # ---- signals and shutdown ----------------------------------------------

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, self._request_stop, sig)
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(signal.SIGHUP, self._request_reload)

    def _request_stop(self, sig) -> None:
        log.info("%s received; shutting down", signal.Signals(sig).name)
        self._stop.set()
        self._wake.set()
        self.uplink.stop()

    def _request_reload(self) -> None:
        log.info("SIGHUP received; reloading config")
        self._reload = True
        self._wake.set()

    async def _shutdown(self, runner) -> None:
        """Stop everything, and above all do not lose what is already buffered."""
        self._stop.set()
        self.uplink.stop()
        await self.pollers.stop()
        try:
            await self.buffer.flush()
        except Exception:
            log.error("failed to flush the buffer on shutdown", exc_info=True)
        await self.buffer.close()
        with contextlib.suppress(Exception):
            await runner.cleanup()
        log.info("stopped cleanly")


async def run(config: Config, *, config_path=None, overrides=None) -> int:
    return await Supervisor(config, config_path=config_path, overrides=overrides).run()
