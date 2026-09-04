"""One supervised task per device.

This is the module that answers "issues with one device do not slow polling on
other devices", and it answers it structurally: **there is no shared loop.**
Each device gets its own task and its own connection, so a strip that hangs for
its whole budget costs every other strip exactly nothing.

That is a deliberate departure from juice's recorder, which walks its devices
sequentially (`juice/recorder.py`, `poll_once`) over a session with no timeout
(`juice/collector.py`, `connect`). There, one wedged device stalls every other
device for up to aiohttp's five-minute default. Here the worst case a device can
impose on itself is `SWEEP_BUDGET`, and on its neighbours, nothing.

The offline state machine is juice's, kept almost verbatim because its best
property is worth preserving: **exactly two log lines per outage**, one entering
and one leaving. A device that dies at 1 Hz must not write a million log lines
about it.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
from datetime import UTC, datetime

from tap.buffer import Buffer
from tap.config import Config, DeviceSpec
from tap.device import DeviceState, Family, PowerDevice
from tap.errors import DeviceAuthError
from tap.health import Health, OutletHealth
from tap.retry import call_with_retry

log = logging.getLogger(__name__)

# The meter refreshes about once a second in hardware; polling faster returns
# the identical value eight or ten times. 1 Hz is the hardware's rate, not a
# compromise we settled for.
POLL_INTERVAL = 1.0
# Deliberately below POLL_INTERVAL so a hung sweep is cancelled before its
# successor is due. Sweeps can never pile up.
SWEEP_BUDGET = 0.8
# Connecting is slower than sweeping (a KLAP handshake is ~1s) and only happens
# off the hot path, so it gets its own, larger budget.
CONNECT_BUDGET = 15.0
# Actuating a relay is a human-facing action and worth more patience than a
# sample; it also must not be bounded by the sweep budget.
COMMAND_BUDGET = 5.0
COMMAND_ATTEMPTS = 4

# Same value and same reasoning as juice: enough to ride out a single transient
# blip without flapping a device to OFFLINE, few enough to cut off the
# per-second error flood quickly.
OFFLINE_FAILURE_THRESHOLD = 3
OFFLINE_REPROBE_SECONDS = 60.0
# A credential failure is not transient. Retrying it at poll cadence across a
# dozen devices is how you get rate-limited out of your own hardware.
AUTH_REPROBE_SECONDS = 300.0


def build_device(spec: DeviceSpec, config: Config) -> PowerDevice:
    """Construct the adapter for a spec whose family is already resolved."""
    from tap.kasa_iot import IotPowerDevice
    from tap.kasa_smart import SmartPowerDevice

    credentials = config.credentials_for(spec)
    cls = SmartPowerDevice if spec.family is Family.SMART else IotPowerDevice
    return cls(spec.host, credentials=credentials, device_id=spec.device_id or "")


class DevicePoller:
    """Polls one device forever, containing every failure it can have."""

    def __init__(
        self,
        spec: DeviceSpec,
        config: Config,
        buffer: Buffer,
        health: Health,
        *,
        interval: float = POLL_INTERVAL,
        sweep_budget: float = SWEEP_BUDGET,
        connect_budget: float = CONNECT_BUDGET,
    ) -> None:
        self.spec = spec
        self.host = spec.host
        self._config = config
        self._buffer = buffer
        self._health = health
        self._interval = interval
        self._sweep_budget = sweep_budget
        self._connect_budget = connect_budget
        self._device: PowerDevice | None = None
        self._state = DeviceState.STARTING
        self._failures = 0
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        # The device constructor, injectable so tests never import python-kasa.
        self.factory = build_device
        # Health is keyed on device_id, which we only learn on connect; until
        # then the host stands in so the status page shows the device at all.
        self._health_key = spec.device_id or f"host:{spec.host}"

    @property
    def state(self) -> DeviceState:
        return self._state

    @property
    def device_id(self) -> str:
        return self._device.device_id if self._device is not None else ""

    def start(self) -> None:
        self._task = asyncio.create_task(self.run(), name=f"poll:{self.host}")

    async def stop(self, *, forget: bool = False) -> None:
        """Stop polling. `forget` drops the device from the status page too.

        Leaving the roster forgets; shutting down does not — on SIGTERM the last
        known state is still the truest thing we can show.
        """
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        if self._device is not None:
            await self._device.close()
            self._device = None
        if forget:
            self._health.forget_device(self._health_key)

    # ---- the loop -----------------------------------------------------------

    async def run(self) -> None:
        # Spread the fleet across the second. Twelve strips firing ~90 round
        # trips in the same millisecond is a self-inflicted burst, and it is
        # the one way devices could interfere with each other here.
        await asyncio.sleep(random.uniform(0, self._interval))  # noqa: S311 — load spreading
        loop = asyncio.get_running_loop()
        while not self._stop.is_set():
            started = loop.time()
            await self._tick()
            elapsed = loop.time() - started
            # Interval is driven by elapsed time, never by a tick counter, so a
            # slow cycle does not silently stretch the schedule.
            delay = max(0.0, self._pause() - elapsed)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=delay)

    def _pause(self) -> float:
        if self._state is DeviceState.UNAUTHORIZED:
            return AUTH_REPROBE_SECONDS
        if self._state is DeviceState.OFFLINE:
            return OFFLINE_REPROBE_SECONDS
        return self._interval

    async def _tick(self) -> None:
        try:
            if self._device is None:
                async with asyncio.timeout(self._connect_budget):
                    await self._connect()
            async with asyncio.timeout(self._sweep_budget):
                sweep = await self._device.sweep()
        except asyncio.CancelledError:
            raise
        except DeviceAuthError as e:
            await self._note_auth_failure(e)
        except BaseException as e:  # noqa: BLE001 — a device may never kill its task
            await self._note_failure(e)
        else:
            await self._note_ok(sweep)

    async def _connect(self) -> None:
        spec = self.spec
        if spec.family is Family.AUTO:
            from tap.kasa_common import probe_family

            family, _model = await probe_family(
                spec.host, credentials=self._config.credentials_for(spec)
            )
            self.spec = spec = DeviceSpec(
                host=spec.host,
                family=family,
                credentials=spec.credentials,
                device_id=spec.device_id,
                pinned=spec.pinned,
            )
            log.info("device %s speaks %s", spec.host, family)
        device = self.factory(spec, self._config)
        await device.open()
        self._device = device
        self._rekey_health(device)

    def _rekey_health(self, device: PowerDevice) -> None:
        """Move this device's health entry from its host placeholder to its id."""
        if device.device_id and device.device_id != self._health_key:
            self._health.forget_device(self._health_key)
            self._health_key = device.device_id
        entry = self._health.device(self._health_key, host=self.host)
        entry.model = device.model
        entry.family = str(device.family)
        entry.pinned = self.spec.pinned

    # ---- state transitions --------------------------------------------------

    async def _note_ok(self, sweep) -> None:
        recovered = self._state in (DeviceState.OFFLINE, DeviceState.UNAUTHORIZED)
        if recovered:
            log.info("device %s (%s) back online", self.host, self.device_id[:12])
            await self._buffer.close_gap(self.device_id, "unreachable", sweep.ts)
        self._state = DeviceState.ONLINE
        self._failures = 0

        self._buffer.submit(sweep)
        entry = self._health.device(self._health_key, host=self.host)
        entry.state = self._state
        entry.record_sweep(sweep.duration_ms)
        for outlet in sweep.outlets:
            live = entry.outlets.get(outlet.child_id)
            if live is None:
                live = OutletHealth(child_id=outlet.child_id)
                entry.outlets[outlet.child_id] = live
            live.alias = outlet.alias
            live.relay_on = outlet.relay_on
            live.power_mw = outlet.power_mw
            live.voltage_mv = outlet.voltage_mv
            live.overcurrent = outlet.overcurrent or outlet.protection_tripped

    async def _note_failure(self, exc: BaseException) -> None:
        self._failures += 1
        entry = self._health.device(self._health_key, host=self.host)
        entry.record_failure(exc)
        if self._failures >= OFFLINE_FAILURE_THRESHOLD and self._state is not DeviceState.OFFLINE:
            self._state = DeviceState.OFFLINE
            # No exc_info: "device is offline" carries no useful stack, and this
            # is the line an operator reads at 11pm.
            log.warning(
                "device %s offline after %d failures (%s); backing off to %.0fs",
                self.host,
                self._failures,
                exc,
                OFFLINE_REPROBE_SECONDS,
            )
            await self._drop_connection("unreachable")
        elif self._state is not DeviceState.OFFLINE:
            self._state = DeviceState.DEGRADED
            log.debug("device %s read failed (%d): %s", self.host, self._failures, exc)
        entry.state = self._state

    async def _note_auth_failure(self, exc: BaseException) -> None:
        entry = self._health.device(self._health_key, host=self.host)
        entry.record_failure(exc)
        if self._state is not DeviceState.UNAUTHORIZED:
            self._state = DeviceState.UNAUTHORIZED
            # ERROR, not WARNING: this one needs a human. Nothing about waiting
            # fixes a rejected credential.
            log.error(
                "device %s rejected our credentials (%s); parking for %.0fs — check "
                "KASA_USERNAME/KASA_PASSWORD",
                self.host,
                exc,
                AUTH_REPROBE_SECONDS,
            )
            await self._drop_connection("unauthorized")
        entry.state = self._state

    async def _drop_connection(self, reason: str) -> None:
        """Close the socket and mark the hole in the data."""
        device_id = self.device_id
        if self._device is not None:
            await self._device.close()
            self._device = None
        if device_id:
            await self._buffer.record_gap(device_id, reason, datetime.now(UTC))

    # ---- commands -----------------------------------------------------------

    async def set_relay(self, child_id: str, on: bool) -> None:
        """Actuate one outlet. Shares the device's protocol lock with the sweep."""
        if self._device is None:
            raise ConnectionError(f"{self.host} is not connected")
        device = self._device

        async def once() -> None:
            async with asyncio.timeout(COMMAND_BUDGET):
                await device.set_relay(child_id, on)

        await call_with_retry(once, max_attempts=COMMAND_ATTEMPTS)


class PollerSet:
    """Owns the device tasks and keeps them in step with the roster.

    Reconciliation never restarts a healthy poller: only genuinely new hosts are
    started and only genuinely departed ones are stopped.
    """

    def __init__(
        self,
        config: Config,
        buffer: Buffer,
        health: Health,
        *,
        interval: float = POLL_INTERVAL,
        sweep_budget: float = SWEEP_BUDGET,
        factory=None,
    ) -> None:
        self._config = config
        self._buffer = buffer
        self._health = health
        self._interval = interval
        self._sweep_budget = sweep_budget
        # Tests substitute a factory so a PollerSet can be driven without any
        # real device library present.
        self._factory = factory
        self._pollers: dict[str, DevicePoller] = {}

    def _make(self, spec: DeviceSpec) -> DevicePoller:
        poller = DevicePoller(
            spec,
            self._config,
            self._buffer,
            self._health,
            interval=self._interval,
            sweep_budget=self._sweep_budget,
        )
        if self._factory is not None:
            poller.factory = self._factory
        return poller

    def __len__(self) -> int:
        return len(self._pollers)

    @property
    def pollers(self) -> dict[str, DevicePoller]:
        return dict(self._pollers)

    def replace_config(self, config: Config) -> None:
        self._config = config

    async def reconcile(self, specs: dict[str, DeviceSpec]) -> None:
        for host, spec in specs.items():
            existing = self._pollers.get(host)
            if existing is None:
                poller = self._make(spec)
                self._pollers[host] = poller
                poller.start()
                log.info("polling %s (%s)", host, spec.family)
            elif existing.spec.family is not spec.family and spec.family is not Family.AUTO:
                # The only in-place change worth a restart: we learned the
                # device speaks a different protocol than we assumed.
                log.info("device %s family changed to %s; restarting poller", host, spec.family)
                await existing.stop(forget=True)
                poller = self._make(spec)
                self._pollers[host] = poller
                poller.start()

        for host in list(self._pollers):
            if host not in specs:
                log.info("device %s left the roster; stopping poller", host)
                await self._pollers.pop(host).stop(forget=True)

    def find(self, device_id: str) -> DevicePoller | None:
        for poller in self._pollers.values():
            if poller.device_id == device_id:
                return poller
        return None

    async def stop(self) -> None:
        for poller in list(self._pollers.values()):
            await poller.stop()
        self._pollers.clear()
