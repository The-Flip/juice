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
import time
from datetime import UTC, datetime

from tap.buffer import Buffer
from tap.config import Config, DeviceSpec
from tap.device import DeviceState, Family, PowerDevice
from tap.errors import DeviceAuthError, DeviceExcludedError, TransientError
from tap.health import Health, OutletHealth, describe_failure
from tap.logmod import RateLimited
from tap.retry import call_with_retry

log = logging.getLogger(__name__)

# The meter refreshes about once a second in hardware; polling faster returns
# the identical value eight or ten times. 1 Hz is the hardware's rate, not a
# compromise we settled for.
POLL_INTERVAL = 1.0
# Well above POLL_INTERVAL, because cancelling a sweep throws away every outlet
# that had already answered. Measured on a real P316M: the strip seizes for
# ~0.6s every ten seconds or so and occasionally for 2s, and a 0.8s budget
# turned those into whole discarded sweeps — 60 readings lost in ten minutes
# where letting them finish lost none. Sweeps still cannot pile up: `run` polls
# one at a time, and a sweep that overruns simply delays its own successor.
# What the budget is left doing is bounding a device that has genuinely hung.
SWEEP_BUDGET = 5.0
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
#
# Note the interaction with the sweep budget: three sweeps that hang for the
# full 5 s is 15 s to OFFLINE, against 2.4 s when the budget was 0.8 s, so the
# gap row opens about twelve seconds later. That is the price of not throwing
# sweeps away; it is well inside the 120 s watchdog, and a device that is
# merely slow rather than hung now stays ONLINE where it used to flap.
OFFLINE_FAILURE_THRESHOLD = 3

# The roster refresh is skipped unless this fraction of the interval is left.
# It costs ~100ms against a 1s interval on real hardware, so a fifth of the
# interval is roughly double its own cost — and expressing it as a fraction
# means a configured sub-second interval does not silently disable it forever.
ROSTER_MARGIN_FRACTION = 0.2
# How many consecutive failed or skipped refreshes before saying so. A roster
# that never refreshes is a device reporting relay state, alias and
# overcurrent status frozen at whatever they were, while power stays live and
# plausible — the sweep keeps succeeding, so nothing else would ever notice.
ROSTER_STALE_THRESHOLD = 30
# How long a roster refresh may take. Above the interval on purpose — see
# `_refresh_roster`. It only has to bound a refresh that will never return.
ROSTER_REFRESH_BUDGET = 5.0
# At 1 Hz a device failing steadily would be 3600 lines an hour. One a minute,
# carrying the suppressed count, is enough to see it without drowning the log.
FAILURE_LOG_INTERVAL = 60.0
# How long to wait before re-probing a parked device, by attempt. Measured:
# eleven hours against a real P316M lost 315 sweeps to a flat 60 s backoff and
# 142 to the timeouts that triggered it, because all five outages were
# three-second blips and the first re-probe succeeded every time. Escalating
# turns a blip into a few seconds of hole while still ending up patient enough
# for a device that has genuinely been unplugged.
OFFLINE_BACKOFF = (1.0, 2.0, 5.0, 15.0, 60.0)
# A credential failure is not transient. Retrying it at poll cadence across a
# dozen devices is how you get rate-limited out of your own hardware.
AUTH_REPROBE_SECONDS = 300.0


def offline_backoff_delay(attempt: int) -> float:
    """Seconds to wait before re-probe number `attempt` (0-based).

    Clamped at both ends: a negative count is the first attempt, and anything
    past the schedule holds at its last step rather than growing without bound.
    """
    if attempt < 0:
        return OFFLINE_BACKOFF[0]
    return OFFLINE_BACKOFF[min(attempt, len(OFFLINE_BACKOFF) - 1)]


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
        # How many times we have re-probed since this device went offline, which
        # is the index into OFFLINE_BACKOFF. Reset the moment it answers.
        self._offline_probes = 0
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        # Which reason the currently-open gap was recorded under, so recovery
        # closes the row it actually opened. Closing "unreachable" after an
        # "unauthorized" outage leaves a gap open forever, which defeats the
        # point of recording it.
        self._open_gap: str | None = None
        # The device constructor, injectable so tests never import python-kasa.
        self.factory = build_device
        # Health is keyed on device_id, which we only learn on connect; until
        # then the host stands in so the status page shows the device at all.
        self._health_key = spec.device_id or f"host:{spec.host}"
        # Per-device, so a fleet of twelve reports twelve outages rather than
        # one device's noise suppressing everyone else's first line.
        self._failure_log = RateLimited(log, interval=FAILURE_LOG_INTERVAL)
        # Consecutive sweeps served by a roster we could not refresh.
        self._roster_stale = 0
        self._roster_log = RateLimited(log, interval=FAILURE_LOG_INTERVAL)

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
            # The outlet roster rides in the idle time, not in the sweep. If
            # the sweep used the whole interval there is no room and the
            # previous roster carries over — relay state and alias go stale,
            # power does not.
            if self._state is DeviceState.ONLINE and self._device is not None:
                room = self._interval - elapsed
                if room > self._interval * ROSTER_MARGIN_FRACTION:
                    await self._refresh_roster()
                    elapsed = loop.time() - started
                else:
                    # Skips and failures are different events and are counted
                    # separately: one is a device we chose not to ask, the
                    # other is a device that would not answer.
                    self._health.device(self._health_key, host=self.host).roster_skips += 1
                    self._note_roster_stale("no room in the interval")
            # Interval is driven by elapsed time, never by a tick counter, so a
            # slow cycle does not silently stretch the schedule.
            # Subtracting the tick's own cost keeps ONLINE polling on a steady
            # 1 Hz schedule. A backoff is different: it is a gap to leave after
            # a failed attempt, and netting off `elapsed` collapsed it — a
            # 15 s connect timeout made every step below 15 s a no-op, so a
            # dead device got four back-to-back reconnects instead of an
            # escalating wait.
            delay = (
                self._pause()
                if self._state is DeviceState.OFFLINE
                else max(0.0, self._pause() - elapsed)
            )
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=delay)

    def _note_roster_stale(self, why: str) -> None:
        """Count a roster that did not refresh, and say so if it never does.

        A frozen roster does not fail a sweep — power keeps flowing and every
        reading looks current — so without this a device can report relay
        state, alias and overcurrent status from hours ago with nothing
        anywhere to show for it.
        """
        self._roster_stale += 1
        if self._roster_stale >= ROSTER_STALE_THRESHOLD:
            self._roster_log.warning(
                "device %s: outlet roster not refreshed for %d sweeps (%s); "
                "relay state and aliases are that stale",
                self.host,
                self._roster_stale,
                why,
            )

    async def _refresh_roster(self) -> None:
        """Re-read relay state and aliases. Never fails a sweep.

        A roster we could not refresh is a roster we keep using, which is the
        same position we are in when there was no room for it.
        """
        entry = self._health.device(self._health_key, host=self.host)
        try:
            # Bounded by its own budget, not by the room left in the interval.
            # Bounding it by the room meant every strip seizure landing on a
            # refresh cancelled a query mid-flight — the very thing raising the
            # sweep budget stopped doing, on a device that evicts sessions once
            # about six are open. Overrunning is cheap here: the next tick
            # starts late, and the roster was never on the critical path.
            async with asyncio.timeout(ROSTER_REFRESH_BUDGET):
                await self._device.refresh_roster()
        except asyncio.CancelledError:
            raise
        except BaseException as e:  # noqa: BLE001 — a stale roster is not an outage
            entry.roster_failures += 1
            self._note_roster_stale(f"{type(e).__name__}: {e}")
        else:
            entry.roster_refreshes += 1
            self._roster_stale = 0

    def _pause(self) -> float:
        if self._state is DeviceState.UNAUTHORIZED:
            return AUTH_REPROBE_SECONDS
        if self._state is DeviceState.OFFLINE:
            return offline_backoff_delay(self._offline_probes)
        return self._interval

    async def _tick(self) -> None:
        started = time.perf_counter()
        phase = "connect"
        try:
            if self._device is None:
                async with asyncio.timeout(self._connect_budget):
                    await self._connect()
            phase = "sweep"
            # Restarted so a sweep's elapsed time means the same thing as the
            # duration record_sweep stores on success — the sweep alone, not a
            # connect handshake in front of it.
            started = time.perf_counter()
            async with asyncio.timeout(self._sweep_budget):
                sweep = await self._device.sweep()
        except asyncio.CancelledError:
            raise
        except DeviceExcludedError as e:
            # Not a failure: config says do not poll this. Stop cleanly rather
            # than retrying something we have been told to leave alone.
            log.warning("device %s: %s; stopping this poller", self.host, e)
            self._state = DeviceState.EXCLUDED
            self._health.device(self._health_key, host=self.host).state = self._state
            self._stop.set()
        except DeviceAuthError as e:
            await self._note_auth_failure(
                e,
                phase=self._phase(phase),
                duration_ms=round((time.perf_counter() - started) * 1000, 2),
            )
        except BaseException as e:  # noqa: BLE001 — a device may never kill its task
            await self._note_failure(
                e,
                phase=self._phase(phase),
                duration_ms=round((time.perf_counter() - started) * 1000, 2),
            )
        else:
            await self._note_ok(sweep)

    def _phase(self, outer: str) -> str:
        """Where the attempt was when it failed.

        A budget cancels the sweep from outside, so the exception carries no
        clue which of the ~seven round trips in a six-outlet sweep was in
        flight — and "the connect was slow" and "outlet 5's emeter hung" want
        different fixes. The adapter records its own position; this reads it.
        """
        if outer != "sweep":
            return outer
        inner = getattr(self._device, "phase", "")
        return f"sweep:{inner}" if inner else "sweep"

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
        try:
            await device.open()
            # Both of these can refuse the device, and both must do so before it
            # is adopted: `_tick` skips `_connect` whenever `self._device` is
            # set, so a device left assigned after a refusal is never checked
            # again — it just gets swept under the identity we rejected.
            self._refuse_if_excluded(device)
            self._rekey_health(device)
        except BaseException:
            # open() connects and then interrogates the device, so a failure at
            # the second step leaves a live session with no owner. One leaked
            # socket per failed attempt, at the re-probe cadence, adds up.
            await device.close()
            raise
        self._device = device

    def _refuse_if_excluded(self, device: PowerDevice) -> None:
        """Apply a device_id exclusion that discovery could not.

        Discovery knows a SMART device's id, but an IOT device only reveals its
        real (cloud) id once connected — python-kasa's property is the MAC. So a
        `device_id` exclusion has to be re-checked here, with the id we will
        actually file readings under.
        """
        rule = self._config.is_excluded(host=self.host, device_id=device.device_id)
        if rule is None:
            return
        raise DeviceExcludedError(
            f"{self.host} is {device.device_id}, which config excludes"
            + (f" ({rule.reason})" if rule.reason else "")
        )

    def _rekey_health(self, device: PowerDevice) -> None:
        """Move this device's health entry from its host placeholder to its id."""
        existing = self._health.devices.get(device.device_id)
        if existing is not None and existing.host != self.host:
            # Two hosts reporting one id would silently share a health entry,
            # and PollerSet.find() would then actuate whichever came first in
            # dict order — a relay command hitting the wrong strip. Refuse
            # loudly instead; it means the identity scheme is wrong.
            raise TransientError(
                f"{self.host} reports device_id {device.device_id}, which "
                f"{existing.host} is already using — refusing to poll both"
            )
        if device.device_id and device.device_id != self._health_key:
            # Move, not forget: the placeholder holds every failure recorded
            # before we could connect, and those are the connect failures.
            self._health.rename_device(self._health_key, device.device_id)
            self._health_key = device.device_id
        entry = self._health.device(self._health_key, host=self.host)
        entry.model = device.model
        entry.family = str(device.family)
        entry.pinned = self.spec.pinned

    # ---- state transitions --------------------------------------------------

    async def _note_ok(self, sweep) -> None:
        recovered = self._state in (DeviceState.OFFLINE, DeviceState.UNAUTHORIZED)
        entry = self._health.device(self._health_key, host=self.host)
        if recovered:
            log.info("device %s (%s) back online", self.host, self.device_id[:12])
            if self._open_gap is not None:
                await self._buffer.close_gap(self.device_id, self._open_gap, sweep.ts)
                self._open_gap = None
        elif self._state is DeviceState.DEGRADED:
            # The line that was missing. A failure below the offline threshold
            # is a lost second of data that nothing ever mentioned: eight hours
            # of real polling dropped 132 sweeps and logged one of them, because
            # the other 131 were isolated and DEBUG-only. Reporting them here,
            # on recovery, is what keeps an outage at two lines — a device on
            # its way offline never gets here.
            self._failure_log.warning(
                "device %s recovered after %d failed sweep(s); last: %s",
                self.host,
                self._failures,
                entry.last_error,
            )
        self._state = DeviceState.ONLINE
        self._failures = 0
        self._offline_probes = 0
        if sweep.listing_ms is not None:
            # This sweep had no roster and fetched one itself — a reconnect,
            # or the very first sweep. The roster is current, so a streak from
            # before the reconnect must not warn about it.
            self._roster_stale = 0

        self._buffer.submit(sweep)
        entry.state = self._state
        entry.record_sweep(
            sweep.duration_ms,
            listing_ms=sweep.listing_ms,
            emeter_total_ms=sweep.emeter_total_ms,
            emeter_max_ms=sweep.emeter_max_ms,
            roster_age=sweep.roster_age,
        )
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

    async def _note_failure(
        self,
        exc: BaseException,
        *,
        phase: str = "",
        duration_ms: float | None = None,
    ) -> None:
        if self._state is DeviceState.UNAUTHORIZED:
            # Already parked for a credential problem. A transient error on the
            # re-probe must not demote it to DEGRADED and drag it back to 1 Hz
            # retries — that is exactly the rate-limiting AUTH_REPROBE_SECONDS
            # exists to avoid.
            self._health.device(self._health_key, host=self.host).record_failure(
                exc, phase=phase, duration_ms=duration_ms
            )
            log.debug("device %s still unauthorized: %s", self.host, exc)
            return
        self._failures += 1
        entry = self._health.device(self._health_key, host=self.host)
        entry.record_failure(exc, phase=phase, duration_ms=duration_ms)
        if self._state is DeviceState.OFFLINE:
            # A failed re-probe: lengthen the next wait.
            self._offline_probes += 1
        if self._failures >= OFFLINE_FAILURE_THRESHOLD and self._state is not DeviceState.OFFLINE:
            self._state = DeviceState.OFFLINE
            self._offline_probes = 0
            # No exc_info: "device is offline" carries no useful stack, and this
            # is the line an operator reads at 11pm. It must therefore say
            # something: interpolating the exception rendered a bare
            # `asyncio.timeout` cancellation as `()`, observed in production.
            log.warning(
                "device %s offline after %d failures (%s); re-probing in %gs",
                self.host,
                self._failures,
                describe_failure(exc, phase=phase, duration_ms=duration_ms),
                offline_backoff_delay(0),
            )
            await self._drop_connection("unreachable")
        elif self._state is not DeviceState.OFFLINE:
            self._state = DeviceState.DEGRADED
            # Still DEBUG here, and deliberately: a device on its way offline
            # would otherwise log this line and then the offline line two
            # seconds later, and an outage is supposed to cost exactly two
            # lines. Failures that *don't* reach the threshold are reported by
            # _note_ok when the device recovers, which is the only moment we
            # know they were isolated.
            log.debug("device %s read failed (%d): %s", self.host, self._failures, exc)
        entry.state = self._state

    async def _note_auth_failure(
        self,
        exc: BaseException,
        *,
        phase: str = "",
        duration_ms: float | None = None,
    ) -> None:
        entry = self._health.device(self._health_key, host=self.host)
        entry.record_failure(exc, phase=phase, duration_ms=duration_ms)
        if self._state is not DeviceState.UNAUTHORIZED:
            self._state = DeviceState.UNAUTHORIZED
            self._failures = 0
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
            self._open_gap = reason

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
        # Relay state now comes from the cached roster, so without this the
        # outlet we just switched keeps reporting its old state until the next
        # successful refresh — which on a busy strip can be many sweeps away.
        # We know what we just did; say so rather than wait to be told.
        device.note_relay(child_id, on)


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
