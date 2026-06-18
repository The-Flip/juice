"""Recording daemon — polls strips and persists readings."""

from __future__ import annotations

import asyncio
import logging
import os
import re
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from juice.collector import Account, Outlet, PlugReading, Strip, _plug_reading, call_with_retry
from juice.flipfix import MachineInfo, report_unplayable
from juice.overload import OVERLOAD_MODES, OverloadWindow, resolve_overload_mode
from juice.store import Store

Device = Strip | Outlet

if TYPE_CHECKING:
    from juice.server import RecorderState

log = logging.getLogger(__name__)

ASSET_TAG_RE = re.compile(r"M\d+")
IDLE_RECHECK_SECONDS = 60
# How often to recompute per-machine power baselines (a heavier 30-day scan, so
# far less often than the 60s metadata refresh). Baselines drift slowly.
BASELINE_REFRESH_SECONDS = 3600
# Consecutive failed reads before a device is considered offline. A small
# threshold rides out single transient cloud blips without flapping a tile to
# OFFLINE, while still cutting off the per-second error flood quickly.
OFFLINE_FAILURE_THRESHOLD = 3


def extract_asset_tag(alias: str) -> str | None:
    """Extract asset tag like M0013 from a plug alias."""
    m = ASSET_TAG_RE.search(alias)
    return m.group(0) if m else None


def note_device_failure(
    state: RecorderState | None,
    device_id: str,
    ts: datetime,
    exc: BaseException,
) -> None:
    """Record a failed device read; mark the device offline at the threshold.

    Logs one concise WARNING on the online->offline transition (no traceback —
    "Device is offline" carries no useful stack) and stays quiet afterwards, so
    a dead device can't flood the console.
    """
    if state is None:
        return
    failures = state.device_failures.get(device_id, 0) + 1
    state.device_failures[device_id] = failures
    if failures >= OFFLINE_FAILURE_THRESHOLD and device_id not in state.offline_since:
        state.offline_since[device_id] = ts
        log.warning(
            "Device %s offline (%s); pausing fast polling until it recovers", device_id, exc
        )
    else:
        log.debug("Device %s read failed (%d): %s", device_id, failures, exc)


def note_device_ok(state: RecorderState | None, device_id: str) -> None:
    """Record a successful device read; clear offline status and log recovery."""
    if state is None:
        return
    if device_id in state.offline_since:
        log.info("Device %s back online", device_id)
    state.offline_since.pop(device_id, None)
    state.device_failures.pop(device_id, None)


def hydrate_assignments(state: RecorderState | None, store: Store) -> None:
    """Pre-fill in-memory assignment state from the DB's open assignments.

    On a cold start this makes every currently-assigned machine appear at once
    — including machines whose plug is offline, which metadata refresh would
    otherwise skip and drop. Live readings and re-assignments layer on top as
    the recorder polls. `year` isn't persisted, so hydrated entries carry None.

    All known plugs hydrate too (not just assigned ones), so the strip outlet
    map shows every outlet of an offline-at-boot strip.
    """
    if state is None:
        return
    for plug_id, device_id, child_id, alias, has_emeter in store.list_plugs():
        state.plugs[plug_id] = (device_id, child_id, alias)
        state.plug_has_emeter[plug_id] = has_emeter
    for (
        plug_id,
        _device_id,
        _child_id,
        _alias,
        _has_emeter,
        asset_id,
        name,
    ) in store.list_open_assignments():
        state.assignments[plug_id] = (name, asset_id, None)
    state.lock_modes = store.get_lock_modes()
    state.power_baselines = store.get_power_baselines()
    state.strip_names = store.get_strip_names()
    state.strip_orders = store.get_strip_orders()
    state.circuit_devices = store.get_circuit_devices()
    state.circuits = {c["circuit_id"]: c for c in store.list_circuits()}


@dataclass
class PlugState:
    # last_watts: float for emeter-equipped plugs, None for no-emeter ON,
    # 0.0 for OFF, -1.0 means never checked.
    last_watts: float | None = -1.0
    last_check: datetime | None = None


def _update_buffer(
    recorder_state: RecorderState,
    plug_id: int,
    watts: float,
) -> None:
    """Append a watts value to the ring buffer for a plug."""
    from juice.server import BUFFER_SIZE

    buf = recorder_state.watt_buffers.get(plug_id)
    if buf is None:
        buf = deque(maxlen=BUFFER_SIZE)
        recorder_state.watt_buffers[plug_id] = buf
    buf.append(watts)


def _refresh_baselines(store: Store, recorder_state: RecorderState | None) -> None:
    """Recompute per-machine power baselines and re-hydrate in-memory state."""
    try:
        store.refresh_power_baselines()
        if recorder_state is not None:
            recorder_state.power_baselines = store.get_power_baselines()
    except Exception:
        log.warning("Power baseline refresh failed", exc_info=True)


async def check_overload(
    state: RecorderState,
    store: Store,
    plug_id: int,
    ts: datetime,
    watts: float,
) -> None:
    """Feed a fresh reading to the plug's overload window; act if it fires.

    A machine is only armed once it has a baseline (enough history). Machines
    already locked off are skipped — they're powered down and can't be re-armed
    until a human clears them. Honors `state.overload_mode` ('off'/'shadow'/'live').
    """
    if state.overload_mode == "off":
        return
    assignment = state.assignments.get(plug_id)
    if assignment is None:
        return
    name, asset_id, _year = assignment
    baseline = state.power_baselines.get(asset_id)
    if baseline is None:  # not enough history -> not armed
        return
    if state.lock_modes.get(asset_id) == "off":  # already shut down for overload
        return

    window = state.overload_windows.get(plug_id)
    if window is None:
        window = OverloadWindow()
        state.overload_windows[plug_id] = window
    window.add(ts, watts)
    fire, mean_w = window.verdict(baseline)
    if not fire:
        return

    window.reset()  # don't re-fire on the same sustained load
    if state.overload_mode == "shadow":
        log.warning(
            "OVERLOAD (shadow) %s (%s): %.0fW sustained vs %.0fW baseline — would shut down",
            name,
            asset_id,
            mean_w,
            baseline,
        )
        try:
            store.record_power_event(
                ts,
                plug_id,
                "turn_off",
                "overload",
                "system",
                "shadow",
                error=f"{mean_w:.0f}W sustained vs {baseline:.0f}W baseline",
            )
        except Exception as e:
            log.warning("Audit write failed for plug %d: %s", plug_id, e)
        _publish_overload(state, plug_id, name, asset_id, mean_w, baseline, shadow=True)
        return

    await _trigger_overload_shutdown(state, store, plug_id, name, asset_id, ts, mean_w, baseline)


async def _trigger_overload_shutdown(
    state: RecorderState,
    store: Store,
    plug_id: int,
    name: str,
    asset_id: str,
    ts: datetime,
    mean_w: float,
    baseline: float,
) -> None:
    """Power the machine off and lock it off after a confirmed overload."""
    plug = state.plug_objects.get(plug_id)
    if plug is None:
        log.error("Overload on %s (%s) but no controllable plug %d", name, asset_id, plug_id)
        return
    log.warning(
        "OVERLOAD %s (%s): %.0fW sustained vs %.0fW baseline — shutting down + locking off",
        name,
        asset_id,
        mean_w,
        baseline,
    )
    try:
        await call_with_retry(plug.turn_off, max_attempts=6)
    except Exception as e:
        log.error("Overload shutdown FAILED for %s (%s): %s", name, asset_id, e)
        try:
            store.record_power_event(
                ts, plug_id, "turn_off", "overload", "system", "error", error=str(e)
            )
        except Exception as ae:
            log.warning("Audit write failed for plug %d: %s", plug_id, ae)
        return

    try:
        store.record_power_event(
            ts,
            plug_id,
            "turn_off",
            "overload",
            "system",
            "ok",
            error=f"{mean_w:.0f}W sustained vs {baseline:.0f}W baseline",
        )
    except Exception as e:
        log.warning("Audit write failed for plug %d: %s", plug_id, e)

    # Lock the machine off so it can't be re-powered until a human inspects it.
    # Set the in-memory lock first so it holds even if the DB write fails, and
    # don't let a persistence error escape (it would kill the recording loop).
    state.lock_modes[asset_id] = "off"
    try:
        machine_id = store.ensure_machine(asset_id, name)
        store.set_machine_lock_mode(machine_id, "off")
    except Exception as e:
        log.warning("Lock persistence failed for %s (%s): %s", name, asset_id, e)

    _publish_overload(state, plug_id, name, asset_id, mean_w, baseline, shadow=False)

    # Record it in FlipFix: file an 'unplayable' problem report and mark the
    # machine broken. Best-effort — report_unplayable never raises, and we only
    # reach here after the power-off already succeeded.
    if state.flipfix_url and state.flipfix_key:
        await report_unplayable(
            state.flipfix_url,
            state.flipfix_key,
            asset_id,
            f"Auto power-off: sustained overload ({mean_w:.0f}W vs {baseline:.0f}W baseline)",
            occurred_at=ts.isoformat(),
        )


def _publish_overload(
    state: RecorderState,
    plug_id: int,
    name: str,
    asset_id: str,
    mean_w: float,
    baseline: float,
    *,
    shadow: bool,
) -> None:
    """Notify dashboard clients of an overload event (no-op without a publisher)."""
    from juice.server import _publish

    _publish(
        state,
        {
            "type": "overload_shutdown",
            "plug_id": plug_id,
            "asset_id": asset_id,
            "machine_name": name,
            "watts": round(mean_w),
            "baseline": round(baseline),
            "shadow": shadow,
            "actor": "system",
            "source": "overload",
        },
    )


async def poll_once(
    devices: list[Device],
    store: Store,
    plug_states: dict[str, PlugState],
    ts: datetime,
    recorder_state: RecorderState | None = None,
) -> None:
    """One polling iteration: fetch sysinfo per device, selectively read emeter.

    Handles both HS300 strips (multi-child, full emeter per child) and
    single-outlet no-emeter devices like EP10 (one synthetic child,
    on/off only — readings are stored with NULL power fields).
    """
    readings_count = 0
    for device in devices:
        # Skip devices already known offline — the 60s metadata refresh is
        # their recovery probe, so the fast loop neither wastes a cloud call
        # nor re-logs the failure every second.
        if recorder_state is not None and device.device_id in recorder_state.offline_since:
            continue
        try:
            children = await device.child_states()
        except Exception as e:
            note_device_failure(recorder_state, device.device_id, ts, e)
            continue
        note_device_ok(recorder_state, device.device_id)

        for child in children:
            child_id = child["id"]
            alias = child["alias"]
            key = f"{device.device_id}:{child_id}"

            # OFF: record 0W to DB (rate-limited), buffer 0W, skip emeter.
            if not child["state"]:
                plug_id = store.ensure_plug(
                    device.device_id, child_id, alias, has_emeter=device.has_emeter
                )
                off_state = plug_states.get(key)
                should_write = (
                    off_state is None
                    or off_state.last_watts != 0.0
                    or off_state.last_check is None
                    or (ts - off_state.last_check).total_seconds() >= IDLE_RECHECK_SECONDS
                )
                if should_write:
                    store.insert_readings([(ts, plug_id, 0.0, 0.0, 0.0, 0.0)])
                    plug_states[key] = PlugState(last_watts=0.0, last_check=ts)
                if recorder_state is not None:
                    if device.has_emeter:
                        _update_buffer(recorder_state, plug_id, 0.0)
                        # Feed 0W to the overload window so an OFF period flushes
                        # any prior high readings (can't fire — 0 < threshold).
                        await check_overload(recorder_state, store, plug_id, ts, 0.0)
                    recorder_state.plug_readings[plug_id] = PlugReading(
                        child_id=child_id,
                        alias=alias,
                        is_on=False,
                        watts=0.0 if device.has_emeter else None,
                        voltage=0.0 if device.has_emeter else None,
                        amps=0.0 if device.has_emeter else None,
                        total_kwh=0.0 if device.has_emeter else None,
                    )
                continue

            # ON, no emeter: record NULL-watts row (rate-limited, immediate
            # write on state transition from OFF / first-ever / 60s elapsed).
            if not device.has_emeter:
                plug_id = store.ensure_plug(device.device_id, child_id, alias, has_emeter=False)
                on_state = plug_states.get(key)
                should_write = (
                    on_state is None
                    or on_state.last_watts is not None  # was OFF or measured; now NULL-ON
                    or on_state.last_check is None
                    or (ts - on_state.last_check).total_seconds() >= IDLE_RECHECK_SECONDS
                )
                if should_write:
                    store.insert_readings([(ts, plug_id, None, None, None, None)])
                    plug_states[key] = PlugState(last_watts=None, last_check=ts)
                if recorder_state is not None:
                    recorder_state.plug_readings[plug_id] = PlugReading(
                        child_id=child_id,
                        alias=alias,
                        is_on=True,
                        watts=None,
                        voltage=None,
                        amps=None,
                        total_kwh=None,
                    )
                continue

            # ON, has emeter: existing path — idle-skip + emeter fetch.
            plug_id_for_skip = None
            if recorder_state is not None:
                plug_id_for_skip = store.ensure_plug(
                    device.device_id, child_id, alias, has_emeter=True
                )
            forced = recorder_state is not None and plug_id_for_skip in recorder_state.force_poll
            state = plug_states.get(key)
            if (
                not forced
                and state is not None
                and state.last_watts == 0.0
                and state.last_check is not None
            ):
                elapsed = (ts - state.last_check).total_seconds()
                if elapsed < IDLE_RECHECK_SECONDS:
                    if recorder_state is not None and plug_id_for_skip is not None:
                        _update_buffer(recorder_state, plug_id_for_skip, 0.0)
                    continue

            try:
                emeter = await device.read_emeter(child_id)
            except Exception:
                log.warning("Failed emeter for %s on %s", child_id, device.device_id, exc_info=True)
                continue

            reading = _plug_reading(child, emeter)
            plug_id = store.ensure_plug(device.device_id, child_id, alias, has_emeter=True)
            store.insert_readings(
                [(ts, plug_id, reading.watts, reading.voltage, reading.amps, reading.total_kwh)]
            )

            plug_states[key] = PlugState(last_watts=reading.watts, last_check=ts)
            readings_count += 1

            if recorder_state is not None:
                recorder_state.plug_readings[plug_id] = reading
                if reading.watts is not None:
                    _update_buffer(recorder_state, plug_id, reading.watts)
                    await check_overload(recorder_state, store, plug_id, ts, reading.watts)
                recorder_state.force_poll.discard(plug_id)

    log.debug("Poll: %d devices, %d readings recorded", len(devices), readings_count)


async def refresh_metadata(
    account: Account,
    store: Store,
    machines: dict[str, MachineInfo],
    ts: datetime,
    recorder_state: RecorderState | None = None,
) -> list[Device]:
    """Refresh device/plug metadata and update assignments. Returns current device list."""
    if recorder_state is not None:
        # Self-healing wholesale refresh of operator-set state; the lock and
        # strip-name endpoints also update these synchronously between refreshes.
        recorder_state.lock_modes = store.get_lock_modes()
        recorder_state.strip_names = store.get_strip_names()
        recorder_state.strip_orders = store.get_strip_orders()
        recorder_state.circuit_devices = store.get_circuit_devices()
        recorder_state.circuits = {c["circuit_id"]: c for c in store.list_circuits()}
    devices = await account.devices()

    for device in devices:
        # refresh_metadata probes every discovered device, so it doubles as the
        # recovery path for ones the fast loop has parked as offline.
        try:
            children = await device.child_states()
            device_plugs = await device.plugs()
        except Exception as e:
            note_device_failure(recorder_state, device.device_id, ts, e)
            continue
        note_device_ok(recorder_state, device.device_id)
        if recorder_state is not None:
            recorder_state.strip_aliases[device.device_id] = device.alias
        plug_obj_by_child = {p.child_id: p for p in device_plugs}
        for child in children:
            child_id = child["id"]
            alias = child["alias"]
            plug_id = store.ensure_plug(
                device.device_id, child_id, alias, has_emeter=device.has_emeter
            )

            if recorder_state is not None:
                recorder_state.plugs[plug_id] = (device.device_id, child_id, alias)
                recorder_state.plug_has_emeter[plug_id] = device.has_emeter
                plug_obj = plug_obj_by_child.get(child_id)
                if plug_obj is not None:
                    recorder_state.plug_objects[plug_id] = plug_obj

            asset_tag = extract_asset_tag(alias)
            if asset_tag and asset_tag in machines:
                info = machines[asset_tag]
                machine_id = store.ensure_machine(asset_tag, info["name"])
                store.update_assignment(plug_id, machine_id, ts)
                if recorder_state is not None:
                    prev = recorder_state.assignments.get(plug_id)
                    recorder_state.assignments[plug_id] = (
                        info["name"],
                        asset_tag,
                        info.get("year"),
                    )
                    # A plug moving to a different machine must not inherit the
                    # previous machine's accumulated load.
                    if prev is None or prev[1] != asset_tag:
                        recorder_state.overload_windows.pop(plug_id, None)
                    cal = store.get_calibration(machine_id)
                    if cal is not None:
                        recorder_state.calibrations[plug_id] = cal
                    else:
                        recorder_state.calibrations.pop(plug_id, None)
            else:
                store.update_assignment(plug_id, None, ts)
                if recorder_state is not None:
                    recorder_state.assignments.pop(plug_id, None)
                    recorder_state.calibrations.pop(plug_id, None)
                    recorder_state.overload_windows.pop(plug_id, None)

    return devices


async def record(
    account: Account,
    store: Store,
    flipfix_url: str | None = None,
    flipfix_key: str | None = None,
    recorder_state: RecorderState | None = None,
) -> None:
    """Main recording loop. Runs forever."""
    from juice.flipfix import get_machines

    plug_states: dict[str, PlugState] = {}
    machines: dict[str, MachineInfo] = {}

    # Hydrate from the DB first so previously-assigned machines (including any
    # whose plug is currently offline) show up immediately; the refresh below
    # then overlays live data and any re-assignments.
    hydrate_assignments(recorder_state, store)

    if recorder_state is not None:
        raw_mode = os.environ.get("JUICE_OVERLOAD_PROTECTION")
        recorder_state.overload_mode = resolve_overload_mode(raw_mode)
        if raw_mode and raw_mode.lower() not in OVERLOAD_MODES:
            # Unrecognized value (typo): fail safe toward protection, not silently
            # off, and make the misconfiguration loud rather than implicit.
            log.warning(
                "Invalid JUICE_OVERLOAD_PROTECTION=%r; expected one of %s — using 'live'",
                raw_mode,
                ", ".join(OVERLOAD_MODES),
            )
        log.info("Overload protection: %s", recorder_state.overload_mode)
        recorder_state.flipfix_url = flipfix_url
        recorder_state.flipfix_key = flipfix_key
    _refresh_baselines(store, recorder_state)

    # Initial metadata fetch
    if flipfix_url and flipfix_key:
        machines = await get_machines(flipfix_url, flipfix_key)
    ts = datetime.now(UTC)
    devices = await refresh_metadata(account, store, machines, ts, recorder_state)
    if recorder_state is not None:
        from juice.server import seed_buffers

        seed_buffers(recorder_state, store)
    # Backfill the rollup tables on startup so the /usage page is
    # populated immediately. Cheap if there's nothing new to compute.
    try:
        store.refresh_hourly_usage()
    except Exception:
        log.warning("Initial hourly_usage refresh failed", exc_info=True)
    try:
        store.refresh_hourly_strip_peak()
    except Exception:
        log.warning("Initial hourly_strip_peak refresh failed", exc_info=True)
    try:
        store.refresh_hourly_circuit_peak()
    except Exception:
        log.warning("Initial hourly_circuit_peak refresh failed", exc_info=True)
    try:
        store.refresh_hourly_play_seconds()
    except Exception:
        log.warning("Initial hourly_play_seconds refresh failed", exc_info=True)
    log.info("Started: %d devices, %d machines", len(devices), len(machines))
    polls_since_refresh = 0
    polls_since_baseline = 0

    while True:
        start = asyncio.get_running_loop().time()
        ts = datetime.now(UTC)

        await poll_once(devices, store, plug_states, ts, recorder_state)

        polls_since_baseline += 1
        if polls_since_baseline >= BASELINE_REFRESH_SECONDS:
            _refresh_baselines(store, recorder_state)
            polls_since_baseline = 0

        polls_since_refresh += 1
        if polls_since_refresh >= IDLE_RECHECK_SECONDS:
            try:
                if flipfix_url and flipfix_key:
                    machines = await get_machines(flipfix_url, flipfix_key)
                devices = await refresh_metadata(account, store, machines, ts, recorder_state)
                log.info("Refreshed: %d devices, %d machines", len(devices), len(machines))
            except Exception:
                log.warning("Metadata refresh failed", exc_info=True)
            try:
                store.refresh_hourly_usage()
            except Exception:
                log.warning("hourly_usage refresh failed", exc_info=True)
            try:
                store.refresh_hourly_strip_peak()
            except Exception:
                log.warning("hourly_strip_peak refresh failed", exc_info=True)
            try:
                store.refresh_hourly_circuit_peak()
            except Exception:
                log.warning("hourly_circuit_peak refresh failed", exc_info=True)
            try:
                store.refresh_hourly_play_seconds()
            except Exception:
                log.warning("hourly_play_seconds refresh failed", exc_info=True)
            polls_since_refresh = 0

        elapsed = asyncio.get_running_loop().time() - start
        await asyncio.sleep(max(0, 1.0 - elapsed))
