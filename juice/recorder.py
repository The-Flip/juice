"""Recording daemon — polls strips and persists readings."""

from __future__ import annotations

import asyncio
import logging
import re
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from juice.collector import Account, Outlet, PlugReading, Strip, _plug_reading
from juice.flipfix import MachineInfo
from juice.store import Store

Device = Strip | Outlet

if TYPE_CHECKING:
    from juice.server import RecorderState

log = logging.getLogger(__name__)

ASSET_TAG_RE = re.compile(r"M\d+")
IDLE_RECHECK_SECONDS = 60


def extract_asset_tag(alias: str) -> str | None:
    """Extract asset tag like M0013 from a plug alias."""
    m = ASSET_TAG_RE.search(alias)
    return m.group(0) if m else None


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
        try:
            children = await device.child_states()
        except Exception:
            log.warning("Failed to fetch sysinfo for %s", device.device_id, exc_info=True)
            continue

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
    devices = await account.devices()

    for device in devices:
        try:
            children = await device.child_states()
            device_plugs = await device.plugs()
        except Exception:
            log.warning("Metadata fetch failed for %s", device.device_id, exc_info=True)
            continue
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
                    recorder_state.assignments[plug_id] = (
                        info["name"],
                        asset_tag,
                        info.get("year"),
                    )
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
        store.refresh_daily_play_seconds()
    except Exception:
        log.warning("Initial daily_play_seconds refresh failed", exc_info=True)
    log.info("Started: %d devices, %d machines", len(devices), len(machines))
    polls_since_refresh = 0

    while True:
        start = asyncio.get_running_loop().time()
        ts = datetime.now(UTC)

        await poll_once(devices, store, plug_states, ts, recorder_state)

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
                store.refresh_daily_play_seconds()
            except Exception:
                log.warning("daily_play_seconds refresh failed", exc_info=True)
            polls_since_refresh = 0

        elapsed = asyncio.get_running_loop().time() - start
        await asyncio.sleep(max(0, 1.0 - elapsed))
