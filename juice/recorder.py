"""Recording daemon — polls strips and persists readings."""

from __future__ import annotations

import asyncio
import logging
import re
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from juice.collector import Account, PlugReading, Strip, _plug_reading
from juice.flipfix import MachineInfo
from juice.store import Store

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
    last_watts: float = -1.0  # -1 means never checked
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
    strips: list[Strip],
    store: Store,
    plug_states: dict[str, PlugState],
    ts: datetime,
    recorder_state: RecorderState | None = None,
) -> None:
    """One polling iteration: fetch sysinfo for all strips, selectively read emeter."""
    readings_count = 0
    for strip in strips:
        try:
            sysinfo = await strip._sysinfo()
        except Exception:
            log.warning("Failed to fetch sysinfo for %s", strip.device_id, exc_info=True)
            continue

        for child in sysinfo["children"]:
            child_id = child["id"]
            key = f"{strip.device_id}:{child_id}"

            # OFF plugs: record 0W to buffer and reading, skip emeter
            if not child["state"]:
                if recorder_state is not None:
                    plug_id = store.ensure_plug(strip.device_id, child_id, child["alias"])
                    _update_buffer(recorder_state, plug_id, 0.0)
                    recorder_state.plug_readings[plug_id] = PlugReading(
                        child_id=child_id,
                        alias=child["alias"],
                        is_on=False,
                        watts=0.0,
                        voltage=0.0,
                        amps=0.0,
                        total_kwh=0.0,
                    )
                continue

            # Check if we should skip idle plugs
            plug_id_for_skip = None
            if recorder_state is not None:
                plug_id_for_skip = store.ensure_plug(strip.device_id, child_id, child["alias"])
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

            # Fetch emeter
            try:
                emeter_resp = await strip._passthrough(
                    {
                        "context": {"child_ids": [child_id]},
                        "emeter": {"get_realtime": {}},
                    }
                )
                emeter = emeter_resp["emeter"]["get_realtime"]
            except Exception:
                log.warning("Failed emeter for %s on %s", child_id, strip.device_id, exc_info=True)
                continue

            reading = _plug_reading(child, emeter)
            plug_id = store.ensure_plug(strip.device_id, child_id, child["alias"])
            store.insert_readings(
                [(ts, plug_id, reading.watts, reading.voltage, reading.amps, reading.total_kwh)]
            )

            plug_states[key] = PlugState(last_watts=reading.watts, last_check=ts)
            readings_count += 1

            if recorder_state is not None:
                recorder_state.plug_readings[plug_id] = reading
                _update_buffer(recorder_state, plug_id, reading.watts)
                recorder_state.force_poll.discard(plug_id)

    log.debug("Poll: %d strips, %d readings recorded", len(strips), readings_count)


async def refresh_metadata(
    account: Account,
    store: Store,
    machines: dict[str, MachineInfo],
    ts: datetime,
    recorder_state: RecorderState | None = None,
) -> list[Strip]:
    """Refresh strip/plug metadata and update assignments. Returns current strip list."""
    strips = await account.strips()

    for strip in strips:
        sysinfo = await strip._sysinfo()
        if recorder_state is not None:
            recorder_state.strip_aliases[strip.device_id] = strip.alias
        for child in sysinfo["children"]:
            child_id = child["id"]
            alias = child["alias"]
            plug_id = store.ensure_plug(strip.device_id, child_id, alias)

            if recorder_state is not None:
                recorder_state.plugs[plug_id] = (strip.device_id, child_id, alias)
                # Store Plug object for power control from the API
                plug_obj = next((p for p in await strip.plugs() if p.child_id == child_id), None)
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
                    # Cache calibration for this plug
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

    return strips


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
    strips = await refresh_metadata(account, store, machines, ts, recorder_state)
    if recorder_state is not None:
        from juice.server import seed_buffers

        seed_buffers(recorder_state, store)
    log.info("Started: %d strips, %d machines", len(strips), len(machines))
    polls_since_refresh = 0

    while True:
        start = asyncio.get_running_loop().time()
        ts = datetime.now(UTC)

        await poll_once(strips, store, plug_states, ts, recorder_state)

        polls_since_refresh += 1
        if polls_since_refresh >= IDLE_RECHECK_SECONDS:
            try:
                if flipfix_url and flipfix_key:
                    machines = await get_machines(flipfix_url, flipfix_key)
                strips = await refresh_metadata(account, store, machines, ts, recorder_state)
                log.info("Refreshed: %d strips, %d machines", len(strips), len(machines))
            except Exception:
                log.warning("Metadata refresh failed", exc_info=True)
            polls_since_refresh = 0

        elapsed = asyncio.get_running_loop().time() - start
        await asyncio.sleep(max(0, 1.0 - elapsed))
