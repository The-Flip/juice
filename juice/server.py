"""HTTP server with API and web dashboard for juice."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from aiohttp import web

from juice.collector import Plug, PlugReading, _SelfPlug, call_with_retry, outlet_number
from juice.state import Calibration, CalibrationError, State, auto_calibrate, classify
from juice.store import Store

# A plug-like object that can be turned on/off and has an `alias` attribute.
Controllable = Plug | _SelfPlug

log = logging.getLogger(__name__)

BUFFER_SIZE = 3600  # ~60 minutes at 1s polling

# Seed calibrations for known machines (keyed by machine name)
SEED_CALIBRATIONS: dict[str, Calibration] = {
    "Eight Ball Deluxe Limited Edition": Calibration(idle_max_rsd=1.0, play_min_rsd=8.0),
    "Godzilla (Premium)": Calibration(idle_max_rsd=2.0, play_min_rsd=12.0),
    "Hyperball": Calibration(idle_max_rsd=None, play_min_rsd=13.0),
    "Revenge From Mars": Calibration(idle_max_rsd=None, play_min_rsd=5.0),
    "The Addams Family": Calibration(idle_max_rsd=2.1, play_min_rsd=7.0),
}


@dataclass
class Operation:
    """An in-flight bulk power operation (All On / All Off)."""

    id: str
    kind: str  # 'all_on' | 'all_off'
    started_at: datetime
    started_by: str
    targets: list[int]
    current_machine: str | None = None
    completed: list[int] = field(default_factory=list)
    failed: list[tuple[int, str]] = field(default_factory=list)
    index: int = 0
    state: str = "running"  # 'running' | 'complete' | 'cancelled'
    cancel_requested: bool = False


def _operation_to_dict(op: Operation) -> dict:
    return {
        "id": op.id,
        "kind": op.kind,
        "started_at": op.started_at.isoformat(),
        "started_by": op.started_by,
        "targets": list(op.targets),
        "current_machine": op.current_machine,
        "completed": list(op.completed),
        "failed": [{"plug_id": p, "error": e} for p, e in op.failed],
        "index": op.index,
        "total": len(op.targets),
        "state": op.state,
    }


@dataclass
class RecorderState:
    """Shared state between the recorder loop and the HTTP API."""

    plug_readings: dict[int, PlugReading] = field(default_factory=dict)
    watt_buffers: dict[int, deque] = field(default_factory=dict)
    assignments: dict[int, tuple[str, str, int | None]] = field(
        default_factory=dict
    )  # plug_id -> (name, asset_id, year)
    plugs: dict[int, tuple[str, str, str]] = field(
        default_factory=dict
    )  # plug_id -> (device_id, child_id, alias)
    calibrations: dict[int, Calibration] = field(default_factory=dict)  # plug_id -> Calibration
    strip_aliases: dict[str, str] = field(default_factory=dict)  # device_id -> strip alias
    # Operator-set strip names (device_id -> name). Display falls back to the
    # Kasa alias when no override is set.
    strip_names: dict[str, str] = field(default_factory=dict)
    plug_objects: dict[int, Controllable] = field(
        default_factory=dict
    )  # plug_id -> Plug or _SelfPlug (for control)
    plug_has_emeter: dict[int, bool] = field(default_factory=dict)  # plug_id -> has_emeter
    # Shutdown-locked machines (by asset_id, so a lock follows the machine
    # across outlet moves). Refused by individual off; skipped by all-off.
    locked_assets: set[str] = field(default_factory=set)
    force_poll: set[int] = field(default_factory=set)  # plug IDs to poll immediately
    current_operation: Operation | None = None
    event_subscribers: set[asyncio.Queue] = field(default_factory=set)
    # Device health: a device is "offline" once it has failed enough
    # consecutive reads. Offline devices are dropped from the fast poll loop
    # (re-probed only by the 60s metadata refresh) and their machines render
    # as OFFLINE rather than vanishing.
    offline_since: dict[str, datetime] = field(default_factory=dict)  # device_id -> marked-at
    device_failures: dict[str, int] = field(default_factory=dict)  # device_id -> consec. failures


def _actor(request: web.Request) -> str:
    """Return the requesting user's display identity for audit logs.

    Prefers email, then name, then OAuth subject, then 'anonymous' when auth is off.
    """
    user = request.get("user") or {}
    return user.get("email") or user.get("name") or user.get("sub") or "anonymous"


def _publish(state: RecorderState, event: dict) -> None:
    """Fan-out a single event to every SSE subscriber.

    Drops messages destined for queues that are full to protect against
    stuck clients holding up the publisher.
    """
    for q in list(state.event_subscribers):
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            log.warning("Dropping SSE event for full subscriber queue")


def seed_buffers(state: RecorderState, store: Store) -> None:
    """Pre-fill watt_buffers from DB so sparklines are available immediately.

    Skips no-emeter plugs (e.g. EP10) — they have no watts to sparkline.
    """
    from collections import deque

    for plug_id in state.assignments:
        if not state.plug_has_emeter.get(plug_id, True):
            continue
        watts = store.get_recent_watts(plug_id, seconds=BUFFER_SIZE)
        if watts:
            state.watt_buffers[plug_id] = deque(watts, maxlen=BUFFER_SIZE)


async def handle_machines(request: web.Request) -> web.Response:
    from juice.auth import is_authenticated

    public = not is_authenticated(request)
    state: RecorderState = request.app["recorder_state"]

    # (sort_key, machine_dict) pairs — the physical-order key is built from
    # state before public redaction, so public ordering matches the layout.
    ranked: list[tuple[tuple[str, bool, int, int], dict]] = []
    for plug_id, (name, asset_id, year) in state.assignments.items():
        reading = state.plug_readings.get(plug_id)
        plug_info = state.plugs.get(plug_id)
        has_emeter = state.plug_has_emeter.get(plug_id, True)

        power = None
        is_on: bool | None = None
        if reading is not None:
            is_on = reading.is_on
            if has_emeter and reading.watts is not None:
                power = {
                    "watts": round(reading.watts, 1),
                    "voltage": round(reading.voltage or 0.0, 1),
                    "amps": round(reading.amps or 0.0, 3),
                    "total_kwh": round(reading.total_kwh or 0.0, 1),
                }

        machine_state = None
        sparkline: list[float] = []
        sparkline_states: list[str] = []
        if has_emeter:
            buf = state.watt_buffers.get(plug_id)
            if buf:
                watts_list = list(buf)
                sparkline = watts_list
                cal = state.calibrations.get(plug_id)
                if cal:
                    classified = classify(watts_list, cal)
                    sparkline_states = [s.value for s in classified]
                    if classified:
                        machine_state = classified[-1].value

        # A machine whose device is parked offline renders as OFFLINE rather
        # than showing stale live data or vanishing from the dashboard.
        offline = plug_info is not None and plug_info[0] in state.offline_since
        if offline:
            machine_state = "OFFLINE"

        # Public viewers don't see plug/strip identifiers (names of power
        # strips and outlets are operational detail). Logged-in users see
        # everything — needed for the detail-page meta bar.
        plug_data: dict | None = None
        if plug_info:
            device_id, child_id, alias = plug_info
            if public:
                plug_data = {"plug_id": plug_id}
            else:
                plug_data = {
                    "plug_id": plug_id,
                    "device_id": device_id,
                    "child_id": child_id,
                    "alias": alias,
                    "outlet_number": outlet_number(child_id),
                }

        strip_device_id = "" if public else (plug_info[0] if plug_info else "")
        strip_alias = (
            "" if public else _strip_display_name(state, plug_info[0] if plug_info else "")
        )

        # Physical sort key built before public redaction, so public ordering
        # still matches the strip layout.
        position = outlet_number(plug_info[1]) if plug_info else None
        sort_key = (
            plug_info[0] if plug_info else "",
            position is None,
            position or 0,
            plug_id,
        )

        ranked.append(
            (
                sort_key,
                {
                    "name": name,
                    "asset_id": asset_id,
                    "year": year,
                    "plug": plug_data,
                    "power": power,
                    "state": machine_state,
                    "is_on": is_on,
                    "has_emeter": has_emeter,
                    "sparkline": sparkline,
                    "sparkline_states": sparkline_states,
                    "strip_device_id": strip_device_id,
                    "strip_alias": strip_alias,
                    "calibrated": plug_id in state.calibrations,
                    "offline": offline,
                    "locked": asset_id in state.locked_assets,
                },
            )
        )

    ranked.sort(key=lambda pair: pair[0])
    machines = [m for _key, m in ranked]

    # A machine that moved to a new outlet keeps a stale open assignment on its
    # old (now offline) plug. Drop the offline copy when the same machine also
    # appears on an online plug, so it shows once — on the new outlet.
    online_assets = {m["asset_id"] for m in machines if not m["offline"]}
    machines = [m for m in machines if not (m["offline"] and m["asset_id"] in online_assets)]

    return web.json_response({"machines": machines})


async def handle_outlets(request: web.Request) -> web.Response:
    """List recently-powered outlets with no machine tag (EP10s, signs, etc.)."""
    state: RecorderState = request.app["recorder_state"]
    store: Store = request.app["store"]

    outlets = []
    for plug_id, device_id, alias, _is_on_db in store.list_unassigned_outlets():
        # Prefer the live reading's on/off state if the recorder has one, using
        # the same _is_on rule as _build_targets so the tile and an all-off agree.
        reading = state.plug_readings.get(plug_id)
        is_on = _is_on(state, plug_id) if reading is not None else _is_on_db
        outlets.append(
            {
                "plug_id": plug_id,
                "device_id": device_id,
                "alias": alias,
                "is_on": is_on,
            }
        )
    return web.json_response({"outlets": outlets})


async def handle_calibrate(request: web.Request) -> web.Response:
    plug_id = int(request.match_info["plug_id"])
    state: RecorderState = request.app["recorder_state"]
    store: Store = request.app["store"]

    assignment = state.assignments.get(plug_id)
    if not assignment:
        return web.json_response({"error": "Plug not assigned to a machine"}, status=400)

    name, asset_id, _year = assignment
    machine_id = store.ensure_machine(asset_id, name)

    watts = store.get_recent_watts(plug_id, seconds=3600)
    try:
        calibration = auto_calibrate(watts)
    except CalibrationError as e:
        log.warning("Calibration failed for %s: %s", name, e)
        return web.json_response({"error": str(e)}, status=400)

    store.set_calibration(machine_id, calibration)
    state.calibrations[plug_id] = calibration
    log.info(
        "Calibrated %s: idle_max_rsd=%s, play_min_rsd=%.1f",
        name,
        calibration.idle_max_rsd,
        calibration.play_min_rsd,
    )

    return web.json_response(
        {
            "machine": name,
            "calibration": {
                "idle_max_rsd": calibration.idle_max_rsd,
                "play_min_rsd": calibration.play_min_rsd,
            },
        }
    )


async def handle_readings(request: web.Request) -> web.Response:
    plug_id = int(request.match_info["plug_id"])
    hours = int(request.query.get("hours", "24"))
    state: RecorderState = request.app["recorder_state"]
    store: Store = request.app["store"]

    from datetime import UTC, datetime, timedelta

    since = datetime.now(UTC) - timedelta(hours=hours)
    rows = store.get_readings_since(plug_id, since)

    watts = [r[1] for r in rows]
    states: list[str] = []
    cal = state.calibrations.get(plug_id)
    if cal and watts:
        states = [s.value for s in classify(watts, cal)]

    return web.json_response(
        {
            "timestamps": [r[0] for r in rows],
            "watts": watts,
            "states": states,
        }
    )


async def handle_power(request: web.Request) -> web.Response:
    from juice.auth import require_capability

    error = require_capability(request, "control_power")
    if error:
        return error

    plug_id = int(request.match_info["plug_id"])
    state: RecorderState = request.app["recorder_state"]
    store: Store = request.app["store"]

    plug = state.plug_objects.get(plug_id)
    if plug is None:
        return web.json_response({"error": "Plug not available"}, status=400)

    body = await request.json()
    on = body.get("on", True)
    actor = _actor(request)
    action = "turn_on" if on else "turn_off"
    ts = datetime.now(UTC)

    if not on:
        assignment = state.assignments.get(plug_id)
        if assignment and assignment[1] in state.locked_assets:
            # Audit write must not mask the refusal response.
            try:
                store.record_power_event(
                    ts,
                    plug_id,
                    action,
                    "individual",
                    actor,
                    "refused",
                    error="machine is shutdown-locked",
                )
            except Exception as e:
                log.warning("Audit write failed for plug %d: %s", plug_id, e)
            return web.json_response(
                {"error": f"{assignment[0]} is shutdown-locked — unlock it before turning off"},
                status=409,
            )

    attempts_made = 1

    def _bump_attempts(attempt: int, exc: BaseException, delay: float) -> None:
        nonlocal attempts_made
        attempts_made = attempt + 1
        log.warning(
            "Retrying plug %d (individual) after attempt %d: %s (sleeping %.1fs)",
            plug_id,
            attempt,
            exc,
            delay,
        )

    try:
        await call_with_retry(
            plug.turn_on if on else plug.turn_off,
            max_attempts=6,
            on_retry=_bump_attempts,
        )
        if on:
            state.force_poll.add(plug_id)
    except Exception as e:
        log.warning("Power control failed for plug %d: %s", plug_id, e)
        err_msg = f"{e} (after {attempts_made} attempts)" if attempts_made > 1 else str(e)
        store.record_power_event(ts, plug_id, action, "individual", actor, "error", error=err_msg)
        return web.json_response({"error": err_msg}, status=500)

    log.info("Plug %d (%s) turned %s by %s", plug_id, plug.alias, "ON" if on else "OFF", actor)
    # Audit write must not fail the response: the device has already toggled.
    try:
        store.record_power_event(ts, plug_id, action, "individual", actor, "ok")
    except Exception as e:
        log.warning("Audit write failed for plug %d: %s", plug_id, e)
    _publish(
        state,
        {
            "type": "power_change",
            "plug_id": plug_id,
            "on": on,
            "actor": actor,
            "source": "individual",
        },
    )
    return web.json_response({"ok": True, "on": on})


async def handle_lock(request: web.Request) -> web.Response:
    """Set or clear the shutdown lock on the machine assigned to a plug."""
    from juice.auth import require_capability

    error = require_capability(request, "control_power")
    if error:
        return error

    plug_id = int(request.match_info["plug_id"])
    state: RecorderState = request.app["recorder_state"]
    store: Store = request.app["store"]

    assignment = state.assignments.get(plug_id)
    if not assignment:
        return web.json_response({"error": "Plug not assigned to a machine"}, status=400)

    name, asset_id, _year = assignment
    body = await request.json()
    locked = body.get("locked", True)
    if not isinstance(locked, bool):
        return web.json_response({"error": "locked must be a boolean"}, status=400)

    machine_id = store.ensure_machine(asset_id, name)
    store.set_machine_locked(machine_id, locked)
    if locked:
        state.locked_assets.add(asset_id)
    else:
        state.locked_assets.discard(asset_id)

    actor = _actor(request)
    log.info("Machine %s (%s) %s by %s", name, asset_id, "locked" if locked else "unlocked", actor)
    _publish(
        state,
        {
            "type": "lock_change",
            "plug_id": plug_id,
            "asset_id": asset_id,
            "locked": locked,
            "actor": actor,
        },
    )
    return web.json_response({"ok": True, "locked": locked})


def _strip_display_name(state: RecorderState, device_id: str) -> str:
    """Operator-set strip name, falling back to the Kasa alias."""
    return state.strip_names.get(device_id) or state.strip_aliases.get(device_id, "")


async def handle_strip_detail(request: web.Request) -> web.Response:
    """All outlets of one strip in physical order, with attached machines."""
    device_id = request.match_info["device_id"]
    state: RecorderState = request.app["recorder_state"]

    # Known either from the cloud refresh (strip_aliases) or DB hydration
    # (plugs) — the latter keeps offline-at-boot strips reachable.
    plug_ids = [pid for pid, (dev, _cid, _alias) in state.plugs.items() if dev == device_id]
    if device_id not in state.strip_aliases and not plug_ids:
        return web.json_response({"error": "Unknown device"}, status=404)

    outlets = []
    for plug_id in plug_ids:
        _dev, child_id, alias = state.plugs[plug_id]
        assignment = state.assignments.get(plug_id)
        reading = state.plug_readings.get(plug_id)
        watts = None
        if reading is not None and reading.watts is not None:
            watts = round(reading.watts, 1)
        outlets.append(
            {
                "plug_id": plug_id,
                "child_id": child_id,
                "outlet_number": outlet_number(child_id),
                "alias": alias,
                "machine": (
                    {"name": assignment[0], "asset_id": assignment[1]} if assignment else None
                ),
                "is_on": _is_on(state, plug_id),
                "watts": watts,
            }
        )
    outlets.sort(key=lambda o: (o["outlet_number"] is None, o["outlet_number"] or 0, o["plug_id"]))

    return web.json_response(
        {
            "device_id": device_id,
            "alias": state.strip_aliases.get(device_id, ""),
            "name": state.strip_names.get(device_id, ""),
            "display_name": _strip_display_name(state, device_id),
            "offline": device_id in state.offline_since,
            "outlets": outlets,
        }
    )


MAX_STRIP_NAME_LEN = 100


async def handle_strip_name(request: web.Request) -> web.Response:
    """Set or clear the human-friendly name of a strip."""
    from juice.auth import require_capability

    error = require_capability(request, "control_power")
    if error:
        return error

    device_id = request.match_info["device_id"]
    state: RecorderState = request.app["recorder_state"]
    store: Store = request.app["store"]

    known = device_id in state.strip_aliases or any(
        dev == device_id for dev, _cid, _alias in state.plugs.values()
    )
    if not known:
        return web.json_response({"error": "Unknown device"}, status=404)

    body = await request.json()
    name = body.get("name")
    if not isinstance(name, str):
        return web.json_response({"error": "name must be a string"}, status=400)
    if len(name) > MAX_STRIP_NAME_LEN:
        return web.json_response(
            {"error": f"name must be at most {MAX_STRIP_NAME_LEN} characters"}, status=400
        )

    name = name.strip()
    store.set_strip_name(device_id, name)
    if name:
        state.strip_names[device_id] = name
    else:
        state.strip_names.pop(device_id, None)

    actor = _actor(request)
    log.info("Strip %s named %r by %s", device_id, name, actor)
    _publish(
        state,
        {"type": "strip_name_change", "device_id": device_id, "name": name, "actor": actor},
    )
    return web.json_response(
        {"ok": True, "name": name, "display_name": _strip_display_name(state, device_id)}
    )


def _is_on(state: RecorderState, plug_id: int) -> bool:
    """Best-effort on/off for a plug from its latest reading.

    No live reading → treat as off (we can't be sure it's on). Emeter plugs use
    watts > 0; no-emeter plugs use the reading's is_on flag.
    """
    reading = state.plug_readings.get(plug_id)
    if reading is None:
        return False
    if state.plug_has_emeter.get(plug_id, True):
        return (reading.watts or 0.0) > 0
    return bool(reading.is_on)


def _build_targets(
    state: RecorderState, kind: str, outlet_plug_ids: list[int] | None = None
) -> list[int]:
    """Plug IDs to act on for an all-on / all-off.

    Machines come first, sorted by year ascending; non-machine outlets (passed in,
    already ordered) are appended **last** so they switch after every machine.

    Mirrors the client-side filter the dashboard used to apply:
    - Skip plugs already in the desired state.
    - When turning off, skip shutdown-locked machines (they must be unlocked
      first). Outlets have no machine, so this gate never applies to them.
    - When turning off, skip PLAYING machines (don't interrupt a game). Outlets
      have no calibration, so this gate never applies to them.
    - With no live reading yet, leave the plug alone on all-off (we can't be sure
      it's on) but include it on all-on (so it's brought up to the desired state).
    """
    on = kind == "all_on"
    ranked: list[tuple[int, int]] = []  # (year_key, plug_id)
    for plug_id, (_name, asset_id, year) in state.assignments.items():
        is_on = _is_on(state, plug_id)

        if on and is_on:
            continue
        if not on and not is_on:
            continue

        if not on and asset_id in state.locked_assets:
            continue

        if not on:
            buf = state.watt_buffers.get(plug_id)
            cal = state.calibrations.get(plug_id)
            if buf and cal:
                classified = classify(list(buf), cal)
                if classified and classified[-1] is State.PLAYING:
                    continue

        ranked.append((year if year is not None else 0, plug_id))
    ranked.sort(key=lambda t: t[0])
    targets = [pid for _, pid in ranked]

    for plug_id in outlet_plug_ids or []:
        is_on = _is_on(state, plug_id)
        if on and is_on:
            continue
        if not on and not is_on:
            continue
        targets.append(plug_id)

    return targets


# A bulk "all on" / "all off" walks every machine, so we keep the per-plug
# retry tight: ride out a transient cloud blip but give up quickly on plugs
# that are genuinely unreachable, otherwise one dead plug spins forever and
# blocks the whole operation. Backoff is 0.5 / 1 / 2 s before the 4th attempt
# (~3.5 s wall per failed plug). Individual power control stays at 6 attempts.
BULK_OP_MAX_ATTEMPTS = 4


async def run_operation(
    state: RecorderState,
    store: Store,
    op: Operation,
    on: bool,
    sleep: float,
) -> None:
    """Execute a bulk power operation, publishing progress events and audit rows.

    Honors `op.cancel_requested` between steps. Records one audit row per
    attempt (including missing-plug and exception failures).
    """
    _publish(state, {"type": "operation_started", "operation": _operation_to_dict(op)})
    action = "turn_on" if on else "turn_off"
    total = len(op.targets)

    for idx, plug_id in enumerate(op.targets):
        if op.cancel_requested:
            op.state = "cancelled"
            break

        op.index = idx
        machine = state.assignments.get(plug_id)
        machine_name: str | None
        if machine:
            machine_name = machine[0]
        else:
            # Non-machine outlet: fall back to its plug alias for the UI/audit.
            plug_info = state.plugs.get(plug_id)
            machine_name = plug_info[2] if plug_info else None
        op.current_machine = machine_name
        plug = state.plug_objects.get(plug_id)
        ts = datetime.now(UTC)

        result: str
        error: str | None = None
        if plug is None:
            error = "plug not available"
            result = "error"
            op.failed.append((plug_id, error))
        else:
            attempts_made = 1  # incremented by _on_retry below

            def _on_retry(
                attempt: int,
                exc: BaseException,
                delay: float,
                *,
                plug_id: int = plug_id,
                machine_name: str | None = machine_name,
                idx: int = idx,
            ) -> None:
                nonlocal attempts_made
                attempts_made = attempt + 1
                log.warning(
                    "Retrying plug %d after attempt %d failed: %s (sleeping %.1fs)",
                    plug_id,
                    attempt,
                    exc,
                    delay,
                )
                _publish(
                    state,
                    {
                        "type": "operation_step_retry",
                        "operation_id": op.id,
                        "plug_id": plug_id,
                        "machine_name": machine_name,
                        "action": action,
                        "attempt": attempt,
                        "next_attempt": attempt + 1,
                        "delay": delay,
                        "error": str(exc),
                        "index": idx,
                        "total": total,
                    },
                )

            try:
                await call_with_retry(
                    plug.turn_on if on else plug.turn_off,
                    should_stop=lambda: op.cancel_requested,
                    max_attempts=BULK_OP_MAX_ATTEMPTS,
                    on_retry=_on_retry,
                )
            except Exception as e:
                log.warning("Power op step failed for plug %d: %s", plug_id, e)
                error = f"{e} (after {attempts_made} attempts)" if attempts_made > 1 else str(e)
                result = "error"
                op.failed.append((plug_id, error))
            else:
                if on:
                    state.force_poll.add(plug_id)
                result = "ok"
                op.completed.append(plug_id)

        store.record_power_event(
            ts,
            plug_id,
            action,
            op.kind,
            op.started_by,
            result,
            operation_id=op.id,
            error=error,
        )
        step_event: dict = {
            "type": "operation_step",
            "operation_id": op.id,
            "plug_id": plug_id,
            "machine_name": machine_name,
            "action": action,
            "result": result,
            "index": idx,
            "total": total,
        }
        if error is not None:
            step_event["error"] = error
        _publish(state, step_event)
        if result == "ok":
            _publish(
                state,
                {
                    "type": "power_change",
                    "plug_id": plug_id,
                    "on": on,
                    "actor": op.started_by,
                    "source": op.kind,
                },
            )

        if idx < total - 1 and sleep > 0:
            await asyncio.sleep(sleep)

    if op.state != "cancelled":
        op.state = "complete"
    op.index = total
    op.current_machine = None
    _publish(
        state,
        {
            "type": "operation_complete",
            "operation_id": op.id,
            "state": op.state,
            "completed": list(op.completed),
            "failed": [{"plug_id": p, "error": e} for p, e in op.failed],
        },
    )
    state.current_operation = None


async def _start_operation(request: web.Request, kind: str) -> web.Response:
    from juice.auth import require_capability

    err = require_capability(request, "control_power")
    if err:
        return err

    state: RecorderState = request.app["recorder_state"]
    store: Store = request.app["store"]

    current = state.current_operation
    if current is not None and current.state == "running":
        return web.json_response(
            {"error": "operation already in progress", "operation_id": current.id},
            status=409,
        )

    # Sweep non-machine outlets too (recently-powered, unassigned) — last.
    outlet_ids = [row[0] for row in store.list_unassigned_outlets()]
    targets = _build_targets(state, kind, outlet_ids)
    op = Operation(
        id=uuid.uuid4().hex,
        kind=kind,
        started_at=datetime.now(UTC),
        started_by=_actor(request),
        targets=targets,
    )
    state.current_operation = op
    on = kind == "all_on"
    sleep = 2.0 if on else 1.0
    asyncio.create_task(run_operation(state, store, op, on, sleep))
    log.info("Started %s op %s by %s (%d targets)", kind, op.id, op.started_by, len(targets))
    return web.json_response({"operation_id": op.id, "targets": len(targets)})


async def handle_all_on(request: web.Request) -> web.Response:
    return await _start_operation(request, "all_on")


async def handle_all_off(request: web.Request) -> web.Response:
    return await _start_operation(request, "all_off")


async def handle_cancel_operation(request: web.Request) -> web.Response:
    from juice.auth import require_capability

    err = require_capability(request, "control_power")
    if err:
        return err

    state: RecorderState = request.app["recorder_state"]
    op_id = request.match_info["id"]
    op = state.current_operation
    if op is None or op.id != op_id:
        return web.json_response({"error": "operation not found"}, status=404)
    op.cancel_requested = True
    log.info("Cancel requested on op %s by %s", op_id, _actor(request))
    return web.json_response({"ok": True})


async def handle_current_operation(request: web.Request) -> web.Response:
    state: RecorderState = request.app["recorder_state"]
    if state.current_operation is None:
        return web.json_response(None)
    return web.json_response(_operation_to_dict(state.current_operation))


async def _sse_stream(
    state: RecorderState,
    write: Callable[[dict], Awaitable[None]],
) -> None:
    """Register an event subscriber, send a hello, then forward events until cancelled."""
    queue: asyncio.Queue = asyncio.Queue(maxsize=64)
    state.event_subscribers.add(queue)
    try:
        await write(
            {
                "type": "hello",
                "current_operation": (
                    _operation_to_dict(state.current_operation)
                    if state.current_operation is not None
                    else None
                ),
            }
        )
        while True:
            event = await queue.get()
            await write(event)
    finally:
        state.event_subscribers.discard(queue)


_MAX_POWER_EVENTS_LIMIT = 200


async def handle_power_events(request: web.Request) -> web.Response:
    """List recent power events (audit log) with cursor pagination via ?before=."""
    store: Store = request.app["store"]
    try:
        limit = int(request.query.get("limit", "50"))
    except ValueError:
        limit = 50
    limit = max(1, min(limit, _MAX_POWER_EVENTS_LIMIT))

    before: int | None = None
    raw_before = request.query.get("before")
    if raw_before is not None:
        try:
            before = int(raw_before)
        except ValueError:
            before = None

    rows = store.recent_power_events(limit=limit, before=before)
    events = [
        {
            "event_id": r["event_id"],
            # DuckDB returns naive datetimes; mark them UTC for the client so
            # `new Date(iso)` doesn't reinterpret as local time.
            "ts": (
                (
                    r["ts"].replace(tzinfo=UTC)
                    if r["ts"].tzinfo is None
                    else r["ts"].astimezone(UTC)
                ).isoformat()
                if hasattr(r["ts"], "isoformat")
                else r["ts"]
            ),
            "plug_id": r["plug_id"],
            "action": r["action"],
            "source": r["source"],
            "operation_id": r["operation_id"],
            "actor": r["actor"],
            "result": r["result"],
            "error": r["error"],
            "plug_alias": r["plug_alias"],
            "machine_name": r["machine_name"],
        }
        for r in rows
    ]
    return web.json_response({"events": events})


# Stable palette for machine bands on the /usage chart. Chosen for high
# contrast on a white background; bigger than the typical 10-color schemes
# so machines retain their colour even with reassignments over time.
_USAGE_PALETTE = (
    "#4e79a7",
    "#f28e2b",
    "#e15759",
    "#76b7b2",
    "#59a14f",
    "#edc948",
    "#b07aa1",
    "#ff9da7",
    "#9c755f",
    "#1f77b4",
    "#ff7f0e",
    "#8c564b",
)
_UNASSIGNED_COLOR = "#aeaeb2"


def _machine_color(machine_id: int | None) -> str:
    if machine_id is None:
        return _UNASSIGNED_COLOR
    return _USAGE_PALETTE[machine_id % len(_USAGE_PALETTE)]


def _floor_hour_utc(dt: datetime) -> datetime:
    """Return the top of the hour containing dt (UTC, tz-aware)."""
    return dt.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def _parse_iso_or_none(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


async def handle_usage(request: web.Request) -> web.Response:
    """Historical power usage for [start, end), bucketed by hour, by machine.

    Query params:
      days  — window length in days (default 30, clamped to [1, 365]).
              Used when `start` and `end` are not provided.
      start — ISO timestamp (UTC) for the window start. Optional.
      end   — ISO timestamp (UTC) for the window end. Optional. Defaults
              to the top of the *next* hour (so the current partial hour
              is included).

    Both bounds are aligned to top-of-hour-UTC. If the rollup table is
    behind, this handler refreshes it defensively before reading.
    """
    store: Store = request.app["store"]

    explicit_start = _parse_iso_or_none(request.query.get("start"))
    explicit_end = _parse_iso_or_none(request.query.get("end"))

    if explicit_end is not None:
        end = _floor_hour_utc(explicit_end)
    else:
        now = datetime.now(UTC)
        end = _floor_hour_utc(now) + timedelta(hours=1)

    if explicit_start is not None:
        start = _floor_hour_utc(explicit_start)
    else:
        try:
            days = int(request.query.get("days", "30"))
        except ValueError:
            days = 30
        days = max(1, min(days, 365))
        start = end - timedelta(days=days)

    # Read straight from the rollup. The recorder owns refreshing it (on
    # startup + every 60s) so the handler doesn't block the event loop on
    # what could be a full-history backfill on a fresh DB. Worst case: the
    # chart's right edge is up to ~60s stale right after server startup.
    rows = store.usage_by_machine(start, end)

    # Build the full list of hour buckets in the window so the client gets
    # continuous bands even where no machine drew power.
    hours: list[datetime] = []
    cur = start
    while cur < end:
        hours.append(cur)
        cur += timedelta(hours=1)
    hour_index = {h: i for i, h in enumerate(hours)}

    # Group rows by (machine_id, machine_name); fill hourly_kwh aligned to hours.
    by_machine: dict[tuple[int | None, str], list[float]] = {}
    for row in rows:
        key = (row["machine_id"], row["machine_name"])
        if key not in by_machine:
            by_machine[key] = [0.0] * len(hours)
        ts = row["hour_ts"]
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        idx = hour_index.get(ts)
        if idx is not None:
            by_machine[key][idx] += float(row["kwh"])

    machines: list[dict] = []
    for (machine_id, name), hourly in by_machine.items():
        rounded = [round(v, 4) for v in hourly]
        # Total is summed from the rounded hourly values so the contract
        # `sum(hourly_kwh) == total_kwh` holds exactly on the client.
        machines.append(
            {
                "machine_id": machine_id,
                "name": name,
                "color": _machine_color(machine_id),
                "hourly_kwh": rounded,
                "total_kwh": round(sum(rounded), 4),
            }
        )
    # Sort biggest contributors first; "Unassigned" sinks to the bottom of
    # the legend regardless of size so it doesn't crowd the real machines.
    machines.sort(key=lambda m: (m["name"] == "Unassigned", -m["total_kwh"], m["name"]))

    return web.json_response(
        {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "hours": [h.isoformat() for h in hours],
            "machines": machines,
            "total_kwh": round(sum(m["total_kwh"] for m in machines), 4),
        }
    )


async def handle_usage_page(request: web.Request) -> web.Response:
    return _render_page(USAGE_HTML, request)


# Local timezone for the play-hours chart's day bucketing. Hardcoded to
# the museum's wall-clock so bars line up with how the user perceives a
# "day" (matches what juice.store uses for the rollup).
_LOCAL_TZ = ZoneInfo("America/Chicago")


def _parse_iso_date_or_none(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


async def handle_play_hours(request: web.Request) -> web.Response:
    """Per-machine play hours per local-day for [start, end).

    Query params:
      days       — window length in days (default 30, clamped to [1, 365]).
                   Used when start/end are absent.
      start, end — ISO local-date (YYYY-MM-DD). Half-open. Defaults to
                   `end = tomorrow_local`, `start = end - days`.

    Only machines with a calibration row contribute (the rollup itself
    enforces this — there's no Unassigned bucket here).
    """
    store: Store = request.app["store"]

    today_local = datetime.now(_LOCAL_TZ).date()
    explicit_start = _parse_iso_date_or_none(request.query.get("start"))
    explicit_end = _parse_iso_date_or_none(request.query.get("end"))

    end = explicit_end if explicit_end is not None else today_local + timedelta(days=1)
    if explicit_start is not None:
        start = explicit_start
    else:
        try:
            days = int(request.query.get("days", "30"))
        except ValueError:
            days = 30
        days = max(1, min(days, 365))
        start = end - timedelta(days=days)

    rows = store.play_hours_by_machine(start, end)

    # Build the full list of day buckets in the window so the client gets
    # consistent x-axis labels even on days with no play.
    days_list: list[date] = []
    cur = start
    while cur < end:
        days_list.append(cur)
        cur += timedelta(days=1)
    day_index = {d: i for i, d in enumerate(days_list)}

    by_machine: dict[tuple[int, str], list[float]] = {}
    for row in rows:
        key = (row["machine_id"], row["machine_name"])
        if key not in by_machine:
            by_machine[key] = [0.0] * len(days_list)
        idx = day_index.get(row["day_local"])
        if idx is not None:
            by_machine[key][idx] += float(row["hours"])

    machines: list[dict] = []
    for (machine_id, name), hourly in by_machine.items():
        rounded = [round(v, 4) for v in hourly]
        machines.append(
            {
                "machine_id": machine_id,
                "name": name,
                "color": _machine_color(machine_id),
                "daily_hours": rounded,
                "total_hours": round(sum(rounded), 4),
            }
        )
    # Biggest contributor first so the legend ranks naturally.
    machines.sort(key=lambda m: (-m["total_hours"], m["name"]))

    return web.json_response(
        {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "days": [d.isoformat() for d in days_list],
            "machines": machines,
            "total_hours": round(sum(m["total_hours"] for m in machines), 4),
        }
    )


async def handle_events(request: web.Request) -> web.StreamResponse:
    response = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
    await response.prepare(request)
    state: RecorderState = request.app["recorder_state"]

    async def _write(event: dict) -> None:
        await response.write(f"data: {json.dumps(event)}\n\n".encode())

    try:
        await _sse_stream(state, _write)
    except asyncio.CancelledError, ConnectionResetError:
        pass
    return response


def _html_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def _render_page(template: str, request: web.Request) -> web.Response:
    """Substitute auth-aware markers into a static HTML template.

    Markers (matched as literal text — `str.replace`, not `.format`, so the
    `{` characters in inline JS don't collide):
      {{PUBLIC_MODE}}    — `true` or `false`, used as a JS boolean.
      {{BODY_CLASS}}     — `public` or `authed`. Templates rely on
                           `body.public .private-only { display: none }`
                           to hide controls without per-element JS.
      {{AUTH_CORNER}}    — top-right login link / user pill markup,
                           or empty when OAuth isn't configured (dev mode).
    """
    from juice.auth import is_authenticated, oauth_config_key

    oauth_enabled = oauth_config_key in request.app
    public = oauth_enabled and not is_authenticated(request)
    user = request.get("user") or {}
    name = user.get("name") or user.get("email") or ""

    if not oauth_enabled:
        auth_corner = ""
    elif public:
        auth_corner = '<a class="auth-corner login-btn" href="/login">Login</a>'
    else:
        auth_corner = (
            '<span class="auth-corner user-pill">'
            f"{_html_escape(name)} &middot; "
            '<a href="/logout">log out</a>'
            "</span>"
        )

    html = (
        template.replace("{{PUBLIC_MODE}}", "true" if public else "false")
        .replace("{{BODY_CLASS}}", "public" if public else "authed")
        .replace("{{AUTH_CORNER}}", auth_corner)
    )
    return web.Response(text=html, content_type="text/html")


async def handle_dashboard(request: web.Request) -> web.Response:
    return _render_page(DASHBOARD_HTML, request)


async def handle_machine_detail(request: web.Request) -> web.Response:
    return _render_page(DETAIL_HTML, request)


async def handle_events_page(request: web.Request) -> web.Response:
    return _render_page(EVENTS_HTML, request)


async def handle_strip_page(request: web.Request) -> web.Response:
    return _render_page(STRIP_HTML, request)


def create_app(
    recorder_state: RecorderState,
    store: Store,
    oauth_config: dict | None = None,
) -> web.Application:
    app = web.Application()
    app["recorder_state"] = recorder_state
    app["store"] = store

    if oauth_config:
        from juice.auth import setup_auth

        setup_auth(app, oauth_config)

    app.router.add_get("/", handle_dashboard)
    app.router.add_get("/machine/{plug_id}", handle_machine_detail)
    app.router.add_get("/api/machines", handle_machines)
    app.router.add_get("/api/outlets", handle_outlets)
    app.router.add_get("/api/machines/{plug_id}/readings", handle_readings)
    app.router.add_post("/api/machines/{plug_id}/calibrate", handle_calibrate)
    app.router.add_post("/api/machines/{plug_id}/power", handle_power)
    app.router.add_post("/api/machines/{plug_id}/lock", handle_lock)
    app.router.add_get("/api/strips/{device_id}", handle_strip_detail)
    app.router.add_post("/api/strips/{device_id}/name", handle_strip_name)
    app.router.add_post("/api/plugs/{plug_id}/power", handle_power)
    app.router.add_post("/api/operations/all-on", handle_all_on)
    app.router.add_post("/api/operations/all-off", handle_all_off)
    app.router.add_post("/api/operations/{id}/cancel", handle_cancel_operation)
    app.router.add_get("/api/operations/current", handle_current_operation)
    app.router.add_get("/api/events", handle_events)
    app.router.add_get("/api/power-events", handle_power_events)
    app.router.add_get("/events", handle_events_page)
    app.router.add_get("/api/usage", handle_usage)
    app.router.add_get("/api/play-hours", handle_play_hours)
    app.router.add_get("/usage", handle_usage_page)
    app.router.add_get("/strip/{device_id}", handle_strip_page)
    return app


async def start_server(
    recorder_state: RecorderState,
    store: Store,
    host: str = "0.0.0.0",  # noqa: S104
    port: int = 8000,
    oauth_config: dict | None = None,
) -> web.AppRunner:
    app = create_app(recorder_state, store, oauth_config=oauth_config)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    return runner


DASHBOARD_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>juice</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
    background: #f5f5f7;
    color: #1d1d1f;
    min-height: 100vh;
  }
  header {
    padding: 20px 28px 14px;
    border-bottom: 1px solid #d2d2d7;
    background: #fff;
    display: flex; align-items: center; gap: 16px;
  }
  header h1 {
    font-size: 17px;
    font-weight: 600;
    color: #86868b;
    flex: 1;
  }
  header h1 span { color: #1d1d1f; }
  .flip-link { color: #007aff; text-decoration: none; }
  .flip-link:hover { text-decoration: underline; }
  .auth-corner { margin-left: auto; font-size: 13px; }
  .login-btn {
    padding: 6px 14px; border-radius: 6px;
    background: #007aff; color: #fff;
    text-decoration: none; font-weight: 600;
  }
  .login-btn:hover { opacity: 0.85; }
  .user-pill { color: #86868b; }
  .user-pill a { color: #007aff; text-decoration: none; margin-left: 6px; }
  .user-pill a:hover { text-decoration: underline; }
  /* Public viewers see no controls or operator-only chrome. */
  body.public .private-only { display: none !important; }
  .header-nav { display: flex; gap: 14px; margin-right: 8px; }
  .header-nav a {
    color: #007aff; text-decoration: none; font-size: 13px; font-weight: 500;
  }
  .header-nav a:hover { text-decoration: underline; }
  .power-btns { display: flex; gap: 8px; }
  .power-btn {
    padding: 6px 16px; border-radius: 6px; font-size: 13px; font-weight: 600;
    cursor: pointer; border: none; color: #fff; transition: opacity 0.15s;
  }
  .power-btn:hover { opacity: 0.85; }
  .power-btn:disabled { opacity: 0.5; cursor: default; }
  .power-btn-on { background: #34c759; }
  .power-btn-off { background: #ff3b30; }
  #content { padding: 20px 28px; }
  .strip-row {
    margin-bottom: 20px;
  }
  .strip-label {
    display: block;
    font-size: 12px;
    font-weight: 600;
    color: #86868b;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 8px;
    text-decoration: none;
  }
  a.strip-label:hover { color: #007aff; text-decoration: underline; }
  .tiles {
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
  }
  .tile {
    width: 140px;
    height: 140px;
    background: #fff;
    border: 1px solid #d2d2d7;
    border-radius: 10px;
    padding: 12px;
    display: flex;
    flex-direction: column;
    position: relative;
    cursor: pointer;
    transition: box-shadow 0.15s;
    text-decoration: none;
    color: inherit;
  }
  .tile:hover {
    box-shadow: 0 2px 12px rgba(0,0,0,0.1);
  }
  .tile-top {
    display: flex;
    align-items: center;
    gap: 6px;
    margin-bottom: 6px;
  }
  .state-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    flex-shrink: 0;
  }
  .state-OFF { background: #1d1d1f; }
  .state-ATTRACT { background: #007aff; }
  .state-PLAYING { background: #34c759; }
  .state-IDLE { background: #f5c41a; }
  .state-null { background: #aeaeb2; border: 1px dashed #c7c7cc; }
  .state-OFFLINE { background: #c7c7cc; }
  .tile.offline { opacity: 0.55; }
  .tile-offline {
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 12px;
    font-weight: 600;
    letter-spacing: 0.5px;
    color: #86868b;
  }
  .machine-name {
    font-size: 12px;
    font-weight: 600;
    line-height: 1.2;
    overflow: hidden;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
    color: #1d1d1f;
  }
  .tile-lock {
    font-size: 11px;
    margin-left: auto;
    flex-shrink: 0;
  }
  .sparkline-wrap {
    flex: 1;
    min-height: 0;
    border-radius: 4px;
    overflow: hidden;
  }
  .sparkline-wrap canvas {
    width: 100%;
    height: 100%;
  }
  .tile-watts {
    font-size: 11px;
    font-weight: 500;
    color: #86868b;
    text-align: right;
    margin-top: 4px;
    font-variant-numeric: tabular-nums;
  }
  .no-data {
    text-align: center;
    padding: 60px 20px;
    color: #86868b;
    font-size: 14px;
  }
  .tile-onoff {
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 13px;
    font-weight: 600;
    color: #86868b;
    letter-spacing: 0.5px;
  }
  .tile-onoff.on { color: #34c759; }
  .tile-onoff.off { color: #1d1d1f; }
  .tile-toggle {
    margin-top: 4px;
    padding: 4px 0;
    border-radius: 6px;
    border: none;
    font-size: 11px;
    font-weight: 600;
    cursor: pointer;
    color: #fff;
    transition: opacity 0.15s;
  }
  .tile-toggle:hover { opacity: 0.85; }
  .tile-toggle.on { background: #34c759; }
  .tile-toggle.off { background: #ff3b30; }
  .outlets-section { margin-top: 8px; }
  .outlets-section .strip-label { color: #86868b; }
  .outlet-tile {
    display: flex;
    flex-direction: column;
  }
  .outlet-tile .outlet-alias {
    font-size: 12px;
    font-weight: 600;
    line-height: 1.2;
    color: #1d1d1f;
    overflow: hidden;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
    margin-bottom: 6px;
  }
  .op-banner {
    display: flex; align-items: center; gap: 16px;
    padding: 12px 28px;
    background: #e3f2fd; color: #0d47a1;
    border-bottom: 1px solid #bbdefb;
    font-size: 14px; font-weight: 500;
  }
  .op-banner-text { flex: 1; }
  .op-banner.cancelled { background: #f5f5f7; color: #86868b; }
  .op-banner.complete  { background: #e8f5e9; color: #1b5e20; }
  .op-banner.retrying  { background: #fff4e0; color: #8a5500; border-bottom-color: #ffd591; }
  .op-banner .retry-spinner {
    display: inline-block; width: 10px; height: 10px; margin-right: 8px;
    border-radius: 50%; background: #f5a623;
    animation: retry-pulse 1.2s ease-in-out infinite;
    vertical-align: middle;
  }
  @keyframes retry-pulse {
    0%, 100% { opacity: 0.35; transform: scale(0.9); }
    50%      { opacity: 1;    transform: scale(1.15); }
  }
  .op-banner-cancel {
    padding: 6px 14px; border-radius: 6px; border: none;
    background: #ff3b30; color: #fff; font-weight: 600; font-size: 12px;
    cursor: pointer; transition: opacity 0.15s;
  }
  .op-banner-cancel:hover { opacity: 0.85; }
  .op-banner-cancel:disabled { opacity: 0.5; cursor: default; }
  .recent-events {
    margin: 0 28px 24px;
    background: #fff; border: 1px solid #d2d2d7; border-radius: 10px;
    padding: 12px 16px;
  }
  .recent-events-header {
    display: flex; justify-content: space-between; align-items: baseline;
    font-size: 11px; font-weight: 600; color: #86868b;
    text-transform: uppercase; letter-spacing: 0.5px;
    margin-bottom: 8px;
  }
  .recent-events-header a {
    text-transform: none; letter-spacing: 0;
    color: #007aff; text-decoration: none; font-size: 12px;
  }
  .recent-events-header a:hover { text-decoration: underline; }
  .recent-events ul { list-style: none; }
  .recent-events li {
    padding: 4px 0; font-size: 12px; color: #1d1d1f;
    font-variant-numeric: tabular-nums;
    display: flex; gap: 8px; align-items: baseline;
  }
  .recent-events .evt-time { color: #86868b; min-width: 64px; }
  .recent-events .evt-action.on  { color: #2e7d32; font-weight: 600; }
  .recent-events .evt-action.off { color: #c62828; font-weight: 600; }
  .recent-events .evt-source { color: #86868b; font-size: 11px; }
  .recent-events .evt-error { color: #c62828; font-size: 11px; }
</style>
</head>
<body class="{{BODY_CLASS}}">
<header>
  <h1>
    <span>juice</span> &mdash; machine status for
    <a class="flip-link" href="https://theflip.museum">The Flip</a>
  </h1>
  <nav class="header-nav">
    <a href="/usage">Usage</a>
    <a class="private-only" href="/events">Events</a>
  </nav>
  <div class="power-btns private-only">
    <button class="power-btn power-btn-on" id="btn-all-on" onclick="startOperation('all-on')">All On</button>
    <button class="power-btn power-btn-off" id="btn-all-off" onclick="startOperation('all-off')">All Off</button>
  </div>
  {{AUTH_CORNER}}
</header>
<div id="op-banner" class="op-banner private-only" hidden>
  <div class="op-banner-text" id="op-banner-text"></div>
  <button class="op-banner-cancel" id="op-banner-cancel" onclick="cancelOperation()">Cancel</button>
</div>
<div id="content">
  <div class="no-data">Connecting...</div>
</div>
<div id="recent-events" class="recent-events private-only" hidden>
  <div class="recent-events-header">
    <span>Recent power events</span>
    <a href="/events">View full log &rarr;</a>
  </div>
  <ul id="recent-events-list"></ul>
</div>
<script>
const PUBLIC_MODE = {{PUBLIC_MODE}};

const STATE_COLORS = {
  OFF: '#1d1d1f', ATTRACT: '#007aff', PLAYING: '#34c759', IDLE: '#f5c41a'
};

function drawSparkline(canvas, data, states) {
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth * dpr;
  const h = canvas.clientHeight * dpr;
  canvas.width = w;
  canvas.height = h;
  ctx.clearRect(0, 0, w, h);
  if (!data || data.length < 2) return;
  const max = 300;
  const step = w / (data.length - 1);
  const pad = 2 * dpr;
  // Draw state backdrop bands
  if (states && states.length === data.length) {
    let i = 0;
    while (i < states.length) {
      const st = states[i];
      let j = i;
      while (j < states.length && states[j] === st) j++;
      const x0 = i === 0 ? 0 : (i - 0.5) * step;
      const x1 = j >= states.length ? w : (j - 0.5) * step;
      const c = STATE_COLORS[st];
      if (c) { ctx.fillStyle = c + '30'; ctx.fillRect(x0, 0, x1 - x0, h); }
      i = j;
    }
  }
  // Line + fill
  const lastState = states && states.length ? states[states.length - 1] : null;
  const color = STATE_COLORS[lastState] || '#aeaeb2';
  ctx.beginPath();
  ctx.moveTo(0, h);
  for (let i = 0; i < data.length; i++) {
    ctx.lineTo(i * step, h - pad - (Math.min(data[i], max) / max) * (h - 2 * pad));
  }
  ctx.lineTo(w, h);
  ctx.closePath();
  ctx.fillStyle = color + '18';
  ctx.fill();
  ctx.beginPath();
  for (let i = 0; i < data.length; i++) {
    const x = i * step;
    const y = h - pad - (Math.min(data[i], max) / max) * (h - 2 * pad);
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  }
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.5 * dpr;
  ctx.stroke();
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  })[c]);
}

function renderMachines(machines, outlets) {
  const el = document.getElementById('content');
  if (!machines.length && (!outlets || !outlets.length)) {
    el.innerHTML = '<div class="no-data">No machines assigned</div>';
    return;
  }

  // Group by strip
  const strips = [];
  const stripMap = new Map();
  for (const m of machines) {
    const key = m.strip_device_id || '';
    if (!stripMap.has(key)) {
      const group = { deviceId: key, alias: m.strip_alias || 'Unknown Strip', machines: [] };
      stripMap.set(key, group);
      strips.push(group);
    }
    stripMap.get(key).machines.push(m);
  }

  let html = '';
  let idx = 0;
  for (const strip of strips) {
    // Public viewers don't see strip names — render the tiles without a
    // group label so we don't leak "Strip 1 / Strip 2" or fall back to a
    // placeholder "Unknown Strip".
    const stripLabel = PUBLIC_MODE
      ? ''
      : (strip.deviceId
          ? `<a class="strip-label" href="/strip/${encodeURIComponent(strip.deviceId)}">${escapeHtml(strip.alias)}</a>`
          : `<div class="strip-label">${escapeHtml(strip.alias)}</div>`);
    html += `<div class="strip-row">${stripLabel}<div class="tiles">`;
    for (const m of strip.machines) {
      const plugId = m.plug ? m.plug.plug_id : 0;
      const offline = !!m.offline;
      if (m.has_emeter === false) {
        // Simplified tile for no-emeter machines (e.g. EP10-backed).
        const isOn = !!m.is_on;
        const dotState = offline ? 'OFFLINE' : (isOn ? 'PLAYING' : 'OFF');
        // No control over an unreachable plug; hide the toggle when offline.
        const toggleBtn = (PUBLIC_MODE || offline) ? '' :
          `<button class="tile-toggle ${isOn ? 'off' : 'on'}"
             onclick="togglePlug(event, ${plugId}, ${isOn ? 'false' : 'true'})">
             ${isOn ? 'Turn Off' : 'Turn On'}
           </button>`;
        const body = offline
          ? `<div class="tile-offline">OFFLINE</div>`
          : `<div class="tile-onoff ${isOn ? 'on' : 'off'}">${isOn ? 'ON' : 'OFF'}</div>${toggleBtn}`;
        html += `
          <a class="tile${offline ? ' offline' : ''}" href="/machine/${plugId}">
            <div class="tile-top">
              <div class="state-dot state-${dotState}"></div>
              <div class="machine-name">${escapeHtml(m.name)}</div>
              ${m.locked ? '<span class="tile-lock" title="Shutdown locked">&#128274;</span>' : ''}
            </div>
            ${body}
          </a>`;
      } else {
        const st = offline ? 'OFFLINE' : (m.state || 'null');
        const watts = m.power ? m.power.watts.toFixed(1) + 'W' : '--';
        const body = offline
          ? `<div class="tile-offline">OFFLINE</div>`
          : `<div class="sparkline-wrap"><canvas id="spark-${idx}"></canvas></div>
             <div class="tile-watts">${watts}</div>`;
        html += `
          <a class="tile${offline ? ' offline' : ''}" href="/machine/${plugId}">
            <div class="tile-top">
              <div class="state-dot state-${st}"></div>
              <div class="machine-name">${escapeHtml(m.name)}</div>
              ${m.locked ? '<span class="tile-lock" title="Shutdown locked">&#128274;</span>' : ''}
            </div>
            ${body}
          </a>`;
      }
      idx++;
    }
    html += '</div></div>';
  }

  // Outlets section: unassigned no-emeter outlets (e.g. snack machine).
  if (outlets && outlets.length) {
    html += '<div class="strip-row outlets-section"><div class="strip-label">Outlets</div><div class="tiles">';
    for (const o of outlets) {
      const isOn = !!o.is_on;
      html += `
        <div class="tile outlet-tile">
          <div class="outlet-alias">${escapeHtml(o.alias)}</div>
          <div class="tile-onoff ${isOn ? 'on' : 'off'}">${isOn ? 'ON' : 'OFF'}</div>
          <button class="tile-toggle ${isOn ? 'off' : 'on'}"
            onclick="togglePlug(event, ${o.plug_id}, ${isOn ? 'false' : 'true'})">
            ${isOn ? 'Turn Off' : 'Turn On'}
          </button>
        </div>`;
    }
    html += '</div></div>';
  }

  el.innerHTML = html;

  // Draw sparklines for emeter-equipped machines only.
  idx = 0;
  for (const strip of strips) {
    for (const m of strip.machines) {
      if (m.has_emeter !== false && !m.offline) {
        const canvas = document.getElementById('spark-' + idx);
        if (canvas && m.sparkline && m.sparkline.length > 1) {
          drawSparkline(canvas, m.sparkline, m.sparkline_states);
        }
      }
      idx++;
    }
  }
}

let lastMachines = [];
let lastOutlets = [];

async function togglePlug(ev, plugId, on) {
  ev.preventDefault();
  ev.stopPropagation();
  try {
    const resp = await fetch('/api/plugs/' + plugId + '/power', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({on})
    });
    if (!resp.ok) {
      const body = await resp.json().catch(() => ({}));
      alert(body.error || 'Power control failed');
    }
  } catch (e) {}
  poll();
}

async function poll() {
  try {
    // Public viewers only fetch machines — /api/outlets requires auth.
    if (PUBLIC_MODE) {
      const mResp = await fetch('/api/machines');
      const mData = await mResp.json();
      lastMachines = mData.machines;
      lastOutlets = [];
      renderMachines(mData.machines, []);
    } else {
      const [mResp, oResp] = await Promise.all([
        fetch('/api/machines'),
        fetch('/api/outlets'),
      ]);
      const mData = await mResp.json();
      const oData = await oResp.json();
      lastMachines = mData.machines;
      lastOutlets = oData.outlets;
      renderMachines(mData.machines, oData.outlets);
    }
  } catch (e) {}
}

// ---- Bulk operation (server-driven) ---------------------------------------

let currentOperation = null;

async function startOperation(kind) {
  if (kind === 'all-off' && !confirm('Turn off all machines and outlets?')) return;
  try {
    const resp = await fetch('/api/operations/' + kind, {method: 'POST'});
    if (resp.status === 409) {
      // Another viewer kicked one off — the SSE stream will fill us in.
      return;
    }
    if (!resp.ok) {
      const body = await resp.json().catch(() => ({}));
      alert(body.error || ('Failed to start ' + kind));
    }
  } catch (e) {
    alert('Failed to start ' + kind);
  }
}

async function cancelOperation() {
  if (!currentOperation) return;
  const btn = document.getElementById('op-banner-cancel');
  btn.disabled = true;
  try {
    await fetch('/api/operations/' + currentOperation.id + '/cancel', {method: 'POST'});
  } catch (e) {}
}

function renderOpBanner() {
  const banner = document.getElementById('op-banner');
  const text = document.getElementById('op-banner-text');
  const cancelBtn = document.getElementById('op-banner-cancel');
  if (!currentOperation) {
    banner.hidden = true;
    banner.classList.remove('cancelled', 'complete', 'retrying');
    return;
  }
  const op = currentOperation;
  banner.hidden = false;
  banner.classList.toggle('cancelled', op.state === 'cancelled');
  banner.classList.toggle('complete', op.state === 'complete');
  const isRetrying = op.state === 'running' && !!op.retrying;
  banner.classList.toggle('retrying', isRetrying);
  const verb = op.kind === 'all_on' ? 'Turning on' : 'Turning off';
  if (op.state === 'cancelled') {
    text.textContent = (op.kind === 'all_on' ? 'All-on' : 'All-off')
      + ' cancelled — ' + op.completed.length + '/' + op.total + ' complete';
    cancelBtn.hidden = true;
  } else if (op.state === 'complete') {
    text.textContent = (op.kind === 'all_on' ? 'All-on' : 'All-off')
      + ' complete — ' + op.completed.length + '/' + op.total
      + (op.failed.length ? ' (' + op.failed.length + ' failed)' : '');
    cancelBtn.hidden = true;
  } else if (isRetrying) {
    const r = op.retrying;
    const target = r.machine_name ? ' ' + r.machine_name : '';
    const delay = r.delay != null ? r.delay.toFixed(1) + 's' : '…';
    text.innerHTML =
      '<span class="retry-spinner"></span>'
      + 'Retrying' + escapeHtml(target)
      + ' (attempt ' + r.next_attempt + '): '
      + escapeHtml(r.error || 'transient failure')
      + '. Next try in ' + delay + '…';
    cancelBtn.hidden = false;
    cancelBtn.disabled = !!op.cancel_requested;
  } else {
    const idx = (op.index || 0) + 1;
    const target = op.current_machine ? ' ' + op.current_machine : '';
    text.textContent = verb + ' ' + idx + '/' + op.total + target + '…';
    cancelBtn.hidden = false;
    cancelBtn.disabled = !!op.cancel_requested;
  }
}

function applyOptimisticPowerChange(plugId, on) {
  for (const m of lastMachines) {
    if (m.plug && m.plug.plug_id === plugId) {
      m.is_on = on;
      if (!on) {
        m.power = null;
        m.state = 'OFF';
      } else if (m.has_emeter === false) {
        m.is_on = true;
      }
    }
  }
  for (const o of lastOutlets) {
    if (o.plug_id === plugId) o.is_on = on;
  }
  renderMachines(lastMachines, lastOutlets);
}

// ---- Audit log preview ----------------------------------------------------

function fmtTimeShort(iso) {
  const d = new Date(iso);
  return d.toLocaleTimeString([], {hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit'});
}

function renderRecentEvent(e) {
  const li = document.createElement('li');
  const target = e.machine_name || e.plug_alias || ('Plug ' + e.plug_id);
  const isOn = e.action === 'turn_on';
  const onCls = isOn ? 'on' : 'off';
  const onLbl = isOn ? 'ON' : 'OFF';
  const src = e.source === 'individual' ? '' : (e.source === 'all_on' ? '(all on)' : '(all off)');
  const err = e.result === 'error' ? ' — ' + (e.error || 'error') : '';
  li.innerHTML =
    '<span class="evt-time">' + escapeHtml(fmtTimeShort(e.ts)) + '</span>'
    + '<span>' + escapeHtml(e.actor) + ' turned</span>'
    + '<span class="evt-action ' + onCls + '">' + onLbl + '</span>'
    + '<span>' + escapeHtml(target) + '</span>'
    + (src ? '<span class="evt-source">' + escapeHtml(src) + '</span>' : '')
    + (err ? '<span class="evt-error">' + escapeHtml(err) + '</span>' : '');
  return li;
}

async function refreshRecentEvents() {
  try {
    const resp = await fetch('/api/power-events?limit=5');
    const data = await resp.json();
    const wrap = document.getElementById('recent-events');
    const list = document.getElementById('recent-events-list');
    list.innerHTML = '';
    if (!data.events.length) {
      wrap.hidden = true;
      return;
    }
    wrap.hidden = false;
    for (const e of data.events) list.appendChild(renderRecentEvent(e));
  } catch (e) {}
}

// ---- SSE wiring -----------------------------------------------------------

function connectEvents() {
  const es = new EventSource('/api/events');
  es.onmessage = (msg) => {
    let ev;
    try { ev = JSON.parse(msg.data); } catch { return; }
    if (ev.type === 'hello') {
      currentOperation = ev.current_operation;
      renderOpBanner();
    } else if (ev.type === 'operation_started') {
      currentOperation = ev.operation;
      renderOpBanner();
    } else if (ev.type === 'operation_step') {
      if (currentOperation && currentOperation.id === ev.operation_id) {
        currentOperation.index = ev.index;
        currentOperation.current_machine = ev.machine_name;
        // A step (success or final failure) resolves any in-flight retry.
        currentOperation.retrying = null;
        if (ev.result === 'ok') {
          currentOperation.completed = currentOperation.completed || [];
          currentOperation.completed.push(ev.plug_id);
        } else {
          currentOperation.failed = currentOperation.failed || [];
          currentOperation.failed.push({plug_id: ev.plug_id, error: ev.error});
        }
        renderOpBanner();
      }
    } else if (ev.type === 'operation_step_retry') {
      if (currentOperation && currentOperation.id === ev.operation_id) {
        currentOperation.retrying = {
          attempt: ev.attempt,
          next_attempt: ev.next_attempt,
          delay: ev.delay,
          error: ev.error,
          machine_name: ev.machine_name,
        };
        currentOperation.index = ev.index;
        currentOperation.current_machine = ev.machine_name;
        renderOpBanner();
      }
    } else if (ev.type === 'operation_complete') {
      if (currentOperation && currentOperation.id === ev.operation_id) {
        currentOperation.state = ev.state;
        currentOperation.completed = ev.completed;
        currentOperation.failed = ev.failed;
        renderOpBanner();
        // Show "complete" briefly then clear.
        setTimeout(() => {
          if (currentOperation && currentOperation.id === ev.operation_id) {
            currentOperation = null;
            renderOpBanner();
          }
        }, 3000);
      }
      poll();
      refreshRecentEvents();
    } else if (ev.type === 'power_change') {
      applyOptimisticPowerChange(ev.plug_id, ev.on);
      refreshRecentEvents();
    } else if (ev.type === 'lock_change' || ev.type === 'strip_name_change') {
      poll();
    }
  };
  es.onerror = () => {
    // The browser auto-reconnects; nothing to do here. Polling still keeps the UI fresh.
  };
}

// ---- Init -----------------------------------------------------------------

poll();
setInterval(poll, 2000);
if (!PUBLIC_MODE) {
  // Audit-log preview and live SSE updates require auth — skip both for
  // anonymous viewers (the polling above still keeps tiles fresh).
  refreshRecentEvents();
  connectEvents();
}
</script>
</body>
</html>
"""


DETAIL_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>juice — machine detail</title>
<script src="https://cdn.jsdelivr.net/npm/d3@7"></script>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
    background: #f5f5f7; color: #1d1d1f; min-height: 100vh;
  }
  header {
    padding: 16px 28px; border-bottom: 1px solid #d2d2d7; background: #fff;
    display: flex; align-items: center; gap: 16px;
  }
  header a { color: #007aff; text-decoration: none; font-size: 14px; font-weight: 500; }
  header a:hover { text-decoration: underline; }
  header h1 { font-size: 17px; font-weight: 600; flex: 1; }
  .meta-bar {
    display: flex; gap: 24px; padding: 16px 28px; background: #fff;
    border-bottom: 1px solid #d2d2d7; flex-wrap: wrap; align-items: center;
  }
  .meta-item { font-size: 13px; color: #86868b; }
  .meta-item .val { color: #1d1d1f; font-weight: 600; font-variant-numeric: tabular-nums; }
  .state-badge {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 4px 10px; border-radius: 6px; font-size: 12px; font-weight: 600;
  }
  .state-badge .dot { width: 8px; height: 8px; border-radius: 50%; }
  .state-OFF { background: #f2f2f7; color: #8e8e93; }
  .state-OFF .dot { background: #1d1d1f; }
  .state-ATTRACT { background: #e3f2fd; color: #1565c0; }
  .state-ATTRACT .dot { background: #007aff; }
  .state-PLAYING { background: #e8f5e9; color: #2e7d32; }
  .state-PLAYING .dot { background: #34c759; }
  .state-IDLE { background: #fffde7; color: #9e8600; }
  .state-IDLE .dot { background: #f5c41a; }
  .actions { display: flex; gap: 8px; margin-left: auto; }
  .btn {
    padding: 6px 16px; border-radius: 6px; font-size: 13px; font-weight: 600;
    cursor: pointer; border: none; transition: opacity 0.15s;
  }
  .btn:hover { opacity: 0.85; }
  .btn:disabled { opacity: 0.5; cursor: default; }
  .btn-power-on { background: #34c759; color: #fff; }
  .btn-power-off { background: #ff3b30; color: #fff; }
  .btn-calibrate { background: #007aff; color: #fff; }
  .btn-lock { background: #8e8e93; color: #fff; }
  .btn-lock.locked { background: #f5a623; }
  .lock-badge {
    display: inline-flex; align-items: center; gap: 4px;
    padding: 4px 10px; border-radius: 6px; font-size: 12px; font-weight: 600;
    background: #fff4e0; color: #9a6c00;
  }
  .chart-wrap { padding: 20px 28px; }
  .chart-area {
    background: #fff; border: 1px solid #d2d2d7; border-radius: 10px;
    padding: 16px; overflow: hidden;
  }
  svg { display: block; }
  .axis text { fill: #86868b; font-size: 11px; }
  .axis path, .axis line { stroke: #d2d2d7; }
  .grid line { stroke: #f0f0f0; }
  .grid path { stroke: none; }
  .chart-tooltip {
    position: absolute; pointer-events: none; background: rgba(255,255,255,0.95);
    border: 1px solid #d2d2d7; border-radius: 6px; padding: 8px 12px;
    font-size: 12px; display: none; box-shadow: 0 2px 8px rgba(0,0,0,0.1);
  }
  .chart-tooltip .tt-time { color: #86868b; }
  .chart-tooltip .tt-watts { font-weight: 600; font-size: 14px; }
  .toast {
    position: fixed; bottom: 20px; left: 50%; transform: translateX(-50%);
    padding: 10px 20px; border-radius: 8px; font-size: 13px; font-weight: 500;
    z-index: 100; transition: opacity 0.3s; box-shadow: 0 4px 16px rgba(0,0,0,0.15);
  }
  .toast-success { background: #34c759; color: #fff; }
  .toast-error { background: #ff3b30; color: #fff; }
  .cal-info { font-size: 11px; color: #86868b; margin-top: 2px; }
  .meta-item .val a { color: #007aff; text-decoration: none; }
  .meta-item .val a:hover { text-decoration: underline; }
  /* Outlet map (mirrors the strip page — intentional duplication) */
  .outlet-map {
    margin: 16px 28px 0; background: #fff; border: 1px solid #d2d2d7;
    border-radius: 10px; padding: 12px 16px;
  }
  .outlet-map-header {
    font-size: 11px; font-weight: 600; color: #86868b;
    text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px;
  }
  .outlet-map-header a { color: #007aff; text-decoration: none; text-transform: none; letter-spacing: 0; }
  .outlet-map-header a:hover { text-decoration: underline; }
  .outlet-row {
    display: flex; align-items: center; gap: 10px;
    padding: 6px 8px; border-top: 1px solid #f0f0f0; font-size: 13px;
  }
  .outlet-row:first-of-type { border-top: none; }
  .outlet-row.current { background: #eef6ff; border-radius: 6px; }
  .outlet-num {
    width: 22px; height: 22px; border-radius: 6px; background: #f2f2f7;
    display: flex; align-items: center; justify-content: center;
    font-weight: 600; font-size: 12px; color: #1d1d1f; flex-shrink: 0;
  }
  .outlet-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
  .outlet-dot.on { background: #34c759; }
  .outlet-dot.off { background: #1d1d1f; }
  .outlet-dot.offline { background: #c7c7cc; }
  .outlet-watts { width: 70px; text-align: right; color: #86868b; font-variant-numeric: tabular-nums; }
  .outlet-machine a { color: #007aff; text-decoration: none; font-weight: 600; }
  .outlet-machine a:hover { text-decoration: underline; }
  .outlet-empty { color: #86868b; }
  .outlet-this { margin-left: auto; font-size: 11px; color: #007aff; font-weight: 600; }
  .flip-link { color: #007aff; text-decoration: none; }
  .flip-link:hover { text-decoration: underline; }
  .auth-corner { margin-left: auto; font-size: 13px; }
  .login-btn {
    padding: 6px 14px; border-radius: 6px;
    background: #007aff; color: #fff;
    text-decoration: none; font-weight: 600;
  }
  .login-btn:hover { opacity: 0.85; }
  .user-pill { color: #86868b; }
  .user-pill a { color: #007aff; text-decoration: none; margin-left: 6px; }
  .user-pill a:hover { text-decoration: underline; }
  body.public .private-only { display: none !important; }
</style>
</head>
<body class="{{BODY_CLASS}}">

<header>
  <a href="/">&larr; Dashboard</a>
  <h1 id="machine-name">Loading...</h1>
  <span class="flip-suffix" style="color:#86868b;font-weight:500;">
    for <a class="flip-link" href="https://theflip.museum">The Flip</a>
  </span>
  {{AUTH_CORNER}}
</header>

<div class="meta-bar" id="meta-bar">
  <div class="meta-item">Loading...</div>
</div>

<div class="outlet-map private-only" id="strip-outlets" hidden>
  <div class="outlet-map-header" id="outlet-map-header">Outlets</div>
  <div id="outlet-rows"></div>
</div>

<div class="chart-wrap">
  <div class="chart-area">
    <svg id="chart"></svg>
  </div>
</div>
<div class="chart-tooltip" id="chart-tooltip"></div>

<script>
const PUBLIC_MODE = {{PUBLIC_MODE}};
const STATE_COLORS = { OFF: '#1d1d1f', ATTRACT: '#007aff', PLAYING: '#34c759', IDLE: '#f5c41a' };
const plugId = parseInt(location.pathname.split('/').pop());

function escapeHtml(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  })[c]);
}

let machineData = null;

async function fetchMachineInfo() {
  const resp = await fetch('/api/machines');
  const data = await resp.json();
  return data.machines.find(m => m.plug && m.plug.plug_id === plugId);
}

function showToast(msg, type) {
  const existing = document.querySelector('.toast');
  if (existing) existing.remove();
  const t = document.createElement('div');
  t.className = 'toast toast-' + type;
  t.textContent = msg;
  document.body.appendChild(t);
  setTimeout(() => { t.style.opacity = '0'; setTimeout(() => t.remove(), 300); }, 4000);
}

function renderMeta(m) {
  if (!m) return;
  machineData = m;
  document.getElementById('machine-name').textContent = m.name;
  document.title = 'juice — ' + m.name;

  const noEmeter = m.has_emeter === false;
  const isOn = noEmeter ? !!m.is_on : !!(m.power && m.power.watts > 0);
  const st = noEmeter ? (isOn ? 'PLAYING' : 'OFF') : (m.state || 'OFF');
  const watts = m.power ? m.power.watts.toFixed(1) + ' W' : (noEmeter ? 'no data' : '--');
  const volts = m.power ? m.power.voltage.toFixed(1) + ' V' : (noEmeter ? '--' : '--');
  const amps = m.power ? m.power.amps.toFixed(3) + ' A' : (noEmeter ? '--' : '--');
  const kwh = m.power ? m.power.total_kwh.toFixed(1) + ' kWh' : (noEmeter ? '--' : '--');

  const bar = document.getElementById('meta-bar');
  // Public viewers don't see plug/strip names or any controls.
  const plugNum = m.plug && m.plug.outlet_number != null ? m.plug.outlet_number : null;
  const plugLabel = m.plug
    ? (plugNum != null ? `#${plugNum} — ${escapeHtml(m.plug.alias)}` : escapeHtml(m.plug.alias))
    : '--';
  const stripLabel = m.plug && m.plug.device_id
    ? `<a href="/strip/${encodeURIComponent(m.plug.device_id)}">${escapeHtml(m.strip_alias || '--')}</a>`
    : escapeHtml(m.strip_alias || '--');
  const plugStripRows = PUBLIC_MODE ? '' :
    `<div class="meta-item">Plug <span class="val">${plugLabel}</span></div>
     <div class="meta-item">Strip <span class="val">${stripLabel}</span></div>`;
  const calButton = (PUBLIC_MODE || noEmeter)
    ? ''
    : `<button class="btn btn-calibrate" id="cal-btn" onclick="calibrate()">${m.calibrated ? 'Recalibrate' : 'Calibrate'}</button>`;
  // Turning OFF a shutdown-locked machine is disabled until it's unlocked;
  // turning ON is always allowed.
  const powerButton = PUBLIC_MODE
    ? ''
    : (isOn && m.locked)
      ? `<button class="btn btn-power-off" id="power-btn" disabled
           title="Unlock to turn off">Locked</button>`
      : `<button class="btn ${isOn ? 'btn-power-off' : 'btn-power-on'}" id="power-btn"
           onclick="togglePower(${isOn ? 'false' : 'true'})">${isOn ? 'Turn Off' : 'Turn On'}</button>`;
  const lockButton = PUBLIC_MODE
    ? ''
    : `<button class="btn btn-lock${m.locked ? ' locked' : ''}" id="lock-btn"
         onclick="toggleLock(${m.locked ? 'false' : 'true'})">${m.locked ? '&#128275; Unlock' : '&#128274; Lock'}</button>`;
  const actions = (powerButton || lockButton || calButton)
    ? `<div class="actions">${powerButton}${lockButton}${calButton}</div>`
    : '';
  const lockBadge = m.locked
    ? '<div class="lock-badge" title="Shutdown locked">&#128274; Locked</div>'
    : '';
  bar.innerHTML = `
    <div class="state-badge state-${st}"><div class="dot"></div>${noEmeter ? (isOn ? 'ON' : 'OFF') : st}</div>
    ${lockBadge}
    <div class="meta-item"><span class="val">${watts}</span></div>
    <div class="meta-item"><span class="val">${volts}</span></div>
    <div class="meta-item"><span class="val">${amps}</span></div>
    <div class="meta-item">Total <span class="val">${kwh}</span></div>
    <div class="meta-item">Asset <span class="val">${escapeHtml(m.asset_id)}</span></div>
    ${plugStripRows}
    ${actions}
  `;
}

async function togglePower(on) {
  const btn = document.getElementById('power-btn');
  btn.disabled = true;
  btn.textContent = on ? 'Turning on...' : 'Turning off...';
  try {
    const resp = await fetch('/api/machines/' + plugId + '/power', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({on})
    });
    const data = await resp.json();
    if (!resp.ok) { showToast(data.error, 'error'); }
    else {
      showToast('Turned ' + (on ? 'on' : 'off'), 'success');
      // Optimistic update — flip button immediately
      if (machineData) {
        if (!on) { machineData.power = null; machineData.state = 'OFF'; }
        renderMeta(machineData);
        return;
      }
    }
  } catch (e) { showToast('Failed', 'error'); }
  btn.disabled = false;
  refreshMeta();
}

async function toggleLock(locked) {
  const btn = document.getElementById('lock-btn');
  btn.disabled = true;
  try {
    const resp = await fetch('/api/machines/' + plugId + '/lock', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({locked})
    });
    const data = await resp.json();
    if (!resp.ok) { showToast(data.error, 'error'); }
    else {
      showToast(locked ? 'Shutdown locked' : 'Unlocked', 'success');
      if (machineData) {
        machineData.locked = locked;
        renderMeta(machineData);
        return;
      }
    }
  } catch (e) { showToast('Failed', 'error'); }
  btn.disabled = false;
  refreshMeta();
}

async function calibrate() {
  const btn = document.getElementById('cal-btn');
  btn.disabled = true;
  btn.textContent = 'Calibrating...';
  try {
    const resp = await fetch('/api/machines/' + plugId + '/calibrate', { method: 'POST' });
    const data = await resp.json();
    if (!resp.ok) { showToast(data.error, 'error'); }
    else {
      const c = data.calibration;
      const idle = c.idle_max_rsd !== null ? c.idle_max_rsd.toFixed(1) : 'N/A';
      showToast(data.machine + ': idle=' + idle + ', play=' + c.play_min_rsd.toFixed(1), 'success');
    }
  } catch (e) { showToast('Calibration failed', 'error'); }
  btn.disabled = false;
  btn.textContent = 'Recalibrate';
}

async function refreshMeta() {
  const m = await fetchMachineInfo();
  if (m) renderMeta(m);
  if (m) refreshStripOutlets(m);
}

// -- Strip outlet map ---------------------------------------------------------

async function refreshStripOutlets(m) {
  // Operators only; needs the device_id, which the public payload omits.
  if (PUBLIC_MODE || !m.plug || !m.plug.device_id) return;
  let strip;
  try {
    const resp = await fetch('/api/strips/' + encodeURIComponent(m.plug.device_id));
    if (!resp.ok) return;
    strip = await resp.json();
  } catch (e) { return; }
  const section = document.getElementById('strip-outlets');
  if (!strip.outlets || strip.outlets.length <= 1) {  // EP10s: nothing to map
    section.hidden = true;
    return;
  }
  section.hidden = false;
  const mine = strip.outlets.find(o => o.plug_id === plugId);
  const n = mine && mine.outlet_number != null ? mine.outlet_number : '?';
  document.getElementById('outlet-map-header').innerHTML =
    `Plug ${n} of ${strip.outlets.length} on ` +
    `<a href="/strip/${encodeURIComponent(strip.device_id)}">${escapeHtml(strip.display_name || strip.device_id)}</a>`;
  document.getElementById('outlet-rows').innerHTML = strip.outlets.map(o => {
    const dot = strip.offline ? 'offline' : (o.is_on ? 'on' : 'off');
    const watts = o.watts != null ? o.watts.toFixed(1) + ' W' : '—';
    const what = o.machine
      ? (o.plug_id === plugId
          ? `<span>${escapeHtml(o.machine.name)}</span>`
          : `<a href="/machine/${o.plug_id}">${escapeHtml(o.machine.name)}</a>`)
      : `<span class="outlet-empty">${escapeHtml(o.alias) || '—'}</span>`;
    const current = o.plug_id === plugId;
    return `
      <div class="outlet-row${current ? ' current' : ''}">
        <div class="outlet-num">${o.outlet_number ?? '·'}</div>
        <div class="outlet-dot ${dot}"></div>
        <div class="outlet-watts">${strip.offline ? 'OFFLINE' : watts}</div>
        <div class="outlet-machine">${what}</div>
        ${current ? '<span class="outlet-this">this machine</span>' : ''}
      </div>`;
  }).join('');
}

// -- Chart -------------------------------------------------------------------

const margin = { top: 12, right: 16, bottom: 36, left: 52 };
const width = Math.min(window.innerWidth - 88, 1200);
const height = 300;
const innerW = width - margin.left - margin.right;
const innerH = height - margin.top - margin.bottom;

const svg = d3.select('#chart').attr('width', width).attr('height', height);
const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

const clipId = 'clip-detail';
svg.append('defs').append('clipPath').attr('id', clipId)
  .append('rect').attr('width', innerW).attr('height', innerH);

const xScale = d3.scaleTime().range([0, innerW]);
const yScale = d3.scaleLinear().range([innerH, 0]);

const xAxisG = g.append('g').attr('class', 'axis').attr('transform', `translate(0,${innerH})`);
const yAxisG = g.append('g').attr('class', 'axis');
const gridG = g.append('g').attr('class', 'grid');
const chartG = g.append('g').attr('clip-path', `url(#${clipId})`);

const areaPath = chartG.append('path').attr('opacity', 0.15);
const linePath = chartG.append('path').attr('fill', 'none').attr('stroke-width', 1);
const hoverLine = chartG.append('line')
  .attr('stroke', '#aaa').attr('stroke-dasharray', '3,3')
  .attr('y1', 0).attr('y2', innerH).style('display', 'none');
const hoverDot = chartG.append('circle').attr('r', 4).style('display', 'none')
  .attr('fill', '#007aff').attr('stroke', '#fff').attr('stroke-width', 2);

const tooltip = d3.select('#chart-tooltip');

async function loadChart() {
  const resp = await fetch('/api/machines/' + plugId + '/readings?hours=24');
  const data = await resp.json();
  if (!data.timestamps.length) return;

  const points = data.timestamps.map((t, i) => ({ ts: new Date(t), watts: data.watts[i], state: data.states[i] || null }));

  xScale.domain(d3.extent(points, d => d.ts));
  yScale.domain([0, d3.max(points, d => d.watts) * 1.1 || 100]).nice();

  xAxisG.call(d3.axisBottom(xScale).ticks(8).tickFormat(d3.timeFormat('%-I:%M %p')));
  yAxisG.call(d3.axisLeft(yScale).ticks(6).tickFormat(d => d + ' W'));
  gridG.call(d3.axisLeft(yScale).ticks(6).tickSize(-innerW).tickFormat(''));

  // State backdrop bands
  chartG.selectAll('.state-band').remove();
  if (data.states && data.states.length) {
    const bands = [];
    let ci = 0;
    while (ci < points.length) {
      const st = points[ci].state;
      let cj = ci;
      while (cj < points.length && points[cj].state === st) cj++;
      bands.push({ state: st, start: points[ci].ts, end: points[cj - 1].ts });
      ci = cj;
    }
    chartG.selectAll('.state-band').data(bands).enter()
      .insert('rect', ':first-child').attr('class', 'state-band')
      .attr('x', d => xScale(d.start))
      .attr('width', d => Math.max(1, xScale(d.end) - xScale(d.start)))
      .attr('y', 0).attr('height', innerH)
      .attr('fill', d => STATE_COLORS[d.state] || '#aeaeb2')
      .attr('opacity', 0.18);
  }

  const line = d3.line().x(d => xScale(d.ts)).y(d => yScale(d.watts));
  const area = d3.area().x(d => xScale(d.ts)).y0(innerH).y1(d => yScale(d.watts));

  linePath.datum(points).attr('d', line).attr('stroke', '#007aff');
  areaPath.datum(points).attr('d', area).attr('fill', '#007aff');

  // Hover
  const bisect = d3.bisector(d => d.ts).left;
  svg.on('mousemove', function(event) {
    const [mx] = d3.pointer(event, g.node());
    if (mx < 0 || mx > innerW) { hoverLine.style('display','none'); hoverDot.style('display','none'); tooltip.style('display','none'); return; }
    const ts = xScale.invert(mx);
    let i = bisect(points, ts, 1);
    if (i >= points.length) i = points.length - 1;
    if (i > 0 && (ts - points[i-1].ts) < (points[i].ts - ts)) i--;
    const d = points[i];
    hoverLine.attr('x1', xScale(d.ts)).attr('x2', xScale(d.ts)).style('display', null);
    hoverDot.attr('cx', xScale(d.ts)).attr('cy', yScale(d.watts)).style('display', null);

    const fmt = d3.timeFormat('%-I:%M:%S %p');
    tooltip.html(`<div class="tt-time">${fmt(d.ts)}</div><div class="tt-watts">${d.watts.toFixed(1)} W</div>`)
      .style('display', 'block');
    const rect = document.getElementById('chart').getBoundingClientRect();
    let left = rect.left + margin.left + xScale(d.ts) + 14;
    let top = rect.top + margin.top + yScale(d.watts) - 20 + window.scrollY;
    if (left + 140 > window.innerWidth) left -= 170;
    tooltip.style('left', left + 'px').style('top', top + 'px');
  }).on('mouseleave', () => {
    hoverLine.style('display','none'); hoverDot.style('display','none'); tooltip.style('display','none');
  });
}

// -- Init --------------------------------------------------------------------

(async () => {
  const m = await fetchMachineInfo();
  if (m) renderMeta(m);
  else document.getElementById('machine-name').textContent = 'Machine not found';
  if (m) refreshStripOutlets(m);
  if (m && m.has_emeter !== false) {
    await loadChart();
  } else {
    document.querySelector('.chart-wrap').innerHTML =
      '<div class="chart-area" style="padding:24px;color:#86868b;font-size:13px;text-align:center;">No power data — this device has no energy monitoring.</div>';
  }
})();

setInterval(refreshMeta, 5000);
</script>
</body>
</html>
"""


EVENTS_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>juice — power events</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
    background: #f5f5f7; color: #1d1d1f; min-height: 100vh;
  }
  header {
    padding: 16px 28px; border-bottom: 1px solid #d2d2d7; background: #fff;
    display: flex; align-items: center; gap: 16px;
  }
  header a { color: #007aff; text-decoration: none; font-size: 14px; font-weight: 500; }
  header a:hover { text-decoration: underline; }
  header h1 { font-size: 17px; font-weight: 600; flex: 1; }
  .wrap { padding: 20px 28px; }
  table {
    width: 100%; border-collapse: collapse; background: #fff;
    border: 1px solid #d2d2d7; border-radius: 10px; overflow: hidden;
  }
  th, td {
    text-align: left; padding: 10px 14px; font-size: 13px;
    border-bottom: 1px solid #f0f0f0; font-variant-numeric: tabular-nums;
  }
  th {
    background: #fafafc; font-weight: 600; font-size: 11px;
    text-transform: uppercase; letter-spacing: 0.4px; color: #86868b;
  }
  tr:last-child td { border-bottom: none; }
  .action-on  { color: #2e7d32; font-weight: 600; }
  .action-off { color: #c62828; font-weight: 600; }
  .source-individual { color: #86868b; }
  .source-all_on, .source-all_off {
    background: #eef3ff; color: #1565c0;
    padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 600;
  }
  .result-error { color: #c62828; }
  .empty, .loading { padding: 32px; text-align: center; color: #86868b; font-size: 13px; }
  .more {
    margin-top: 16px; text-align: center;
  }
  .more button {
    padding: 8px 16px; background: #fff; border: 1px solid #d2d2d7; border-radius: 8px;
    font-size: 13px; cursor: pointer; color: #1d1d1f;
  }
  .more button:hover { background: #f0f0f0; }
  .more button:disabled { opacity: 0.5; cursor: default; }
  .flip-link { color: #007aff; text-decoration: none; }
  .flip-link:hover { text-decoration: underline; }
  .auth-corner { margin-left: auto; font-size: 13px; }
  .login-btn {
    padding: 6px 14px; border-radius: 6px;
    background: #007aff; color: #fff;
    text-decoration: none; font-weight: 600;
  }
  .login-btn:hover { opacity: 0.85; }
  .user-pill { color: #86868b; }
  .user-pill a { color: #007aff; text-decoration: none; margin-left: 6px; }
  .user-pill a:hover { text-decoration: underline; }
  body.public .private-only { display: none !important; }
</style>
</head>
<body class="{{BODY_CLASS}}">

<header>
  <a href="/">&larr; Dashboard</a>
  <h1>Power events for <a class="flip-link" href="https://theflip.museum">The Flip</a></h1>
  {{AUTH_CORNER}}
</header>

<div class="wrap">
  <table id="tbl">
    <thead>
      <tr>
        <th>When</th>
        <th>Who</th>
        <th>Machine / Plug</th>
        <th>Action</th>
        <th>Source</th>
        <th>Result</th>
      </tr>
    </thead>
    <tbody id="rows">
      <tr><td colspan="6" class="loading">Loading…</td></tr>
    </tbody>
  </table>
  <div class="more">
    <button id="more-btn" onclick="loadMore()">Load older</button>
  </div>
</div>

<script>
function escapeHtml(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  })[c]);
}

function fmtTs(iso) {
  // DB stores UTC; render in local time.
  const d = new Date(iso);
  return d.toLocaleString();
}

function renderRow(e) {
  const isOn = e.action === 'turn_on';
  const actionLabel = isOn ? 'ON' : 'OFF';
  const actionCls = isOn ? 'action-on' : 'action-off';
  const target = e.machine_name || e.plug_alias || ('Plug ' + e.plug_id);
  const sourceCls = 'source-' + e.source;
  const sourceLabel = e.source === 'individual' ? 'individual' : e.source.replace('_', ' ');
  const result = e.result === 'ok' ? 'ok' : 'error';
  const resultCls = e.result === 'error' ? 'result-error' : '';
  const detail = e.error ? ' — ' + escapeHtml(e.error) : '';
  return (
    '<tr>'
    + '<td>' + escapeHtml(fmtTs(e.ts)) + '</td>'
    + '<td>' + escapeHtml(e.actor) + '</td>'
    + '<td>' + escapeHtml(target) + '</td>'
    + '<td class="' + actionCls + '">' + actionLabel + '</td>'
    + '<td><span class="' + sourceCls + '">' + escapeHtml(sourceLabel) + '</span></td>'
    + '<td class="' + resultCls + '">' + result + detail + '</td>'
    + '</tr>'
  );
}

let oldestId = null;
let exhausted = false;

async function loadPage(before) {
  const url = new URL('/api/power-events', location.origin);
  url.searchParams.set('limit', '100');
  if (before !== null && before !== undefined) url.searchParams.set('before', String(before));
  const resp = await fetch(url);
  const data = await resp.json();
  return data.events;
}

async function init() {
  const events = await loadPage(null);
  const tbody = document.getElementById('rows');
  if (!events.length) {
    tbody.innerHTML = '<tr><td colspan="6" class="empty">No events yet.</td></tr>';
    document.getElementById('more-btn').disabled = true;
    return;
  }
  tbody.innerHTML = events.map(renderRow).join('');
  oldestId = events[events.length - 1].event_id;
  if (events.length < 100) {
    exhausted = true;
    document.getElementById('more-btn').disabled = true;
    document.getElementById('more-btn').textContent = 'No more events';
  }
}

async function loadMore() {
  if (exhausted) return;
  const btn = document.getElementById('more-btn');
  btn.disabled = true;
  const events = await loadPage(oldestId);
  if (events.length) {
    document.getElementById('rows').insertAdjacentHTML('beforeend', events.map(renderRow).join(''));
    oldestId = events[events.length - 1].event_id;
  }
  if (events.length < 100) {
    exhausted = true;
    btn.textContent = 'No more events';
  } else {
    btn.disabled = false;
  }
}

init();
</script>
</body>
</html>
"""


USAGE_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>juice — power usage</title>
<script src="https://cdn.jsdelivr.net/npm/d3@7"></script>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
    background: #f5f5f7; color: #1d1d1f; min-height: 100vh;
  }
  header {
    padding: 16px 28px; border-bottom: 1px solid #d2d2d7; background: #fff;
    display: flex; align-items: center; gap: 16px;
  }
  header a { color: #007aff; text-decoration: none; font-size: 14px; font-weight: 500; }
  header a:hover { text-decoration: underline; }
  header h1 { font-size: 17px; font-weight: 600; flex: 1; }
  .wrap { padding: 20px 28px; max-width: 1600px; margin: 0 auto; }
  .content {
    display: flex; gap: 16px; align-items: stretch;
  }
  .chart-area {
    flex: 1 1 auto; min-width: 0;  /* min-width: 0 lets flex actually shrink it */
    background: #fff; border: 1px solid #d2d2d7; border-radius: 10px;
    padding: 16px; overflow: hidden;
  }
  svg { display: block; width: 100%; }
  .axis text { fill: #86868b; font-size: 11px; }
  .axis path, .axis line { stroke: #d2d2d7; }
  .grid line { stroke: #f0f0f0; }
  .grid path { stroke: none; }
  .chart-tooltip {
    position: absolute; pointer-events: none; background: rgba(255,255,255,0.97);
    border: 1px solid #d2d2d7; border-radius: 6px; padding: 8px 12px;
    font-size: 12px; display: none; box-shadow: 0 2px 8px rgba(0,0,0,0.1);
    min-width: 200px; max-width: 280px;
  }
  .chart-tooltip .tt-time { color: #86868b; margin-bottom: 4px; }
  .chart-tooltip .tt-row {
    display: flex; align-items: center; gap: 6px;
    font-variant-numeric: tabular-nums;
  }
  .chart-tooltip .tt-row .swatch { width: 8px; height: 8px; border-radius: 2px; }
  .chart-tooltip .tt-row .name { flex: 1; }
  .chart-tooltip .tt-row .kwh { font-weight: 600; }
  .chart-tooltip .tt-total { margin-top: 4px; padding-top: 4px; border-top: 1px solid #f0f0f0;
                             font-weight: 600; display: flex; justify-content: space-between; }
  .legend {
    flex: 0 0 auto; min-width: 220px; max-width: 320px;
    align-self: flex-start;
    background: #fff; border: 1px solid #d2d2d7; border-radius: 10px;
    padding: 12px 16px;
  }
  .legend-title {
    font-size: 11px; font-weight: 600; color: #86868b;
    text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px;
  }
  .legend ul { list-style: none; }
  .legend li {
    display: flex; align-items: center; gap: 8px;
    padding: 4px 0; font-size: 13px;
    font-variant-numeric: tabular-nums;
  }
  .legend .swatch { width: 12px; height: 12px; border-radius: 3px; flex-shrink: 0; }
  .legend .name { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .legend .kwh { color: #1d1d1f; font-weight: 500; }
  .legend .total {
    margin-top: 8px; padding-top: 8px; border-top: 1px solid #f0f0f0;
    font-weight: 700; display: flex; justify-content: space-between;
  }
  .empty { padding: 60px 20px; text-align: center; color: #86868b; font-size: 14px; }
  .section-title {
    margin: 24px 0 12px;
    font-size: 14px; font-weight: 600;
    color: #1d1d1f;
    letter-spacing: 0;
  }
  .section-title:first-of-type { margin-top: 8px; }
  .flip-link { color: #007aff; text-decoration: none; }
  .flip-link:hover { text-decoration: underline; }
  .auth-corner { margin-left: auto; font-size: 13px; }
  .login-btn {
    padding: 6px 14px; border-radius: 6px;
    background: #007aff; color: #fff;
    text-decoration: none; font-weight: 600;
  }
  .login-btn:hover { opacity: 0.85; }
  .user-pill { color: #86868b; }
  .user-pill a { color: #007aff; text-decoration: none; margin-left: 6px; }
  .user-pill a:hover { text-decoration: underline; }
  body.public .private-only { display: none !important; }
  /* On phone / narrow viewport: stack chart on top, legend below at full width. */
  @media (max-width: 720px) {
    .wrap { padding: 12px; }
    .content { flex-direction: column; }
    .legend { max-width: none; width: 100%; min-width: 0; }
  }
</style>
</head>
<body class="{{BODY_CLASS}}">

<header>
  <a href="/">&larr; Dashboard</a>
  <h1>Usage — last 30 days for <a class="flip-link" href="https://theflip.museum">The Flip</a></h1>
  {{AUTH_CORNER}}
</header>

<div class="wrap">
  <h2 class="section-title">Energy</h2>
  <div class="content" id="kwh-section">
    <div class="chart-area" id="kwh-chart-area">
      <svg id="chart"></svg>
      <div id="empty" class="empty" style="display:none">No usage data yet.</div>
    </div>
    <div class="legend" id="legend" style="display:none">
      <div class="legend-title">Per-machine usage</div>
      <ul id="legend-list"></ul>
      <div class="total"><span>Total</span><span id="legend-total">&mdash;</span></div>
    </div>
  </div>

  <h2 class="section-title">Play hours per day</h2>
  <div class="content" id="play-section">
    <div class="chart-area" id="play-chart-area">
      <svg id="play-chart"></svg>
      <div id="play-empty" class="empty" style="display:none">
        No play hours yet — needs a machine calibration.
      </div>
    </div>
    <div class="legend" id="play-legend" style="display:none">
      <div class="legend-title">Per-machine play hours</div>
      <ul id="play-legend-list"></ul>
      <div class="total"><span>Total</span><span id="play-legend-total">&mdash;</span></div>
    </div>
  </div>

  <div class="chart-tooltip" id="tooltip"></div>
</div>

<script>
function escapeHtml(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  })[c]);
}

const margin = { top: 16, right: 24, bottom: 36, left: 56 };
// Height responds to viewport too — tall on desktop, shorter on phone.
function chartHeight() { return Math.max(220, Math.min(420, window.innerHeight * 0.45)); }

const chartAreaEl = document.getElementById('kwh-chart-area');
const svg = d3.select('#chart');
const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

const xScale = d3.scaleTime();
const yScale = d3.scaleLinear();

const xAxisG = g.append('g').attr('class', 'axis');
const yAxisG = g.append('g').attr('class', 'axis');
const gridG = g.append('g').attr('class', 'grid');
const layersG = g.append('g');
const hoverLine = g.append('line')
  .attr('stroke', '#aaa').attr('stroke-dasharray', '3,3')
  .style('display', 'none');

const tooltip = d3.select('#tooltip');

let lastData = null;

function chartWidth() {
  // chart-area has 16px padding on each side.
  return Math.max(280, chartAreaEl.clientWidth - 32);
}

function render(data) {
  lastData = data;
  const empty = document.getElementById('empty');
  const legend = document.getElementById('legend');
  if (!data.machines.length || !data.hours.length) {
    empty.style.display = 'block';
    svg.style('display', 'none');
    legend.style.display = 'none';
    return;
  }
  empty.style.display = 'none';
  svg.style('display', 'block');
  legend.style.display = 'block';

  // Sizing — recomputed on every render to track container width.
  const width = chartWidth();
  const height = chartHeight();
  const innerW = width - margin.left - margin.right;
  const innerH = height - margin.top - margin.bottom;
  svg.attr('width', width).attr('height', height);
  xScale.range([0, innerW]);
  yScale.range([innerH, 0]);
  xAxisG.attr('transform', `translate(0,${innerH})`);
  hoverLine.attr('y1', 0).attr('y2', innerH);

  const hours = data.hours.map(h => new Date(h));
  // Stack order: machines as returned by the server (biggest first,
  // Unassigned last). d3.stack defaults to bottom-up, so the first key
  // sits on the x-axis. We key by a stable per-machine id (`machine_id`,
  // with 'unassigned' as the sentinel for the null bucket) — `m.name`
  // isn't unique and would collapse same-named machines into one band.
  const keyOf = m => 'm' + (m.machine_id == null ? 'unassigned' : m.machine_id);
  const keys = data.machines.map(keyOf);
  const colorByKey = new Map(data.machines.map(m => [keyOf(m), m.color]));

  // Flatten: one record per hour with each machine's kwh as a property,
  // keyed by the stable id so duplicate display names don't collide.
  const records = hours.map((ts, i) => {
    const rec = { ts };
    for (const m of data.machines) rec[keyOf(m)] = m.hourly_kwh[i] || 0;
    return rec;
  });

  const stack = d3.stack().keys(keys);
  const series = stack(records);

  xScale.domain(d3.extent(hours));
  const yMax = d3.max(series, s => d3.max(s, d => d[1])) || 1;
  yScale.domain([0, yMax]).nice();

  // Tick counts scale with available width so axes don't collide on phones.
  const xTicks = Math.max(3, Math.min(8, Math.floor(innerW / 80)));
  const yTicks = Math.max(3, Math.min(6, Math.floor(innerH / 50)));
  xAxisG.call(d3.axisBottom(xScale).ticks(xTicks).tickFormat(d3.timeFormat('%b %-d')));
  yAxisG.call(d3.axisLeft(yScale).ticks(yTicks).tickFormat(d => d + ' kWh'));
  gridG.call(d3.axisLeft(yScale).ticks(yTicks).tickSize(-innerW).tickFormat(''));

  const area = d3.area()
    .x((_, i) => xScale(hours[i]))
    .y0(d => yScale(d[0]))
    .y1(d => yScale(d[1]));

  const paths = layersG.selectAll('path').data(series, s => s.key);
  paths.exit().remove();
  paths.enter().append('path')
    .merge(paths)
    .attr('fill', d => colorByKey.get(d.key) || '#aeaeb2')
    .attr('opacity', 0.85)
    .attr('d', area);

  // Hover.
  svg.on('mousemove', function(event) {
    const [mx] = d3.pointer(event, g.node());
    if (mx < 0 || mx > innerW) {
      hoverLine.style('display', 'none');
      tooltip.style('display', 'none');
      return;
    }
    const ts = xScale.invert(mx);
    const bisect = d3.bisector(d => d).left;
    let i = bisect(hours, ts);
    if (i >= hours.length) i = hours.length - 1;
    if (i > 0 && (ts - hours[i-1]) < (hours[i] - ts)) i--;
    const hov = hours[i];
    hoverLine.attr('x1', xScale(hov)).attr('x2', xScale(hov)).style('display', null);

    // Tooltip content — biggest contributor first, skip 0-kWh rows.
    const fmt = d3.timeFormat('%a %b %-d, %-I %p');
    let html = `<div class="tt-time">${escapeHtml(fmt(hov))}</div>`;
    let total = 0;
    const rows = data.machines
      .map(m => ({ name: m.name, color: m.color, kwh: m.hourly_kwh[i] || 0 }))
      .filter(r => r.kwh > 0.0005);
    rows.sort((a, b) => b.kwh - a.kwh);
    for (const r of rows) {
      total += r.kwh;
      html += `<div class="tt-row">
        <span class="swatch" style="background:${escapeHtml(r.color)}"></span>
        <span class="name">${escapeHtml(r.name)}</span>
        <span class="kwh">${r.kwh.toFixed(3)} kWh</span>
      </div>`;
    }
    if (!rows.length) html += `<div class="tt-row"><span class="name">(idle)</span></div>`;
    html += `<div class="tt-total"><span>Total</span><span>${total.toFixed(3)} kWh</span></div>`;
    tooltip.html(html).style('display', 'block');
    const rect = document.getElementById('chart').getBoundingClientRect();
    let left = rect.left + margin.left + xScale(hov) + 14;
    let top = rect.top + margin.top + 8 + window.scrollY;
    if (left + 260 > window.innerWidth) left = Math.max(8, left - 280);
    tooltip.style('left', left + 'px').style('top', top + 'px');
  }).on('mouseleave', () => {
    hoverLine.style('display', 'none');
    tooltip.style('display', 'none');
  });

  // Legend.
  const listEl = document.getElementById('legend-list');
  listEl.innerHTML = '';
  for (const m of data.machines) {
    const li = document.createElement('li');
    li.innerHTML =
      '<span class="swatch" style="background:' + escapeHtml(m.color) + '"></span>'
      + '<span class="name" title="' + escapeHtml(m.name) + '">' + escapeHtml(m.name) + '</span>'
      + '<span class="kwh">' + m.total_kwh.toFixed(2) + ' kWh</span>';
    listEl.appendChild(li);
  }
  document.getElementById('legend-total').textContent = data.total_kwh.toFixed(2) + ' kWh';
}

async function load() {
  const resp = await fetch('/api/usage?days=30');
  const data = await resp.json();
  render(data);
}

// Re-render when the chart area is resized (window resize, orientation flip,
// devtools docking, etc). Debounced via rAF so a flood of resize events
// coalesces into one render.
let resizeRaf = 0;
const ro = new ResizeObserver(() => {
  if (resizeRaf) cancelAnimationFrame(resizeRaf);
  resizeRaf = requestAnimationFrame(() => {
    resizeRaf = 0;
    if (lastData) render(lastData);
  });
});
ro.observe(chartAreaEl);

load();

// ---------------------------------------------------------------------------
// Play-hours bar chart
// ---------------------------------------------------------------------------

const playAreaEl = document.getElementById('play-chart-area');
const playSvg = d3.select('#play-chart');
const playG = playSvg.append('g')
  .attr('transform', `translate(${margin.left},${margin.top})`);

const playXScale = d3.scaleBand().paddingInner(0.15).paddingOuter(0.05);
const playYScale = d3.scaleLinear();

const playXAxisG = playG.append('g').attr('class', 'axis');
const playYAxisG = playG.append('g').attr('class', 'axis');
const playGridG = playG.append('g').attr('class', 'grid');
const playLayersG = playG.append('g');
const playHoverLine = playG.append('line')
  .attr('stroke', '#aaa').attr('stroke-dasharray', '3,3')
  .style('display', 'none');

let lastPlayData = null;

function playChartWidth() {
  return Math.max(280, playAreaEl.clientWidth - 32);
}

function renderPlay(data) {
  lastPlayData = data;
  const emptyEl = document.getElementById('play-empty');
  const legendEl = document.getElementById('play-legend');
  if (!data.machines.length || !data.days.length) {
    emptyEl.style.display = 'block';
    playSvg.style('display', 'none');
    legendEl.style.display = 'none';
    return;
  }
  emptyEl.style.display = 'none';
  playSvg.style('display', 'block');
  legendEl.style.display = 'block';

  const width = playChartWidth();
  const height = chartHeight();
  const innerW = width - margin.left - margin.right;
  const innerH = height - margin.top - margin.bottom;
  playSvg.attr('width', width).attr('height', height);
  playXScale.range([0, innerW]).domain(data.days);
  playYScale.range([innerH, 0]);
  playXAxisG.attr('transform', `translate(0,${innerH})`);
  playHoverLine.attr('y1', 0).attr('y2', innerH);

  // Stack order matches the server response (biggest total first → bottom).
  const keyOf = m => 'm' + m.machine_id;
  const keys = data.machines.map(keyOf);
  const colorByKey = new Map(data.machines.map(m => [keyOf(m), m.color]));
  const records = data.days.map((day, i) => {
    const rec = { day };
    for (const m of data.machines) rec[keyOf(m)] = m.daily_hours[i] || 0;
    return rec;
  });
  const series = d3.stack().keys(keys)(records);

  const yMax = d3.max(series, s => d3.max(s, d => d[1])) || 1;
  playYScale.domain([0, yMax]).nice();

  // Show at most ~8 x-axis ticks; pick every Nth day.
  const targetTicks = Math.max(3, Math.min(8, Math.floor(innerW / 90)));
  const tickEvery = Math.max(1, Math.ceil(data.days.length / targetTicks));
  const tickValues = data.days.filter((_, i) => i % tickEvery === 0);
  const xAxis = d3.axisBottom(playXScale)
    .tickValues(tickValues)
    .tickFormat(d => {
      const dt = new Date(d + 'T00:00:00');
      return d3.timeFormat('%b %-d')(dt);
    });
  playXAxisG.call(xAxis);
  const yTicks = Math.max(3, Math.min(6, Math.floor(innerH / 50)));
  playYAxisG.call(d3.axisLeft(playYScale).ticks(yTicks).tickFormat(d => d + ' h'));
  playGridG.call(d3.axisLeft(playYScale).ticks(yTicks).tickSize(-innerW).tickFormat(''));

  // Draw stacked bars. Clear and re-render keeps the join logic simple.
  playLayersG.selectAll('g.bar-layer').remove();
  const layers = playLayersG.selectAll('g.bar-layer')
    .data(series)
    .enter().append('g')
      .attr('class', 'bar-layer')
      .attr('fill', s => colorByKey.get(s.key) || '#aeaeb2');
  layers.selectAll('rect')
    .data(s => s)
    .enter().append('rect')
      .attr('x', (_, i) => playXScale(data.days[i]))
      .attr('y', d => playYScale(d[1]))
      .attr('width', playXScale.bandwidth())
      .attr('height', d => Math.max(0, playYScale(d[0]) - playYScale(d[1])));

  // Hover: snap to the nearest day band.
  playSvg.on('mousemove', function(event) {
    const [mx] = d3.pointer(event, playG.node());
    if (mx < 0 || mx > innerW) {
      playHoverLine.style('display', 'none');
      tooltip.style('display', 'none');
      return;
    }
    // scaleBand doesn't have invert(); compute the band manually.
    const step = playXScale.step();
    const offset = playXScale.range()[0];
    let i = Math.floor((mx - offset) / step);
    i = Math.max(0, Math.min(data.days.length - 1, i));
    const day = data.days[i];
    const cx = playXScale(day) + playXScale.bandwidth() / 2;
    playHoverLine.attr('x1', cx).attr('x2', cx).style('display', null);

    const rows = data.machines
      .map(m => ({ name: m.name, color: m.color, hours: m.daily_hours[i] || 0 }))
      .filter(r => r.hours > 0.0005);
    rows.sort((a, b) => b.hours - a.hours);
    const fmt = d3.timeFormat('%a %b %-d');
    const dayDate = new Date(day + 'T00:00:00');
    let html = `<div class="tt-time">${escapeHtml(fmt(dayDate))}</div>`;
    let total = 0;
    for (const r of rows) {
      total += r.hours;
      html += `<div class="tt-row">
        <span class="swatch" style="background:${escapeHtml(r.color)}"></span>
        <span class="name">${escapeHtml(r.name)}</span>
        <span class="kwh">${r.hours.toFixed(2)} h</span>
      </div>`;
    }
    if (!rows.length) html += `<div class="tt-row"><span class="name">(no play)</span></div>`;
    html += `<div class="tt-total"><span>Total</span><span>${total.toFixed(2)} h</span></div>`;
    tooltip.html(html).style('display', 'block');
    const rect = document.getElementById('play-chart').getBoundingClientRect();
    let left = rect.left + margin.left + cx + 14;
    let top = rect.top + margin.top + 8 + window.scrollY;
    if (left + 260 > window.innerWidth) left = Math.max(8, left - 280);
    tooltip.style('left', left + 'px').style('top', top + 'px');
  }).on('mouseleave', () => {
    playHoverLine.style('display', 'none');
    tooltip.style('display', 'none');
  });

  // Legend.
  const listEl = document.getElementById('play-legend-list');
  listEl.innerHTML = '';
  for (const m of data.machines) {
    const li = document.createElement('li');
    li.innerHTML =
      '<span class="swatch" style="background:' + escapeHtml(m.color) + '"></span>'
      + '<span class="name" title="' + escapeHtml(m.name) + '">' + escapeHtml(m.name) + '</span>'
      + '<span class="kwh">' + m.total_hours.toFixed(2) + ' h</span>';
    listEl.appendChild(li);
  }
  document.getElementById('play-legend-total').textContent = data.total_hours.toFixed(2) + ' h';
}

async function loadPlay() {
  const resp = await fetch('/api/play-hours?days=30');
  const data = await resp.json();
  renderPlay(data);
}

let playResizeRaf = 0;
const playRo = new ResizeObserver(() => {
  if (playResizeRaf) cancelAnimationFrame(playResizeRaf);
  playResizeRaf = requestAnimationFrame(() => {
    playResizeRaf = 0;
    if (lastPlayData) renderPlay(lastPlayData);
  });
});
playRo.observe(playAreaEl);

loadPlay();
</script>
</body>
</html>
"""


# Strip page: dashboard scoped to one strip, with an outlet map on top and an
# editable human-friendly strip name. Auth-only (the middleware redirects
# anonymous viewers to /login), but the {{...}} markers keep rendering uniform.
STRIP_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>juice — strip</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
    background: #f5f5f7; color: #1d1d1f; min-height: 100vh;
  }
  header {
    padding: 16px 28px; border-bottom: 1px solid #d2d2d7; background: #fff;
    display: flex; align-items: center; gap: 16px; flex-wrap: wrap;
  }
  header a { color: #007aff; text-decoration: none; font-size: 14px; font-weight: 500; }
  header a:hover { text-decoration: underline; }
  header h1 { font-size: 17px; font-weight: 600; display: flex; align-items: center; gap: 8px; }
  .alias-hint { font-size: 12px; font-weight: 400; color: #86868b; }
  .flip-suffix { color: #86868b; font-weight: 500; flex: 1; }
  .flip-link { color: #007aff; text-decoration: none; }
  .flip-link:hover { text-decoration: underline; }
  .auth-corner { margin-left: auto; font-size: 13px; }
  .login-btn {
    padding: 6px 14px; border-radius: 6px;
    background: #007aff; color: #fff;
    text-decoration: none; font-weight: 600;
  }
  .login-btn:hover { opacity: 0.85; }
  .user-pill { color: #86868b; }
  .user-pill a { color: #007aff; text-decoration: none; margin-left: 6px; }
  .user-pill a:hover { text-decoration: underline; }
  body.public .private-only { display: none !important; }
  .edit-name-btn {
    border: none; background: none; cursor: pointer; font-size: 13px;
    color: #007aff; padding: 2px;
  }
  .name-input {
    font-size: 15px; padding: 4px 8px; border: 1px solid #d2d2d7;
    border-radius: 6px; width: 220px;
  }
  .btn {
    padding: 6px 16px; border-radius: 6px; font-size: 13px; font-weight: 600;
    cursor: pointer; border: none; transition: opacity 0.15s;
  }
  .btn:hover { opacity: 0.85; }
  .btn:disabled { opacity: 0.5; cursor: default; }
  .btn-save { background: #007aff; color: #fff; }
  .btn-cancel { background: #f2f2f7; color: #1d1d1f; }
  .offline-banner {
    padding: 10px 28px; background: #fff4e0; color: #8a5500;
    border-bottom: 1px solid #ffd591; font-size: 13px; font-weight: 500;
  }
  /* Outlet map */
  .outlet-map {
    margin: 20px 28px; background: #fff; border: 1px solid #d2d2d7;
    border-radius: 10px; padding: 12px 16px;
  }
  .outlet-map-header {
    font-size: 11px; font-weight: 600; color: #86868b;
    text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px;
  }
  .outlet-row {
    display: flex; align-items: center; gap: 10px;
    padding: 6px 0; border-top: 1px solid #f0f0f0; font-size: 13px;
  }
  .outlet-row:first-of-type { border-top: none; }
  .outlet-num {
    width: 22px; height: 22px; border-radius: 6px; background: #f2f2f7;
    display: flex; align-items: center; justify-content: center;
    font-weight: 600; font-size: 12px; color: #1d1d1f; flex-shrink: 0;
  }
  .outlet-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
  .outlet-dot.on { background: #34c759; }
  .outlet-dot.off { background: #1d1d1f; }
  .outlet-dot.offline { background: #c7c7cc; }
  .outlet-watts { width: 70px; text-align: right; color: #86868b; font-variant-numeric: tabular-nums; }
  .outlet-machine a { color: #007aff; text-decoration: none; font-weight: 600; }
  .outlet-machine a:hover { text-decoration: underline; }
  .outlet-empty { color: #86868b; }
  /* Tiles (mirrors the dashboard) */
  #content { padding: 0 28px 20px; }
  .tiles { display: flex; gap: 10px; flex-wrap: wrap; }
  .tile {
    width: 140px; height: 140px; background: #fff;
    border: 1px solid #d2d2d7; border-radius: 10px; padding: 12px;
    display: flex; flex-direction: column; position: relative;
    cursor: pointer; transition: box-shadow 0.15s;
    text-decoration: none; color: inherit;
  }
  .tile:hover { box-shadow: 0 2px 12px rgba(0,0,0,0.1); }
  .tile-top { display: flex; align-items: center; gap: 6px; margin-bottom: 6px; }
  .state-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
  .state-OFF { background: #1d1d1f; }
  .state-ATTRACT { background: #007aff; }
  .state-PLAYING { background: #34c759; }
  .state-IDLE { background: #f5c41a; }
  .state-null { background: #aeaeb2; border: 1px dashed #c7c7cc; }
  .state-OFFLINE { background: #c7c7cc; }
  .tile.offline { opacity: 0.55; }
  .tile-offline {
    flex: 1; display: flex; align-items: center; justify-content: center;
    font-size: 12px; font-weight: 600; letter-spacing: 0.5px; color: #86868b;
  }
  .machine-name {
    font-size: 12px; font-weight: 600; line-height: 1.2; overflow: hidden;
    display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical;
    color: #1d1d1f;
  }
  .tile-lock { font-size: 11px; margin-left: auto; flex-shrink: 0; }
  .sparkline-wrap { flex: 1; min-height: 0; border-radius: 4px; overflow: hidden; }
  .sparkline-wrap canvas { width: 100%; height: 100%; }
  .tile-watts {
    font-size: 11px; font-weight: 500; color: #86868b; text-align: right;
    margin-top: 4px; font-variant-numeric: tabular-nums;
  }
  .tile-onoff {
    flex: 1; display: flex; align-items: center; justify-content: center;
    font-size: 13px; font-weight: 600; color: #86868b; letter-spacing: 0.5px;
  }
  .tile-onoff.on { color: #34c759; }
  .tile-onoff.off { color: #1d1d1f; }
  .tile-toggle {
    margin-top: 4px; padding: 4px 0; border-radius: 6px; border: none;
    font-size: 11px; font-weight: 600; cursor: pointer; color: #fff;
    transition: opacity 0.15s;
  }
  .tile-toggle:hover { opacity: 0.85; }
  .tile-toggle.on { background: #34c759; }
  .tile-toggle.off { background: #ff3b30; }
  .no-data { text-align: center; padding: 40px 20px; color: #86868b; font-size: 14px; }
  .toast {
    position: fixed; bottom: 20px; left: 50%; transform: translateX(-50%);
    padding: 10px 20px; border-radius: 8px; font-size: 13px; font-weight: 500;
    z-index: 100; transition: opacity 0.3s; box-shadow: 0 4px 16px rgba(0,0,0,0.15);
  }
  .toast-success { background: #34c759; color: #fff; }
  .toast-error { background: #ff3b30; color: #fff; }
</style>
</head>
<body class="{{BODY_CLASS}}">

<header>
  <a href="/">&larr; Dashboard</a>
  <h1 id="strip-title"><span id="strip-name">Loading...</span></h1>
  <span class="flip-suffix">
    for <a class="flip-link" href="https://theflip.museum">The Flip</a>
  </span>
  {{AUTH_CORNER}}
</header>
<div class="offline-banner" id="offline-banner" hidden>
  Strip is OFFLINE — showing last-known outlet data.
</div>

<div class="outlet-map private-only">
  <div class="outlet-map-header" id="outlet-map-header">Outlets</div>
  <div id="outlet-rows"><div class="no-data">Loading...</div></div>
</div>

<div id="content">
  <div class="no-data">Loading...</div>
</div>

<script>
const PUBLIC_MODE = {{PUBLIC_MODE}};
const STATE_COLORS = {
  OFF: '#1d1d1f', ATTRACT: '#007aff', PLAYING: '#34c759', IDLE: '#f5c41a'
};
const deviceId = decodeURIComponent(location.pathname.split('/').pop());

function escapeHtml(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  })[c]);
}

function showToast(msg, type) {
  const existing = document.querySelector('.toast');
  if (existing) existing.remove();
  const t = document.createElement('div');
  t.className = 'toast toast-' + type;
  t.textContent = msg;
  document.body.appendChild(t);
  setTimeout(() => { t.style.opacity = '0'; setTimeout(() => t.remove(), 300); }, 4000);
}

// Mirrors the dashboard's sparkline renderer (intentional duplication —
// pages are self-contained inline templates).
function drawSparkline(canvas, data, states) {
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth * dpr;
  const h = canvas.clientHeight * dpr;
  canvas.width = w;
  canvas.height = h;
  ctx.clearRect(0, 0, w, h);
  if (!data || data.length < 2) return;
  const max = 300;
  const step = w / (data.length - 1);
  const pad = 2 * dpr;
  if (states && states.length === data.length) {
    let i = 0;
    while (i < states.length) {
      const st = states[i];
      let j = i;
      while (j < states.length && states[j] === st) j++;
      const x0 = i === 0 ? 0 : (i - 0.5) * step;
      const x1 = j >= states.length ? w : (j - 0.5) * step;
      const c = STATE_COLORS[st];
      if (c) { ctx.fillStyle = c + '30'; ctx.fillRect(x0, 0, x1 - x0, h); }
      i = j;
    }
  }
  const lastState = states && states.length ? states[states.length - 1] : null;
  const color = STATE_COLORS[lastState] || '#aeaeb2';
  ctx.beginPath();
  ctx.moveTo(0, h);
  for (let i = 0; i < data.length; i++) {
    ctx.lineTo(i * step, h - pad - (Math.min(data[i], max) / max) * (h - 2 * pad));
  }
  ctx.lineTo(w, h);
  ctx.closePath();
  ctx.fillStyle = color + '18';
  ctx.fill();
  ctx.beginPath();
  for (let i = 0; i < data.length; i++) {
    const x = i * step;
    const y = h - pad - (Math.min(data[i], max) / max) * (h - 2 * pad);
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  }
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.5 * dpr;
  ctx.stroke();
}

let stripData = null;
let editingName = false;

function renderHeader(strip) {
  if (editingName) return;  // don't clobber the editor mid-edit
  const title = document.getElementById('strip-title');
  const display = strip.display_name || strip.device_id;
  const aliasHint = (strip.name && strip.alias && strip.alias !== strip.name)
    ? `<span class="alias-hint">(alias: ${escapeHtml(strip.alias)})</span>` : '';
  title.innerHTML = `
    <span id="strip-name">${escapeHtml(display)}</span>
    ${aliasHint}
    <button class="edit-name-btn private-only" title="Rename strip"
      onclick="startEditName()">&#9998;</button>`;
  document.title = 'juice — ' + display;
  document.getElementById('offline-banner').hidden = !strip.offline;
}

function startEditName() {
  if (!stripData) return;
  editingName = true;
  const title = document.getElementById('strip-title');
  title.innerHTML = `
    <input class="name-input" id="name-input" maxlength="100"
      value="${escapeHtml(stripData.name)}" placeholder="${escapeHtml(stripData.alias)}">
    <button class="btn btn-save" onclick="saveName()">Save</button>
    <button class="btn btn-cancel" onclick="cancelEditName()">Cancel</button>`;
  const input = document.getElementById('name-input');
  input.focus();
  input.addEventListener('keydown', (e) => {
    if (e.key === 'Enter') saveName();
    if (e.key === 'Escape') cancelEditName();
  });
}

function cancelEditName() {
  editingName = false;
  if (stripData) renderHeader(stripData);
}

async function saveName() {
  const name = document.getElementById('name-input').value;
  try {
    const resp = await fetch('/api/strips/' + encodeURIComponent(deviceId) + '/name', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({name})
    });
    const data = await resp.json();
    if (!resp.ok) { showToast(data.error || 'Rename failed', 'error'); return; }
    showToast(data.name ? 'Strip renamed' : 'Name cleared', 'success');
    editingName = false;
    await poll();
  } catch (e) { showToast('Rename failed', 'error'); }
}

function renderOutlets(strip) {
  const el = document.getElementById('outlet-rows');
  document.getElementById('outlet-map-header').textContent =
    'Outlets (' + strip.outlets.length + ')';
  if (!strip.outlets.length) {
    el.innerHTML = '<div class="no-data">No outlets discovered</div>';
    return;
  }
  el.innerHTML = strip.outlets.map(o => {
    const dot = strip.offline ? 'offline' : (o.is_on ? 'on' : 'off');
    const watts = o.watts != null ? o.watts.toFixed(1) + ' W' : '—';
    const what = o.machine
      ? `<a href="/machine/${o.plug_id}">${escapeHtml(o.machine.name)}</a>
         <span class="outlet-empty">(${escapeHtml(o.machine.asset_id)})</span>`
      : `<span class="outlet-empty">${escapeHtml(o.alias) || '—'}</span>`;
    return `
      <div class="outlet-row">
        <div class="outlet-num">${o.outlet_number ?? '·'}</div>
        <div class="outlet-dot ${dot}"></div>
        <div class="outlet-watts">${strip.offline ? 'OFFLINE' : watts}</div>
        <div class="outlet-machine">${what}</div>
      </div>`;
  }).join('');
}

function renderTiles(machines) {
  const el = document.getElementById('content');
  const mine = machines.filter(m => m.strip_device_id === deviceId);
  if (!mine.length) {
    el.innerHTML = '<div class="no-data">No machines on this strip</div>';
    return;
  }
  let html = '<div class="tiles">';
  let idx = 0;
  for (const m of mine) {
    const plugId = m.plug ? m.plug.plug_id : 0;
    const offline = !!m.offline;
    if (m.has_emeter === false) {
      const isOn = !!m.is_on;
      const dotState = offline ? 'OFFLINE' : (isOn ? 'PLAYING' : 'OFF');
      const toggleBtn = (PUBLIC_MODE || offline) ? '' :
        `<button class="tile-toggle ${isOn ? 'off' : 'on'}"
           onclick="togglePlug(event, ${plugId}, ${isOn ? 'false' : 'true'})">
           ${isOn ? 'Turn Off' : 'Turn On'}
         </button>`;
      const body = offline
        ? `<div class="tile-offline">OFFLINE</div>`
        : `<div class="tile-onoff ${isOn ? 'on' : 'off'}">${isOn ? 'ON' : 'OFF'}</div>${toggleBtn}`;
      html += `
        <a class="tile${offline ? ' offline' : ''}" href="/machine/${plugId}">
          <div class="tile-top">
            <div class="state-dot state-${dotState}"></div>
            <div class="machine-name">${escapeHtml(m.name)}</div>
            ${m.locked ? '<span class="tile-lock" title="Shutdown locked">&#128274;</span>' : ''}
          </div>
          ${body}
        </a>`;
    } else {
      const st = offline ? 'OFFLINE' : (m.state || 'null');
      const watts = m.power ? m.power.watts.toFixed(1) + 'W' : '--';
      const body = offline
        ? `<div class="tile-offline">OFFLINE</div>`
        : `<div class="sparkline-wrap"><canvas id="spark-${idx}"></canvas></div>
           <div class="tile-watts">${watts}</div>`;
      html += `
        <a class="tile${offline ? ' offline' : ''}" href="/machine/${plugId}">
          <div class="tile-top">
            <div class="state-dot state-${st}"></div>
            <div class="machine-name">${escapeHtml(m.name)}</div>
            ${m.locked ? '<span class="tile-lock" title="Shutdown locked">&#128274;</span>' : ''}
          </div>
          ${body}
        </a>`;
    }
    idx++;
  }
  html += '</div>';
  el.innerHTML = html;

  idx = 0;
  for (const m of mine) {
    if (m.has_emeter !== false && !m.offline) {
      const canvas = document.getElementById('spark-' + idx);
      if (canvas && m.sparkline && m.sparkline.length > 1) {
        drawSparkline(canvas, m.sparkline, m.sparkline_states);
      }
    }
    idx++;
  }
}

async function togglePlug(ev, plugId, on) {
  ev.preventDefault();
  ev.stopPropagation();
  try {
    const resp = await fetch('/api/plugs/' + plugId + '/power', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({on})
    });
    if (!resp.ok) {
      const body = await resp.json().catch(() => ({}));
      showToast(body.error || 'Power control failed', 'error');
    }
  } catch (e) {}
  poll();
}

async function poll() {
  try {
    const [sResp, mResp] = await Promise.all([
      fetch('/api/strips/' + encodeURIComponent(deviceId)),
      fetch('/api/machines'),
    ]);
    if (sResp.status === 404) {
      document.getElementById('outlet-rows').innerHTML =
        '<div class="no-data">Unknown strip</div>';
      document.getElementById('strip-name').textContent = 'Unknown strip';
      return;
    }
    stripData = await sResp.json();
    const mData = await mResp.json();
    renderHeader(stripData);
    renderOutlets(stripData);
    renderTiles(mData.machines);
  } catch (e) {
    // Transient fetch failure — keep last render; next poll retries.
  }
}

poll();
setInterval(poll, 2000);
</script>
</body>
</html>
"""
