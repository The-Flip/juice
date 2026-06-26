"""HTTP server with API and web dashboard for juice."""

from __future__ import annotations

import asyncio
import hmac
import json
import logging
import os
import re
import tempfile
import uuid
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from aiohttp import web

from juice.collector import Plug, PlugReading, _SelfPlug, call_with_retry, outlet_number
from juice.overload import OverloadWindow
from juice.state import (
    OFF_WATTS,
    Calibration,
    CalibrationError,
    State,
    auto_calibrate,
    classify,
)
from juice.store import Store

# A plug-like object that can be turned on/off and has an `alias` attribute.
Controllable = Plug | _SelfPlug

log = logging.getLogger(__name__)

BUFFER_SIZE = 3600  # ~60 minutes at 1s polling
# Dashboard sparkline tiles are only a few hundred px wide, so the full 1 Hz
# buffer is wildly oversampled. We downsample to this many points on the wire.
SPARK_POINTS = 200

# How long a reboot (power-cycle) holds the machine off before powering it back
# on. Module-level so tests can set it to 0 instead of sleeping.
REBOOT_HOLD_SECONDS = 3.0

# Strong references to fire-and-forget background tasks (e.g. the reboot
# power-on), so the event loop can't garbage-collect them mid-flight.
_background_tasks: set[asyncio.Task] = set()

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
    # Operator-set dashboard order (device_id -> position). Strips without a
    # position sort after positioned ones, by display name.
    strip_orders: dict[str, int] = field(default_factory=dict)
    # Circuit membership and metadata, hydrated from the store.
    circuit_devices: dict[str, int] = field(default_factory=dict)  # device_id -> circuit_id
    circuits: dict[int, dict] = field(default_factory=dict)  # circuit_id -> circuit row dict
    plug_objects: dict[int, Controllable] = field(
        default_factory=dict
    )  # plug_id -> Plug or _SelfPlug (for control)
    plug_has_emeter: dict[int, bool] = field(default_factory=dict)  # plug_id -> has_emeter
    # Locked machines by asset_id (the lock follows the machine across outlet
    # moves). 'on' = locked-on (refuse off; skipped by all-off); 'off' =
    # locked-off (refuse on; skipped by all-on). Unlocked machines are absent.
    lock_modes: dict[str, str] = field(default_factory=dict)
    # Overload detection. Per-machine "normal" sustained power (asset_id -> watts,
    # absent until enough history) and a trailing-window watt accumulator per plug.
    power_baselines: dict[str, float] = field(default_factory=dict)
    overload_windows: dict[int, OverloadWindow] = field(default_factory=dict)
    # When each plug's current above-threshold streak began (plug_id -> ts), so a
    # shutdown can report how long the machine was actually overloading.
    overload_onsets: dict[int, datetime] = field(default_factory=dict)
    # Auto-shutdown behavior: 'live' acts, 'shadow' only logs/audits, 'off' disables.
    overload_mode: str = "live"
    # FlipFix creds, so an overload shutdown can file a problem report + mark the
    # machine broken. None when FlipFix isn't configured (reporting skipped).
    flipfix_url: str | None = None
    flipfix_key: str | None = None
    # Juice's own public base URL (e.g. https://juice.theflip.museum), used to deep
    # link from a FlipFix report back to the machine page. None -> link omitted.
    public_url: str | None = None
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


@web.middleware
async def compress_middleware(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> web.StreamResponse:
    """Gzip buffered responses (negotiated from the client's Accept-Encoding).

    The `/api/machines` payload is dominated by sparkline floats, which compress
    ~5-10x. Streaming responses pass through untouched: the SSE stream
    (`/api/events`) and the `/api/backup` download both rely on their own
    chunked flushing, which response-level compression would break.
    """
    resp = await handler(request)
    if isinstance(resp, web.Response) and resp.content_type != "text/event-stream":
        resp.enable_compression()
    return resp


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

    # (sort_key, machine_dict) pairs — the sort key is built from state before
    # public redaction, so public ordering matches the operator's order.
    ranked: list[tuple[tuple[int, int, str, str, bool, int, int], dict]] = []
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
                # Downsample to tile resolution — the full 1 Hz buffer is far more
                # detail than a few-hundred-px sparkline can show. machine_state is
                # taken from the full-res classification above, so it's unaffected.
                sparkline, sparkline_states = _downsample_spark(sparkline, sparkline_states)

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

        # Sort key built before public redaction, so public ordering matches
        # the operator's order. Strip rank first (operator position, else
        # after by display name), then physical outlet position within a strip.
        device_id = plug_info[0] if plug_info else ""
        position = outlet_number(plug_info[1]) if plug_info else None
        strip_order = state.strip_orders.get(device_id)
        sort_key = (
            0 if strip_order is not None else 1,  # positioned strips first
            strip_order if strip_order is not None else 0,
            _strip_display_name(state, device_id).lower(),  # then by name
            device_id,  # stable tiebreak
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
                    "power_status": _power_status(reading, has_emeter, offline),
                    "has_emeter": has_emeter,
                    "sparkline": sparkline,
                    "sparkline_states": sparkline_states,
                    "strip_device_id": strip_device_id,
                    "strip_alias": strip_alias,
                    "calibrated": plug_id in state.calibrations,
                    "offline": offline,
                    "locked": state.lock_modes.get(asset_id) is not None,
                    "lock_mode": state.lock_modes.get(asset_id),
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
    for plug_id, device_id, alias, drawing_db in store.list_unassigned_outlets():
        # Prefer the live relay state when the recorder has a reading, so the tile
        # agrees with all-off (which also keys on the relay via _build_targets) and
        # an energized but no-draw outlet reads as on. With no live reading we can't
        # know the relay (history persists only watts), so fall back to last-known
        # *draw* as a best-effort proxy — None (unknown) for a no-emeter plug, which
        # then renders off via power_status below.
        reading = state.plug_readings.get(plug_id)
        is_on = _relay_on(state, plug_id) if reading is not None else drawing_db
        offline = device_id in state.offline_since
        has_emeter = state.plug_has_emeter.get(plug_id, True)
        outlets.append(
            {
                "plug_id": plug_id,
                "device_id": device_id,
                "alias": alias,
                "is_on": is_on,
                "power_status": _power_status(reading, has_emeter, offline),
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

    assignment = state.assignments.get(plug_id)
    if assignment:
        mode = state.lock_modes.get(assignment[1])
        # 'on' forbids turning OFF; 'off' forbids turning ON.
        if (not on and mode == "on") or (on and mode == "off"):
            verb = "shutdown-locked" if mode == "on" else "startup-locked"
            # Audit write must not mask the refusal response.
            try:
                store.record_power_event(
                    ts,
                    plug_id,
                    action,
                    "individual",
                    actor,
                    "refused",
                    error=f"machine is {verb}",
                )
            except Exception as e:
                log.warning("Audit write failed for plug %d: %s", plug_id, e)
            direction = "turning off" if mode == "on" else "turning on"
            return web.json_response(
                {"error": f"{assignment[0]} is {verb} — unlock it before {direction}"},
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


async def _reboot_power_on(
    state: RecorderState, store: Store, plug: Controllable, plug_id: int, actor: str
) -> None:
    """Background tail of a reboot: hold the machine off, then power it back on.

    Fire-and-forget (like `run_operation`) so the request returns as soon as the
    off-step succeeds. Force-polls the plug so the recorder reports — and the SSE
    `readings` tick pushes — the back-on state within ~1s.
    """
    try:
        await asyncio.sleep(REBOOT_HOLD_SECONDS)
        # Re-check the lock: another operator could have locked the machine off
        # during the hold, and that must veto the delayed power-on.
        assignment = state.assignments.get(plug_id)
        if assignment and state.lock_modes.get(assignment[1]) is not None:
            log.info("Reboot power-on skipped for plug %d — locked during hold", plug_id)
            try:
                store.record_power_event(
                    datetime.now(UTC),
                    plug_id,
                    "turn_on",
                    "reboot",
                    actor,
                    "refused",
                    error="machine was locked during reboot",
                )
            except Exception as ae:
                log.warning("Audit write failed for plug %d: %s", plug_id, ae)
            _publish(
                state, {"type": "reboot", "plug_id": plug_id, "phase": "abort", "actor": actor}
            )
            return
        await call_with_retry(plug.turn_on, max_attempts=6)
    except Exception as e:
        log.warning("Reboot power-on failed for plug %d: %s", plug_id, e)
        try:
            store.record_power_event(
                datetime.now(UTC), plug_id, "turn_on", "reboot", actor, "error", error=str(e)
            )
        except Exception as ae:
            log.warning("Audit write failed for plug %d: %s", plug_id, ae)
        _publish(state, {"type": "reboot", "plug_id": plug_id, "phase": "abort", "actor": actor})
        return

    state.force_poll.add(plug_id)
    log.info("Plug %d (%s) powered back on (reboot) by %s", plug_id, plug.alias, actor)
    try:
        store.record_power_event(datetime.now(UTC), plug_id, "turn_on", "reboot", actor, "ok")
    except Exception as e:
        log.warning("Audit write failed for plug %d: %s", plug_id, e)
    _publish(
        state,
        {
            "type": "power_change",
            "plug_id": plug_id,
            "on": True,
            "actor": actor,
            "source": "reboot",
        },
    )
    _publish(state, {"type": "reboot", "plug_id": plug_id, "phase": "on", "actor": actor})


async def handle_reboot(request: web.Request) -> web.Response:
    """Power-cycle a machine: turn off, hold ~REBOOT_HOLD_SECONDS, turn back on.

    The off-step is synchronous (so the operator sees immediate success/failure);
    the hold + on-step run in the background. Refused if the machine is locked in
    either direction, since a reboot cycles off->on.
    """
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

    actor = _actor(request)
    ts = datetime.now(UTC)

    # A reboot cycles off->on, which a lock in either direction forbids.
    assignment = state.assignments.get(plug_id)
    if assignment and state.lock_modes.get(assignment[1]) is not None:
        try:
            store.record_power_event(
                ts, plug_id, "reboot", "reboot", actor, "refused", error="machine is locked"
            )
        except Exception as e:
            log.warning("Audit write failed for plug %d: %s", plug_id, e)
        return web.json_response(
            {"error": f"{assignment[0]} is locked — unlock it before rebooting"}, status=409
        )

    # Signal every viewer that a reboot has begun, so the power button disables
    # (machine still on) before the off-step lands — see the DETAIL_HTML state machine.
    _publish(state, {"type": "reboot", "plug_id": plug_id, "phase": "start", "actor": actor})

    # Turn off synchronously so the operator gets immediate feedback.
    try:
        await call_with_retry(plug.turn_off, max_attempts=6)
    except Exception as e:
        log.warning("Reboot power-off failed for plug %d: %s", plug_id, e)
        # Audit write must not mask the relay-failure response.
        try:
            store.record_power_event(
                ts, plug_id, "turn_off", "reboot", actor, "error", error=str(e)
            )
        except Exception as ae:
            log.warning("Audit write failed for plug %d: %s", plug_id, ae)
        # Un-stick viewers' buttons: the cycle never started.
        _publish(state, {"type": "reboot", "plug_id": plug_id, "phase": "abort", "actor": actor})
        return web.json_response({"error": str(e)}, status=500)

    log.info("Plug %d (%s) powered off (reboot) by %s", plug_id, plug.alias, actor)
    try:
        store.record_power_event(ts, plug_id, "turn_off", "reboot", actor, "ok")
    except Exception as e:
        log.warning("Audit write failed for plug %d: %s", plug_id, e)
    _publish(
        state,
        {
            "type": "power_change",
            "plug_id": plug_id,
            "on": False,
            "actor": actor,
            "source": "reboot",
        },
    )
    _publish(state, {"type": "reboot", "plug_id": plug_id, "phase": "off", "actor": actor})

    task = asyncio.create_task(_reboot_power_on(state, store, plug, plug_id, actor))
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return web.json_response({"ok": True, "rebooting": True})


async def handle_lock(request: web.Request) -> web.Response:
    """Lock or unlock the machine assigned to a plug, in its current power state.

    Locking pins the machine to whatever state it's in right now: a running
    machine is locked ON (off refused), a powered-off machine is locked OFF (on
    refused). The direction is derived server-side from the live reading, not
    chosen by the client.
    """
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

    # Lock pins the current outlet state; unlock clears it. Keys on the relay
    # (not measured watts) so locking an energized-but-idle outlet pins it 'on'.
    mode = ("on" if _relay_on(state, plug_id) else "off") if locked else None

    machine_id = store.ensure_machine(asset_id, name)
    store.set_machine_lock_mode(machine_id, mode)
    if mode is None:
        state.lock_modes.pop(asset_id, None)
    else:
        state.lock_modes[asset_id] = mode

    actor = _actor(request)
    log.info("Machine %s (%s) lock set to %s by %s", name, asset_id, mode or "none", actor)
    _publish(
        state,
        {
            "type": "lock_change",
            "plug_id": plug_id,
            "asset_id": asset_id,
            "locked": mode is not None,
            "mode": mode,
            "actor": actor,
        },
    )
    return web.json_response({"ok": True, "locked": mode is not None, "mode": mode})


def _strip_display_name(state: RecorderState, device_id: str) -> str:
    """Operator-set strip name, falling back to the Kasa alias."""
    return state.strip_names.get(device_id) or state.strip_aliases.get(device_id, "")


def _strip_plug_ids(state: RecorderState, device_id: str) -> list[int] | None:
    """Plug IDs of a strip, or None when the device is unknown.

    A device is known either from the cloud refresh (strip_aliases) or DB
    hydration (plugs) — the latter keeps offline-at-boot strips reachable.
    A known device with no plugs yet yields [].
    """
    plug_ids = [pid for pid, (dev, _cid, _alias) in state.plugs.items() if dev == device_id]
    if device_id not in state.strip_aliases and not plug_ids:
        return None
    return plug_ids


async def handle_strip_detail(request: web.Request) -> web.Response:
    """All outlets of one strip in physical order, with attached machines."""
    device_id = request.match_info["device_id"]
    state: RecorderState = request.app["recorder_state"]

    plug_ids = _strip_plug_ids(state, device_id)
    if plug_ids is None:
        return web.json_response({"error": "Unknown device"}, status=404)

    outlets = []
    watts_values: list[float] = []
    for plug_id in plug_ids:
        _dev, child_id, alias = state.plugs[plug_id]
        assignment = state.assignments.get(plug_id)
        reading = state.plug_readings.get(plug_id)
        watts = None
        if reading is not None and reading.watts is not None:
            watts = round(reading.watts, 1)
            watts_values.append(watts)
        outlets.append(
            {
                "plug_id": plug_id,
                "child_id": child_id,
                "outlet_number": outlet_number(child_id),
                "alias": alias,
                "machine": (
                    {"name": assignment[0], "asset_id": assignment[1]} if assignment else None
                ),
                "is_on": _relay_on(state, plug_id),
                "watts": watts,
                "power_status": _power_status(
                    reading,
                    state.plug_has_emeter.get(plug_id, True),
                    device_id in state.offline_since,
                ),
            }
        )
    outlets.sort(key=lambda o: (o["outlet_number"] is None, o["outlet_number"] or 0, o["plug_id"]))

    # Sum of the rounded per-outlet values so the headline number always
    # equals the sum of the visible rows; None when nothing has a reading.
    total_watts = round(sum(watts_values), 1) if watts_values else None

    return web.json_response(
        {
            "device_id": device_id,
            "alias": state.strip_aliases.get(device_id, ""),
            "name": state.strip_names.get(device_id, ""),
            "display_name": _strip_display_name(state, device_id),
            "offline": device_id in state.offline_since,
            "total_watts": total_watts,
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

    if _strip_plug_ids(state, device_id) is None:
        return web.json_response({"error": "Unknown device"}, status=404)

    body = await request.json()
    if not isinstance(body, dict):
        return web.json_response({"error": "body must be a JSON object"}, status=400)
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


async def handle_strip_order(request: web.Request) -> web.Response:
    """Set the dashboard order of strips from a full ordered device_id list."""
    from juice.auth import require_capability

    error = require_capability(request, "control_power")
    if error:
        return error

    state: RecorderState = request.app["recorder_state"]
    store: Store = request.app["store"]

    body = await request.json()
    if not isinstance(body, dict):
        return web.json_response({"error": "body must be a JSON object"}, status=400)
    device_ids = body.get("device_ids")
    if not isinstance(device_ids, list) or not all(isinstance(d, str) for d in device_ids):
        return web.json_response({"error": "device_ids must be a list of strings"}, status=400)

    # Deduplicate (keep first occurrence) so positions don't collide.
    seen: set[str] = set()
    deduped: list[str] = []
    for d in device_ids:
        if d not in seen:
            seen.add(d)
            deduped.append(d)
    device_ids = deduped

    # Only accept real strips — known from the cloud refresh (strip_aliases) or
    # DB hydration (plugs). Keeps junk out of the order.
    known = set(state.strip_aliases) | {dev for dev, _cid, _alias in state.plugs.values()}
    unknown = [d for d in device_ids if d not in known]
    if unknown:
        return web.json_response(
            {"error": f"unknown device_ids: {', '.join(unknown[:5])}"}, status=400
        )

    store.set_strip_orders(device_ids)
    # Full replace (the store clears prior positions), so state mirrors the DB.
    state.strip_orders = {d: i for i, d in enumerate(device_ids)}

    actor = _actor(request)
    log.info("Strip order set (%d strips) by %s", len(device_ids), actor)
    _publish(state, {"type": "strip_order_change", "actor": actor})
    return web.json_response({"ok": True, "count": len(device_ids)})


# Three distinct notions of "on-ness" live here — keep them straight to avoid the
# class of bug where a call site grabs the wrong one:
#   * RELAY ENERGIZED — `PlugReading.is_on` (the device's relay flag), surfaced by
#     `_relay_on`. The ONLY notion control acts on (turn on/off, locks, all-off).
#     The API `is_on` field always means this.
#   * DRAWING POWER — `watts` vs the single threshold `OFF_WATTS`. A value derived
#     from watts alone describes *drawing*, never *on* (name such things `drawing`).
#   * DISPLAY STATUS — `_power_status` → offline|off|no_draw|on, the one derivation
#     every UI surface shows (it folds relay + draw + reachability together).
# So: "is the relay on?" → `_relay_on`; "what do we show?" → `_power_status`.
def _relay_on(state: RecorderState, plug_id: int) -> bool:
    """Whether the outlet relay is energized, independent of measured draw.

    Reflects the hardware relay (`reading.is_on`), so an energized outlet whose
    machine draws nothing still reads as on. No live reading → False.
    """
    reading = state.plug_readings.get(plug_id)
    return bool(reading.is_on) if reading is not None else False


def _power_status(reading: PlugReading | None, has_emeter: bool, offline: bool) -> str:
    """Derive the displayed power status from relay state + measured draw.

    'offline' (unreachable) | 'off' (relay off) | 'no_draw' (emeter relay on but
    drawing < OFF_WATTS — machine off/unplugged/faulted) | 'on' (drawing power,
    or a no-emeter relay that's on). One source of truth for every UI surface.
    """
    if offline:
        return "offline"
    if reading is None or not reading.is_on:
        return "off"
    # Only claim no-draw on an actual measurement — a missing watts value means
    # we don't know the draw, so fall through to 'on' rather than asserting the
    # machine is off/unplugged.
    if has_emeter and reading.watts is not None and reading.watts < OFF_WATTS:
        return "no_draw"
    return "on"


def _downsample_spark(
    watts: list[float], states: list[str], target: int = SPARK_POINTS
) -> tuple[list[float], list[str]]:
    """Bucket a long sparkline down to <= `target` points for a dashboard tile.

    Each bucket becomes one point: the mean watt (rounded) and the *last* state
    in the bucket (categorical — keeps the most-recent band per bucket). `states`
    may be empty (uncalibrated plug); it's only downsampled when aligned 1:1 with
    `watts`, otherwise passed through. Short inputs are returned unchanged.
    """
    n = len(watts)
    if n <= target:
        return watts, states
    have_states = len(states) == n
    out_w: list[float] = []
    out_s: list[str] = []
    for i in range(target):
        lo = i * n // target
        hi = max((i + 1) * n // target, lo + 1)
        bucket = watts[lo:hi]
        out_w.append(round(sum(bucket) / len(bucket), 1))
        if have_states:
            out_s.append(states[hi - 1])
    return out_w, (out_s if have_states else states)


def _readings_snapshot(state: RecorderState) -> list[dict]:
    """Lightweight per-machine live values for the SSE 'readings' tick.

    Keyed by `plug_id` with no device/strip identifiers, so it's safe to push to
    public and authenticated viewers alike. Mirrors the live fields of
    `handle_machines` (power, state, is_on, power_status, offline) but carries no
    sparkline — the client appends `watt` to its own local buffer instead.
    """
    out: list[dict] = []
    for plug_id in state.assignments:
        reading = state.plug_readings.get(plug_id)
        plug_info = state.plugs.get(plug_id)
        has_emeter = state.plug_has_emeter.get(plug_id, True)

        power = None
        is_on: bool | None = None
        watt: float | None = None
        if reading is not None:
            is_on = reading.is_on
            if has_emeter and reading.watts is not None:
                watt = round(reading.watts, 1)
                power = {
                    "watts": watt,
                    "voltage": round(reading.voltage or 0.0, 1),
                    "amps": round(reading.amps or 0.0, 3),
                    "total_kwh": round(reading.total_kwh or 0.0, 1),
                }

        machine_state = None
        if has_emeter:
            buf = state.watt_buffers.get(plug_id)
            if buf:
                cal = state.calibrations.get(plug_id)
                if cal:
                    classified = classify(list(buf), cal)
                    if classified:
                        machine_state = classified[-1].value

        offline = plug_info is not None and plug_info[0] in state.offline_since
        if offline:
            machine_state = "OFFLINE"

        out.append(
            {
                "plug_id": plug_id,
                "power": power,
                "state": machine_state,
                "is_on": is_on,
                "power_status": _power_status(reading, has_emeter, offline),
                "offline": offline,
                "watt": watt,
            }
        )
    return out


def _build_targets(
    state: RecorderState, kind: str, outlet_plug_ids: list[int] | None = None
) -> list[int]:
    """Plug IDs to act on for an all-on / all-off.

    Machines come first, sorted by year ascending; non-machine outlets (passed in,
    already ordered) are appended **last** so they switch after every machine.

    Mirrors the client-side filter the dashboard used to apply:
    - Skip plugs whose *relay* is already in the desired state. This keys on the
      relay (via _relay_on), not measured draw, so an energized but no-draw outlet
      (relay on, machine off/unplugged) is still turned off by all-off rather than
      mistaken for already-off.
    - When turning off, skip locked-on machines; when turning on, skip locked-off
      machines (both must be unlocked first). Outlets have no machine, so this
      gate never applies to them.
    - When turning off, skip PLAYING machines (don't interrupt a game). Outlets
      have no calibration, so this gate never applies to them.
    - With no live reading yet, leave the plug alone on all-off (we can't be sure
      it's on) but include it on all-on (so it's brought up to the desired state).
    """
    on = kind == "all_on"
    ranked: list[tuple[int, int]] = []  # (year_key, plug_id)
    for plug_id, (_name, asset_id, year) in state.assignments.items():
        relay_on = _relay_on(state, plug_id)

        if on and relay_on:
            continue
        if not on and not relay_on:
            continue

        mode = state.lock_modes.get(asset_id)
        if not on and mode == "on":  # locked-on: keep it on, skip from all-off
            continue
        if on and mode == "off":  # locked-off: keep it off, skip from all-on
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
        relay_on = _relay_on(state, plug_id)
        if on and relay_on:
            continue
        if not on and not relay_on:
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


# Event types an unauthenticated (public) SSE subscriber is allowed to receive.
# 'readings' carries only live power/state keyed by plug_id — no strip/device
# names or operator detail. Everything else (operations, lock/strip-name/order
# changes, power_change, overload_shutdown) is operator-only and would leak
# fields the public /api/machines view deliberately redacts (e.g. strip aliases).
PUBLIC_SSE_EVENTS = frozenset({"readings"})


async def _sse_stream(
    state: RecorderState,
    write: Callable[[dict], Awaitable[None]],
    public: bool = False,
) -> None:
    """Register an event subscriber, send a hello, then forward events until cancelled.

    Public (unauthenticated) subscribers receive only `PUBLIC_SSE_EVENTS` and a
    hello with no operation detail — they get live tile updates without seeing
    operator-only events.
    """
    queue: asyncio.Queue = asyncio.Queue(maxsize=64)
    state.event_subscribers.add(queue)
    try:
        await write(
            {
                "type": "hello",
                "current_operation": (
                    _operation_to_dict(state.current_operation)
                    if state.current_operation is not None and not public
                    else None
                ),
            }
        )
        while True:
            event = await queue.get()
            if public and event.get("type") not in PUBLIC_SSE_EVENTS:
                continue
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

    plug_id: int | None = None
    raw_plug_id = request.query.get("plug_id")
    if raw_plug_id is not None:
        try:
            plug_id = int(raw_plug_id)
        except ValueError:
            plug_id = None

    rows = store.recent_power_events(limit=limit, before=before, plug_id=plug_id)
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


def _parse_usage_window(request: web.Request) -> tuple[datetime, datetime]:
    """Resolve the [start, end) hour-aligned UTC window for usage queries.

    Query params:
      days  — window length in days (default 30, clamped to [1, 365]).
              Used when `start` and `end` are not provided.
      start — ISO timestamp (UTC) for the window start. Optional.
      end   — ISO timestamp (UTC) for the window end. Optional. Defaults
              to the top of the *next* hour (so the current partial hour
              is included).
    """
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

    return start, end


def _hour_buckets(start: datetime, end: datetime) -> list[datetime]:
    """All top-of-hour buckets in [start, end), so charts get continuous bands."""
    hours: list[datetime] = []
    cur = start
    while cur < end:
        hours.append(cur)
        cur += timedelta(hours=1)
    return hours


async def handle_usage(request: web.Request) -> web.Response:
    """Historical power usage for [start, end), bucketed by hour, by machine.

    See _parse_usage_window for the query params. Both bounds are aligned
    to top-of-hour-UTC.
    """
    store: Store = request.app["store"]

    start, end = _parse_usage_window(request)

    # Read straight from the rollup. The recorder owns refreshing it (on
    # startup + every 60s) so the handler doesn't block the event loop on
    # what could be a full-history backfill on a fresh DB. Worst case: the
    # chart's right edge is up to ~60s stale right after server startup.
    rows = store.usage_by_machine(start, end)

    hours = _hour_buckets(start, end)
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


async def handle_strip_usage(request: web.Request) -> web.Response:
    """Historical power usage of one strip, summed across its outlets.

    Same window query params as /api/usage (see _parse_usage_window), but a
    single hourly series instead of a per-machine breakdown.
    """
    device_id = request.match_info["device_id"]
    state: RecorderState = request.app["recorder_state"]
    store: Store = request.app["store"]

    plug_ids = _strip_plug_ids(state, device_id)
    if plug_ids is None:
        return web.json_response({"error": "Unknown device"}, status=404)

    start, end = _parse_usage_window(request)
    rows = store.usage_for_plugs(plug_ids, start, end)

    hours = _hour_buckets(start, end)
    hour_index = {h: i for i, h in enumerate(hours)}
    hourly = [0.0] * len(hours)
    for ts, kwh in rows:
        # DuckDB yields naive timestamps (session pinned UTC); re-attach the
        # zone or every bucket lookup misses and the chart is silently empty.
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        idx = hour_index.get(ts)
        if idx is not None:
            hourly[idx] += kwh

    # Peaks over the same window: actual = max simultaneous draw (strip
    # rollup); theoretical = every outlet peaking at once (sum of per-plug
    # maxes).
    plug_peak_map = store.plug_peaks(plug_ids, start, end)
    theoretical = round(sum(plug_peak_map.values()), 1) if plug_peak_map else None
    actual = store.strip_peaks(start, end).get(device_id)

    # Total is summed from the rounded hourly values so the contract
    # `sum(hourly_kwh) == total_kwh` holds exactly on the client.
    rounded = [round(v, 4) for v in hourly]
    return web.json_response(
        {
            "device_id": device_id,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "hours": [h.isoformat() for h in hours],
            "hourly_kwh": rounded,
            "total_kwh": round(sum(rounded), 4),
            "peak_watts_actual": round(actual, 1) if actual is not None else None,
            "peak_watts_theoretical": theoretical,
        }
    )


async def handle_machine_peak(request: web.Request) -> web.Response:
    """Peak single-second watts for one plug over the usage window.

    Public-readable, like the other detail-page numbers. Unknown plugs
    yield peak_watts=null rather than 404 (consistent with handle_readings).
    """
    try:
        plug_id = int(request.match_info["plug_id"])
    except ValueError:
        return web.json_response({"error": "plug_id must be an integer"}, status=400)
    store: Store = request.app["store"]

    start, end = _parse_usage_window(request)
    peak = store.plug_peaks([plug_id], start, end).get(plug_id)
    return web.json_response(
        {
            "plug_id": plug_id,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "peak_watts": round(peak, 1) if peak is not None else None,
        }
    )


async def handle_strip_peaks(request: web.Request) -> web.Response:
    """Per-strip current draw + actual/theoretical peaks. Operators only.

    One row per device with at least one emeter plug, sorted by display
    name. current_watts follows handle_strip_detail's rule: sum of the
    rounded live per-outlet readings, null when nothing has a reading.
    """
    state: RecorderState = request.app["recorder_state"]
    store: Store = request.app["store"]

    start, end = _parse_usage_window(request)

    plugs_by_device: dict[str, list[int]] = {}
    for pid, (dev, _cid, _alias) in state.plugs.items():
        plugs_by_device.setdefault(dev, []).append(pid)

    emeter_ids = [p for p in state.plugs if state.plug_has_emeter.get(p, True)]
    plug_peak_map = store.plug_peaks(emeter_ids, start, end)
    actual_map = store.strip_peaks(start, end)

    strips = []
    for device_id, pids in plugs_by_device.items():
        if not any(state.plug_has_emeter.get(p, True) for p in pids):
            continue  # EP10-style outlets: no emeter, no peaks to report
        watts_values = [
            round(r.watts, 1)
            for p in pids
            if (r := state.plug_readings.get(p)) is not None and r.watts is not None
        ]
        theo_values = [plug_peak_map[p] for p in pids if p in plug_peak_map]
        actual = actual_map.get(device_id)
        strips.append(
            {
                "device_id": device_id,
                "display_name": _strip_display_name(state, device_id),
                "current_watts": round(sum(watts_values), 1) if watts_values else None,
                "peak_watts_actual": round(actual, 1) if actual is not None else None,
                "peak_watts_theoretical": (round(sum(theo_values), 1) if theo_values else None),
            }
        )
    strips.sort(key=lambda s: (s["display_name"].lower(), s["device_id"]))

    return web.json_response({"start": start.isoformat(), "end": end.isoformat(), "strips": strips})


# --- Circuits ---------------------------------------------------------------

MAX_CIRCUIT_FIELD_LEN = 100
# Branch-circuit nominal voltage; capacity_watts = amps * VOLTS. US 120V.
CIRCUIT_VOLTS = 120.0


def _circuit_plug_ids(state: RecorderState, circuit_id: int) -> list[int]:
    """All plug IDs on strips currently assigned to the circuit."""
    devices = {d for d, c in state.circuit_devices.items() if c == circuit_id}
    return [pid for pid, (dev, _cid, _alias) in state.plugs.items() if dev in devices]


def _circuit_label(circuit: dict) -> str:
    """Human label e.g. 'P1 B20 — coin-op ceiling drop'."""
    loc = f"{circuit['panel']} {circuit['breaker']}".strip()
    desc = circuit.get("description") or ""
    return f"{loc} — {desc}" if desc else loc


def _validate_circuit_fields(body: object) -> tuple[dict, web.Response | None]:
    """Parse/validate a circuit create/update body. Returns (fields, error)."""
    if not isinstance(body, dict):
        return {}, web.json_response({"error": "body must be a JSON object"}, status=400)
    panel = body.get("panel")
    breaker = body.get("breaker")
    for label, val in (("panel", panel), ("breaker", breaker)):
        if not isinstance(val, str) or not val.strip():
            return {}, web.json_response(
                {"error": f"{label} must be a non-empty string"}, status=400
            )
        if len(val) > MAX_CIRCUIT_FIELD_LEN:
            return {}, web.json_response({"error": f"{label} is too long"}, status=400)
    description = body.get("description", "")
    if not isinstance(description, str) or len(description) > MAX_CIRCUIT_FIELD_LEN:
        return {}, web.json_response(
            {"error": "description must be a string ≤ 100 chars"}, status=400
        )
    amps = body.get("amps")
    if amps is not None:
        if isinstance(amps, bool) or not isinstance(amps, (int, float)) or amps <= 0:
            return {}, web.json_response({"error": "amps must be a positive number"}, status=400)
        amps = float(amps)
    return {
        "panel": panel.strip(),
        "breaker": breaker.strip(),
        "description": description.strip(),
        "amps": amps,
    }, None


async def handle_circuits(request: web.Request) -> web.Response:
    """List all circuits with their assigned strips. Operators only."""
    state: RecorderState = request.app["recorder_state"]
    store: Store = request.app["store"]

    devices_by_circuit: dict[int, list[str]] = {}
    for dev, cid in state.circuit_devices.items():
        devices_by_circuit.setdefault(cid, []).append(dev)

    circuits = []
    for c in store.list_circuits():
        devs = sorted(devices_by_circuit.get(c["circuit_id"], []))
        circuits.append(
            {
                **c,
                "device_ids": devs,
                "display_names": [_strip_display_name(state, d) for d in devs],
            }
        )
    return web.json_response({"circuits": circuits})


def _circuit_id_param(request: web.Request) -> tuple[int | None, web.Response | None]:
    """Parse the {id} path param as int, or return a 400 response."""
    try:
        return int(request.match_info["id"]), None
    except ValueError:
        return None, web.json_response({"error": "circuit id must be an integer"}, status=400)


async def handle_circuit_create(request: web.Request) -> web.Response:
    """Create a circuit. Operators only."""
    from juice.auth import require_capability
    from juice.store import DuplicateCircuitError

    error = require_capability(request, "control_power")
    if error:
        return error

    state: RecorderState = request.app["recorder_state"]
    store: Store = request.app["store"]

    fields, verr = _validate_circuit_fields(await request.json())
    if verr:
        return verr

    try:
        cid = store.create_circuit(
            fields["panel"], fields["breaker"], fields["description"], fields["amps"]
        )
    except DuplicateCircuitError as e:
        return web.json_response({"error": str(e)}, status=409)
    created = store.get_circuit(cid)
    assert created is not None  # just inserted
    state.circuits[cid] = created
    _publish(state, {"type": "circuit_change", "circuit_id": cid, "actor": _actor(request)})
    return web.json_response({"ok": True, "circuit_id": cid, **created})


async def handle_circuit_update(request: web.Request) -> web.Response:
    """Update a circuit's fields. Operators only."""
    from juice.auth import require_capability
    from juice.store import DuplicateCircuitError

    error = require_capability(request, "control_power")
    if error:
        return error

    state: RecorderState = request.app["recorder_state"]
    store: Store = request.app["store"]

    circuit_id, id_err = _circuit_id_param(request)
    if id_err:
        return id_err
    assert circuit_id is not None  # id_err is None ⇒ parsed
    if store.get_circuit(circuit_id) is None:
        return web.json_response({"error": "Unknown circuit"}, status=404)

    fields, verr = _validate_circuit_fields(await request.json())
    if verr:
        return verr

    try:
        store.update_circuit(
            circuit_id, fields["panel"], fields["breaker"], fields["description"], fields["amps"]
        )
    except DuplicateCircuitError as e:
        return web.json_response({"error": str(e)}, status=409)
    updated = store.get_circuit(circuit_id)
    assert updated is not None  # existence checked above
    state.circuits[circuit_id] = updated
    _publish(state, {"type": "circuit_change", "circuit_id": circuit_id, "actor": _actor(request)})
    return web.json_response({"ok": True, **updated})


async def handle_circuit_delete(request: web.Request) -> web.Response:
    """Delete a circuit; its strips revert to unassigned. Operators only."""
    from juice.auth import require_capability

    error = require_capability(request, "control_power")
    if error:
        return error

    state: RecorderState = request.app["recorder_state"]
    store: Store = request.app["store"]

    circuit_id, id_err = _circuit_id_param(request)
    if id_err:
        return id_err
    assert circuit_id is not None  # id_err is None ⇒ parsed
    if store.get_circuit(circuit_id) is None:
        return web.json_response({"error": "Unknown circuit"}, status=404)

    store.delete_circuit(circuit_id)
    # Sync in-memory state from the committed DB change first, so a failing
    # rebuild can't leave /api/circuits reporting stale membership.
    state.circuits.pop(circuit_id, None)
    for dev in [d for d, c in state.circuit_devices.items() if c == circuit_id]:
        state.circuit_devices.pop(dev, None)
    store.rebuild_hourly_circuit_peak()
    _publish(state, {"type": "circuit_change", "circuit_id": circuit_id, "actor": _actor(request)})
    return web.json_response({"ok": True})


async def handle_strip_circuit_assign(request: web.Request) -> web.Response:
    """Assign a strip to a circuit (or clear it). Operators only."""
    from juice.auth import require_capability

    error = require_capability(request, "control_power")
    if error:
        return error

    device_id = request.match_info["device_id"]
    state: RecorderState = request.app["recorder_state"]
    store: Store = request.app["store"]

    if _strip_plug_ids(state, device_id) is None:
        return web.json_response({"error": "Unknown device"}, status=404)

    body = await request.json()
    if not isinstance(body, dict):
        return web.json_response({"error": "body must be a JSON object"}, status=400)
    circuit_id = body.get("circuit_id")
    if circuit_id is not None:
        if not isinstance(circuit_id, int) or isinstance(circuit_id, bool):
            return web.json_response({"error": "circuit_id must be an integer or null"}, status=400)
        if store.get_circuit(circuit_id) is None:
            return web.json_response({"error": "Unknown circuit"}, status=404)

    store.set_device_circuit(device_id, circuit_id)
    # Sync state from the committed DB change before the (heavier, fallible)
    # rebuild, so a rebuild failure can't leave membership reads stale.
    if circuit_id is None:
        state.circuit_devices.pop(device_id, None)
    else:
        state.circuit_devices[device_id] = circuit_id
    store.rebuild_hourly_circuit_peak()
    _publish(
        state,
        {
            "type": "circuit_assignment_change",
            "device_id": device_id,
            "circuit_id": circuit_id,
            "actor": _actor(request),
        },
    )
    return web.json_response({"ok": True, "circuit_id": circuit_id})


async def handle_circuit_peaks(request: web.Request) -> web.Response:
    """Per-circuit current draw + actual/theoretical peaks + % of breaker
    capacity, for the usage-page table. Operators only."""
    state: RecorderState = request.app["recorder_state"]
    store: Store = request.app["store"]

    start, end = _parse_usage_window(request)

    emeter_ids = [p for p in state.plugs if state.plug_has_emeter.get(p, True)]
    plug_peak_map = store.plug_peaks(emeter_ids, start, end)
    actual_map = store.circuit_peaks(start, end)

    circuits = []
    for c in store.list_circuits():
        cid = c["circuit_id"]
        pids = _circuit_plug_ids(state, cid)
        watts_values = [
            round(r.watts, 1)
            for p in pids
            if (r := state.plug_readings.get(p)) is not None and r.watts is not None
        ]
        theo_values = [plug_peak_map[p] for p in pids if p in plug_peak_map]
        actual = actual_map.get(cid)
        amps = c["amps"]
        capacity = round(amps * CIRCUIT_VOLTS, 1) if amps is not None else None
        actual_w = round(actual, 1) if actual is not None else None
        pct = round(100.0 * actual_w / capacity, 1) if capacity and actual_w is not None else None
        circuits.append(
            {
                **c,
                "label": _circuit_label(c),
                "current_watts": round(sum(watts_values), 1) if watts_values else None,
                "peak_watts_actual": actual_w,
                "peak_watts_theoretical": (round(sum(theo_values), 1) if theo_values else None),
                "capacity_watts": capacity,
                "pct_of_capacity": pct,
            }
        )
    circuits.sort(key=lambda c: (c["panel"], c["breaker"]))

    return web.json_response(
        {"start": start.isoformat(), "end": end.isoformat(), "circuits": circuits}
    )


async def handle_circuit_usage(request: web.Request) -> web.Response:
    """Historical usage of one circuit, summed across all its strips' plugs.

    Same shape as handle_strip_usage. Operators only.
    """
    state: RecorderState = request.app["recorder_state"]
    store: Store = request.app["store"]

    circuit_id = int(request.match_info["id"])
    if store.get_circuit(circuit_id) is None:
        return web.json_response({"error": "Unknown circuit"}, status=404)

    plug_ids = _circuit_plug_ids(state, circuit_id)
    start, end = _parse_usage_window(request)
    rows = store.usage_for_plugs(plug_ids, start, end)

    hours = _hour_buckets(start, end)
    hour_index = {h: i for i, h in enumerate(hours)}
    hourly = [0.0] * len(hours)
    for ts, kwh in rows:
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        idx = hour_index.get(ts)
        if idx is not None:
            hourly[idx] += kwh

    plug_peak_map = store.plug_peaks(plug_ids, start, end)
    theoretical = round(sum(plug_peak_map.values()), 1) if plug_peak_map else None
    actual = store.circuit_peaks(start, end).get(circuit_id)

    rounded = [round(v, 4) for v in hourly]
    return web.json_response(
        {
            "circuit_id": circuit_id,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "hours": [h.isoformat() for h in hours],
            "hourly_kwh": rounded,
            "total_kwh": round(sum(rounded), 4),
            "peak_watts_actual": round(actual, 1) if actual is not None else None,
            "peak_watts_theoretical": theoretical,
        }
    )


def _bearer_token(request: web.Request) -> str | None:
    """Extract a bearer token from the Authorization header (header-only so
    the secret never lands in URLs / access logs)."""
    header = request.headers.get("Authorization", "")
    scheme, _, value = header.partition(" ")
    if scheme.lower() == "bearer" and value:
        return value
    return None


async def handle_backup(request: web.Request) -> web.StreamResponse:
    """Stream a consistent point-in-time copy of the DuckDB database.

    Token-gated (Authorization: Bearer <JUICE_BACKUP_TOKEN>), independent of
    OAuth so a script/cron can pull. Only registered when the token is set.
    """
    expected: str = request.app["backup_token"]
    provided = _bearer_token(request)
    if provided is None or not hmac.compare_digest(provided, expected):
        return web.json_response({"error": "Not authorized"}, status=401)

    store: Store = request.app["store"]

    # Snapshot to a fresh temp file ON THE SAME FILESYSTEM AS THE DB (COPY
    # needs a non-existent dest), then stream it. Staging beside the DB
    # matters in prod: the DB lives on a mounted volume while /tmp may be a
    # small tmpfs that a full-size copy would overflow. Unlink right after
    # opening so the temp is cleaned up even if the client disconnects.
    db_dir = os.path.dirname(store.path) or None  # None → system temp (in-memory/bare path)
    fd, tmp_path = tempfile.mkstemp(suffix=".duckdb", dir=db_dir)
    os.close(fd)
    os.unlink(tmp_path)  # snapshot_to needs the path absent
    store.snapshot_to(tmp_path)

    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    response = web.StreamResponse(
        headers={
            "Content-Type": "application/octet-stream",
            "Content-Disposition": f'attachment; filename="juice-{stamp}.duckdb"',
        }
    )
    f = open(tmp_path, "rb")  # noqa: SIM115 — closed in finally after streaming
    os.unlink(tmp_path)  # inode survives via the open fd
    try:
        await response.prepare(request)
        while chunk := f.read(1 << 20):
            await response.write(chunk)
        await response.write_eof()
    finally:
        f.close()
    return response


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


async def handle_busy_grid(request: web.Request) -> web.Response:
    """Play-utilization bubble grid: PLAYING time / on-time per (local date, hour).

    Only measurable (emeter + calibrated) machines contribute (the rollup
    enforces it), and only (date, hour) cells with play are returned. Query
    params mirror /api/play-hours (default days=28).
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
            days = int(request.query.get("days", "28"))
        except ValueError:
            days = 28
        days = max(1, min(days, 365))
        start = end - timedelta(days=days)

    rows = store.play_utilization_grid(
        datetime.combine(start, datetime.min.time()),
        datetime.combine(end, datetime.min.time()),
    )
    cells = [
        {
            "date": r["date_local"].isoformat(),
            "hour": r["hour"],
            "ratio": round(r["ratio"], 4),
            "play_hours": round(r["play_hours"], 4),
            "on_hours": round(r["on_hours"], 4),
        }
        for r in rows
    ]
    return web.json_response(
        {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "dates": sorted({c["date"] for c in cells}),
            "hours": sorted({c["hour"] for c in cells}),
            "cells": cells,
            "max_ratio": max((c["ratio"] for c in cells), default=0.0),
        }
    )


def _iso_z(dt: datetime | None) -> str | None:
    """ISO-8601 with a 'Z' suffix so the client parses DB-naive UTC correctly."""
    return dt.isoformat() + "Z" if dt is not None else None


# Air-quality metric fields surfaced by the API, mirroring store column order.
_AIR_METRICS = ("temperature", "humidity", "co2", "pm25", "pm10", "tvoc", "noise", "battery")


async def handle_air(request: web.Request) -> web.Response:
    """All air monitors with their latest reading (one row per sensor)."""
    store: Store = request.app["store"]
    latest = store.air_latest()
    sensors = []
    for s in store.list_air_sensors():
        reading = latest.get(s["mac"], {})
        sensors.append(
            {
                "mac": s["mac"],
                "name": s["name"],
                "online": s["online"],
                "last_seen": _iso_z(s["last_seen"]),
                "ts": _iso_z(reading.get("ts")),
                **{k: reading.get(k) for k in _AIR_METRICS},
            }
        )
    return web.json_response({"sensors": sensors})


async def handle_air_history(request: web.Request) -> web.Response:
    """Raw reading series for one monitor over [from, to) (default last 7 days)."""
    store: Store = request.app["store"]
    mac = request.match_info["mac"]

    now = datetime.now(UTC)
    start = _parse_iso_dt(request.query.get("from")) or (now - timedelta(days=7))
    end = _parse_iso_dt(request.query.get("to")) or now

    rows = store.air_history(mac, start, end)
    return web.json_response(
        {
            "mac": mac,
            "from": _iso_z(start.replace(tzinfo=None)),
            "to": _iso_z(end.replace(tzinfo=None)),
            "readings": [{"ts": _iso_z(r["ts"]), **{k: r[k] for k in _AIR_METRICS}} for r in rows],
        }
    )


def _parse_iso_dt(s: str | None) -> datetime | None:
    """Parse an ISO datetime query param to UTC, or None if absent/invalid."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt.astimezone(UTC) if dt.tzinfo else dt.replace(tzinfo=UTC)


async def handle_air_page(request: web.Request) -> web.Response:
    return _render_page(AIR_HTML, request)


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

    from juice.auth import is_authenticated

    # An unauthenticated subscriber is "public" and gets readings-only. With no
    # auth wired up at all (bare create_app in tests) everyone is the operator.
    public = not is_authenticated(request)

    async def _write(event: dict) -> None:
        await response.write(f"data: {json.dumps(event)}\n\n".encode())

    try:
        await _sse_stream(state, _write, public=public)
    except asyncio.CancelledError, ConnectionResetError:
        pass
    return response


def _html_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


# Shared top-of-page navigation, injected into every page via the {{NAV}} marker
# so all the major views (dashboard, usage, air, machine detail, …) can reach one
# another. The Events link is operator-only (hidden for public viewers by
# `body.public .private-only`).
_NAV_HTML = (
    '<nav class="header-nav">'
    '<a href="/">Home</a>'
    '<a href="/usage">Usage</a>'
    '<a href="/air">Air</a>'
    '<a class="private-only" href="/events">Events</a>'
    "</nav>"
)


# Extracted, unit-tested frontend modules (see juice/web/README.md). Each is ESM
# (so `node --test` can import it); for the browser we strip the leading `export `
# so every declaration becomes a page-scope global — exactly how the surrounding
# inline page code already calls these helpers. The result is inlined into the
# templates via {{JS_<NAME>}} markers (substituted last in _render_page).
_WEB_DIR = Path(__file__).parent / "web"


def _web_js(name: str) -> str:
    """Load a juice/web module as browser-inlinable JS.

    ESM `export ` is stripped (declarations become page-scope globals) and
    single-line named `import { … } from './x.js'` lines are removed — the
    imported names are provided by another module already inlined on the page
    (the test_inline_js.py guard enforces that), and node imports them normally
    for unit tests. Anything outside these supported forms fails fast at import
    rather than silently corrupting the page.
    """
    src = (_WEB_DIR / name).read_text()
    src = re.sub(r"^export(?=\s+(?:function|const|let|class)\s)", "", src, flags=re.M)
    if re.search(r"^\s*export\b", src, flags=re.M):
        raise ValueError(f"{name}: unsupported `export` form for browser inlining")

    # Strip single-line named imports. Aliasing (`as`) would break the inlined-
    # global model (the alias has no matching global), so forbid it.
    def _strip_import(m: re.Match[str]) -> str:
        if re.search(r"\bas\b", m.group(1)):  # any whitespace, not just " as "
            raise ValueError(f"{name}: aliased import is not supported (no matching global)")
        return ""

    src = re.sub(
        r"^import[ \t]+\{([^}\n]*)\}[ \t]+from[ \t]+['\"][^'\"\n]+['\"];?[ \t]*$",
        _strip_import,
        src,
        flags=re.M,
    )
    # Backstop: any other import form (multi-line, default, namespace, dynamic).
    if re.search(r"^\s*import\b", src, flags=re.M):
        raise ValueError(f"{name}: only single-line `import {{ … }}` is supported (see web/README)")

    # A literal {{MARKER}} in module text (even in a comment) would be re-substituted
    # by _render_page's marker pass and could inject another module into pages that
    # never asked for it — refer to markers in prose ("the JS_FORMAT marker") instead.
    if re.search(r"\{\{[A-Z_]+\}\}", src):
        raise ValueError(f"{name}: module source must not contain a {{{{MARKER}}}}")
    return src


_WEB_JS: dict[str, str] = {
    "JS_FORMAT": _web_js("format.js"),
    "JS_POWER": _web_js("power.js"),
    "JS_AIR": _web_js("air.js"),
    "JS_USAGE": _web_js("usage.js"),
    "JS_EVENTS": _web_js("events.js"),
    "JS_DETAIL": _web_js("detail.js"),
    "JS_TILES": _web_js("tiles.js"),
    "JS_TOAST": _web_js("toast.js"),
    "JS_PEAKS": _web_js("peaks.js"),
    "JS_CIRCUIT": _web_js("circuit.js"),
    "JS_STRIP": _web_js("strip.js"),
    "JS_CIRCUIT_PAGE": _web_js("circuit_page.js"),
}


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
      {{NAV}}            — shared cross-page navigation (see _NAV_HTML).
      {{JS_*}}           — extracted frontend modules from juice/web (see _WEB_JS),
                           inlined into a page's <script>. Substituted LAST so the
                           injected code isn't re-scanned for the markers above.
    """
    from juice.auth import dev_auth_key, is_authenticated, oauth_config_key

    # Auth is "active" under real OAuth or the local dev login shim; only then
    # is there a logged-out state (and a login/logout corner) to show.
    auth_active = oauth_config_key in request.app or dev_auth_key in request.app
    public = auth_active and not is_authenticated(request)
    user = request.get("user") or {}
    name = user.get("name") or user.get("email") or ""

    if not auth_active:
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
        .replace("{{NAV}}", _NAV_HTML)
    )
    # Inline extracted JS modules last, so injected code isn't re-scanned for the
    # markers above. Only substitutes markers a template actually contains.
    for marker, js in _WEB_JS.items():
        html = html.replace("{{" + marker + "}}", js)
    return web.Response(text=html, content_type="text/html")


async def handle_dashboard(request: web.Request) -> web.Response:
    return _render_page(DASHBOARD_HTML, request)


async def handle_machine_detail(request: web.Request) -> web.Response:
    return _render_page(DETAIL_HTML, request)


async def handle_events_page(request: web.Request) -> web.Response:
    return _render_page(EVENTS_HTML, request)


async def handle_strip_page(request: web.Request) -> web.Response:
    return _render_page(STRIP_HTML, request)


async def handle_circuit_page(request: web.Request) -> web.Response:
    return _render_page(CIRCUIT_HTML, request)


# Favicon: the FlipFix mark (a single #33BEF2 blob) reshaped into a jagged
# lightning bolt — fitting for a power-monitoring app, while keeping FlipFix's
# brand blue so juice reads as part of the same family. The bolt is rotated to
# lie along the same axis as FlipFix's flipper (its teardrop runs ~28° above the
# horizontal, rising to the right), so the two marks sit at a matching tilt.
FAVICON_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" '
    'viewBox="0 0 16 16" fill="none">'
    '<path transform="rotate(51 8 8)" '
    'd="M9 1 L3.5 8.6 L7.2 8.6 L6.2 15 L12.5 6.6 L8.5 6.6 Z" fill="#33BEF2"/>'
    "</svg>"
)


async def handle_favicon(request: web.Request) -> web.Response:
    return web.Response(
        text=FAVICON_SVG,
        content_type="image/svg+xml",
        headers={"Cache-Control": "public, max-age=86400"},
    )


def create_app(
    recorder_state: RecorderState,
    store: Store,
    oauth_config: dict | None = None,
    backup_token: str | None = None,
    dev_auth: bool = False,
) -> web.Application:
    app = web.Application()
    app["recorder_state"] = recorder_state
    app["store"] = store

    # Outermost middleware (registered first) so it compresses the final body,
    # including responses produced by the auth middleware.
    app.middlewares.append(compress_middleware)

    if oauth_config:
        from juice.auth import setup_auth

        setup_auth(app, oauth_config)
    elif dev_auth:
        # Explicit local-dev opt-in only (never on by default): install a
        # one-click login shim so `juice serve` shows the logged-out → login →
        # logout flow without a real OAuth provider. The CLI refuses to start
        # without OAuth unless --dev-auth/JUICE_DEV_AUTH is set, so this can't
        # silently grant control_power on a deployment whose OAuth env is
        # missing. With neither oauth_config nor dev_auth, no auth is wired up
        # at all (handler-level unit tests).
        from juice.auth import setup_dev_auth

        setup_dev_auth(app)

    app.router.add_get("/", handle_dashboard)
    app.router.add_get("/favicon.svg", handle_favicon)
    app.router.add_get("/favicon.ico", handle_favicon)
    app.router.add_get("/machine/{plug_id}", handle_machine_detail)
    app.router.add_get("/api/machines", handle_machines)
    app.router.add_get("/api/outlets", handle_outlets)
    app.router.add_get("/api/machines/{plug_id}/readings", handle_readings)
    app.router.add_post("/api/machines/{plug_id}/calibrate", handle_calibrate)
    app.router.add_post("/api/machines/{plug_id}/power", handle_power)
    app.router.add_post("/api/machines/{plug_id}/reboot", handle_reboot)
    app.router.add_post("/api/machines/{plug_id}/lock", handle_lock)
    app.router.add_get("/api/strips/{device_id}", handle_strip_detail)
    app.router.add_get("/api/strips/{device_id}/usage", handle_strip_usage)
    app.router.add_post("/api/strips/{device_id}/name", handle_strip_name)
    app.router.add_post("/api/strips/{device_id}/circuit", handle_strip_circuit_assign)
    app.router.add_post("/api/strip-order", handle_strip_order)
    app.router.add_get("/api/machines/{plug_id}/peak", handle_machine_peak)
    app.router.add_get("/api/strip-peaks", handle_strip_peaks)
    app.router.add_get("/api/circuits", handle_circuits)
    app.router.add_post("/api/circuits", handle_circuit_create)
    app.router.add_get("/api/circuit-peaks", handle_circuit_peaks)
    app.router.add_get("/api/circuits/{id}/usage", handle_circuit_usage)
    app.router.add_post("/api/circuits/{id}", handle_circuit_update)
    app.router.add_delete("/api/circuits/{id}", handle_circuit_delete)
    app.router.add_post("/api/plugs/{plug_id}/power", handle_power)
    app.router.add_post("/api/plugs/{plug_id}/reboot", handle_reboot)
    app.router.add_post("/api/operations/all-on", handle_all_on)
    app.router.add_post("/api/operations/all-off", handle_all_off)
    app.router.add_post("/api/operations/{id}/cancel", handle_cancel_operation)
    app.router.add_get("/api/operations/current", handle_current_operation)
    app.router.add_get("/api/events", handle_events)
    app.router.add_get("/api/power-events", handle_power_events)
    app.router.add_get("/events", handle_events_page)
    app.router.add_get("/api/usage", handle_usage)
    app.router.add_get("/api/play-hours", handle_play_hours)
    app.router.add_get("/api/busy-grid", handle_busy_grid)
    app.router.add_get("/usage", handle_usage_page)
    app.router.add_get("/api/air", handle_air)
    app.router.add_get("/api/air/{mac}/history", handle_air_history)
    app.router.add_get("/air", handle_air_page)
    app.router.add_get("/strip/{device_id}", handle_strip_page)
    app.router.add_get("/circuit/{id}", handle_circuit_page)

    # Backup download is registered only when a token is configured — an
    # unset token leaves the route absent (404), so dev/local never exposes
    # it. The handler does its own constant-time token check.
    if backup_token:
        app["backup_token"] = backup_token
        app.router.add_get("/api/backup", handle_backup)

    return app


async def start_server(
    recorder_state: RecorderState,
    store: Store,
    host: str = "0.0.0.0",  # noqa: S104
    port: int = 8000,
    oauth_config: dict | None = None,
    backup_token: str | None = None,
    dev_auth: bool = False,
) -> web.AppRunner:
    app = create_app(
        recorder_state,
        store,
        oauth_config=oauth_config,
        backup_token=backup_token,
        dev_auth=dev_auth,
    )
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
<link rel="icon" type="image/svg+xml" href="/favicon.svg">
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
  /* ...and conversely, .public-only chrome shows only for public viewers. */
  .public-only { display: none; }
  body.public .public-only { display: block; }
  .public-intro {
    padding: 14px 28px; background: #f5f5f7; border-bottom: 1px solid #d2d2d7;
    font-size: 13px; line-height: 1.5; color: #515154;
  }
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
  /* Reorder mode (desktop only — drag is awkward on touch). */
  .reorder-link {
    margin: 0 28px 28px; color: #007aff; cursor: pointer; font-size: 13px;
    font-weight: 500;
  }
  .reorder-link:hover { text-decoration: underline; }
  @media (max-width: 768px) { .desktop-only { display: none !important; } }
  .reorder-panel { padding: 8px 28px 28px; }
  .reorder-panel h2 { font-size: 16px; font-weight: 600; margin-bottom: 4px; }
  .reorder-hint { font-size: 13px; color: #86868b; margin-bottom: 14px; }
  .reorder-list { list-style: none; max-width: 480px; }
  .reorder-item {
    display: flex; align-items: center; gap: 10px;
    padding: 12px 14px; margin-bottom: 8px;
    background: #fff; border: 1px solid #d2d2d7; border-radius: 8px;
    font-weight: 600; cursor: grab;
  }
  .reorder-item.dragging { opacity: 0.45; }
  .reorder-item .grip {
    color: #c7c7cc; letter-spacing: -2px; user-select: none;
  }
  .reorder-actions { margin-top: 16px; display: flex; gap: 8px; }
  .reorder-cancel { background: #8e8e93; }
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
  .state-NO_DRAW { background: #ff9500; }
  .state-null { background: #aeaeb2; border: 1px dashed #c7c7cc; }
  .state-OFFLINE { background: #c7c7cc; }
  .tile-note { font-size: 11px; color: #b25e00; margin-top: 2px; }
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
  .recent-events .evt-time { color: #86868b; min-width: 112px; }
  .recent-events .evt-action.on  { color: #2e7d32; font-weight: 600; }
  .recent-events .evt-action.off { color: #c62828; font-weight: 600; }
  .recent-events .evt-source { color: #86868b; font-size: 11px; }
  .recent-events .evt-error { color: #c62828; font-size: 11px; }
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
  <h1>
    <span>juice</span> &mdash; live data about
    <a class="flip-link" href="https://theflip.museum">The Flip</a>
  </h1>
  {{NAV}}
  <div class="power-btns private-only">
    <button class="power-btn power-btn-on" id="btn-all-on" onclick="startOperation('all-on')">All On</button>
    <button class="power-btn power-btn-off" id="btn-all-off" onclick="startOperation('all-off')">All Off</button>
  </div>
  {{AUTH_CORNER}}
</header>
<div class="public-intro public-only">
  Juice is The Flip's system for tracking what's going on in our museum. Currently it
  monitors and controls power usage and measures environmental quality. That also lets us
  keep track of game usage and some game problems. The software is open source and is
  available <a class="flip-link" href="https://github.com/The-Flip/juice">on GitHub</a>.
</div>
<div id="op-banner" class="op-banner private-only" hidden>
  <div class="op-banner-text" id="op-banner-text"></div>
  <button class="op-banner-cancel" id="op-banner-cancel" onclick="cancelOperation()">Cancel</button>
</div>
<div id="content">
  <div class="no-data">Connecting...</div>
</div>
<div class="reorder-link private-only desktop-only" id="reorder-link" onclick="startReorder()">
  &#10247; Reorder strips
</div>
<div class="reorder-panel private-only" id="reorder-panel" hidden>
  <h2>Reorder strips</h2>
  <p class="reorder-hint">Drag the strips into the order you want, then Done.</p>
  <ul class="reorder-list" id="reorder-list"></ul>
  <div class="reorder-actions">
    <button class="power-btn power-btn-on" onclick="saveReorder()">Done</button>
    <button class="power-btn reorder-cancel" onclick="cancelReorder()">Cancel</button>
  </div>
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

// showToast comes from juice/web/toast.js (inlined via the JS_TOAST marker).
{{JS_TOAST}}

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

{{JS_FORMAT}}

// groupByStrip + buildTiles come from juice/web/tiles.js (inlined via JS_TILES).
{{JS_TILES}}

function renderMachines(machines, outlets) {
  const el = document.getElementById('content');
  if (!machines.length && (!outlets || !outlets.length)) {
    el.innerHTML = '<div class="no-data">No machines assigned</div>';
    return;
  }

  // groupByStrip + buildTiles come from juice/web/tiles.js (the strip grouping is
  // shared so the spark-<idx> canvas ids line up with the draw loop below).
  const strips = groupByStrip(machines);
  el.innerHTML = buildTiles(strips, outlets, { publicMode: PUBLIC_MODE, pendingPlugs });

  // Draw sparklines for emeter-equipped machines only. idx walks the same
  // strip-grouped, flattened order buildTiles used for the spark-<idx> ids.
  let idx = 0;
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

// Reorder mode (operators, desktop only). Clicking "Reorder strips" hides the
// tiles and shows a plain draggable list — and pauses polling so a 2s refresh
// can't rebuild the DOM mid-drag.
let reordering = false;

function startReorder() {
  const groups = [];
  const seen = new Set();
  for (const m of lastMachines) {
    const d = m.strip_device_id;
    if (!d || seen.has(d)) continue;
    seen.add(d);
    groups.push({ deviceId: d, alias: m.strip_alias || d });
  }
  document.getElementById('reorder-list').innerHTML = groups.map(g =>
    `<li class="reorder-item" draggable="true" data-device-id="${escapeHtml(g.deviceId)}">`
    + `<span class="grip">&#10247;</span>${escapeHtml(g.alias)}</li>`).join('');
  wireReorderDnD();
  reordering = true;
  document.getElementById('content').hidden = true;
  document.getElementById('reorder-link').hidden = true;
  document.getElementById('reorder-panel').hidden = false;
}

function wireReorderDnD() {
  const ul = document.getElementById('reorder-list');
  let dragged = null;
  ul.querySelectorAll('.reorder-item').forEach(li => {
    li.addEventListener('dragstart', (e) => {
      dragged = li;
      li.classList.add('dragging');
      e.dataTransfer.effectAllowed = 'move';
    });
    li.addEventListener('dragend', () => { li.classList.remove('dragging'); dragged = null; });
    li.addEventListener('dragover', (e) => {
      e.preventDefault();
      if (!dragged || dragged === li) return;
      const rect = li.getBoundingClientRect();
      if (e.clientY > rect.top + rect.height / 2) li.after(dragged);
      else li.before(dragged);
    });
  });
}

async function saveReorder() {
  const ids = Array.from(document.querySelectorAll('#reorder-list .reorder-item'))
    .map(li => li.getAttribute('data-device-id'));
  try {
    await fetch('/api/strip-order', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({device_ids: ids}),
    });
  } catch (e) {}
  exitReorder();
  poll();
}

function cancelReorder() { exitReorder(); }

function exitReorder() {
  reordering = false;
  document.getElementById('reorder-panel').hidden = true;
  document.getElementById('content').hidden = false;
  document.getElementById('reorder-link').hidden = false;
}

let lastMachines = [];
let lastOutlets = [];

// Per-plug pending power action for the dashboard tiles + outlets. Mirrors the
// detail page: a tile's toggle disables with a neutral label from the moment it's
// clicked until the relay settles on the target (or it times out), so a stale
// `readings` tick can't flip the button mid-request.
const pendingPlugs = new Map();        // plug_id -> 'turn_on' | 'turn_off'
const pendingPlugTimers = new Map();   // plug_id -> timeout id
const PLUG_PENDING_TIMEOUT_MS = 10000;

function beginPlugPending(plugId, on) {
  const prev = pendingPlugTimers.get(plugId);
  if (prev) clearTimeout(prev);
  pendingPlugs.set(plugId, on ? 'turn_on' : 'turn_off');
  pendingPlugTimers.set(plugId, setTimeout(() => {
    clearPlugPending(plugId);
    renderMachines(lastMachines, lastOutlets);  // accept the real relay state
  }, PLUG_PENDING_TIMEOUT_MS));
}

function clearPlugPending(plugId) {
  const t = pendingPlugTimers.get(plugId);
  if (t) clearTimeout(t);
  pendingPlugTimers.delete(plugId);
  pendingPlugs.delete(plugId);
}

// Clear a plug's pending state once its relay has reached the requested target.
function reconcilePlugPending(plugId, relayOn) {
  const action = pendingPlugs.get(plugId);
  if (!action) return;
  if (action === 'turn_on' && relayOn) clearPlugPending(plugId);
  else if (action === 'turn_off' && !relayOn) clearPlugPending(plugId);
}

async function togglePlug(ev, plugId, on) {
  ev.preventDefault();
  ev.stopPropagation();
  beginPlugPending(plugId, on);
  renderMachines(lastMachines, lastOutlets);  // disable the toggle immediately
  try {
    const resp = await fetch('/api/plugs/' + plugId + '/power', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({on})
    });
    if (!resp.ok) {
      const body = await resp.json().catch(() => ({}));
      alert(body.error || 'Power control failed');
      clearPlugPending(plugId);
      poll();
    }
    // On success stay pending; the `readings`/`power_change` tick reconciles it.
  } catch (e) {
    clearPlugPending(plugId);
    poll();
  }
}

async function poll() {
  // Don't churn the DOM while the operator is dragging in reorder mode.
  if (reordering) return;
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
  // A confirmed relay change settles any matching pending toggle for this plug.
  reconcilePlugPending(plugId, on);
  renderMachines(lastMachines, lastOutlets);
}

// ---- Live readings (SSE push) ---------------------------------------------
// The recorder pushes a lightweight per-machine snapshot ~1x/sec. We merge it
// into lastMachines by plug_id and append the new watt to each local sparkline,
// so tiles stay live without re-fetching the full /api/machines payload.

const SPARK_CAP = 600;  // bound local buffer growth + redraw cost

function applyReadings(readings) {
  // Don't churn the DOM while dragging, or do work the user can't see.
  if (reordering || pageHidden) return;
  // Wait for the first full poll to establish tile structure + baselines.
  if (!lastMachines.length) return;
  const byId = new Map();
  for (const m of lastMachines) if (m.plug) byId.set(m.plug.plug_id, m);
  for (const r of readings) {
    const m = byId.get(r.plug_id);
    if (!m) continue;  // stale/offline-duplicate plug not shown — ignore
    m.power = r.power;
    m.state = r.state;
    m.is_on = r.is_on;
    m.power_status = r.power_status;
    m.offline = r.offline;
    // Settle a pending tile toggle once its relay reaches the requested state.
    reconcilePlugPending(r.plug_id, !!r.is_on && r.power_status !== 'offline');
    if (m.has_emeter && r.watt != null) {
      if (!Array.isArray(m.sparkline)) m.sparkline = [];
      if (!Array.isArray(m.sparkline_states)) m.sparkline_states = [];
      m.sparkline.push(r.watt);
      // Keep the state band aligned only when it already is (calibrated). An
      // uncalibrated machine has no states; don't start a desynced band.
      if (m.sparkline_states.length === m.sparkline.length - 1) {
        m.sparkline_states.push(r.state || '');
      }
      if (m.sparkline.length > SPARK_CAP) {
        m.sparkline.splice(0, m.sparkline.length - SPARK_CAP);
        if (m.sparkline_states.length > SPARK_CAP) {
          m.sparkline_states.splice(0, m.sparkline_states.length - SPARK_CAP);
        }
      }
    }
  }
  renderMachines(lastMachines, lastOutlets);
}

// ---- Audit log preview ----------------------------------------------------

// fmtTimeShort comes from juice/web/format.js (inlined via the JS_FORMAT marker).

// buildRecentEventRow comes from juice/web/events.js (inlined via JS_EVENTS).
{{JS_EVENTS}}

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
    for (const e of data.events) {
      const li = document.createElement('li');
      li.innerHTML = buildRecentEventRow(e);
      list.appendChild(li);
    }
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
    } else if (ev.type === 'readings') {
      applyReadings(ev.machines);
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
    } else if (ev.type === 'overload_shutdown') {
      const verb = ev.shadow ? 'would auto-shut-down' : 'auto-shut-down + locked off';
      showToast('⚠ ' + ev.machine_name + ' ' + verb + ': ' + ev.watts
                + 'W sustained vs ' + ev.baseline + 'W baseline', 'error');
      poll();
    } else if (ev.type === 'lock_change' || ev.type === 'strip_name_change'
               || ev.type === 'strip_order_change') {
      poll();
    }
  };
  es.onerror = () => {
    // The browser auto-reconnects; nothing to do here. Polling still keeps the UI fresh.
  };
}

// ---- Init -----------------------------------------------------------------

// Live updates now arrive via SSE 'readings' ticks, so the old 2s poll is gone.
// A slow resync poll only catches structural changes (machines added/removed,
// reorders) and corrects any drift; per-second values come over the stream.
let pageHidden = document.hidden;
let resyncTimer = null;
const RESYNC_MS = 30000;

function startResync() {
  if (resyncTimer) clearInterval(resyncTimer);
  resyncTimer = setInterval(() => { if (!pageHidden) poll(); }, RESYNC_MS);
}

document.addEventListener('visibilitychange', () => {
  pageHidden = document.hidden;
  if (pageHidden) {
    // Stop all work while the tab is unseen; SSE stays connected but its ticks
    // are ignored (applyReadings bails on pageHidden).
    if (resyncTimer) { clearInterval(resyncTimer); resyncTimer = null; }
  } else {
    // Back in view — full refresh immediately, then resume the resync cadence.
    poll();
    startResync();
  }
});

poll();
if (!pageHidden) startResync();
// SSE drives live tiles for every viewer (public + authed).
connectEvents();
if (!PUBLIC_MODE) {
  // Audit-log preview requires auth — skip for anonymous viewers.
  refreshRecentEvents();
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
<link rel="icon" type="image/svg+xml" href="/favicon.svg">
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
  .header-nav { display: flex; gap: 14px; }
  .header-nav a { font-size: 13px; }
  header h1 { font-size: 17px; font-weight: 600; flex: 1; }
  .meta-bar {
    display: flex; gap: 24px; padding: 16px 28px; background: #fff;
    border-bottom: 1px solid #d2d2d7; flex-wrap: wrap; align-items: center;
  }
  .meta-item { font-size: 13px; color: #86868b; }
  .meta-item .val { color: #1d1d1f; font-weight: 600; font-variant-numeric: tabular-nums; }
  /* Fixed-width, right-aligned numeric readouts so a changing value (e.g. watts
     swinging during power-up) can't reflow the row and shove the action buttons. */
  .meta-item.num .val { display: inline-block; min-width: 72px; text-align: right; }
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
  .state-NO_DRAW { background: #fff1e0; color: #b25e00; }
  .state-NO_DRAW .dot { background: #ff9500; }
  .state-OFFLINE { background: #f2f2f7; color: #8e8e93; }
  .state-OFFLINE .dot { background: #c7c7cc; }
  .no-draw-hint { font-size: 12px; color: #b25e00; }
  .actions { display: flex; gap: 8px; margin-left: auto; }
  .btn {
    padding: 6px 16px; border-radius: 6px; font-size: 13px; font-weight: 600;
    cursor: pointer; border: none; transition: opacity 0.15s;
  }
  .btn:hover { opacity: 0.85; }
  .btn:disabled { opacity: 0.5; cursor: default; }
  .btn-power-on { background: #34c759; color: #fff; }
  .btn-power-off { background: #ff3b30; color: #fff; }
  .btn-reboot { background: #ff9500; color: #fff; }
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
  .outlet-dot.no_draw { background: #ff9500; }
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
  .recent-events {
    margin: 0 28px 28px;
    background: #fff; border: 1px solid #d2d2d7; border-radius: 10px;
    padding: 12px 16px;
  }
  .recent-events-header {
    font-size: 11px; font-weight: 600; color: #86868b;
    text-transform: uppercase; letter-spacing: 0.5px;
    margin-bottom: 8px;
  }
  .recent-events ul { list-style: none; }
  .recent-events li {
    padding: 4px 0; font-size: 12px; color: #1d1d1f;
    font-variant-numeric: tabular-nums;
    display: flex; gap: 8px; align-items: baseline;
  }
  .recent-events .evt-time { color: #86868b; min-width: 112px; }
  .recent-events .evt-action.on  { color: #2e7d32; font-weight: 600; }
  .recent-events .evt-action.off { color: #c62828; font-weight: 600; }
  .recent-events .evt-source { color: #86868b; font-size: 11px; }
  .recent-events .evt-error { color: #c62828; font-size: 11px; }
</style>
</head>
<body class="{{BODY_CLASS}}">

<header>
  <h1 id="machine-name">Loading...</h1>
  <span class="flip-suffix" style="color:#86868b;font-weight:500;">
    for <a class="flip-link" href="https://theflip.museum">The Flip</a>
  </span>
  {{NAV}}
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

<div id="detail-events" class="recent-events private-only" hidden>
  <div class="recent-events-header">Recent power events</div>
  <ul id="detail-events-list"></ul>
</div>

<script>
const PUBLIC_MODE = {{PUBLIC_MODE}};
const STATE_COLORS = { OFF: '#1d1d1f', ATTRACT: '#007aff', PLAYING: '#34c759', IDLE: '#f5c41a' };
const plugId = parseInt(location.pathname.split('/').pop());

{{JS_FORMAT}}

let machineData = null;
let peakWatts = null;  // 30-day peak; loaded separately at a slower cadence
// Power-control action state machine. `pending` is null when idle; while a
// turn-on / turn-off / reboot is in flight it's { action, sawOff } and every
// control renders disabled with a neutral in-progress label.
//
// The settle rule is the whole point: the authoritative `readings` relay stream
// is what CLEARS a pending action — never the server's transient `reboot`/
// `power_change` events. So the render that drops `pending` always uses a real,
// current relay reading and can't flicker to a stale value afterwards. Reboot
// settles only after the relay is observed to go off and THEN back on (sawOff);
// settling on a plain relay-match would fire prematurely on the pre-off "on".
//
// pcReduceReading + pcPowerButton are extracted, unit-tested pure logic; they're
// inlined here from juice/web/power.js (see juice/web/README.md).
{{JS_POWER}}

let pending = null;  // null | { action: 'turn_on'|'turn_off'|'reboot', sawOff: bool }
let pendingTimer = null;
const PENDING_TIMEOUT_MS = { turn_on: 10000, turn_off: 10000, reboot: 20000 };

function beginPending(action) {
  clearPending();
  pending = { action: action, sawOff: false };
  pendingTimer = setTimeout(() => {
    pendingTimer = null;
    pending = null;
    refreshMeta();  // timed out — accept whatever the relay actually reads now
  }, PENDING_TIMEOUT_MS[action]);
  if (machineData) renderMeta(machineData);
}

function clearPending() {
  if (pendingTimer) { clearTimeout(pendingTimer); pendingTimer = null; }
  pending = null;
}

async function fetchMachineInfo() {
  const resp = await fetch('/api/machines');
  const data = await resp.json();
  return data.machines.find(m => m.plug && m.plug.plug_id === plugId);
}

// showToast comes from juice/web/toast.js (inlined via the JS_TOAST marker).
{{JS_TOAST}}

// buildMeta (the meta-bar + action buttons) comes from juice/web/detail.js,
// inlined via the JS_DETAIL marker. renderMeta keeps the thin DOM glue.
{{JS_DETAIL}}

function renderMeta(m) {
  if (!m) return;
  machineData = m;
  document.getElementById('machine-name').textContent = m.name;
  document.title = 'juice — ' + m.name;
  document.getElementById('meta-bar').innerHTML =
    buildMeta(m, { publicMode: PUBLIC_MODE, pending, peakWatts });
}

async function togglePower(on) {
  // Go pending immediately: controls disable + neutral label, and stay that way
  // until the relay reading settles on the target (pcReduceReading) or we time out.
  beginPending(on ? 'turn_on' : 'turn_off');
  try {
    const resp = await fetch('/api/machines/' + plugId + '/power', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({on})
    });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) {
      showToast(data.error || 'Power control failed', 'error');
      clearPending();
      refreshMeta();
      return;
    }
    showToast('Turned ' + (on ? 'on' : 'off'), 'success');
    // Stay pending; the `readings` tick reconciles the real relay state within
    // ~1s (handle_power force-polls the plug), which clears the pending state.
  } catch (e) {
    showToast('Failed', 'error');
    clearPending();
    refreshMeta();
  }
}

async function rebootMachine() {
  if (machineData && machineData.state === 'PLAYING'
      && !confirm(machineData.name + ' is currently being played. Reboot anyway?')) {
    return;
  }
  // Disable the controls immediately (the SSE `reboot` start event does the same
  // for other viewers); the `reboot` lifecycle events then clear the pending state.
  beginPending('reboot');
  try {
    const resp = await fetch('/api/machines/' + plugId + '/reboot', {method: 'POST'});
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) {
      showToast(data.error || 'Reboot failed', 'error');
      clearPending();
      refreshMeta();
      return;
    }
    showToast('Rebooting…', 'success');
    // The server's `reboot` events (on, or abort) clear the pending state.
  } catch (e) {
    showToast('Reboot failed', 'error');
    clearPending();
    refreshMeta();
  }
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
      showToast(data.mode === 'on' ? 'Locked on' : data.mode === 'off' ? 'Locked off' : 'Unlocked', 'success');
      if (machineData) {
        machineData.locked = data.locked;
        machineData.lock_mode = data.mode;
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
    const dot = strip.offline ? 'offline' : (o.power_status || (o.is_on ? 'on' : 'off'));
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
        <div class="outlet-dot ${dot}" title="${dot === 'no_draw' ? 'Outlet on — machine off, unplugged, or faulted' : ''}"></div>
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
  refreshDetailEvents();
  if (m && m.has_emeter !== false) {
    await loadChart();
  } else {
    document.querySelector('.chart-wrap').innerHTML =
      '<div class="chart-area" style="padding:24px;color:#86868b;font-size:13px;text-align:center;">No power data — this device has no energy monitoring.</div>';
  }
})();

async function loadPeak() {
  try {
    const resp = await fetch('/api/machines/' + plugId + '/peak?days=30');
    if (!resp.ok) return;
    const data = await resp.json();
    peakWatts = data.peak_watts;
    if (machineData) renderMeta(machineData);
  } catch (e) {
    // Transient failure — next refresh retries.
  }
}

// -- Recent power events (this machine) -------------------------------------

// fmtTimeShort comes from juice/web/format.js (inlined via the JS_FORMAT marker).

// buildRecentEventRow comes from juice/web/events.js (inlined via JS_EVENTS).
{{JS_EVENTS}}

async function refreshDetailEvents() {
  if (PUBLIC_MODE) return;  // audit log carries actor identities — operators only
  try {
    const resp = await fetch('/api/power-events?plug_id=' + plugId + '&limit=20');
    if (!resp.ok) return;
    const data = await resp.json();
    const wrap = document.getElementById('detail-events');
    const list = document.getElementById('detail-events-list');
    list.innerHTML = '';
    if (!data.events.length) { wrap.hidden = true; return; }
    wrap.hidden = false;
    for (const e of data.events) {
      const li = document.createElement('li');
      li.innerHTML = buildRecentEventRow(e);
      list.appendChild(li);
    }
  } catch (e) {}
}

// -- Live updates via SSE (replaces the old fixed 5s meta poll) -------------
// The recorder pushes a `readings` tick ~1x/sec; we merge this machine's entry
// into the meta bar. Power actions (and reboot) force-poll server-side, so the
// real relay state reconciles within ~1s with no polling.
let pageHidden = document.hidden;

function applyDetailReadings(readings) {
  if (pageHidden || !machineData) return;
  const r = readings.find(x => x.plug_id === plugId);
  if (!r) return;
  machineData.power = r.power;
  machineData.state = r.state;
  machineData.is_on = r.is_on;
  machineData.power_status = r.power_status;
  machineData.offline = r.offline;
  // The authoritative relay reading settles any pending action (incl. reboot's
  // off→on cycle). Because this is the value we just rendered from, the button
  // can't flip to a stale state after it clears. Route a settle through
  // clearPending so the timeout timer is cancelled too.
  const next = pcReduceReading(pending, !!r.is_on && r.power_status !== 'offline');
  if (pending !== null && next === null) clearPending();
  else pending = next;
  renderMeta(machineData);
}

function connectEvents() {
  const es = new EventSource('/api/events');
  es.onmessage = (msg) => {
    let ev;
    try { ev = JSON.parse(msg.data); } catch { return; }
    if (ev.type === 'readings') {
      applyDetailReadings(ev.machines);
    } else if (ev.type === 'reboot' && ev.plug_id === plugId) {
      // `start` puts OTHER viewers (who didn't click) into the pending state so
      // their controls disable too. `abort` cancels. The `off`/`on` phases are
      // deliberately ignored for settling — the `readings` relay stream clears
      // the pending state (off→on) so the button never flickers to a value the
      // recorder hasn't caught up to yet.
      if (ev.phase === 'start') {
        if (!pending) beginPending('reboot');
      } else if (ev.phase === 'abort') {
        clearPending();
        refreshDetailEvents();  // abort wrote a power_events row — surface it now
        refreshMeta();  // reboot failed/aborted — resync the true state
      }
    } else if (ev.type === 'power_change' && ev.plug_id === plugId) {
      // The audit list updates on any on/off (this viewer's, another's, or a reboot step).
      refreshDetailEvents();
    } else if (ev.type === 'overload_shutdown' && ev.plug_id === plugId) {
      // Mirror the dashboard wording: shadow mode only reports, it doesn't act.
      const verb = ev.shadow ? 'would auto-shut-down' : 'auto-shut-down + locked off';
      showToast('⚠ ' + ev.machine_name + ' ' + verb + ': ' + ev.watts
                + 'W sustained vs ' + ev.baseline + 'W baseline', 'error');
      refreshDetailEvents();
      if (!ev.shadow) refreshMeta();
    } else if (ev.type === 'lock_change' && ev.plug_id === plugId) {
      // Lock state isn't in the readings tick — resync so a lock toggled by
      // another viewer shows here too.
      refreshMeta();
    }
  };
  es.onerror = () => {};  // EventSource auto-reconnects; readings resume on their own.
}

document.addEventListener('visibilitychange', () => {
  pageHidden = document.hidden;
  // Back in view — resync immediately to catch anything missed while hidden.
  if (!pageHidden) { refreshMeta(); refreshDetailEvents(); }
});

connectEvents();
loadPeak();
setInterval(loadPeak, 60000);
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
<link rel="icon" type="image/svg+xml" href="/favicon.svg">
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
  .header-nav { display: flex; gap: 14px; }
  .header-nav a { font-size: 13px; }
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
  <h1>Power events for <a class="flip-link" href="https://theflip.museum">The Flip</a></h1>
  {{NAV}}
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
{{JS_FORMAT}}

// buildEventRow comes from juice/web/events.js (inlined via the JS_EVENTS marker).
{{JS_EVENTS}}

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
  tbody.innerHTML = events.map(buildEventRow).join('');
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
    document.getElementById('rows').insertAdjacentHTML('beforeend', events.map(buildEventRow).join(''));
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
<link rel="icon" type="image/svg+xml" href="/favicon.svg">
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
  .header-nav { display: flex; gap: 14px; }
  .header-nav a { font-size: 13px; }
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
    scroll-margin-top: 16px;
  }
  .section-title:first-of-type { margin-top: 8px; }
  .anchor-link { color: inherit; text-decoration: none; }
  .anchor-link:hover { text-decoration: underline; }
  .anchor-link::after {
    content: "#"; color: #c7c7cc; margin-left: 6px; opacity: 0; font-weight: 500;
  }
  .anchor-link:hover::after { opacity: 1; }
  .section-sub { margin: -6px 0 10px; font-size: 12px; color: #86868b; }
  .busy-controls { display: flex; justify-content: flex-end; margin-bottom: 8px; }
  .seg { display: inline-flex; border: 1px solid #d2d2d7; border-radius: 7px; overflow: hidden; }
  .seg-btn { border: none; background: #fff; color: #1d1d1f; font-size: 12px; font-weight: 500; padding: 5px 12px; cursor: pointer; }
  .seg-btn + .seg-btn { border-left: 1px solid #d2d2d7; }
  .seg-btn.active { background: #007aff; color: #fff; }
  /* Strip peaks: a table with a modest bullet bar (theoretical = track,
     actual = inset bar, current = solid bar) + readable numeric columns. */
  .peak-table-wrap { overflow-x: auto; }
  .peak-table {
    width: 100%; border-collapse: collapse; font-size: 13px;
  }
  .peak-table th {
    text-align: right; padding: 4px 12px 8px; font-size: 11px; font-weight: 600;
    color: #86868b; text-transform: uppercase; letter-spacing: 0.5px;
    white-space: nowrap;
  }
  .peak-table th:first-child, .peak-table th.bar-col { text-align: left; }
  .peak-table td {
    padding: 8px 12px; border-top: 1px solid #f0f0f0; vertical-align: middle;
  }
  .peak-name {
    font-weight: 600; max-width: 220px;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
  }
  .peak-name a { color: #007aff; text-decoration: none; }
  .peak-name a:hover { text-decoration: underline; }
  .peak-table .bar-cell { width: 240px; }
  .peak-track {
    position: relative; height: 18px;
    background: #f2f2f7; border-radius: 4px; overflow: hidden;
  }
  .peak-bar-theoretical {
    position: absolute; inset: 0 auto 0 0; background: #e4ecf7; border-radius: 4px;
  }
  .peak-bar-actual {
    position: absolute; top: 3px; bottom: 3px; left: 0;
    background: #9ec2eb; border-radius: 3px;
  }
  .peak-bar-current {
    position: absolute; top: 6px; bottom: 6px; left: 0;
    background: #007aff; border-radius: 2px;
  }
  .peak-table td.peak-num {
    text-align: right; color: #86868b;
    font-variant-numeric: tabular-nums; white-space: nowrap;
  }
  .peak-table td.peak-num.now { color: #1d1d1f; font-weight: 600; }
  .peak-table td.peak-num.warn { color: #b86a00; font-weight: 600; }
  .peak-table td.peak-num.over { color: #ff3b30; font-weight: 700; }
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
  <h1>Usage — last 30 days for <a class="flip-link" href="https://theflip.museum">The Flip</a></h1>
  {{NAV}}
  {{AUTH_CORNER}}
</header>

<div class="wrap">
  <h2 class="section-title" id="energy"><a class="anchor-link" href="#energy">Energy</a></h2>
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

  <h2 class="section-title" id="play-hours"><a class="anchor-link" href="#play-hours">Play hours per day</a></h2>
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

  <h2 class="section-title" id="busy"><a class="anchor-link" href="#busy">When we're busy</a></h2>
  <div class="section-sub">Share of on-time spent in active play, by hour &mdash; last 28 days.</div>
  <div class="content" id="busy-section">
    <div class="busy-controls">
      <div class="seg">
        <button class="seg-btn active" id="busy-day">By day</button>
        <button class="seg-btn" id="busy-week">Avg week</button>
      </div>
    </div>
    <div class="chart-area" id="busy-chart-area">
      <svg id="busy-chart"></svg>
      <div id="busy-empty" class="empty" style="display:none">
        No play yet &mdash; needs calibrated machines with recorded play.
      </div>
    </div>
  </div>

  <h2 class="section-title private-only" id="strip-peaks"><a class="anchor-link" href="#strip-peaks">Strip peaks (30 days)</a></h2>
  <div class="content private-only" id="strip-peaks-section">
    <div class="chart-area">
      <div id="strip-peaks-rows"></div>
      <div id="strip-peaks-empty" class="empty" style="display:none">No strip peak data yet.</div>
    </div>
  </div>

  <h2 class="section-title private-only" id="circuit-peaks"><a class="anchor-link" href="#circuit-peaks">Circuit peaks (30 days)</a></h2>
  <div class="content private-only" id="circuit-peaks-section">
    <div class="chart-area">
      <div id="circuit-peaks-rows"></div>
      <div id="circuit-peaks-empty" class="empty" style="display:none">No circuits defined yet.</div>
    </div>
  </div>

  <div class="chart-tooltip" id="tooltip"></div>
</div>

<script>
const PUBLIC_MODE = {{PUBLIC_MODE}};

{{JS_FORMAT}}

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

// ---------------------------------------------------------------------------
// "When we're busy" bubble grid (play time / on-time, per date x hour)
// ---------------------------------------------------------------------------

const busyAreaEl = document.getElementById('busy-chart-area');
const busySvg = d3.select('#busy-chart');
const busyG = busySvg.append('g')
  .attr('transform', `translate(${margin.left},${margin.top})`);
const busyXScale = d3.scaleBand().paddingInner(0.1).paddingOuter(0.05);
const busyYScale = d3.scaleBand().paddingInner(0.1).paddingOuter(0.05);
const busyXAxisG = busyG.append('g').attr('class', 'axis');
const busyYAxisG = busyG.append('g').attr('class', 'axis');
const busyCellsG = busyG.append('g');

let lastBusyData = null;
let busyMode = 'day';  // 'day' | 'week'

const BUSY_WEEKDAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
// busyWeekdayIdx + busyWeekAggregate come from juice/web/usage.js (JS_USAGE marker).
{{JS_USAGE}}

// Circle colour shifts with usage: green up to 76%, then smoothly green→yellow
// →orange→full red at 100%.
const busyColor = d3.scaleLinear()
  .domain([0, 0.76, 0.84, 0.92, 1.0])
  .range(['#59a14f', '#59a14f', '#f2c80a', '#ff9500', '#ff3b30'])
  .interpolate(d3.interpolateRgb)
  .clamp(true);

function busyChartWidth() {
  return Math.max(280, busyAreaEl.clientWidth - 32);
}
function hourLabel(h) {
  return d3.timeFormat('%-I %p')(new Date(2000, 0, 1, h));
}

// Build the display view for the current mode. 'day' = one column per date;
// 'week' = 7 columns Mon–Sun, pooling play/on across each weekday's occurrences
// (a play-weighted average of busy-ness).
function busyView(data) {
  if (busyMode === 'week') {
    const { cells, hours } = busyWeekAggregate(data.cells);  // pure pooling (usage.js)
    return {
      cols: [0, 1, 2, 3, 4, 5, 6], cells, hours,
      max_ratio: d3.max(cells, c => c.ratio) || 1,
      thinTicks: false,
      colLabel: i => BUSY_WEEKDAYS[i],
      ttTime: c => `${BUSY_WEEKDAYS[c.col]}s, ${hourLabel(c.hour)}`,
    };
  }
  return {
    cols: data.dates,
    cells: data.cells.map(c => ({ ...c, col: c.date })),
    hours: data.hours,
    max_ratio: data.max_ratio || 1,
    thinTicks: true,
    colLabel: d => d3.timeFormat('%b %-d')(new Date(d + 'T00:00:00')),
    ttTime: c => `${d3.timeFormat('%a %b %-d')(new Date(c.date + 'T00:00:00'))}, ${hourLabel(c.hour)}`,
  };
}

function renderBusy(data) {
  lastBusyData = data;
  const emptyEl = document.getElementById('busy-empty');
  const view = busyView(data);
  if (!view.cells.length) {
    emptyEl.style.display = 'block';
    busySvg.style('display', 'none');
    return;
  }
  emptyEl.style.display = 'none';
  busySvg.style('display', 'block');

  const width = busyChartWidth();
  // Height grows with the number of active hour-rows so cells stay legible.
  const rowH = 30;
  const innerH = Math.max(rowH, view.hours.length * rowH);
  const height = innerH + margin.top + margin.bottom;
  const innerW = width - margin.left - margin.right;
  busySvg.attr('width', width).attr('height', height);

  busyXScale.range([0, innerW]).domain(view.cols);
  busyYScale.range([0, innerH]).domain(view.hours);  // earliest hour at top
  const rMax = Math.max(3, Math.min(busyXScale.bandwidth(), busyYScale.bandwidth()) / 2 - 2);
  // Area proportional to ratio, normalized so the busiest cell ≈ a full cell.
  const rScale = d3.scaleSqrt().domain([0, view.max_ratio || 1]).range([0, rMax]);

  const xAxis = d3.axisBottom(busyXScale).tickFormat(view.colLabel);
  if (view.thinTicks) {
    const targetTicks = Math.max(3, Math.min(10, Math.floor(innerW / 70)));
    const every = Math.max(1, Math.ceil(view.cols.length / targetTicks));
    xAxis.tickValues(view.cols.filter((_, i) => i % every === 0));
  }
  busyXAxisG.attr('transform', `translate(0,${innerH})`).call(xAxis);
  busyYAxisG.call(d3.axisLeft(busyYScale).tickFormat(hourLabel));

  const cx = c => busyXScale(c.col) + busyXScale.bandwidth() / 2;
  const cy = c => busyYScale(c.hour) + busyYScale.bandwidth() / 2;

  busyCellsG.selectAll('g.busy-cell').remove();
  const g = busyCellsG.selectAll('g.busy-cell')
    .data(view.cells)
    .enter().append('g').attr('class', 'busy-cell');
  // Transparent full-cell rect so the whole cell is hoverable, not just the dot.
  g.append('rect')
    .attr('x', c => busyXScale(c.col)).attr('y', c => busyYScale(c.hour))
    .attr('width', busyXScale.bandwidth()).attr('height', busyYScale.bandwidth())
    .attr('fill', 'transparent');
  g.append('circle')
    .attr('cx', cx).attr('cy', cy)
    .attr('r', c => rScale(c.ratio))
    .attr('fill', c => busyColor(c.ratio)).attr('fill-opacity', 0.9);

  // Tooltip per cell.
  g.on('mousemove', function(event, c) {
    const html = `<div class="tt-time">${escapeHtml(view.ttTime(c))}</div>`
      + `<div class="tt-row"><span>Busy</span><span>${Math.round(c.ratio * 100)}%</span></div>`
      + `<div class="tt-row"><span>Play</span><span>${c.play_hours.toFixed(2)} h</span></div>`
      + `<div class="tt-row"><span>On</span><span>${c.on_hours.toFixed(2)} h</span></div>`;
    tooltip.html(html).style('display', 'block')
      .style('left', (event.pageX + 12) + 'px')
      .style('top', (event.pageY + 12) + 'px');
  }).on('mouseleave', () => tooltip.style('display', 'none'));
}

function setBusyMode(mode) {
  busyMode = mode;
  document.getElementById('busy-day').classList.toggle('active', mode === 'day');
  document.getElementById('busy-week').classList.toggle('active', mode === 'week');
  if (lastBusyData) renderBusy(lastBusyData);
}
document.getElementById('busy-day').onclick = () => setBusyMode('day');
document.getElementById('busy-week').onclick = () => setBusyMode('week');

async function loadBusy() {
  const resp = await fetch('/api/busy-grid?days=28');
  const data = await resp.json();
  renderBusy(data);
}

let busyResizeRaf = 0;
const busyRo = new ResizeObserver(() => {
  if (busyResizeRaf) cancelAnimationFrame(busyResizeRaf);
  busyResizeRaf = requestAnimationFrame(() => {
    busyResizeRaf = 0;
    if (lastBusyData) renderBusy(lastBusyData);
  });
});
busyRo.observe(busyAreaEl);

loadBusy();

// ---- Strip peaks (operators only) -------------------------------------------

// buildStripPeaks/buildCircuitPeaks come from juice/web/peaks.js (JS_PEAKS).
{{JS_PEAKS}}

function renderStripPeaks(data) {
  const html = buildStripPeaks(data.strips);
  document.getElementById('strip-peaks-empty').style.display = html ? 'none' : 'block';
  document.getElementById('strip-peaks-rows').innerHTML = html;
}

async function loadStripPeaks() {
  try {
    const resp = await fetch('/api/strip-peaks?days=30');
    if (!resp.ok) return;
    renderStripPeaks(await resp.json());
  } catch (e) {
    // Transient failure — next refresh retries.
  }
}

function renderCircuitPeaks(data) {
  const html = buildCircuitPeaks(data.circuits || []);
  document.getElementById('circuit-peaks-empty').style.display = html ? 'none' : 'block';
  document.getElementById('circuit-peaks-rows').innerHTML = html;
}

async function loadCircuitPeaks() {
  try {
    const resp = await fetch('/api/circuit-peaks?days=30');
    if (!resp.ok) return;
    renderCircuitPeaks(await resp.json());
  } catch (e) {
    // Transient failure — next refresh retries.
  }
}

if (!PUBLIC_MODE) {
  // Operators only: the API 401s for anonymous viewers and CSS hides the
  // section, so skip the fetch entirely in public mode.
  loadStripPeaks();
  setInterval(loadStripPeaks, 60000);
  loadCircuitPeaks();
  setInterval(loadCircuitPeaks, 60000);
}
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
<link rel="icon" type="image/svg+xml" href="/favicon.svg">
<title>juice — strip</title>
<script src="https://cdn.jsdelivr.net/npm/d3@7"></script>
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
  .header-nav { display: flex; gap: 14px; }
  .header-nav a { font-size: 13px; }
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
  .circuit-line {
    margin: 16px 28px 0; font-size: 13px; color: #86868b;
    display: flex; align-items: center; gap: 8px; flex-wrap: wrap;
  }
  .circuit-line a { color: #007aff; text-decoration: none; font-weight: 600; }
  .circuit-line a:hover { text-decoration: underline; }
  .circuit-line select {
    font-size: 13px; padding: 4px 8px; border: 1px solid #d2d2d7; border-radius: 6px;
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
  .outlet-dot.no_draw { background: #ff9500; }
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
  .state-NO_DRAW { background: #ff9500; }
  .state-null { background: #aeaeb2; border: 1px dashed #c7c7cc; }
  .state-OFFLINE { background: #c7c7cc; }
  .tile-note { font-size: 11px; color: #b25e00; margin-top: 2px; }
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
  /* Usage card (chart styles mirror the /usage page — intentional duplication) */
  .usage-card {
    margin: 0 28px 20px; background: #fff; border: 1px solid #d2d2d7;
    border-radius: 10px; padding: 12px 16px; overflow: hidden;
  }
  .usage-header {
    display: flex; align-items: baseline; gap: 12px; margin-bottom: 8px;
  }
  .usage-title {
    font-size: 11px; font-weight: 600; color: #86868b;
    text-transform: uppercase; letter-spacing: 0.5px;
  }
  .usage-now {
    font-size: 22px; font-weight: 700; font-variant-numeric: tabular-nums;
  }
  .usage-now.offline { color: #86868b; }
  .usage-peak {
    font-size: 12px; color: #86868b; font-variant-numeric: tabular-nums;
  }
  .usage-total {
    margin-left: auto; font-size: 12px; color: #86868b;
    font-variant-numeric: tabular-nums;
  }
  #usage-chart { display: block; width: 100%; }
  .axis text { fill: #86868b; font-size: 11px; }
  .axis path, .axis line { stroke: #d2d2d7; }
  .grid line { stroke: #f0f0f0; }
  .grid path { stroke: none; }
  .chart-tooltip {
    position: absolute; pointer-events: none; background: rgba(255,255,255,0.97);
    border: 1px solid #d2d2d7; border-radius: 6px; padding: 8px 12px;
    font-size: 12px; display: none; box-shadow: 0 2px 8px rgba(0,0,0,0.1);
  }
  .chart-tooltip .tt-time { color: #86868b; margin-bottom: 2px; }
  .chart-tooltip .tt-kwh { font-weight: 600; font-variant-numeric: tabular-nums; }
</style>
</head>
<body class="{{BODY_CLASS}}">

<header>
  <h1 id="strip-title"><span id="strip-name">Loading...</span></h1>
  <span class="flip-suffix">
    for <a class="flip-link" href="https://theflip.museum">The Flip</a>
  </span>
  {{NAV}}
  {{AUTH_CORNER}}
</header>
<div class="offline-banner" id="offline-banner" hidden>
  Strip is OFFLINE — showing last-known outlet data.
</div>

<div class="circuit-line private-only" id="circuit-line"></div>

<div class="outlet-map private-only">
  <div class="outlet-map-header" id="outlet-map-header">Outlets</div>
  <div id="outlet-rows"><div class="no-data">Loading...</div></div>
</div>

<div class="usage-card private-only">
  <div class="usage-header">
    <span class="usage-title">Usage</span>
    <span class="usage-now"><span id="total-watts">&mdash;</span></span>
    <span class="usage-peak" id="usage-peak"></span>
    <span class="usage-total" id="usage-total"></span>
  </div>
  <svg id="usage-chart"></svg>
  <div id="usage-empty" class="no-data" style="display:none">No usage data yet.</div>
</div>
<div class="chart-tooltip" id="usage-tooltip"></div>

<div id="content">
  <div class="no-data">Loading...</div>
</div>

<script>
const PUBLIC_MODE = {{PUBLIC_MODE}};
const STATE_COLORS = {
  OFF: '#1d1d1f', ATTRACT: '#007aff', PLAYING: '#34c759', IDLE: '#f5c41a'
};
const deviceId = decodeURIComponent(location.pathname.split('/').pop());

{{JS_FORMAT}}

// showToast comes from juice/web/toast.js (inlined via the JS_TOAST marker).
{{JS_TOAST}}

// circuitLabel comes from juice/web/circuit.js (inlined via the JS_CIRCUIT marker).
{{JS_CIRCUIT}}

// buildStripHeader/buildOutletRows/buildCircuitLine come from juice/web/strip.js
// (inlined via the JS_STRIP marker).
{{JS_STRIP}}

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
  document.getElementById('strip-title').innerHTML = buildStripHeader(strip);
  document.title = 'juice — ' + (strip.display_name || strip.device_id);
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
  document.getElementById('outlet-map-header').textContent =
    'Outlets (' + strip.outlets.length + ')';
  document.getElementById('outlet-rows').innerHTML = buildOutletRows(strip);
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
            ${m.lock_mode ? `<span class="tile-lock" title="Locked ${m.lock_mode}">&#128274;</span>` : ''}
          </div>
          ${body}
        </a>`;
    } else {
      const noDraw = m.power_status === 'no_draw';
      const st = offline ? 'OFFLINE'
        : noDraw ? 'NO_DRAW'
        : (m.power_status === 'off' ? 'OFF' : (m.state || 'null'));
      const watts = m.power ? m.power.watts.toFixed(1) + 'W' : '--';
      const body = offline
        ? `<div class="tile-offline">OFFLINE</div>`
        : `<div class="sparkline-wrap"><canvas id="spark-${idx}"></canvas></div>
           <div class="tile-watts">${watts}</div>
           ${noDraw ? '<div class="tile-note" title="Outlet on — machine off, unplugged, or faulted">outlet on · no draw</div>' : ''}`;
      html += `
        <a class="tile${offline ? ' offline' : ''}" href="/machine/${plugId}">
          <div class="tile-top">
            <div class="state-dot state-${st}"></div>
            <div class="machine-name">${escapeHtml(m.name)}</div>
            ${m.lock_mode ? `<span class="tile-lock" title="Locked ${m.lock_mode}">&#128274;</span>` : ''}
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
    renderTotalWatts(stripData);
    renderTiles(mData.machines);
  } catch (e) {
    // Transient fetch failure — keep last render; next poll retries.
  }
}

function renderTotalWatts(strip) {
  const el = document.getElementById('total-watts');
  el.textContent = strip.total_watts != null ? strip.total_watts.toFixed(1) + ' W' : '\\u2014';
  // Stale last-known sum while the strip is dark — dim it.
  el.parentElement.classList.toggle('offline', !!strip.offline);
}

// -- Circuit assignment -------------------------------------------------------

let allCircuits = [];

function renderCircuit() {
  document.getElementById('circuit-line').innerHTML = buildCircuitLine(allCircuits, deviceId);
  document.getElementById('circuit-select').onchange = onCircuitChange;
}

async function onCircuitChange(ev) {
  const v = ev.target.value;
  if (v === '') return;
  if (v === 'new') return createAndAssign();
  const circuitId = v === 'none' ? null : parseInt(v, 10);
  await assignCircuit(circuitId);
}

async function assignCircuit(circuitId) {
  try {
    const resp = await fetch('/api/strips/' + encodeURIComponent(deviceId) + '/circuit', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({circuit_id: circuitId}),
    });
    if (!resp.ok) {
      const data = await resp.json().catch(() => ({}));
      showToast(data.error || 'Assignment failed', 'error'); return;
    }
    showToast(circuitId == null ? 'Unassigned' : 'Assigned to circuit', 'success');
    await loadCircuits();
  } catch (e) { showToast('Assignment failed', 'error'); }
}

async function createAndAssign() {
  const panel = prompt('Panel (e.g. P1):');
  if (!panel) { renderCircuit(); return; }
  const breaker = prompt('Breaker (e.g. B20):');
  if (!breaker) { renderCircuit(); return; }
  const ampsRaw = prompt('Breaker amps (optional, e.g. 20):', '');
  const payload = {
    panel: panel.trim(), breaker: breaker.trim(), description: '',
    amps: ampsRaw && ampsRaw.trim() !== '' ? Number(ampsRaw) : null,
  };
  try {
    const resp = await fetch('/api/circuits', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload),
    });
    const data = await resp.json();
    if (!resp.ok) { showToast(data.error || 'Create failed', 'error'); renderCircuit(); return; }
    await assignCircuit(data.circuit_id);
  } catch (e) { showToast('Create failed', 'error'); renderCircuit(); }
}

async function loadCircuits() {
  try {
    const resp = await fetch('/api/circuits');
    if (!resp.ok) return;
    allCircuits = (await resp.json()).circuits || [];
    renderCircuit();
  } catch (e) {}
}

// -- Usage history chart ------------------------------------------------------
// Single-series clone of the /usage page's energy chart (intentional
// duplication — pages are self-contained inline templates).

const usageMargin = { top: 12, right: 16, bottom: 32, left: 56 };
const usageCardEl = document.querySelector('.usage-card');
const usageSvg = d3.select('#usage-chart');
const usageG = usageSvg.append('g')
  .attr('transform', `translate(${usageMargin.left},${usageMargin.top})`);
const usageX = d3.scaleTime();
const usageY = d3.scaleLinear();
const usageXAxis = usageG.append('g').attr('class', 'axis');
const usageYAxis = usageG.append('g').attr('class', 'axis');
const usageGrid = usageG.append('g').attr('class', 'grid');
const usagePath = usageG.append('path')
  .attr('fill', '#007aff22').attr('stroke', '#007aff').attr('stroke-width', 1.5);
const usageHoverLine = usageG.append('line')
  .attr('stroke', '#aaa').attr('stroke-dasharray', '3,3')
  .style('display', 'none');
const usageTooltip = d3.select('#usage-tooltip');

let lastUsageData = null;

function usageChartWidth() {
  return Math.max(280, usageCardEl.clientWidth - 32);
}

function renderUsage(data) {
  lastUsageData = data;
  const empty = document.getElementById('usage-empty');
  document.getElementById('usage-total').textContent =
    data.total_kwh.toFixed(1) + ' kWh / 30 days';
  // Omit the theoretical suffix when unknown — a coerced "0.0 W" would
  // misread as a real measurement.
  document.getElementById('usage-peak').textContent =
    data.peak_watts_actual != null
      ? 'Peak ' + data.peak_watts_actual.toFixed(1) + ' W'
        + (data.peak_watts_theoretical != null
            ? ' \\u00b7 max possible ' + data.peak_watts_theoretical.toFixed(1) + ' W'
            : '')
      : '';
  if (!data.hours.length || data.total_kwh === 0) {
    empty.style.display = 'block';
    usageSvg.style('display', 'none');
    return;
  }
  empty.style.display = 'none';
  usageSvg.style('display', 'block');

  const width = usageChartWidth();
  const height = 180;
  const innerW = width - usageMargin.left - usageMargin.right;
  const innerH = height - usageMargin.top - usageMargin.bottom;
  usageSvg.attr('width', width).attr('height', height);
  usageX.range([0, innerW]);
  usageY.range([innerH, 0]);
  usageXAxis.attr('transform', `translate(0,${innerH})`);
  usageHoverLine.attr('y1', 0).attr('y2', innerH);

  const hours = data.hours.map(h => new Date(h));
  usageX.domain(d3.extent(hours));
  usageY.domain([0, d3.max(data.hourly_kwh) || 1]).nice();

  const xTicks = Math.max(3, Math.min(8, Math.floor(innerW / 80)));
  usageXAxis.call(d3.axisBottom(usageX).ticks(xTicks).tickFormat(d3.timeFormat('%b %-d')));
  usageYAxis.call(d3.axisLeft(usageY).ticks(4).tickFormat(d => d + ' kWh'));
  usageGrid.call(d3.axisLeft(usageY).ticks(4).tickSize(-innerW).tickFormat(''));

  const area = d3.area()
    .x((_, i) => usageX(hours[i]))
    .y0(usageY(0))
    .y1(d => usageY(d));
  usagePath.attr('d', area(data.hourly_kwh));

  usageSvg.on('mousemove', function(event) {
    const [mx] = d3.pointer(event, usageG.node());
    if (mx < 0 || mx > innerW) {
      usageHoverLine.style('display', 'none');
      usageTooltip.style('display', 'none');
      return;
    }
    const ts = usageX.invert(mx);
    const bisect = d3.bisector(d => d).left;
    let i = bisect(hours, ts);
    if (i >= hours.length) i = hours.length - 1;
    if (i > 0 && (ts - hours[i-1]) < (hours[i] - ts)) i--;
    const hov = hours[i];
    usageHoverLine.attr('x1', usageX(hov)).attr('x2', usageX(hov)).style('display', null);

    const fmt = d3.timeFormat('%a %b %-d, %-I %p');
    usageTooltip.html(
      `<div class="tt-time">${escapeHtml(fmt(hov))}</div>` +
      `<div class="tt-kwh">${(data.hourly_kwh[i] || 0).toFixed(3)} kWh</div>`
    ).style('display', 'block');
    const rect = document.getElementById('usage-chart').getBoundingClientRect();
    let left = rect.left + usageMargin.left + usageX(hov) + 14;
    const top = rect.top + usageMargin.top + 8 + window.scrollY;
    if (left + 180 > window.innerWidth) left = Math.max(8, left - 200);
    usageTooltip.style('left', left + 'px').style('top', top + 'px');
  }).on('mouseleave', () => {
    usageHoverLine.style('display', 'none');
    usageTooltip.style('display', 'none');
  });
}

async function loadUsage() {
  try {
    const resp = await fetch('/api/strips/' + encodeURIComponent(deviceId) + '/usage?days=30');
    if (!resp.ok) return;
    renderUsage(await resp.json());
  } catch (e) {
    // Transient failure — next refresh retries.
  }
}

let usageResizeRaf = 0;
const usageRo = new ResizeObserver(() => {
  if (usageResizeRaf) cancelAnimationFrame(usageResizeRaf);
  usageResizeRaf = requestAnimationFrame(() => {
    usageResizeRaf = 0;
    if (lastUsageData) renderUsage(lastUsageData);
  });
});
usageRo.observe(usageCardEl);

poll();
setInterval(poll, 2000);
loadUsage();
// Refresh at the rollup cadence (recorder refreshes hourly_usage every 60s) —
// no point hammering it from the 2s poll.
setInterval(loadUsage, 60000);
if (!PUBLIC_MODE) {
  loadCircuits();
  setInterval(loadCircuits, 30000);
}
</script>
</body>
</html>
"""


# Circuit page: editable breaker metadata, member strips, peak-vs-capacity
# headline, and a 30-day usage chart. Operators-only (middleware redirects
# anonymous viewers to /login).
CIRCUIT_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" type="image/svg+xml" href="/favicon.svg">
<title>juice — circuit</title>
<script src="https://cdn.jsdelivr.net/npm/d3@7"></script>
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
  .header-nav { display: flex; gap: 14px; }
  .header-nav a { font-size: 13px; }
  header h1 { font-size: 17px; font-weight: 600; display: flex; align-items: center; gap: 8px; }
  .flip-suffix { color: #86868b; font-weight: 500; flex: 1; }
  .flip-link { color: #007aff; text-decoration: none; }
  .flip-link:hover { text-decoration: underline; }
  .auth-corner { margin-left: auto; font-size: 13px; }
  .login-btn {
    padding: 6px 14px; border-radius: 6px; background: #007aff; color: #fff;
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
  .btn {
    padding: 6px 16px; border-radius: 6px; font-size: 13px; font-weight: 600;
    cursor: pointer; border: none; transition: opacity 0.15s;
  }
  .btn:hover { opacity: 0.85; }
  .btn-save { background: #007aff; color: #fff; }
  .btn-cancel { background: #f2f2f7; color: #1d1d1f; }
  .btn-danger { background: #ff3b30; color: #fff; }
  .card {
    margin: 20px 28px; background: #fff; border: 1px solid #d2d2d7;
    border-radius: 10px; padding: 16px;
  }
  .card-header {
    font-size: 11px; font-weight: 600; color: #86868b;
    text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 10px;
  }
  .edit-form { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
  .edit-form input {
    font-size: 14px; padding: 5px 8px; border: 1px solid #d2d2d7; border-radius: 6px;
  }
  .edit-form input.short { width: 70px; }
  .edit-form input.desc { width: 240px; }
  /* Headline numbers */
  .headline { display: flex; gap: 28px; flex-wrap: wrap; align-items: baseline; }
  .metric .num { font-size: 22px; font-weight: 700; font-variant-numeric: tabular-nums; }
  .metric .lbl { font-size: 11px; color: #86868b; text-transform: uppercase; letter-spacing: 0.5px; }
  .cap-wrap { margin-top: 12px; }
  .cap-track {
    position: relative; height: 22px; background: #f2f2f7; border-radius: 6px; overflow: hidden;
  }
  .cap-bar { position: absolute; inset: 0 auto 0 0; background: #007aff; border-radius: 6px; }
  .cap-bar.warn { background: #f5a623; }
  .cap-bar.over { background: #ff3b30; }
  .cap-text { font-size: 12px; color: #86868b; margin-top: 4px; font-variant-numeric: tabular-nums; }
  /* Members */
  .member-row {
    display: flex; align-items: center; gap: 10px;
    padding: 8px 0; border-top: 1px solid #f0f0f0; font-size: 14px;
  }
  .member-row:first-of-type { border-top: none; }
  .member-row a { color: #007aff; text-decoration: none; font-weight: 600; }
  .member-row a:hover { text-decoration: underline; }
  .member-row .spacer { flex: 1; }
  .add-strip { margin-top: 12px; display: flex; gap: 8px; align-items: center; }
  .add-strip select {
    font-size: 13px; padding: 5px 8px; border: 1px solid #d2d2d7; border-radius: 6px;
  }
  /* Usage chart (mirrors the strip page) */
  #usage-chart { display: block; width: 100%; }
  .axis text { fill: #86868b; font-size: 11px; }
  .axis path, .axis line { stroke: #d2d2d7; }
  .grid line { stroke: #f0f0f0; }
  .grid path { stroke: none; }
  .chart-tooltip {
    position: absolute; pointer-events: none; background: rgba(255,255,255,0.97);
    border: 1px solid #d2d2d7; border-radius: 6px; padding: 8px 12px;
    font-size: 12px; display: none; box-shadow: 0 2px 8px rgba(0,0,0,0.1);
  }
  .chart-tooltip .tt-time { color: #86868b; margin-bottom: 2px; }
  .chart-tooltip .tt-kwh { font-weight: 600; font-variant-numeric: tabular-nums; }
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
  <h1 id="circuit-title"><span id="circuit-name">Loading...</span></h1>
  <span class="flip-suffix">
    for <a class="flip-link" href="https://theflip.museum">The Flip</a>
  </span>
  {{NAV}}
  {{AUTH_CORNER}}
</header>

<div class="card">
  <div class="card-header">Load vs. breaker capacity</div>
  <div class="headline">
    <div class="metric"><div class="num" id="m-current">&mdash;</div><div class="lbl">Current</div></div>
    <div class="metric"><div class="num" id="m-peak">&mdash;</div><div class="lbl">Peak (30d)</div></div>
    <div class="metric"><div class="num" id="m-theo">&mdash;</div><div class="lbl">Max possible</div></div>
    <div class="metric"><div class="num" id="m-cap">&mdash;</div><div class="lbl">Breaker capacity</div></div>
  </div>
  <div class="cap-wrap">
    <div class="cap-track"><div class="cap-bar" id="cap-bar" style="width:0%"></div></div>
    <div class="cap-text">Peak as % of capacity: <span id="cap-pct">&mdash;</span></div>
  </div>
</div>

<div class="card private-only">
  <div class="card-header">Strips on this circuit</div>
  <div id="member-rows"><div class="no-data">Loading...</div></div>
  <div class="add-strip">
    <select id="add-strip-select"><option value="">Add a strip…</option></select>
  </div>
</div>

<div class="card">
  <div class="card-header" id="usage-card-header">Usage (30 days)</div>
  <svg id="usage-chart"></svg>
  <div id="usage-empty" class="no-data" style="display:none">No usage data yet.</div>
</div>
<div class="chart-tooltip" id="usage-tooltip"></div>

<script>
const PUBLIC_MODE = {{PUBLIC_MODE}};
const circuitId = parseInt(location.pathname.split('/').pop(), 10);

{{JS_FORMAT}}
function fmtW(v) { return v != null ? v.toFixed(1) + ' W' : '\\u2014'; }

// showToast comes from juice/web/toast.js (inlined via the JS_TOAST marker).
{{JS_TOAST}}

// circuitLabel comes from juice/web/circuit.js (inlined via the JS_CIRCUIT marker).
{{JS_CIRCUIT}}

// buildCircuitHeader/buildMemberRows/buildAddStripOptions come from
// juice/web/circuit_page.js (inlined via the JS_CIRCUIT_PAGE marker).
{{JS_CIRCUIT_PAGE}}

let circuit = null;       // row from /api/circuit-peaks
let members = [];         // [{device_id, display_name}]
let allStrips = [];       // [{device_id, display_name}] from /api/strip-peaks
let editing = false;

function renderHeader() {
  if (editing || !circuit) return;
  document.getElementById('circuit-title').innerHTML = buildCircuitHeader(circuit);
  document.title = 'juice — ' + circuitLabel(circuit);
}

function startEdit() {
  if (!circuit) return;
  editing = true;
  const title = document.getElementById('circuit-title');
  title.innerHTML = `
    <span class="edit-form">
      <input id="f-panel" class="short" placeholder="Panel" value="${escapeHtml(circuit.panel)}">
      <input id="f-breaker" class="short" placeholder="Breaker" value="${escapeHtml(circuit.breaker)}">
      <input id="f-amps" class="short" placeholder="Amps" value="${circuit.amps != null ? circuit.amps : ''}">
      <input id="f-desc" class="desc" placeholder="Description" value="${escapeHtml(circuit.description)}">
      <button class="btn btn-save" onclick="saveEdit()">Save</button>
      <button class="btn btn-cancel" onclick="cancelEdit()">Cancel</button>
      <button class="btn btn-danger" onclick="deleteCircuit()">Delete</button>
    </span>`;
}

function cancelEdit() { editing = false; renderHeader(); }

async function saveEdit() {
  const ampsRaw = document.getElementById('f-amps').value.trim();
  const payload = {
    panel: document.getElementById('f-panel').value.trim(),
    breaker: document.getElementById('f-breaker').value.trim(),
    description: document.getElementById('f-desc').value.trim(),
    amps: ampsRaw === '' ? null : Number(ampsRaw),
  };
  try {
    const resp = await fetch('/api/circuits/' + circuitId, {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload),
    });
    const data = await resp.json();
    if (!resp.ok) { showToast(data.error || 'Save failed', 'error'); return; }
    editing = false;
    showToast('Circuit updated', 'success');
    await load();
  } catch (e) { showToast('Save failed', 'error'); }
}

async function deleteCircuit() {
  if (!confirm('Delete this circuit? Its strips become unassigned.')) return;
  try {
    const resp = await fetch('/api/circuits/' + circuitId, { method: 'DELETE' });
    if (!resp.ok) {
      const data = await resp.json().catch(() => ({}));
      showToast(data.error || 'Delete failed', 'error'); return;
    }
    location.href = '/usage';
  } catch (e) { showToast('Delete failed', 'error'); }
}

function renderHeadline() {
  if (!circuit) return;
  document.getElementById('m-current').textContent = fmtW(circuit.current_watts);
  document.getElementById('m-peak').textContent = fmtW(circuit.peak_watts_actual);
  document.getElementById('m-theo').textContent = fmtW(circuit.peak_watts_theoretical);
  document.getElementById('m-cap').textContent =
    circuit.capacity_watts != null
      ? circuit.capacity_watts.toFixed(0) + ' W (' + circuit.amps + 'A)'
      : '\\u2014';
  const bar = document.getElementById('cap-bar');
  const pct = circuit.pct_of_capacity;
  bar.style.width = (pct != null ? Math.min(100, pct) : 0) + '%';
  bar.classList.toggle('over', pct != null && pct >= 80);
  bar.classList.toggle('warn', pct != null && pct >= 60 && pct < 80);
  document.getElementById('cap-pct').textContent =
    pct != null ? pct.toFixed(1) + '%' : '\\u2014';
}

function renderMembers() {
  document.getElementById('member-rows').innerHTML = buildMemberRows(members);
  const sel = document.getElementById('add-strip-select');
  sel.innerHTML = buildAddStripOptions(allStrips, members);
  sel.onchange = () => { if (sel.value) assignStrip(encodeURIComponent(sel.value), circuitId); };
}

async function assignStrip(encodedDevice, targetCircuit) {
  try {
    const resp = await fetch('/api/strips/' + encodedDevice + '/circuit', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({circuit_id: targetCircuit}),
    });
    if (!resp.ok) {
      const data = await resp.json().catch(() => ({}));
      showToast(data.error || 'Assignment failed', 'error'); return;
    }
    await load();
  } catch (e) { showToast('Assignment failed', 'error'); }
}

async function load() {
  try {
    const [pResp, cResp, sResp] = await Promise.all([
      fetch('/api/circuit-peaks?days=30'),
      fetch('/api/circuits'),
      fetch('/api/strip-peaks?days=30'),
    ]);
    const peaks = (await pResp.json()).circuits || [];
    circuit = peaks.find(c => c.circuit_id === circuitId);
    if (!circuit) {
      document.getElementById('circuit-name').textContent = 'Unknown circuit';
      return;
    }
    const cRow = ((await cResp.json()).circuits || []).find(c => c.circuit_id === circuitId);
    members = cRow
      ? cRow.device_ids.map((d, i) => ({device_id: d, display_name: cRow.display_names[i]}))
      : [];
    allStrips = ((await sResp.json()).strips || [])
      .map(s => ({device_id: s.device_id, display_name: s.display_name}));
    renderHeader();
    renderHeadline();
    renderMembers();
  } catch (e) {
    // Transient failure — next refresh retries.
  }
}

// -- Usage chart (clone of the strip page's single-series chart) --------------
const usageMargin = { top: 12, right: 16, bottom: 32, left: 56 };
const usageCardEl = document.getElementById('usage-chart').parentElement;
const usageSvg = d3.select('#usage-chart');
const usageG = usageSvg.append('g')
  .attr('transform', `translate(${usageMargin.left},${usageMargin.top})`);
const usageX = d3.scaleTime();
const usageY = d3.scaleLinear();
const usageXAxis = usageG.append('g').attr('class', 'axis');
const usageYAxis = usageG.append('g').attr('class', 'axis');
const usageGrid = usageG.append('g').attr('class', 'grid');
const usagePath = usageG.append('path')
  .attr('fill', '#007aff22').attr('stroke', '#007aff').attr('stroke-width', 1.5);
const usageHoverLine = usageG.append('line')
  .attr('stroke', '#aaa').attr('stroke-dasharray', '3,3').style('display', 'none');
const usageTooltip = d3.select('#usage-tooltip');
let lastUsageData = null;
function usageChartWidth() { return Math.max(280, usageCardEl.clientWidth - 32); }

function renderUsage(data) {
  lastUsageData = data;
  const empty = document.getElementById('usage-empty');
  document.getElementById('usage-card-header').textContent =
    'Usage (30 days) \\u00b7 ' + data.total_kwh.toFixed(1) + ' kWh';
  if (!data.hours.length || data.total_kwh === 0) {
    empty.style.display = 'block';
    usageSvg.style('display', 'none');
    return;
  }
  empty.style.display = 'none';
  usageSvg.style('display', 'block');
  const width = usageChartWidth();
  const height = 180;
  const innerW = width - usageMargin.left - usageMargin.right;
  const innerH = height - usageMargin.top - usageMargin.bottom;
  usageSvg.attr('width', width).attr('height', height);
  usageX.range([0, innerW]);
  usageY.range([innerH, 0]);
  usageXAxis.attr('transform', `translate(0,${innerH})`);
  usageHoverLine.attr('y1', 0).attr('y2', innerH);
  const hours = data.hours.map(h => new Date(h));
  usageX.domain(d3.extent(hours));
  usageY.domain([0, d3.max(data.hourly_kwh) || 1]).nice();
  const xTicks = Math.max(3, Math.min(8, Math.floor(innerW / 80)));
  usageXAxis.call(d3.axisBottom(usageX).ticks(xTicks).tickFormat(d3.timeFormat('%b %-d')));
  usageYAxis.call(d3.axisLeft(usageY).ticks(4).tickFormat(d => d + ' kWh'));
  usageGrid.call(d3.axisLeft(usageY).ticks(4).tickSize(-innerW).tickFormat(''));
  const area = d3.area().x((_, i) => usageX(hours[i])).y0(usageY(0)).y1(d => usageY(d));
  usagePath.attr('d', area(data.hourly_kwh));
  usageSvg.on('mousemove', function(event) {
    const [mx] = d3.pointer(event, usageG.node());
    if (mx < 0 || mx > innerW) {
      usageHoverLine.style('display', 'none');
      usageTooltip.style('display', 'none');
      return;
    }
    const ts = usageX.invert(mx);
    const bisect = d3.bisector(d => d).left;
    let i = bisect(hours, ts);
    if (i >= hours.length) i = hours.length - 1;
    if (i > 0 && (ts - hours[i-1]) < (hours[i] - ts)) i--;
    const hov = hours[i];
    usageHoverLine.attr('x1', usageX(hov)).attr('x2', usageX(hov)).style('display', null);
    const fmt = d3.timeFormat('%a %b %-d, %-I %p');
    usageTooltip.html(
      `<div class="tt-time">${escapeHtml(fmt(hov))}</div>` +
      `<div class="tt-kwh">${(data.hourly_kwh[i] || 0).toFixed(3)} kWh</div>`
    ).style('display', 'block');
    const rect = document.getElementById('usage-chart').getBoundingClientRect();
    let left = rect.left + usageMargin.left + usageX(hov) + 14;
    const top = rect.top + usageMargin.top + 8 + window.scrollY;
    if (left + 180 > window.innerWidth) left = Math.max(8, left - 200);
    usageTooltip.style('left', left + 'px').style('top', top + 'px');
  }).on('mouseleave', () => {
    usageHoverLine.style('display', 'none');
    usageTooltip.style('display', 'none');
  });
}

async function loadUsage() {
  try {
    const resp = await fetch('/api/circuits/' + circuitId + '/usage?days=30');
    if (!resp.ok) return;
    renderUsage(await resp.json());
  } catch (e) {}
}

let usageResizeRaf = 0;
const usageRo = new ResizeObserver(() => {
  if (usageResizeRaf) cancelAnimationFrame(usageResizeRaf);
  usageResizeRaf = requestAnimationFrame(() => {
    usageResizeRaf = 0;
    if (lastUsageData) renderUsage(lastUsageData);
  });
});
usageRo.observe(usageCardEl);

load();
setInterval(load, 5000);
loadUsage();
setInterval(loadUsage, 60000);
</script>
</body>
</html>
"""


AIR_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" type="image/svg+xml" href="/favicon.svg">
<title>juice — air quality</title>
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
  .header-nav { display: flex; gap: 14px; }
  .header-nav a { font-size: 13px; }
  header h1 { font-size: 17px; font-weight: 600; flex: 1; }
  .flip-link { color: #007aff; text-decoration: none; }
  .flip-link:hover { text-decoration: underline; }
  .auth-corner { margin-left: auto; font-size: 13px; }
  .login-btn { padding: 6px 14px; border-radius: 6px; background: #007aff; color: #fff;
    text-decoration: none; font-weight: 600; }
  .login-btn:hover { opacity: 0.85; }
  .user-pill { color: #86868b; }
  .user-pill a { color: #007aff; text-decoration: none; margin-left: 6px; }
  body.public .private-only { display: none !important; }
  .wrap { padding: 20px 28px; max-width: 1400px; margin: 0 auto; }
  .cards { display: grid; gap: 16px; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); }
  .card {
    background: #fff; border: 1px solid #d2d2d7; border-radius: 12px; padding: 16px 18px;
    cursor: pointer; transition: box-shadow 0.15s, opacity 0.15s;
  }
  .card:hover { box-shadow: 0 2px 12px rgba(0,0,0,0.08); }
  .card:focus-visible { outline: 2px solid #007aff; outline-offset: 2px; }
  /* Cards double as the chart's device toggles: included by default, dimmed when
     excluded. The swatch ties a card to the colour of its line in the charts. */
  .card.excluded { opacity: 0.45; }
  .card.excluded .card-swatch { background: #c7c7cc !important; }
  .card-head { display: flex; align-items: center; gap: 8px; margin-bottom: 14px; }
  .card-swatch { width: 12px; height: 12px; border-radius: 3px; flex-shrink: 0; }
  .card-name { font-size: 15px; font-weight: 600; flex: 1; overflow: hidden;
    text-overflow: ellipsis; white-space: nowrap; }
  .badge { font-size: 11px; font-weight: 600; padding: 2px 8px; border-radius: 10px;
    text-transform: uppercase; letter-spacing: 0.3px; }
  .badge.online { background: #e9f9ee; color: #248a3d; }
  .badge.offline { background: #f2f2f7; color: #86868b; }
  .metrics { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 12px 16px; }
  .metric .label { font-size: 11px; color: #86868b; text-transform: uppercase;
    letter-spacing: 0.4px; }
  .metric .value { font-size: 20px; font-weight: 600; font-variant-numeric: tabular-nums;
    line-height: 1.2; }
  .metric .value .unit { font-size: 12px; font-weight: 500; color: #86868b; margin-left: 2px; }
  .value.good { color: #248a3d; }
  .value.warn { color: #b86a00; }
  .value.bad  { color: #ff3b30; }
  .secondary { margin-top: 12px; padding-top: 10px; border-top: 1px solid #f0f0f0;
    display: flex; flex-wrap: wrap; gap: 6px 14px; font-size: 12px; color: #86868b;
    font-variant-numeric: tabular-nums; }
  .stale { color: #b86a00; font-size: 11px; margin-top: 8px; }
  .empty { padding: 60px 20px; text-align: center; color: #86868b; font-size: 14px; }
  .chart-section { margin-top: 28px; background: #fff; border: 1px solid #d2d2d7;
    border-radius: 12px; padding: 16px 18px; }
  .controls-row { display: flex; align-items: baseline; gap: 12px; flex-wrap: wrap;
    margin-bottom: 6px; }
  .controls-label { font-size: 12px; font-weight: 600; color: #86868b;
    text-transform: uppercase; letter-spacing: 0.4px; }
  .hint { font-size: 12px; color: #86868b; margin: 2px 0 14px; }
  .chips { display: flex; flex-wrap: wrap; gap: 8px; }
  .chip { border: 1px solid #d2d2d7; background: #fff; color: #1d1d1f; font-size: 13px;
    font-weight: 500; padding: 6px 13px; border-radius: 16px; cursor: pointer; }
  .chip:hover { border-color: #b0b0b5; }
  .chip:focus-visible { outline: 2px solid #007aff; outline-offset: 2px; }
  .chip.active { background: #007aff; border-color: #007aff; color: #fff; }
  .legend { display: flex; flex-wrap: wrap; gap: 8px 16px; margin: 14px 0 4px; }
  .legend .item { display: flex; align-items: center; gap: 6px; font-size: 12px; color: #1d1d1f; }
  .legend .swatch { width: 12px; height: 12px; border-radius: 3px; flex-shrink: 0; }
  .panel { margin-top: 14px; scroll-margin-top: 16px; }
  .panel-title { font-size: 13px; font-weight: 600; color: #1d1d1f; margin-bottom: 2px; }
  .panel-title .unit { color: #86868b; font-weight: 500; }
  .anchor-link { color: inherit; text-decoration: none; }
  .anchor-link:hover { text-decoration: underline; }
  .anchor-link::after {
    content: "#"; color: #c7c7cc; margin-left: 6px; opacity: 0; font-weight: 500;
  }
  .anchor-link:hover::after { opacity: 1; }
  svg { display: block; width: 100%; }
  .axis text { fill: #86868b; font-size: 11px; }
  .axis path, .axis line { stroke: #d2d2d7; }
  .grid line { stroke: #f0f0f0; }
  .grid path { stroke: none; }
  /* A thin rule at each local midnight, so the eye can read day boundaries. */
  .midnight { stroke: #e2e2e7; stroke-width: 1; shape-rendering: crispEdges; }
  /* Light backdrop over the hours The Flip is closed. */
  .closed { fill: #eeeef1; }
  .overlay { cursor: crosshair; }
  .crosshair-line { stroke: #86868b; stroke-width: 1; stroke-dasharray: 3,3; }
  .chart-tooltip { position: fixed; pointer-events: none; z-index: 20;
    background: rgba(255,255,255,0.97); border: 1px solid #d2d2d7; border-radius: 6px;
    padding: 8px 10px; font-size: 12px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);
    display: none; min-width: 170px; max-width: 280px; }
  .chart-tooltip .tt-time { color: #86868b; margin-bottom: 4px; }
  .chart-tooltip .tt-row { display: flex; align-items: center; gap: 6px;
    font-variant-numeric: tabular-nums; }
  .chart-tooltip .sw { width: 8px; height: 8px; border-radius: 2px; flex-shrink: 0; }
  .chart-tooltip .nm { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .chart-tooltip .vv { font-weight: 600; }
  .chart-empty { padding: 50px 20px; text-align: center; color: #86868b; font-size: 13px; }
</style>
</head>
<body class="{{BODY_CLASS}}">

<header>
  <h1>Air quality &mdash; <a class="flip-link" href="https://theflip.museum">The Flip</a></h1>
  {{NAV}}
  {{AUTH_CORNER}}
</header>

<div class="wrap">
  <div class="cards" id="cards"></div>
  <div id="empty" class="empty" style="display:none">No air monitors reporting yet.</div>

  <div class="chart-section" id="chart-section" style="display:none">
    <div class="controls-row">
      <span class="controls-label">Readings</span>
      <div class="chips" id="metric-chips"></div>
    </div>
    <div class="controls-row">
      <span class="controls-label">Range</span>
      <div class="chips" id="range-chips"></div>
    </div>
    <div class="hint">Pick one or more readings to chart. Tap a sensor card above to
      include or exclude it &mdash; each panel overlays a line per sensor.</div>
    <div class="legend" id="legend"></div>
    <div id="panels"></div>
  </div>
</div>
<div class="chart-tooltip" id="chart-tooltip"></div>

<script>
const METRICS = {
  co2:         { label: 'CO\\u2082',     unit: 'ppm',  primary: true,
                 bands: [[800,'good'],[1200,'warn'],[Infinity,'bad']] },
  pm25:        { label: 'PM2.5',         unit: '\\u00b5g/m\\u00b3', primary: true,
                 bands: [[12,'good'],[35,'warn'],[Infinity,'bad']] },
  pm10:        { label: 'PM10',          unit: '\\u00b5g/m\\u00b3', primary: true,
                 bands: [[54,'good'],[154,'warn'],[Infinity,'bad']] },
  noise:       { label: 'Noise',         unit: 'dB', primary: true,
                 bands: [[55,'good'],[70,'warn'],[Infinity,'bad']] },
  temperature: { label: 'Temp',          unit: '\\u00b0F', primary: true, decimals: 1 },
  humidity:    { label: 'Humidity',      unit: '%',    primary: true },
  tvoc:        { label: 'TVOC',          unit: 'ppb' },
  battery:     { label: 'Battery',       unit: '%' },
};
// Order of the big tiles on each card, the chartable readings (the chips), and
// the chart panels.
const PRIMARY = ['noise','temperature','humidity','co2','pm25','pm10'];
// Front and Back are two parts of the same space -> related cool colours
// (green/blue); the Workshop is a separate space -> orange. Any other sensor
// falls back to PALETTE by position. The card swatch + legend reuse colorFor().
const ROLE_COLORS = { front: '#34c759', back: '#007aff', workshop: '#ff9500' };
const PALETTE = ['#af52de','#ff2d55','#5ac8fa','#ffcc00','#30b0c7','#8e8e93'];
// Selectable history windows (the range control).
const RANGES = [{label:'1D',days:1},{label:'2D',days:2},{label:'7D',days:7},
                {label:'14D',days:14},{label:'30D',days:30}];

let SENSORS = [];
let HISTORY = {};                       // mac -> [{t, ...metrics}]
const selectedMetrics = new Set(PRIMARY);  // all readings charted by default
const selectedDevices = new Set();      // macs charted; cards toggle membership
let devicesInitialized = false;         // include all on first load only
let rangeDays = 7;                      // current history window
let rangeFrom = null, rangeTo = null;   // its [from, to) as Dates; the chart x-domain
let historyReqSeq = 0;                  // guards against out-of-order history responses
let historyAbort = null;                // aborts the in-flight fetch when a newer one starts

// Sensor ordering, the closed-hours backdrop (closedIntervals), and the chip/
// legend builders (buildMetricChips/buildRangeChips/buildLegend) come from
// juice/web/air.js, inlined via the JS_AIR marker.
{{JS_AIR}}

function colorFor(mac) {
  const s = SENSORS.find(x => x.mac === mac);
  const role = roleOf(s);
  if (role) return ROLE_COLORS[role];           // front=green, back=blue, workshop=orange
  const i = SENSORS.findIndex(x => x.mac === mac);
  return PALETTE[(i < 0 ? 0 : i) % PALETTE.length];
}
// API stores temperature in °C; the dashboard shows °F. Convert on ingest so
// cards, charts, axes, and tooltips all read Fahrenheit.
function cToF(c) { return (c === null || c === undefined) ? c : c * 9 / 5 + 32; }
function sensorName(mac) {
  const s = SENSORS.find(x => x.mac === mac);
  return s ? (s.name || s.mac) : mac;
}
function bandClass(metric, v) {
  const m = METRICS[metric];
  if (!m || !m.bands || v === null || v === undefined) return '';
  for (const [hi, cls] of m.bands) { if (v < hi) return cls; }
  return '';
}
// fmt + staleLabel + buildSensorCards come from juice/web/air.js (JS_AIR marker).
{{JS_FORMAT}}

function renderCards() {
  const el = document.getElementById('cards');
  const empty = document.getElementById('empty');
  if (!SENSORS.length) { el.innerHTML = ''; empty.style.display = 'block'; return; }
  empty.style.display = 'none';
  el.innerHTML = buildSensorCards(SENSORS, {
    primary: PRIMARY, metrics: METRICS, selectedDevices, colorFor, bandClass,
  });
  el.querySelectorAll('.card').forEach(c => {
    const activate = () => toggleDevice(c.dataset.mac);
    c.addEventListener('click', activate);
    c.addEventListener('keydown', e => {
      if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); activate(); }
    });
  });
}

function renderMetricChips() {
  const el = document.getElementById('metric-chips');
  el.innerHTML = buildMetricChips(PRIMARY, selectedMetrics, METRICS);  // air.js
  el.querySelectorAll('.chip').forEach(b =>
    b.addEventListener('click', () => toggleMetric(b.dataset.metric)));
}

function toggleDevice(mac) {
  if (selectedDevices.has(mac)) selectedDevices.delete(mac); else selectedDevices.add(mac);
  renderCards();
  renderCharts();
}
function toggleMetric(m) {
  if (selectedMetrics.has(m)) selectedMetrics.delete(m); else selectedMetrics.add(m);
  renderMetricChips();
  renderCharts();
}

function renderRangeChips() {
  const el = document.getElementById('range-chips');
  el.innerHTML = buildRangeChips(RANGES, rangeDays);  // air.js
  el.querySelectorAll('.chip').forEach(b =>
    b.addEventListener('click', () => setRange(+b.dataset.days)));
}
function setRange(days) {
  if (days === rangeDays) return;
  rangeDays = days;
  renderRangeChips();
  loadHistories();  // refetch the new window, then re-render
}

async function loadSensors() {
  const data = await (await fetch('/api/air')).json();
  SENSORS = orderSensors(data.sensors || [])    // front, back, workshop, then the rest
    .map(s => ({ ...s, temperature: cToF(s.temperature) }));
  const macs = new Set(SENSORS.map(s => s.mac));
  if (!devicesInitialized && SENSORS.length) {
    SENSORS.forEach(s => selectedDevices.add(s.mac));
    devicesInitialized = true;
  }
  [...selectedDevices].forEach(m => { if (!macs.has(m)) selectedDevices.delete(m); });
  renderCards();
  renderMetricChips();
  renderRangeChips();
  const empty = document.getElementById('empty');
  const section = document.getElementById('chart-section');
  if (!SENSORS.length) { empty.style.display = 'block'; section.style.display = 'none'; return; }
  empty.style.display = 'none';
  section.style.display = 'block';
  await loadHistories();
}

async function loadHistories() {
  // Guard against out-of-order responses: a quick range switch (or the 60s
  // auto-refresh interleaving) can leave a slower earlier request resolving
  // last and repainting with the wrong window. Tag each call and abort the
  // previous one; only the newest call applies its result.
  const seq = ++historyReqSeq;
  if (historyAbort) historyAbort.abort();
  historyAbort = new AbortController();
  const signal = historyAbort.signal;

  // Pin the window now so the chart x-domain matches the requested range exactly
  // (even where data is sparse), and every panel shares it.
  rangeTo = new Date();
  rangeFrom = new Date(rangeTo.getTime() - rangeDays * 86400000);
  const qs = `?from=${encodeURIComponent(rangeFrom.toISOString())}`
    + `&to=${encodeURIComponent(rangeTo.toISOString())}`;
  const macs = SENSORS.filter(s => selectedDevices.has(s.mac)).map(s => s.mac);
  const datas = await Promise.all(macs.map(m =>
    fetch(`/api/air/${encodeURIComponent(m)}/history${qs}`, { signal })
      .then(r => r.json()).catch(() => ({readings: []}))));
  if (seq !== historyReqSeq) return;  // a newer request superseded this one
  HISTORY = {};
  datas.forEach((d, i) => {
    HISTORY[macs[i]] = (d.readings || []).map(x =>
      ({ ...x, temperature: cToF(x.temperature), t: new Date(x.ts) }));
  });
  renderCharts();
}

function renderLegend(devices) {
  document.getElementById('legend').innerHTML = buildLegend(devices, colorFor);  // air.js
}

function renderCharts() {
  const panelsEl = document.getElementById('panels');
  const metrics = PRIMARY.filter(m => selectedMetrics.has(m));
  const devices = SENSORS.filter(s => selectedDevices.has(s.mac));
  renderLegend(devices);
  if (!devices.length || !metrics.length) {
    panelsEl.innerHTML = `<div class="chart-empty">`
      + (!devices.length ? 'Select at least one sensor (tap a card above).'
                         : 'Select at least one reading.')
      + `</div>`;
    return;
  }
  // Shared time domain = the selected window, so panels line up and the axis
  // always spans the chosen range even where data is sparse.
  const xExtent = (rangeFrom && rangeTo) ? [rangeFrom, rangeTo]
    : [new Date(Date.now() - rangeDays * 86400000), new Date()];

  panelsEl.innerHTML = metrics.map(m =>
    `<div class="panel" id="air-${m}"><div class="panel-title">`
    + `<a class="anchor-link" href="#air-${m}">${METRICS[m].label} `
    + `<span class="unit">${METRICS[m].unit}</span></a></div>`
    + `<svg data-metric="${m}"></svg></div>`).join('');

  metrics.forEach((m, i) => drawPanel(m, xExtent, devices, i === metrics.length - 1));
}

function drawPanel(metric, xExtent, devices, showXAxis) {
  const svg = d3.select(`#panels svg[data-metric="${metric}"]`);
  svg.selectAll('*').remove();
  const W = svg.node().clientWidth || 800;
  const ih = 170;
  const margin = { top: 8, right: 16, bottom: showXAxis ? 34 : 12, left: 48 };
  svg.attr('height', ih + margin.top + margin.bottom);
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);
  const iw = W - margin.left - margin.right;

  const x = d3.scaleTime().domain(xExtent).range([0, iw]);
  const series = devices.map(s => ({
    mac: s.mac, color: colorFor(s.mac),
    pts: (HISTORY[s.mac] || []).map(d => ({ t: d.t, v: d[metric] }))
                               .filter(d => d.v !== null && d.v !== undefined),
  })).filter(se => se.pts.length);

  const allV = series.flatMap(se => se.pts.map(p => p.v));
  if (!allV.length) {
    g.append('text').attr('x', iw / 2).attr('y', ih / 2).attr('text-anchor', 'middle')
      .attr('fill', '#86868b').attr('font-size', '12px').text('No data for this reading');
    return;
  }
  const lo = d3.min(allV), hi = d3.max(allV), pad = (hi - lo) * 0.1 || 1;
  const y = d3.scaleLinear().domain([lo - pad, hi + pad]).nice().range([ih, 0]);

  // Closed-hours backdrop (drawn first, so it sits behind grid + lines).
  g.append('g').selectAll('rect.closed')
    .data(closedIntervals(xExtent[0], xExtent[1])).join('rect')
    .attr('class', 'closed')
    .attr('x', d => x(d[0])).attr('width', d => Math.max(0, x(d[1]) - x(d[0])))
    .attr('y', 0).attr('height', ih);

  g.append('g').attr('class', 'grid').call(d3.axisLeft(y).tickSize(-iw).tickFormat(''));
  g.append('g').attr('class', 'axis').call(d3.axisLeft(y).ticks(5));

  // Thin rule at each local midnight in the window.
  const mids = d3.timeDay.range(d3.timeDay.ceil(xExtent[0]), xExtent[1]);
  g.append('g').selectAll('line.midnight').data(mids).join('line')
    .attr('class', 'midnight')
    .attr('x1', d => x(d)).attr('x2', d => x(d)).attr('y1', 0).attr('y2', ih);

  if (showXAxis) {
    const ax = d3.axisBottom(x);
    const spanDays = (xExtent[1] - xExtent[0]) / 86400000;
    if (spanDays <= 2.5) {
      // Short windows read by time of day; let d3 pick hour ticks + format.
      ax.ticks(6);
    } else {
      // Longer windows: label midnights by date, thinned to ~8 so they don't crowd.
      const every = Math.ceil(mids.length / 8) || 1;
      ax.tickValues(mids.filter((_, i) => i % every === 0)).tickFormat(d3.timeFormat('%b %e'));
    }
    g.append('g').attr('class', 'axis').attr('transform', `translate(0,${ih})`).call(ax);
  }

  const line = d3.line().x(d => x(d.t)).y(d => y(d.v)).curve(d3.curveMonotoneX);
  series.forEach(se => g.append('path').datum(se.pts).attr('fill', 'none')
    .attr('stroke', se.color).attr('stroke-width', 1.8).attr('d', line));

  // Hover: a crosshair at the nearest reading + a tooltip listing each sensor's
  // value at that time. The overlay is added last so it captures the pointer.
  const tooltip = d3.select('#chart-tooltip');
  const bisect = d3.bisector(d => d.t).left;
  const TOL = 30 * 60 * 1000;  // only show a sensor's value if it has a reading within 30 min
  const fmtT = d3.timeFormat('%a %b %e, %H:%M');
  const focus = g.append('g').style('display', 'none');
  focus.append('line').attr('class', 'crosshair-line').attr('y1', 0).attr('y2', ih);

  function nearest(pts, t) {
    const i = bisect(pts, t);
    let best = null;
    [pts[i - 1], pts[i]].filter(Boolean).forEach(p => {
      const dd = Math.abs(p.t - t);
      if (!best || dd < best.dd) best = { dd, p };
    });
    return best;
  }

  g.append('rect').attr('class', 'overlay').attr('width', iw).attr('height', ih)
    .style('fill', 'none').style('pointer-events', 'all')
    .on('pointermove', (event) => {
      const t0 = x.invert(d3.pointer(event)[0]);
      let snap = null;
      series.forEach(se => {
        const n = nearest(se.pts, t0);
        if (n && (!snap || n.dd < snap.dd)) snap = { dd: n.dd, t: n.p.t };
      });
      if (!snap) return;
      const tt = snap.t;
      focus.style('display', null);
      focus.select('.crosshair-line').attr('x1', x(tt)).attr('x2', x(tt));
      const rows = series.map(se => {
        const n = nearest(se.pts, tt);
        return { se, p: (n && n.dd <= TOL) ? n.p : null };
      });
      // List sensors top-to-bottom in the same order they stack on the chart at
      // this x (highest value = topmost line); sensors without a nearby reading
      // sink to the bottom.
      rows.sort((a, b) => {
        if (a.p && b.p) return b.p.v - a.p.v;
        return (a.p ? 0 : 1) - (b.p ? 0 : 1);
      });
      focus.selectAll('circle').data(rows.filter(r => r.p)).join('circle')
        .attr('r', 3.5).attr('fill', d => d.se.color)
        .attr('cx', d => x(d.p.t)).attr('cy', d => y(d.p.v));
      tooltip.html(`<div class="tt-time">${fmtT(tt)}</div>` + rows.map(r =>
        `<div class="tt-row"><span class="sw" style="background:${r.se.color}"></span>`
        + `<span class="nm">${escapeHtml(sensorName(r.se.mac))}</span>`
        + `<span class="vv">${r.p ? fmt(r.p.v, METRICS[metric].decimals) + ' ' + METRICS[metric].unit : '\\u2014'}</span></div>`
      ).join(''));
      tooltip.style('display', 'block')
        .style('left', (event.clientX + 14) + 'px').style('top', (event.clientY + 14) + 'px');
    })
    .on('pointerleave', () => { focus.style('display', 'none'); tooltip.style('display', 'none'); });
}

loadSensors();
setInterval(loadSensors, 60000);  // air changes slowly; a 1-min refresh is plenty
window.addEventListener('resize', renderCharts);
</script>
</body>
</html>
"""
