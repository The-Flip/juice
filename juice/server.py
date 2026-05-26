"""HTTP server with API and web dashboard for juice."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

from aiohttp import web

from juice.collector import Plug, PlugReading, _SelfPlug, call_with_retry
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
    plug_objects: dict[int, Controllable] = field(
        default_factory=dict
    )  # plug_id -> Plug or _SelfPlug (for control)
    plug_has_emeter: dict[int, bool] = field(default_factory=dict)  # plug_id -> has_emeter
    force_poll: set[int] = field(default_factory=set)  # plug IDs to poll immediately
    current_operation: Operation | None = None
    event_subscribers: set[asyncio.Queue] = field(default_factory=set)


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
    state: RecorderState = request.app["recorder_state"]

    machines = []
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

        plug_data = None
        if plug_info:
            device_id, child_id, alias = plug_info
            plug_data = {
                "plug_id": plug_id,
                "device_id": device_id,
                "child_id": child_id,
                "alias": alias,
            }

        strip_device_id = plug_info[0] if plug_info else ""
        strip_alias = state.strip_aliases.get(strip_device_id, "")

        machines.append(
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
            }
        )

    machines.sort(key=lambda m: (m["strip_device_id"], m["plug"]["plug_id"] if m["plug"] else 0))
    return web.json_response({"machines": machines})


async def handle_outlets(request: web.Request) -> web.Response:
    """List unassigned no-emeter outlets (e.g. EP10s with no machine tag)."""
    state: RecorderState = request.app["recorder_state"]
    store: Store = request.app["store"]

    outlets = []
    for plug_id, device_id, alias, _is_on_db in store.list_unassigned_outlets():
        # Prefer the live reading's on/off state if the recorder has one.
        reading = state.plug_readings.get(plug_id)
        is_on = reading.is_on if reading is not None else _is_on_db
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


def _build_targets(state: RecorderState, kind: str) -> list[int]:
    """Plug IDs to act on for an all-on / all-off, sorted by year ascending.

    Mirrors the client-side filter the dashboard used to apply:
    - Skip plugs already in the desired state.
    - When turning off, skip PLAYING machines (don't interrupt a game).
    - With no live reading yet, leave the plug alone on all-off (we can't be sure
      it's on) but include it on all-on (so it's brought up to the desired state).
    """
    on = kind == "all_on"
    ranked: list[tuple[int, int]] = []  # (year_key, plug_id)
    for plug_id, (_name, _asset_id, year) in state.assignments.items():
        reading = state.plug_readings.get(plug_id)
        has_emeter = state.plug_has_emeter.get(plug_id, True)
        if reading is None:
            is_on = False
        elif has_emeter:
            is_on = (reading.watts or 0.0) > 0
        else:
            is_on = bool(reading.is_on)

        if on and is_on:
            continue
        if not on and not is_on:
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
    return [pid for _, pid in ranked]


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
        machine_name = machine[0] if machine else None
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

    targets = _build_targets(state, kind)
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


async def handle_dashboard(request: web.Request) -> web.Response:
    return web.Response(text=DASHBOARD_HTML, content_type="text/html")


async def handle_machine_detail(request: web.Request) -> web.Response:
    return web.Response(text=DETAIL_HTML, content_type="text/html")


async def handle_events_page(request: web.Request) -> web.Response:
    return web.Response(text=EVENTS_HTML, content_type="text/html")


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
    app.router.add_post("/api/plugs/{plug_id}/power", handle_power)
    app.router.add_post("/api/operations/all-on", handle_all_on)
    app.router.add_post("/api/operations/all-off", handle_all_off)
    app.router.add_post("/api/operations/{id}/cancel", handle_cancel_operation)
    app.router.add_get("/api/operations/current", handle_current_operation)
    app.router.add_get("/api/events", handle_events)
    app.router.add_get("/api/power-events", handle_power_events)
    app.router.add_get("/events", handle_events_page)
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
    font-size: 12px;
    font-weight: 600;
    color: #86868b;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 8px;
  }
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
<body>
<header>
  <h1><span>juice</span> &mdash; machine status</h1>
  <div class="power-btns">
    <button class="power-btn power-btn-on" id="btn-all-on" onclick="startOperation('all-on')">All On</button>
    <button class="power-btn power-btn-off" id="btn-all-off" onclick="startOperation('all-off')">All Off</button>
  </div>
</header>
<div id="op-banner" class="op-banner" hidden>
  <div class="op-banner-text" id="op-banner-text"></div>
  <button class="op-banner-cancel" id="op-banner-cancel" onclick="cancelOperation()">Cancel</button>
</div>
<div id="content">
  <div class="no-data">Connecting...</div>
</div>
<div id="recent-events" class="recent-events" hidden>
  <div class="recent-events-header">
    <span>Recent power events</span>
    <a href="/events">View full log &rarr;</a>
  </div>
  <ul id="recent-events-list"></ul>
</div>
<script>
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
      const group = { alias: m.strip_alias || 'Unknown Strip', machines: [] };
      stripMap.set(key, group);
      strips.push(group);
    }
    stripMap.get(key).machines.push(m);
  }

  let html = '';
  let idx = 0;
  for (const strip of strips) {
    html += `<div class="strip-row"><div class="strip-label">${escapeHtml(strip.alias)}</div><div class="tiles">`;
    for (const m of strip.machines) {
      const plugId = m.plug ? m.plug.plug_id : 0;
      if (m.has_emeter === false) {
        // Simplified tile for no-emeter machines (e.g. EP10-backed).
        const isOn = !!m.is_on;
        html += `
          <a class="tile" href="/machine/${plugId}">
            <div class="tile-top">
              <div class="state-dot state-${isOn ? 'PLAYING' : 'OFF'}"></div>
              <div class="machine-name">${escapeHtml(m.name)}</div>
            </div>
            <div class="tile-onoff ${isOn ? 'on' : 'off'}">${isOn ? 'ON' : 'OFF'}</div>
            <button class="tile-toggle ${isOn ? 'off' : 'on'}"
              onclick="togglePlug(event, ${plugId}, ${isOn ? 'false' : 'true'})">
              ${isOn ? 'Turn Off' : 'Turn On'}
            </button>
          </a>`;
      } else {
        const st = m.state || 'null';
        const watts = m.power ? m.power.watts.toFixed(1) + 'W' : '--';
        html += `
          <a class="tile" href="/machine/${plugId}">
            <div class="tile-top">
              <div class="state-dot state-${st}"></div>
              <div class="machine-name">${escapeHtml(m.name)}</div>
            </div>
            <div class="sparkline-wrap"><canvas id="spark-${idx}"></canvas></div>
            <div class="tile-watts">${watts}</div>
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
      if (m.has_emeter !== false) {
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
    await fetch('/api/plugs/' + plugId + '/power', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({on})
    });
  } catch (e) {}
  poll();
}

async function poll() {
  try {
    const [mResp, oResp] = await Promise.all([
      fetch('/api/machines'),
      fetch('/api/outlets'),
    ]);
    const mData = await mResp.json();
    const oData = await oResp.json();
    lastMachines = mData.machines;
    lastOutlets = oData.outlets;
    renderMachines(mData.machines, oData.outlets);
  } catch (e) {}
}

// ---- Bulk operation (server-driven) ---------------------------------------

let currentOperation = null;

async function startOperation(kind) {
  if (kind === 'all-off' && !confirm('Turn off all machines?')) return;
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
    }
  };
  es.onerror = () => {
    // The browser auto-reconnects; nothing to do here. Polling still keeps the UI fresh.
  };
}

// ---- Init -----------------------------------------------------------------

poll();
setInterval(poll, 2000);
refreshRecentEvents();
connectEvents();
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
</style>
</head>
<body>

<header>
  <a href="/">&larr; Dashboard</a>
  <h1 id="machine-name">Loading...</h1>
</header>

<div class="meta-bar" id="meta-bar">
  <div class="meta-item">Loading...</div>
</div>

<div class="chart-wrap">
  <div class="chart-area">
    <svg id="chart"></svg>
  </div>
</div>
<div class="chart-tooltip" id="chart-tooltip"></div>

<script>
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
  const calButton = noEmeter
    ? ''
    : `<button class="btn btn-calibrate" id="cal-btn" onclick="calibrate()">${m.calibrated ? 'Recalibrate' : 'Calibrate'}</button>`;
  bar.innerHTML = `
    <div class="state-badge state-${st}"><div class="dot"></div>${noEmeter ? (isOn ? 'ON' : 'OFF') : st}</div>
    <div class="meta-item"><span class="val">${watts}</span></div>
    <div class="meta-item"><span class="val">${volts}</span></div>
    <div class="meta-item"><span class="val">${amps}</span></div>
    <div class="meta-item">Total <span class="val">${kwh}</span></div>
    <div class="meta-item">Asset <span class="val">${escapeHtml(m.asset_id)}</span></div>
    <div class="meta-item">Plug <span class="val">${escapeHtml(m.plug ? m.plug.alias : '--')}</span></div>
    <div class="meta-item">Strip <span class="val">${escapeHtml(m.strip_alias || '--')}</span></div>
    <div class="actions">
      <button class="btn ${isOn ? 'btn-power-off' : 'btn-power-on'}" id="power-btn"
        onclick="togglePower(${isOn ? 'false' : 'true'})">${isOn ? 'Turn Off' : 'Turn On'}</button>
      ${calButton}
    </div>
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
</style>
</head>
<body>

<header>
  <a href="/">&larr; Dashboard</a>
  <h1>Power events</h1>
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
