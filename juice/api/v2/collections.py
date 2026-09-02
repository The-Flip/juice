"""Collections for /api/v2 — outlets, strips, circuits and the audit log.

The pieces the floor view doesn't cover: the physical hierarchy a technician
navigates (circuit -> strip -> outlet), and the record of who did what.

Two things follow from the design docs rather than from convenience:

  * **Unassigned outlets share the machine status vocabulary.** An outlet with no
    machine still has a relay, a draw and a reachability, so `no_draw` and
    `unreachable` mean the same thing there. v1 gave outlets their own rendering
    path and their own shape; keeping one vocabulary is what lets the Problems
    filter span both without a second query.

  * **`/power-events` is named for what it is.** v1 calls its SSE stream
    `/api/events` and its audit log `/api/power-events`; reusing `events` here
    for the log would collide with `/api/v2/stream`'s obvious alternative name
    and mean two different things across versions.
"""

from __future__ import annotations

from datetime import UTC
from typing import Any

from aiohttp import web

from juice.api.access import Access, access
from juice.api.v2 import errors
from juice.api.v2.views import blank_when_unreachable, redact
from juice.collector import outlet_number
from juice.status import derive_status, read_axes

_MAX_EVENTS = 200
_DEFAULT_EVENTS = 50


def _utc_iso(value: Any) -> str | None:
    """RFC 3339 with a real offset.

    DuckDB hands back naive datetimes stored as UTC. v1 normalises these the
    same way; several v1 endpoints instead hand-append a literal "Z" to a naive
    isoformat, which is not the same thing and is one of the inconsistencies v2
    exists to end.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value
    stamped = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    return stamped.isoformat()


def _is_public(request: web.Request) -> bool:
    from juice.auth import is_authenticated

    return not is_authenticated(request)


def _outlet_view(state, plug_id: int, *, public: bool) -> dict[str, Any]:
    """One outlet, with the same status derivation machines use."""
    plug_info = state.plugs.get(plug_id)
    device_id = plug_info[0] if plug_info else ""
    reading = state.plug_readings.get(plug_id)
    has_emeter = state.plug_has_emeter.get(plug_id, True)
    offline = bool(plug_info and device_id in state.offline_since)

    # No activity axis: an outlet has no machine to classify, so it is never
    # attract/playing/abandoned. It is still honestly powered, off, no_draw or
    # unreachable — which is the point of sharing the vocabulary.
    axes = read_axes(reading, has_emeter=has_emeter, offline=offline)
    assignment = state.assignments.get(plug_id)
    tracked = state.status_since.get(plug_id)

    view: dict[str, Any] = {
        "plug_id": plug_id,
        "alias": plug_info[2] if plug_info else "",
        "status": derive_status(axes),
        "relay": axes.relay,
        "draw_watts": None if axes.draw is None else round(axes.draw, 1),
        "status_since": tracked[1].isoformat() if tracked else None,
        "has_emeter": has_emeter,
        "machine": (
            None if assignment is None else {"asset_id": assignment[1], "name": assignment[0]}
        ),
        "device_id": device_id,
        "outlet": outlet_number(plug_info[1]) if plug_info else None,
    }
    return redact(blank_when_unreachable(view), public=public)


@access(Access.AUTHED)
async def handle_outlets(request: web.Request) -> web.Response:
    """Every switchable outlet, assigned or not.

    Requires a session but NOT `control_power`. That capability means "may turn
    machines on and off"; gating a *read* on it would mean anyone allowed to look
    at the wiring must also be allowed to switch the museum. domain_model.md
    section 6 names three audiences — anonymous public, authenticated viewer,
    operator with control_power — and this serves the second. Matches v1, where
    none of the equivalent read endpoints calls require_capability.
    """
    state = request.app["recorder_state"]
    outlets = [_outlet_view(state, plug_id, public=False) for plug_id in sorted(state.plugs)]
    return web.json_response({"outlets": outlets})


@access(Access.AUTHED)
async def handle_outlet(request: web.Request) -> web.Response:
    state = request.app["recorder_state"]
    raw = request.match_info["plug_id"]
    try:
        plug_id = int(raw)
    except ValueError:
        return errors.error(400, errors.BAD_REQUEST, "plug_id must be an integer")
    if plug_id not in state.plugs:
        return errors.error(404, errors.UNKNOWN_OUTLET, f"no outlet with id {plug_id}")
    return web.json_response(_outlet_view(state, plug_id, public=False))


def _strip_display_name_or_empty(state, device_id: str) -> str:
    from juice.server import _strip_display_name

    return _strip_display_name(state, device_id)


def _strip_label(state, device_id: str, plug_ids: list[int]) -> str:
    """A name a human can act on, always.

    `_strip_display_name` returns "" for a device with neither an operator name
    nor a Kasa alias — true of the single-outlet plugs, which aren't
    cloud-discovered strips. v1 never showed those as strips so never hit it;
    a strips *collection* does. A blank row is bad, and because "" sorts first
    it would head the list.

    A single-outlet device is best described by its outlet, which carries the
    machine's name. Otherwise fall back to a short device id: unlovely, but it
    identifies the thing well enough to go and look at it.
    """
    from juice.server import _strip_display_name

    name = _strip_display_name(state, device_id)
    if name:
        return name
    if len(plug_ids) == 1:
        info = state.plugs.get(plug_ids[0])
        if info and info[2]:
            return info[2]
    return f"Device {device_id[:8]}"


def _device_label(state, device_id: str) -> str:
    """The label for a device, wherever it is named.

    Wraps _strip_label so a caller doesn't have to gather plug ids first — the
    omission that let /circuits emit blank member names while /strips did not.
    One entry point, so a third call site can't reintroduce it.
    """
    plug_ids = sorted(p for p, info in state.plugs.items() if info[0] == device_id)
    return _strip_label(state, device_id, plug_ids)


def _strip_view(state, device_id: str) -> dict[str, Any]:
    plug_ids = sorted(p for p, info in state.plugs.items() if info[0] == device_id)
    offline = device_id in state.offline_since
    outlets = [_outlet_view(state, p, public=False) for p in plug_ids]

    # Sum only what we actually measured. A strip with one unmetered outlet
    # should not report a total that silently omits it without saying so.
    measured = [o["draw_watts"] for o in outlets if o["draw_watts"] is not None]
    return {
        "device_id": device_id,
        "name": _strip_label(state, device_id, plug_ids),
        "named": bool(_strip_display_name_or_empty(state, device_id)),
        "status": "unreachable" if offline else "reachable",
        "since": state.offline_since[device_id].isoformat() if offline else None,
        "circuit_id": state.circuit_devices.get(device_id),
        "sort_order": state.strip_orders.get(device_id),
        "draw_watts": round(sum(measured), 1) if measured else None,
        "unmeasured_outlets": sum(1 for o in outlets if o["draw_watts"] is None),
        "outlets": outlets,
    }


@access(Access.AUTHED)
async def handle_strips(request: web.Request) -> web.Response:
    """Every strip, in the operator's configured order.

    v1 has no strips collection at all — the dashboard groups them client-side
    from the machines payload, which is why the ordering logic lives in two
    places there.
    """
    state = request.app["recorder_state"]
    device_ids = sorted({info[0] for info in state.plugs.values() if info[0]})
    strips = [_strip_view(state, d) for d in device_ids]
    # `named` before the label: a device whose name we derived shouldn't outrank
    # one an operator actually named, whatever the two happen to spell.
    strips.sort(
        key=lambda s: (
            s["sort_order"] is None,
            s["sort_order"] or 0,
            not s["named"],
            s["name"].lower(),
        )
    )
    return web.json_response({"strips": strips})


@access(Access.AUTHED)
async def handle_strip(request: web.Request) -> web.Response:
    state = request.app["recorder_state"]
    device_id = request.match_info["device_id"]
    if not any(info[0] == device_id for info in state.plugs.values()):
        return errors.error(404, errors.UNKNOWN_STRIP, f"no strip with device id {device_id}")
    return web.json_response(_strip_view(state, device_id))


@access(Access.AUTHED)
async def handle_circuits(request: web.Request) -> web.Response:
    """Breakers, with the strips on them and how loaded they are."""
    state = request.app["recorder_state"]
    store = request.app["store"]

    circuits = []
    for row in store.list_circuits():
        circuit_id = row["circuit_id"]
        device_ids = sorted(d for d, c in state.circuit_devices.items() if c == circuit_id)
        capacity = row["amps"] * 120 if row["amps"] else None
        circuits.append(
            {
                **row,
                "label": f"{row['panel']} {row['breaker']}",
                "capacity_watts": capacity,
                "strips": [{"device_id": d, "name": _device_label(state, d)} for d in device_ids],
            }
        )
    return web.json_response({"circuits": circuits})


@access(Access.AUTHED)
async def handle_power_events(request: web.Request) -> web.Response:
    """The audit log: who switched what, when, and whether it worked.

    Cursor-paginated on `before` (an event id), which is how v1 does it and the
    only pagination in either API. Answers both "who turned this off?" (J10) and
    "what has my colleague already tried?" (J6) — the same data serves both, so
    it is one endpoint rather than two.
    """
    store = request.app["store"]

    try:
        limit = int(request.query.get("limit", _DEFAULT_EVENTS))
    except ValueError:
        return errors.error(400, errors.BAD_REQUEST, "'limit' must be an integer")
    if not 1 <= limit <= _MAX_EVENTS:
        return errors.error(
            400, errors.BAD_REQUEST, f"'limit' must be between 1 and {_MAX_EVENTS}", max=_MAX_EVENTS
        )

    before = request.query.get("before")
    before_id: int | None = None
    if before is not None:
        try:
            before_id = int(before)
        except ValueError:
            return errors.error(400, errors.BAD_REQUEST, "'before' must be an event id")

    asset_id = request.query.get("asset_id")
    plug_id: int | None = None
    if asset_id is not None:
        from juice.identity import resolve_asset

        resolution = resolve_asset(request.app["recorder_state"], asset_id)
        if not resolution.found:
            return errors.error(404, errors.UNKNOWN_MACHINE, f"no machine with asset id {asset_id}")
        plug_id = resolution.plug_id

    rows = store.recent_power_events(limit=limit, before=before_id, plug_id=plug_id)
    events = [
        {
            "event_id": r["event_id"],
            "ts": _utc_iso(r["ts"]),
            "action": r["action"],
            "source": r["source"],
            "result": r["result"],
            "actor": r["actor"],
            "error": r["error"],
            "operation_id": r["operation_id"],
            "machine": (
                None if not r.get("machine_name") else {"name": r["machine_name"], "asset_id": None}
            ),
            "plug_id": r["plug_id"],
        }
        for r in rows
    ]
    # A next cursor only when the page was full; otherwise this is the end of
    # history and the client should stop rather than issue a request that
    # returns nothing.
    next_before = events[-1]["event_id"] if len(events) == limit else None
    return web.json_response({"events": events, "next_before": next_before})
