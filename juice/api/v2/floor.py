"""GET /api/v2/floor — the whole Tier-1 floor view in one request.

user_needs.md's headline finding is that opening and closing the museum, and
reading the floor at a glance, are ~90% of what juice is used for. The operator
doing that is on a phone, standing up, in a building. So the floor view is one
request, not four, and it leads with what's wrong.

`problems` is a **filter on status**, not a hand-maintained list, so it cannot
drift from what the tiles show — the property status_vocabulary.md §2 calls out.
"""

from __future__ import annotations

from typing import Any

from aiohttp import web

from juice.api.access import Access, access
from juice.api.v2.machines import _is_public, _view_for
from juice.identity import resolve_asset

# Statuses that mean a machine on the floor needs a human. `unreachable` is
# deliberately absent: it's a device we can't talk to, which is real trouble but
# a different kind — infrastructure, not a machine physically present and
# misbehaving. It's reported separately so the two don't blur when triaging.
PROBLEM_STATUSES = frozenset({"no_draw", "abandoned"})


def _machines(state, *, public: bool) -> list[dict[str, Any]]:
    """Every machine once, on the outlet it's actually on."""
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for plug_id in sorted(state.assignments):
        asset_id = state.assignments[plug_id][1]
        if asset_id in seen:
            continue
        resolution = resolve_asset(state, asset_id)
        if resolution.ambiguous:
            continue
        seen.add(asset_id)
        view = _view_for(state, resolution.plug_id or plug_id, public=public)
        if view is not None:
            out.append(view)
    return out


@access(Access.ANON_READ)
async def handle_floor(request: web.Request) -> web.Response:
    state = request.app["recorder_state"]
    public = _is_public(request)
    machines = _machines(state, public=public)

    # A machine with a command in flight is excluded from problems. One five
    # seconds into a reboot is genuinely `no_draw`, and without this the panel
    # fills with machines that are merely still starting every time someone
    # opens the museum — which is exactly when it is most needed.
    problems = [
        {
            "asset_id": m["asset_id"],
            "name": m["name"],
            "status": m["status"],
            "since": m["status_since"],
        }
        for m in machines
        if m["status"] in PROBLEM_STATUSES and m["pending_command"] is None
    ]

    # Unreachable devices, collapsed to one entry each rather than one per
    # machine: a dead six-outlet strip is one problem to go and look at, not six.
    infrastructure: list[dict[str, Any]] = []
    if not public:
        for device_id, since in sorted(state.offline_since.items()):
            affected = [
                state.assignments[p][1]
                for p, info in sorted(state.plugs.items())
                if info[0] == device_id and p in state.assignments
            ]
            infrastructure.append(
                {
                    "device_id": device_id,
                    "name": _strip_name(state, device_id),
                    "kind": "unreachable_device",
                    "since": since.isoformat(),
                    "affects": affected,
                }
            )

    groups = _grouped(state, machines, public=public)

    from juice.server import _operation_to_dict

    operation = state.current_operation
    return web.json_response(
        {
            "counts": {
                "total": len(machines),
                "powered": sum(
                    1
                    for m in machines
                    if m["status"] in {"powered", "attract", "playing", "abandoned"}
                ),
                "playing": sum(1 for m in machines if m["status"] == "playing"),
                "problems": len(problems),
            },
            "problems": problems,
            "infrastructure": infrastructure,
            "groups": groups,
            # Public viewers never see who is running a bulk operation — an
            # actor is an email address. Mirrors v1's public SSE behaviour.
            "operation": (
                None
                if public or operation is None or operation.state != "running"
                else _operation_to_dict(operation)
            ),
        }
    )


def _strip_name(state, device_id: str) -> str:
    from juice.server import _strip_display_name

    return _strip_display_name(state, device_id)


def _grouped(state, machines: list[dict], *, public: bool) -> list[dict[str, Any]]:
    """Machines grouped the way the operator walks the building.

    Public viewers get one unlabelled group: strip names are operational detail,
    but the tiles still need an order, and it must match the operator's so the
    two views agree.
    """
    if public:
        return [{"device_id": None, "name": None, "machines": machines}]

    order: list[str] = []
    buckets: dict[str, list[dict]] = {}
    for machine in machines:
        device_id = machine.get("device_id") or ""
        if device_id not in buckets:
            buckets[device_id] = []
            order.append(device_id)
        buckets[device_id].append(machine)

    return [
        {
            "device_id": device_id or None,
            "name": _strip_name(state, device_id) if device_id else None,
            "machines": buckets[device_id],
        }
        for device_id in order
    ]
