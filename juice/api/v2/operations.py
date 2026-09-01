"""Bulk operations for /api/v2 — all-on and all-off.

Operations stay a **single global slot**, as v1 has them. That is a deliberate
physical constraint rather than a limitation: the museum is one power domain, the
stagger between steps exists to limit inrush current, and interleaving two bulk
cycles would defeat it.

What changes is that a refusal explains itself. v1 returns a bare
`{"error": "operation already in progress", "operation_id": "..."}`, which tells
an operator nothing they can act on. Here the 409 carries the whole operation —
who started it, its scope, how far along it is — so the UI can say "Dana started
All On 40 seconds ago, 12 of 31" and offer to watch or cancel it.
"""

from __future__ import annotations

from aiohttp import web

from juice.api.access import Access, access
from juice.api.v2 import errors

_KINDS = {"all_on", "all_off"}


@access(Access.AUTHED)
async def handle_current(request: web.Request) -> web.Response:
    """GET /api/v2/operations/current.

    Always an object, never a bare `null` — v1 returns top-level `null` when
    idle, which is the only endpoint in the API that does and forces clients to
    special-case it.
    """
    from juice.server import _operation_to_dict

    state = request.app["recorder_state"]
    operation = state.current_operation
    running = operation is not None and operation.state == "running"
    return web.json_response({"operation": _operation_to_dict(operation) if running else None})


@access(Access.CONTROL)
async def handle_start(request: web.Request) -> web.Response:
    """POST /api/v2/operations — {"kind": "all_on"|"all_off", "scope": {...}}."""
    from juice.server import _operation_to_dict, _start_operation

    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}

    kind = body.get("kind")
    if kind not in _KINDS:
        return errors.error(
            400, errors.BAD_REQUEST, "'kind' must be 'all_on' or 'all_off'", allowed=sorted(_KINDS)
        )

    scope = body.get("scope") or {}
    if not isinstance(scope, dict):
        return errors.error(400, errors.BAD_REQUEST, "'scope' must be an object")
    device_id = scope.get("device_id")
    if device_id is not None and not isinstance(device_id, str):
        return errors.error(400, errors.BAD_REQUEST, "'scope.device_id' must be a string")

    state = request.app["recorder_state"]
    current = state.current_operation
    if current is not None and current.state == "running":
        # The whole operation, not just its id: an operator who is told "busy"
        # and nothing else has no way to decide whether to wait or intervene.
        return errors.error(
            409,
            errors.OPERATION_IN_PROGRESS,
            f"{current.started_by} started {current.kind} — {current.index} of {len(current.targets)}",
            operation=_operation_to_dict(current),
        )

    response = await _start_operation(request, kind, device_id)
    if response.status >= 400:
        return _recode(response)

    operation = state.current_operation
    return web.json_response(
        {"operation": _operation_to_dict(operation) if operation else None},
        status=202,
    )


@access(Access.CONTROL)
async def handle_cancel(request: web.Request) -> web.Response:
    """POST /api/v2/operations/{operation_id}/cancel."""
    from juice.server import _operation_to_dict

    state = request.app["recorder_state"]
    operation_id = request.match_info["operation_id"]
    current = state.current_operation

    if current is None or current.id != operation_id:
        return errors.error(
            404, errors.UNKNOWN_OPERATION, f"no running operation with id {operation_id}"
        )

    current.cancel_requested = True
    return web.json_response({"operation": _operation_to_dict(current)})


def _recode(response: web.Response) -> web.Response:
    """Translate v1's prose error into v2's coded envelope."""
    message = errors.message_from_v1(response, "operation failed")
    code = errors.UNKNOWN_MACHINE if response.status == 404 else errors.BAD_REQUEST
    return errors.error(response.status, code, message)
