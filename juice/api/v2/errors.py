"""Error envelope for /api/v2.

v1 returns `{"error": "<prose>"}`, which a client can only show to a human — it
can't branch on it. v2 carries a stable machine-readable `code` so the UI can
tell "this machine is locked" (offer to unlock) from "someone else is acting on
it" (offer to watch) without parsing English.
"""

from __future__ import annotations

import json
from typing import Any

from aiohttp import web

# Stable codes. Add rather than rename: clients branch on these.
UNAUTHENTICATED = "unauthenticated"
FORBIDDEN = "forbidden"
UNKNOWN_MACHINE = "unknown_machine"
AMBIGUOUS_ASSIGNMENT = "ambiguous_assignment"
BAD_REQUEST = "bad_request"
MACHINE_LOCKED = "machine_locked"
NOT_CONTROLLABLE = "not_controllable"
COMMAND_IN_FLIGHT = "command_in_flight"
OPERATION_IN_PROGRESS = "operation_in_progress"
UNKNOWN_OPERATION = "unknown_operation"
UNKNOWN_OUTLET = "unknown_outlet"
UNKNOWN_STRIP = "unknown_strip"


def error(status: int, code: str, message: str, **detail: Any) -> web.Response:
    """A JSON error with a code clients can act on.

    `detail` carries whatever context makes the error actionable — the competing
    outlets for an ambiguous asset, the in-flight command for a conflict.
    """
    body: dict[str, Any] = {"error": {"code": code, "message": message}}
    if detail:
        body["error"]["detail"] = detail
    return web.json_response(body, status=status)


def message_from_v1(response: web.Response, fallback: str) -> str:
    """Pull the prose out of a v1 handler's error body.

    v2 delegates actuation to the v1 handlers so both APIs cannot drift on what
    a power action does, which means occasionally re-coding their `{"error":
    "..."}` shape. `response.body` is typed `bytes | Payload`, so this is
    deliberately defensive rather than a cast.
    """
    body = getattr(response, "body", None)
    if not isinstance(body, bytes | bytearray):
        return fallback
    try:
        parsed = json.loads(body)
    except ValueError, TypeError:
        return fallback
    if isinstance(parsed, dict):
        error = parsed.get("error")
        if isinstance(error, str):
            return error
    return fallback
