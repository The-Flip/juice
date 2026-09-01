"""Error envelope for /api/v2.

v1 returns `{"error": "<prose>"}`, which a client can only show to a human — it
can't branch on it. v2 carries a stable machine-readable `code` so the UI can
tell "this machine is locked" (offer to unlock) from "someone else is acting on
it" (offer to watch) without parsing English.
"""

from __future__ import annotations

from typing import Any

from aiohttp import web

# Stable codes. Add rather than rename: clients branch on these.
UNAUTHENTICATED = "unauthenticated"
FORBIDDEN = "forbidden"
UNKNOWN_MACHINE = "unknown_machine"
AMBIGUOUS_ASSIGNMENT = "ambiguous_assignment"
BAD_REQUEST = "bad_request"


def error(status: int, code: str, message: str, **detail: Any) -> web.Response:
    """A JSON error with a code clients can act on.

    `detail` carries whatever context makes the error actionable — the competing
    outlets for an ambiguous asset, the in-flight command for a conflict.
    """
    body: dict[str, Any] = {"error": {"code": code, "message": message}}
    if detail:
        body["error"]["detail"] = detail
    return web.json_response(body, status=status)
