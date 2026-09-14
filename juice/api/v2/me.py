"""`GET /api/v2/me` — which audience (api_v2.md §8) the caller is in.

Every other v2 read answers anonymous and operator callers alike, with
operator-only keys absent for the former — so a client could only learn which
it was by noticing whether `plug_id` came back, or by calling v1's `/api/me`,
which is the one thing the v1 freeze asks it not to do. This says so directly,
and never 401s: "you are anonymous" is an answer, not a refusal.
"""

from __future__ import annotations

from typing import Any

from aiohttp import web

from juice.api.access import Access, access
from juice.auth import is_authenticated


def _identity(body: dict[str, Any]) -> web.Response:
    # Varies by session and carries a name and an email: a shared cache that
    # kept it would hand one visitor's identity to the next.
    return web.json_response(body, headers={"Cache-Control": "no-store"})


@access(Access.ANON_READ)
async def handle_me(request: web.Request) -> web.Response:
    # With no auth wired at all -- `create_app` called directly, as
    # handler-level unit tests do -- everyone is the operator
    # (`is_authenticated`, `require_capability`), and this says so rather
    # than reporting an "authenticated" caller who can in fact do everything.
    if not is_authenticated(request):
        return _identity({"audience": "anonymous", "capabilities": []})
    if "user" not in request:
        return _identity(
            {
                "audience": "control_power",
                "capabilities": ["control_power"],
                "name": "",
                "email": "",
            }
        )
    capabilities = list(request.get("capabilities", []))
    user = request.get("user") or {}
    return _identity(
        {
            # The §8 table's own words, so a client can compare against the
            # document rather than derive the level from the capability list.
            "audience": "control_power" if "control_power" in capabilities else "authenticated",
            "capabilities": capabilities,
            "name": user.get("name", ""),
            "email": user.get("email", ""),
        }
    )
