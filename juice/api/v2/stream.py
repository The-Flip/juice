"""GET /api/v2/stream — live updates for the new interface.

Shares one event bus with v1 but projects a different vocabulary onto it. That
split is the point: v1's pages read `power_change` / `reboot` / `operation_step`
and must keep doing so, while v2 clients read `command`, which expresses the same
actions with a lifecycle attached (juice/commands.py).

What v2 relies on, all landed earlier:

  * dense per-connection `seq`, so a gap is detectable and the client resyncs by
    re-fetching /api/v2/floor instead of polling on a timer;
  * `resync_required` when a subscriber falls behind, rather than silent drops;
  * `epoch` in `hello`, so reconnecting to a restarted process triggers a full
    resync rather than a meaningless seq comparison;
  * a heartbeat, so a connection killed by a proxy stops looking merely idle.
"""

from __future__ import annotations

import json
from typing import Any

from aiohttp import web

from juice.api.access import Access, access
from juice.api.v2.views import redact

# Event types a v2 client understands. Everything else on the bus is v1's
# vocabulary for the same underlying facts and would be noise here:
# `power_change`/`reboot` are superseded by `command`, and
# `operation_step_retry` by the retry phases a command already reports.
V2_EVENT_TYPES = frozenset(
    {"hello", "reading_tick", "command", "operation", "resync_required", "bye"}
)

# Renames applied on the way out. The v1 name stays on the bus untouched.
_RENAMES = {"readings": "reading_tick"}

# Delivered to anonymous subscribers. A reading tick carries only plug ids and
# status — no strip names, no actors — and a resync notice is equally true for a
# public viewer. Command and operation traffic names people, so it stays out.
PUBLIC_V2_EVENTS = frozenset({"hello", "reading_tick", "resync_required", "bye"})


def project(event: dict, *, public: bool) -> dict | None:
    """Reshape a bus event for a v2 subscriber, or drop it.

    Returning None drops the event *before* it consumes a sequence number, which
    is what keeps `seq` dense for a filtered subscriber — see `_sse_stream`.
    """
    kind = _RENAMES.get(event.get("type", ""), event.get("type", ""))
    if kind not in V2_EVENT_TYPES:
        return None
    if public and kind not in PUBLIC_V2_EVENTS:
        return None

    out: dict[str, Any] = {**event, "type": kind}

    if kind == "reading_tick":
        # v1's keys for the same facts; a v2 client reads status/activity.
        out["machines"] = [
            {
                "plug_id": m["plug_id"],
                "status": m.get("status"),
                "activity": m.get("activity"),
                "activity_unknown_because": m.get("activity_unknown_because"),
                "status_since": m.get("status_since"),
                "relay": "on" if m.get("is_on") else "off",
                "draw_watts": m.get("watt"),
            }
            for m in event.get("machines", [])
        ]
    elif kind == "command" and public:  # pragma: no cover - filtered above
        out = redact(out, public=True)

    return out


@access(Access.ANON_READ)
async def handle_stream(request: web.Request) -> web.StreamResponse:
    from juice.auth import is_authenticated
    from juice.server import _sse_stream

    response = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
    await response.prepare(request)
    state = request.app["recorder_state"]
    public = not is_authenticated(request)

    async def write(event: dict) -> None:
        projected = project(event, public=public)
        if projected is None:
            return
        await response.write(f"data: {json.dumps(projected)}\n\n".encode())

    async def ping() -> None:
        await response.write(b": ping\n\n")

    try:
        # Projection is passed to _sse_stream rather than applied in write():
        # dropping an event here would still have consumed a sequence number,
        # producing exactly the gaps the dense-seq contract exists to avoid.
        await _sse_stream(
            state,
            write,
            public=public,
            project=lambda event: project(event, public=public),
            ping=ping,
        )
    except TimeoutError, ConnectionResetError:
        pass
    return response
