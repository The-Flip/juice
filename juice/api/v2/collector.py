"""Is the collector there? The one question three handlers share.

On a tap-driven floor the collector is a process on another box, and it can be
absent in two ways that look identical from the readings -- every device goes
quiet -- and mean different things to an operator. **Offline**: no tap is
connected; nothing can be switched and the floor's silence is juice's, not the
strips'. **Silent**: a tap is connected but has sent no live frame recently,
which is what a tap catching up on backfill looks like (it suppresses live
frames while it is more than `live_max_lag_s` behind); commands still work,
the tiles do not move. The cloud recorder has neither state: the collector is
this process.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from aiohttp import web

CollectorState = Literal["offline", "silent"]

COLLECTOR_OFFLINE = "the collector is offline: no tap is connected, so nothing can be switched"


def collector_state(
    app: web.Application, now: datetime | None = None
) -> tuple[CollectorState | None, datetime | None]:
    """`(state, since)`: `(None, None)` when the collector is present, or cloud."""
    control = app.get("tap_control")
    if control is None:
        return None, None
    if not control.connected:
        return "offline", control.disconnected_at
    projector = app.get("tap_live")
    silent_since = getattr(projector, "silent_since", None)
    if silent_since is None:
        return None, None
    since = silent_since(now or datetime.now(UTC))
    if since is None:
        return None, None
    return "silent", since


def collector_offline(app: web.Application) -> bool:
    return collector_state(app)[0] == "offline"
