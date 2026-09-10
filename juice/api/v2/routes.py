"""The v2 route table.

Registration also builds the access map the auth middleware consults, so a route
cannot exist without a declared audience.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from aiohttp import web

from juice.api.access import access_of
from juice.api.v2 import (
    collections,
    floor,
    ingest,
    machines,
    metrics,
    operations,
    stream,
    writes,
)

V2_PREFIX = "/api/v2/"


@dataclass(frozen=True)
class Route:
    method: str
    path: str
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]]


ROUTES: tuple[Route, ...] = (
    Route("GET", "/api/v2/floor", floor.handle_floor),
    Route("GET", "/api/v2/machines", machines.handle_machines),
    Route("GET", "/api/v2/machines/{asset_id}", machines.handle_machine),
    Route("GET", "/api/v2/stream", stream.handle_stream),
    Route("POST", "/api/v2/machines/{asset_id}/power", writes.handle_power),
    Route("POST", "/api/v2/machines/{asset_id}/reboot", writes.handle_reboot),
    Route("GET", "/api/v2/operations/current", operations.handle_current),
    Route("POST", "/api/v2/operations", operations.handle_start),
    Route("POST", "/api/v2/operations/{operation_id}/cancel", operations.handle_cancel),
    Route("GET", "/api/v2/outlets", collections.handle_outlets),
    Route("GET", "/api/v2/outlets/{plug_id}", collections.handle_outlet),
    Route("GET", "/api/v2/strips", collections.handle_strips),
    Route("GET", "/api/v2/strips/{device_id}", collections.handle_strip),
    Route("GET", "/api/v2/circuits", collections.handle_circuits),
    Route("GET", "/api/v2/power-events", collections.handle_power_events),
    Route("GET", "/api/v2/metrics/energy", metrics.handle_energy),
    Route("GET", "/api/v2/metrics/play-hours", metrics.handle_play_hours),
    Route("GET", "/api/v2/metrics/utilization", metrics.handle_utilization),
    Route("GET", "/api/v2/metrics/cost", metrics.handle_cost),
    Route("GET", "/api/v2/metrics/peaks", metrics.handle_peaks),
)


# Routes for machine principals, registered only when their secret is set.
# Kept out of ROUTES because they are conditional and because the generated
# anonymous-access matrix in tests/test_api_v2.py iterates ROUTES expecting
# browser-shaped routes; a WebSocket endpoint there would need special-casing.
# They still go through the same "declare your audience" guard below.
SERVICE_ROUTES: tuple[Route, ...] = (Route("GET", "/api/v2/ingest", ingest.handle_ingest),)


def _add(app: web.Application, routes: tuple[Route, ...]) -> None:
    for route in routes:
        if access_of(route.handler) is None:
            raise RuntimeError(
                f"{route.method} {route.path} has no @access level - "
                "every v2 route must declare its audience"
            )
        app.router.add_route(route.method, route.path, route.handler)


def register_service_routes(app: web.Application) -> None:
    """Mount the machine-to-machine routes.

    Called only when the corresponding secret is configured, so an unset
    `JUICE_INGEST_TOKEN` leaves the path genuinely absent rather than merely
    refusing -- the same shape as `/api/backup`.
    """
    _add(app, SERVICE_ROUTES)


def register_v2(app: web.Application) -> None:
    """Mount the v2 routes on the shared application.

    Same `web.Application` as v1 deliberately: v2 then inherits the existing
    session, auth middleware and compression with no extra wiring. A separate
    app or port would mean duplicating the cookie/session setup.
    """
    _add(app, ROUTES)
