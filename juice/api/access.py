"""Declarative access levels for API routes.

v1 gates routes two ways: a hardcoded regex tuple in `juice/auth.py`
(`PUBLIC_READABLE_PATTERNS`) decides who may *read*, and each write handler
remembers to call `require_capability` itself. That second half is opt-in, which
is exactly how `handle_calibrate` shipped ungated — any logged-in user could
trigger a retroactive rewrite of play-hours history (fixed in #79).

Here the level is declared *on the handler*, so it can't drift from it, and the
middleware enforces it. There is no per-handler call left to forget, and a route
whose level someone neglected to declare fails **closed**: no v2 path matches
v1's regexes, so it 401s rather than leaking.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from enum import StrEnum
from typing import TypeVar

from aiohttp import web

ACCESS_ATTR = "__juice_access__"


class Access(StrEnum):
    ANON_READ = "anon_read"  # readable logged-out; handler redacts operator detail
    AUTHED = "authed"  # any logged-in user
    CONTROL = "control"  # requires the control_power capability


Handler = TypeVar("Handler", bound=Callable[[web.Request], Awaitable[web.StreamResponse]])


def access(level: Access) -> Callable[[Handler], Handler]:
    """Declare who may call this handler.

    The level travels with the function rather than living in a separate table,
    so moving or renaming a handler cannot silently change its gating.
    """

    def decorate(handler: Handler) -> Handler:
        setattr(handler, ACCESS_ATTR, level)
        return handler

    return decorate


def access_of(handler: object) -> Access | None:
    """The declared level, or None for a handler that never declared one
    (every v1 handler, which keeps using the legacy regex path)."""
    return getattr(handler, ACCESS_ATTR, None)
