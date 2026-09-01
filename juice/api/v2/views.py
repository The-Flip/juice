"""Serializers for /api/v2, and the one place public redaction happens.

v1 redacts inline in each handler, which is how the same data ends up public at
one granularity and hidden at another (`/api/machines` hides strip names, while
`/api/usage` publishes machine names and kWh freely). Keeping it in one function
means the boundary is reviewable, and a test can walk the operator payload and
assert every operator-only key is absent from the public one.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from juice.commands import Command
from juice.status import Axes, derive_status

# Keys stripped for anonymous viewers: physical/operational detail that says how
# the building is wired, not what the floor is doing.
OPERATOR_ONLY_KEYS = frozenset({"plug_id", "device_id", "outlet", "strip", "calibration"})


def machine_view(
    *,
    asset_id: str,
    name: str,
    year: int | None,
    axes: Axes,
    plug_id: int,
    device_id: str,
    strip_name: str,
    outlet_number: int | None,
    lock_mode: str | None,
    calibrated: bool,
    public: bool,
    status_since: datetime | None = None,
    pending_command: Command | None = None,
) -> dict[str, Any]:
    """One machine, with its status derived once via juice.status.

    `pending_command` is deliberately a field of its own rather than a status
    value. `status` reports what is *observed*; a command in flight is *intent*,
    and it comes from a different source. Folding them together would repeat the
    mistake domain_model.md §7.2 catalogues, where OFFLINE was injected into the
    State enum at the presentation layer. The UI renders "Rebooting…" with visual
    precedence over the status; the data model keeps them separate.

    No sparkline here: the floor view is what the front-desk tablet re-fetches on
    every resync and holds all day, and sparkline floats dominate the payload.
    Series are served separately, and the live band comes from the stream.
    """
    view: dict[str, Any] = {
        "asset_id": asset_id,
        "name": name,
        "year": year,
        "status": derive_status(axes),
        "activity": axes.activity.value if axes.activity else None,
        "activity_unknown_because": axes.activity_unknown_because,
        "relay": axes.relay,
        "draw_watts": None if axes.draw is None else round(axes.draw, 1),
        "lock_mode": lock_mode,
        "status_since": status_since.isoformat() if status_since else None,
        "pending_command": _command_view(pending_command, public=public),
        "plug_id": plug_id,
        "device_id": device_id,
        "strip": strip_name,
        "outlet": outlet_number,
        "calibration": {"calibrated": calibrated},
    }
    return redact(view, public=public)


def _command_view(command: Command | None, *, public: bool) -> dict[str, Any] | None:
    """The in-flight command, if any.

    `actor` is an OAuth email (see `_actor` in juice/server.py) and is dropped
    for anonymous viewers. It has to be dropped *here*: `redact()` only removes
    top-level keys, so a nested identity would sail straight past it. The
    pending state itself is fine to show publicly — a tile reading "Rebooting…"
    leaks nothing; who pressed it does.
    """
    if command is None:
        return None
    view: dict[str, Any] = {
        "command_id": command.id,
        "kind": command.kind,
        "phase": command.phase,
        "attempt": command.attempt,
    }
    if not public:
        # Operators need this: two people converging on one machine have to see
        # who is already acting on it (user_needs.md J6).
        view["actor"] = command.actor
    return view


def redact(view: dict[str, Any], *, public: bool) -> dict[str, Any]:
    """Drop operator-only keys for anonymous viewers.

    The single redaction boundary — if `if public:` starts appearing in the
    endpoint modules, it has already scattered.

    Note this removes **top-level** keys only. Anything nested that carries
    operator identity must redact itself at construction (see `_command_view`);
    a key-list assertion cannot see into a sub-object, which is how an operator
    email once reached the public payload inside `pending_command`.
    """
    if not public:
        return view
    return {k: v for k, v in view.items() if k not in OPERATOR_ONLY_KEYS}
