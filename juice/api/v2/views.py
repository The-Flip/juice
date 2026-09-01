"""Serializers for /api/v2, and the one place public redaction happens.

v1 redacts inline in each handler, which is how the same data ends up public at
one granularity and hidden at another (`/api/machines` hides strip names, while
`/api/usage` publishes machine names and kWh freely). Keeping it in one function
means the boundary is reviewable, and a test can walk the operator payload and
assert every operator-only key is absent from the public one.
"""

from __future__ import annotations

from typing import Any

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
) -> dict[str, Any]:
    """One machine, with its status derived once via juice.status."""
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
        "plug_id": plug_id,
        "device_id": device_id,
        "strip": strip_name,
        "outlet": outlet_number,
        "calibration": {"calibrated": calibrated},
    }
    return redact(view, public=public)


def redact(view: dict[str, Any], *, public: bool) -> dict[str, Any]:
    """Drop operator-only keys for anonymous viewers.

    The single redaction boundary — if `if public:` starts appearing in the
    endpoint modules, it has already scattered.
    """
    if not public:
        return view
    return {k: v for k, v in view.items() if k not in OPERATOR_ONLY_KEYS}
