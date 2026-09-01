"""Aggregate metrics for /api/v2.

Every endpoint here takes the same window (juice/api/v2/window.py) and echoes the
resolved range back, which is the whole point: v1 spreads three conventions
across these same numbers, so a client has to learn each endpoint separately.

The aggregation itself is unchanged — these call the existing `juice/store.py`
methods. What differs is the contract around them.
"""

from __future__ import annotations

from typing import Any

from aiohttp import web

from juice.api.access import Access, access
from juice.api.v2 import errors
from juice.api.v2.window import parse_window

# Cost is operator-only in v1 too: what the collection costs to run is a
# business fact, not part of the public "what's on the floor" view.
COST_PER_KWH = 0.31


@access(Access.ANON_READ)
async def handle_energy(request: web.Request) -> web.Response:
    """Energy use per machine over the window."""
    window, failure = parse_window(request)
    if failure is not None:
        return failure
    assert window is not None

    store = request.app["store"]
    rows = store.kwh_by_machine_and_local_day(window.from_day, window.to_day)

    machines = []
    for (machine_id, name), group in _rows_by_machine(rows).items():
        total = sum(float(r["kwh"]) for r in group)
        machines.append(
            {
                "machine_id": machine_id,
                "name": name,
                "kwh": round(total, 3),
                "daily": [
                    {"day": r["day_local"].isoformat(), "kwh": round(float(r["kwh"]), 3)}
                    for r in sorted(group, key=lambda r: r["day_local"])
                ],
            }
        )
    machines.sort(key=lambda m: -m["kwh"])
    return web.json_response(
        {
            "window": window.echo(),
            "machines": machines,
            "total_kwh": round(sum(m["kwh"] for m in machines), 3),
        }
    )


def _rows_by_machine(rows: list[dict]) -> dict[tuple[Any, str], list[dict]]:
    """Group store rows by (machine_id, machine_name).

    Both `kwh_by_machine_and_local_day` and `play_hours_by_machine` return
    `machine_name` — not `name`. Writing a second grouper on the assumption they
    differed is how this shipped a KeyError that only real data reached.
    """
    grouped: dict[tuple[Any, str], list[dict]] = {}
    for row in rows:
        grouped.setdefault((row["machine_id"], row["machine_name"]), []).append(row)
    return grouped


@access(Access.ANON_READ)
async def handle_play_hours(request: web.Request) -> web.Response:
    """Play time per machine.

    Only calibrated machines contribute — an uncalibrated one has no measurable
    play, which is different from zero play. The response says how many are
    missing rather than letting a zero be read as "nobody played it".
    """
    window, failure = parse_window(request)
    if failure is not None:
        return failure
    assert window is not None

    store = request.app["store"]
    state = request.app["recorder_state"]
    rows = store.play_hours_by_machine(window.from_day, window.to_day)

    machines = []
    for (machine_id, name), group in _rows_by_machine(rows).items():
        machines.append(
            {
                "machine_id": machine_id,
                "name": name,
                "hours": round(sum(float(r["hours"]) for r in group), 2),
                "daily": [
                    {"day": r["day_local"].isoformat(), "hours": round(float(r["hours"]), 2)}
                    for r in sorted(group, key=lambda r: r["day_local"])
                ],
            }
        )
    machines.sort(key=lambda m: -m["hours"])

    measurable = len(state.calibrations)
    return web.json_response(
        {
            "window": window.echo(),
            "machines": machines,
            "total_hours": round(sum(m["hours"] for m in machines), 2),
            # Without this an operator reads a short list as "these are all the
            # machines" rather than "these are the ones we can measure".
            "measurable_machines": measurable,
            "unmeasurable_machines": max(0, len(state.assignments) - measurable),
        }
    )


@access(Access.ANON_READ)
async def handle_utilization(request: web.Request) -> web.Response:
    """When the museum is busy: play utilization by local date and hour.

    Dense rather than sparse. v1 returns only cells with data, so a client must
    reconstruct the zeroes itself — and a missing cell is ambiguous between "no
    play" and "we weren't open".
    """
    window, failure = parse_window(request, grain="hour")
    if failure is not None:
        return failure
    assert window is not None

    from datetime import datetime

    store = request.app["store"]
    rows = store.play_utilization_grid(
        datetime.combine(window.from_day, datetime.min.time()),
        datetime.combine(window.to_day, datetime.min.time()),
    )
    cells = {
        (
            r["date_local"].isoformat()
            if hasattr(r["date_local"], "isoformat")
            else r["date_local"],
            int(r["hour"]),
        ): r
        for r in rows
    }
    dates = sorted({d for d, _h in cells})
    hours = list(range(24))

    grid = [
        {
            "date": day,
            "hour": hour,
            "ratio": round(float(cells[(day, hour)]["ratio"]), 3) if (day, hour) in cells else 0.0,
            "measured": (day, hour) in cells,
        }
        for day in dates
        for hour in hours
    ]
    return web.json_response(
        {
            "window": window.echo(),
            "dates": dates,
            "hours": hours,
            "cells": grid,
            "max_ratio": round(max((c["ratio"] for c in grid), default=0.0), 3),
        }
    )


@access(Access.AUTHED)
async def handle_cost(request: web.Request) -> web.Response:
    """What the collection costs to run. Operator-only, as in v1."""
    window, failure = parse_window(request)
    if failure is not None:
        return failure
    assert window is not None

    store = request.app["store"]
    rows = store.kwh_by_machine_and_local_day(window.from_day, window.to_day)

    by_day: dict[str, float] = {}
    machines = []
    for (machine_id, name), group in _rows_by_machine(rows).items():
        kwh = sum(float(r["kwh"]) for r in group)
        for r in group:
            by_day[r["day_local"].isoformat()] = by_day.get(
                r["day_local"].isoformat(), 0.0
            ) + float(r["kwh"])
        machines.append(
            {
                "machine_id": machine_id,
                "name": name,
                "kwh": round(kwh, 3),
                "cost": round(kwh * COST_PER_KWH, 2),
            }
        )
    machines.sort(key=lambda m: -m["cost"])

    daily = [
        {"day": day, "kwh": round(kwh, 3), "cost": round(kwh * COST_PER_KWH, 2)}
        for day, kwh in sorted(by_day.items())
    ]
    total_kwh = sum(float(r["kwh"]) for r in rows)
    return web.json_response(
        {
            "window": window.echo(),
            "rate_per_kwh": COST_PER_KWH,
            "machines": machines,
            "daily": daily,
            "total_kwh": round(total_kwh, 3),
            # Rounded once from the true total, not summed from rounded parts —
            # v1 has two totals that can differ by a cent for that reason.
            "total_cost": round(total_kwh * COST_PER_KWH, 2),
        }
    )


@access(Access.AUTHED)
async def handle_peaks(request: web.Request) -> web.Response:
    """Peak simultaneous draw, by strip or circuit — the breaker-trip number."""
    by = request.query.get("by", "circuit")
    if by not in {"strip", "circuit"}:
        return errors.error(
            400,
            errors.BAD_REQUEST,
            "'by' must be 'strip' or 'circuit'",
            allowed=["circuit", "strip"],
        )

    window, failure = parse_window(request, grain="hour")
    if failure is not None:
        return failure
    assert window is not None

    from juice.api.v2.collections import _device_label

    store = request.app["store"]
    state = request.app["recorder_state"]

    if by == "strip":
        peaks = store.strip_peaks(window.from_utc, window.to_utc)
        items = [
            {"device_id": d, "name": _device_label(state, d), "peak_watts": round(w, 1)}
            for d, w in sorted(peaks.items(), key=lambda kv: -kv[1])
        ]
    else:
        peaks = store.circuit_peaks(window.from_utc, window.to_utc)
        rows = {c["circuit_id"]: c for c in store.list_circuits()}
        items = []
        for circuit_id, watts in sorted(peaks.items(), key=lambda kv: -kv[1]):
            row = rows.get(circuit_id, {})
            capacity = row.get("amps") * 120 if row.get("amps") else None
            items.append(
                {
                    "circuit_id": circuit_id,
                    "label": f"{row.get('panel', '?')} {row.get('breaker', '?')}",
                    "peak_watts": round(watts, 1),
                    "capacity_watts": capacity,
                    # None rather than a guess: a breaker with no recorded
                    # amperage has no headroom we can honestly report.
                    "pct_of_capacity": (round(100 * watts / capacity, 1) if capacity else None),
                }
            )

    return web.json_response({"window": window.echo(), "by": by, "items": items})
