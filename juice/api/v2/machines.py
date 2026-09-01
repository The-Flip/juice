"""GET /api/v2/machines and /api/v2/machines/{asset_id}.

The single-machine endpoint fills a real gap: v1 has none, so the detail page
fetches every machine and filters client-side.
"""

from __future__ import annotations

from aiohttp import web

from juice.api.access import Access, access
from juice.api.v2 import errors
from juice.api.v2.views import machine_view
from juice.collector import outlet_number
from juice.identity import resolve_asset
from juice.state import UNCALIBRATED_CALIBRATION, classify
from juice.status import read_axes


def _is_public(request: web.Request) -> bool:
    from juice.auth import is_authenticated

    return not is_authenticated(request)


def _view_for(state, plug_id: int, *, public: bool) -> dict | None:
    assignment = state.assignments.get(plug_id)
    if assignment is None:
        return None
    name, asset_id, year = assignment

    plug_info = state.plugs.get(plug_id)
    device_id = plug_info[0] if plug_info else ""
    reading = state.plug_readings.get(plug_id)
    has_emeter = state.plug_has_emeter.get(plug_id, True)
    offline = bool(plug_info and device_id in state.offline_since)

    # A machine with no usable calibration reports `powered`, not `attract` —
    # honest about what we can and can't tell. See status_vocabulary.md §8.2.
    calibration = state.calibrations.get(plug_id)
    activity = None
    if has_emeter and (buf := state.watt_buffers.get(plug_id)):
        classified = classify(list(buf), calibration or UNCALIBRATED_CALIBRATION)
        activity = classified[-1] if classified else None

    axes = read_axes(
        reading,
        has_emeter=has_emeter,
        offline=offline,
        activity=activity,
        calibrated=calibration is not None,
    )
    from juice.server import _strip_display_name

    return machine_view(
        asset_id=asset_id,
        name=name,
        year=year,
        axes=axes,
        plug_id=plug_id,
        device_id=device_id,
        strip_name=_strip_display_name(state, device_id),
        outlet_number=outlet_number(plug_info[1]) if plug_info else None,
        lock_mode=state.lock_modes.get(asset_id),
        calibrated=calibration is not None,
        public=public,
    )


@access(Access.ANON_READ)
async def handle_machines(request: web.Request) -> web.Response:
    """Every assigned machine, most-recently-problematic ordering left to the client."""
    state = request.app["recorder_state"]
    public = _is_public(request)

    # A machine that moved has a stale assignment on the old offline outlet;
    # resolve_asset picks the live one, so dedupe through it rather than
    # emitting the machine twice.
    seen: set[str] = set()
    machines = []
    for plug_id in sorted(state.assignments):
        asset_id = state.assignments[plug_id][1]
        if asset_id in seen:
            continue
        resolution = resolve_asset(state, asset_id)
        if resolution.ambiguous:
            # Both outlets are live and claim the tag. Surfacing both would
            # imply we know which is which; the machine endpoint reports it.
            continue
        seen.add(asset_id)
        view = _view_for(state, resolution.plug_id or plug_id, public=public)
        if view is not None:
            machines.append(view)
    return web.json_response({"machines": machines})


@access(Access.ANON_READ)
async def handle_machine(request: web.Request) -> web.Response:
    """One machine by asset_id — durable across outlet moves."""
    state = request.app["recorder_state"]
    asset_id = request.match_info["asset_id"]
    resolution = resolve_asset(state, asset_id)

    if not resolution.found:
        return errors.error(404, errors.UNKNOWN_MACHINE, f"no machine with asset id {asset_id}")
    if resolution.ambiguous:
        # A Kasa label typo. Say which outlets, so the fix is obvious.
        return errors.error(
            409,
            errors.AMBIGUOUS_ASSIGNMENT,
            f"{asset_id} is claimed by more than one online outlet — fix the Kasa label",
            candidates=list(resolution.candidates),
        )

    assert resolution.plug_id is not None
    view = _view_for(state, resolution.plug_id, public=_is_public(request))
    if view is None:  # pragma: no cover - resolution guarantees an assignment
        return errors.error(404, errors.UNKNOWN_MACHINE, f"no machine with asset id {asset_id}")
    return web.json_response(view)
