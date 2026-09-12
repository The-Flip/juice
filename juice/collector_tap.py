"""The live and roster projections for the `tap` collector.

`juice/api/v2/ingest.py` is a protocol shim and stays one: it parses frames and
answers them. This module is what a frame *means* -- the replacement for the live
half of `juice/recorder.py`, not an API concern. Keeping it out of `juice/api/v2/`
matters for a practical reason as well as a tidy one: the module that owns the
floor's live state should not be scheduled for deletion alongside the v1 API, and
`api_v2.md` describes `/api/v2/ingest` as a wire protocol, which stops being true
if the receiver also does machine assignment.

**`readings` never drives any of this.** That channel is replayable and allowed to
be days behind, so it goes to DuckDB and nowhere else -- feeding it here would run
overload detection across history and fire shutdowns for events that ended on
Tuesday. Only `devices` and `live` reach this module.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Any

from juice.recorder import extract_asset_tag
from juice.store import Store

if TYPE_CHECKING:  # pragma: no cover - import cycle; RecorderState lives in server
    from juice.server import RecorderState

log = logging.getLogger(__name__)

# Said once per process rather than once per frame: tap re-sends its roster
# whenever it changes, and a FlipFix outage would otherwise produce this line
# every 60 seconds for its duration.
_warned_empty_roster = False


def apply_devices(
    state: RecorderState | None,
    store: Store,
    entries: list[dict],
    machines: Mapping[str, Any],
    ts: datetime,
) -> None:
    """Project tap's roster onto plugs, machines and assignments.

    The same work as `recorder.refresh_metadata`'s inner loop, driven by a frame
    instead of a device poll. The alias is the whole point: ingest creates plugs
    for outlets it has never seen with an **empty** alias, deliberately, because
    it has no roster to write -- so until this runs, a tap-only juice shows those
    outlets unassigned however well their readings are stored.

    **An empty `machines` skips assignment entirely.** `refresh_metadata` closes
    the assignment of every outlet whose tag is not in the roster, which is safe
    there only by accident of ordering: `record()` awaits `get_machines` before its
    first refresh. A frame has no such ordering -- it arrives when tap connects,
    which may be before juice has ever reached FlipFix, or during a FlipFix
    outage, or with a misconfigured key. Running the unassign branch then would
    clear every machine on the floor, and the dashboard is keyed off assignments.
    Aliases are still recorded, because they are what a later pass assigns *from*.
    """
    global _warned_empty_roster
    if not machines:
        if not _warned_empty_roster:
            _warned_empty_roster = True
            log.warning(
                "tap roster applied with no FlipFix machines known: recording aliases but "
                "leaving assignments alone. Assignments resume once FlipFix answers."
            )
    else:
        _warned_empty_roster = False

    for entry in entries:
        device_id = entry.get("device_id") or ""
        child_id = entry.get("child_id") or ""
        alias = entry.get("alias") or ""
        if not device_id:
            # Nothing can be keyed off a missing device id, and a plug created
            # for one would be unreachable forever.
            continue
        # Absent means metered: `refresh_hourly_usage` filters on this, so the
        # wrong FALSE hides a real outlet from every energy chart. An older tap
        # omits the field entirely.
        has_emeter = bool(entry.get("has_emeter", True))
        plug_id = store.ensure_plug(device_id, child_id, alias, has_emeter=has_emeter)

        if state is not None:
            state.plugs[plug_id] = (device_id, child_id, alias)
            state.plug_has_emeter[plug_id] = has_emeter
            # Only when tap sent one: an older tap omits it, and overwriting a
            # known strip name with "" would blank the dashboard's heading.
            device_alias = entry.get("device_alias") or ""
            if device_alias:
                state.strip_aliases[device_id] = device_alias

        if not machines:
            continue

        asset_tag = extract_asset_tag(alias)
        if asset_tag and asset_tag in machines:
            _assign(state, store, plug_id, asset_tag, machines[asset_tag], ts)
        else:
            _unassign(state, store, plug_id, ts)


def _assign(
    state: RecorderState | None,
    store: Store,
    plug_id: int,
    asset_tag: str,
    info: Any,
    ts: datetime,
) -> None:
    machine_id = store.ensure_machine(asset_tag, info["name"])
    store.update_assignment(plug_id, machine_id, ts)
    if state is None:
        return
    previous = state.assignments.get(plug_id)
    state.assignments[plug_id] = (info["name"], asset_tag, info.get("year"))
    # A plug moving to a different machine must not inherit the previous
    # machine's accumulated load.
    if previous is None or previous[1] != asset_tag:
        state.overload_windows.pop(plug_id, None)
        state.overload_onsets.pop(plug_id, None)
    calibration = store.get_calibration(machine_id)
    if calibration is not None:
        state.calibrations[plug_id] = calibration
    else:
        state.calibrations.pop(plug_id, None)


def _unassign(state: RecorderState | None, store: Store, plug_id: int, ts: datetime) -> None:
    store.update_assignment(plug_id, None, ts)
    if state is None:
        return
    state.assignments.pop(plug_id, None)
    state.calibrations.pop(plug_id, None)
    state.overload_windows.pop(plug_id, None)
    state.overload_onsets.pop(plug_id, None)
