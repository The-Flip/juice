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
from dataclasses import dataclass
from datetime import UTC, datetime
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


@dataclass(frozen=True, slots=True)
class RosterDiff:
    """What would change if tap's roster were applied. Empty means safe.

    The gate before flipping the collector is that this stays empty over a couple
    of days of real operation, because every entry is something that moves the
    moment tap becomes the source of truth -- and the floor is how the museum opens.
    """

    # Outlets tap reports that juice has no plug for. The worst category: each one
    # is a machine that would appear from nowhere, or -- if the tag matches a
    # machine already assigned elsewhere -- move without anyone asking.
    unknown_outlets: tuple[tuple[str, str], ...] = ()
    # Outlets juice knows and tap did not mention. Those devices are ones tap
    # cannot reach, so at cutover their machines go dark rather than move.
    missing_outlets: tuple[tuple[str, str], ...] = ()
    # (child_id, current asset_id or None, tap's asset_id or None).
    assignment_changes: tuple[tuple[str, str | None, str | None], ...] = ()
    # (child_id, current has_emeter, tap's has_emeter). `refresh_hourly_usage`
    # filters on this, so a disagreement is an energy chart that changes.
    metering_changes: tuple[tuple[str, bool, bool], ...] = ()

    @property
    def clean(self) -> bool:
        return not (
            self.unknown_outlets
            or self.missing_outlets
            or self.assignment_changes
            or self.metering_changes
        )

    def describe(self) -> str:
        """One line per finding, for the log. Empty string when clean."""
        parts = []
        for device_id, child_id in self.unknown_outlets:
            parts.append(f"outlet {device_id}/{child_id} never seen by the cloud recorder")
        for device_id, child_id in self.missing_outlets:
            parts.append(f"outlet {device_id}/{child_id} known here but absent from tap's roster")
        for child_id, current, proposed in self.assignment_changes:
            parts.append(
                f"outlet {child_id} would move from {current or 'unassigned'} to "
                f"{proposed or 'unassigned'}"
            )
        for child_id, was, becomes in self.metering_changes:
            parts.append(f"outlet {child_id} metering would change from {was} to {becomes}")
        return "; ".join(parts)


def shadow_devices(
    state: RecorderState | None,
    store: Store,
    entries: list[dict],
    machines: Mapping[str, Any],
) -> RosterDiff:
    """Diff tap's roster against the live state, changing nothing.

    Runs with the cloud recorder still authoritative, so it must be side-effect
    free in the strictest sense: no plug created, no assignment touched, no
    `RecorderState` mutated. A shadow pass that wrote anything would be a cutover
    rather than a rehearsal, on a production floor, unannounced.

    Reads the current assignment from the store rather than `RecorderState`, so it
    reports against what would actually be there after a restart.
    """
    known = {
        (device_id, child_id): (plug_id, alias, has_emeter)
        for plug_id, device_id, child_id, alias, has_emeter in store.list_plugs()
    }
    current_assets = {
        plug_id: asset_id
        for plug_id, _device, _child, _alias, _emeter, asset_id, _name in (
            store.list_open_assignments()
        )
    }

    unknown: list[tuple[str, str]] = []
    assignment_changes: list[tuple[str, str | None, str | None]] = []
    metering_changes: list[tuple[str, bool, bool]] = []
    seen: set[tuple[str, str]] = set()

    for entry in entries:
        device_id = entry.get("device_id") or ""
        child_id = entry.get("child_id") or ""
        if not device_id:
            continue
        key = (device_id, child_id)
        seen.add(key)
        existing = known.get(key)
        if existing is None:
            unknown.append(key)
            continue
        plug_id, _alias, has_emeter = existing

        proposed_emeter = bool(entry.get("has_emeter", True))
        if bool(has_emeter) != proposed_emeter:
            metering_changes.append((child_id, bool(has_emeter), proposed_emeter))

        # Only meaningful with a roster to resolve against; with none, `apply_devices`
        # would leave assignments alone, so there is nothing to disagree about.
        if machines:
            tag = extract_asset_tag(entry.get("alias") or "")
            proposed = tag if (tag and tag in machines) else None
            current = current_assets.get(plug_id)
            if current != proposed:
                assignment_changes.append((child_id, current, proposed))

    missing = tuple(sorted(key for key in known if key not in seen))
    return RosterDiff(
        unknown_outlets=tuple(unknown),
        missing_outlets=missing,
        assignment_changes=tuple(assignment_changes),
        metering_changes=tuple(metering_changes),
    )


class ShadowProjector:
    """The `app["tap_devices"]` callable for `serve --tap-shadow`.

    Diffs every roster frame against the live cloud-driven state with
    `shadow_devices`, logs the result, and keeps `clean_since` -- when the roster
    last *started* agreeing -- so the 48h gate is a number an operator can read
    rather than a log to grep. A single disagreement resets it: the gate is about
    the roster being right continuously, not on average.

    Reads `state.flipfix_machines` on every frame rather than capturing it once,
    because `record()` refetches FlipFix every 60s and a machine added there
    mid-rehearsal has to count.
    """

    def __init__(self, state: RecorderState, store: Store) -> None:
        self._state = state
        self._store = store
        self.frames = 0
        self.clean_since: datetime | None = None
        self.last_diff: RosterDiff | None = None

    def __call__(self, entries: list[dict]) -> None:
        now = datetime.now(UTC)
        diff = shadow_devices(self._state, self._store, entries, self._state.flipfix_machines)
        self.frames += 1
        self.last_diff = diff
        if diff.clean:
            if self.clean_since is None:
                self.clean_since = now
            log.info(
                "tap shadow: roster agrees with the cloud recorder (%d outlets; clean since %s)",
                len(entries),
                self.clean_since.isoformat(timespec="seconds"),
            )
        else:
            self.clean_since = None
            log.warning("tap shadow: roster DISAGREES -- %s", diff.describe())


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
