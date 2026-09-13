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

import asyncio
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
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

# How long an outlet may go unheard-from before shadow mode stops expecting tap
# to report it. A week: comfortably past any device outage worth waiting out, and
# far short of the months the two dead plugs in production have been silent.
STALE_AFTER = timedelta(days=7)


def _metered(entry: dict) -> bool:
    """`has_emeter` with the safe default for both absent *and* null.

    `bool(None)` is False, and False is the wrong error here: an outlet wrongly
    marked unmetered vanishes from every energy chart. An older tap omits the
    field; a broken one might send null. Both mean "assume metered".
    """
    value = entry.get("has_emeter")
    return True if value is None else bool(value)


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
    # Outlets juice has heard from *recently* and tap did not mention. Those
    # devices are ones tap cannot reach, so at cutover their machines go dark
    # rather than move.
    missing_outlets: tuple[tuple[str, str], ...] = ()
    # Outlets juice knows but has not heard from in `STALE_AFTER`: dead, or on a
    # strip that was swapped out. Named, because an operator should know -- but
    # not counted against `clean`, or a perfect roster would read as disagreeing
    # for as long as the dead rows exist. Two such plugs sit in production today.
    stale_outlets: tuple[tuple[str, str], ...] = ()
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
        for device_id, child_id in self.stale_outlets:
            parts.append(
                f"outlet {device_id}/{child_id} absent from tap's roster but stale here "
                f"(no reading in {STALE_AFTER.days}d; not counted)"
            )
        for child_id, current, proposed in self.assignment_changes:
            parts.append(
                f"outlet {child_id} would move from {current or 'unassigned'} to "
                f"{proposed or 'unassigned'}"
            )
        for child_id, was, becomes in self.metering_changes:
            parts.append(f"outlet {child_id} metering would change from {was} to {becomes}")
        return "; ".join(parts)


def shadow_devices(
    store: Store,
    entries: list[dict],
    machines: Mapping[str, Any],
    *,
    now: datetime | None = None,
) -> RosterDiff:
    """Diff tap's roster against what juice durably holds, changing nothing.

    Runs with the cloud recorder still authoritative, so it must be side-effect
    free in the strictest sense: no plug created, no assignment touched. A shadow
    pass that wrote anything would be a cutover rather than a rehearsal, on a
    production floor, unannounced.

    Deliberately takes no `RecorderState`: it diffs against the **store**, because
    in-memory state is whatever the cloud recorder happens to hold right now,
    while the store is what survives a restart and what `hydrate_assignments`
    reads back. A diff against memory would report clean for a roster that
    diverges the moment juice restarts.
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

        proposed_emeter = _metered(entry)
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

    # "Missing" means the cloud recorder heard from it recently and tap did not
    # report it. A plug silent for longer than STALE_AFTER is dead as far as
    # either collector is concerned, and is reported as such rather than held
    # against the roster.
    live = store.plugs_reporting_since((now or datetime.now(UTC)) - STALE_AFTER)
    absent = sorted(key for key in known if key not in seen)
    missing = tuple(key for key in absent if known[key][0] in live)
    stale = tuple(key for key in absent if known[key][0] not in live)
    return RosterDiff(
        unknown_outlets=tuple(unknown),
        missing_outlets=missing,
        stale_outlets=stale,
        assignment_changes=tuple(assignment_changes),
        metering_changes=tuple(metering_changes),
    )


class ShadowProjector:
    """The `app["tap_devices"]` callable for `serve --tap-shadow`.

    Diffs the roster against what juice durably holds, logs the result, and
    keeps `clean_since` -- when the roster last *started* agreeing -- so the 48h
    gate is a number an operator can read rather than a log to grep. A single
    disagreement resets it: the gate is about the roster being right
    continuously, not on average.

    **Two things make "continuously" true rather than aspirational.** tap
    re-sends its roster only when it changes, so a healthy tap sends one frame per
    connection and the verdict on that frame would otherwise stand until the next
    relabel. So the projector keeps the last roster and `rediff` re-evaluates it
    on a timer (`shadow_loop`), against whatever FlipFix and the store say *now*.
    And a frame that arrives before juice has a FlipFix roster -- which is the
    normal case at startup, since the server is up before `record()` has fetched
    it -- is **not** a clean verdict: the assignment half of the diff was skipped,
    so `clean_since` stays unset until a real comparison has happened.
    """

    def __init__(self, state: RecorderState, store: Store) -> None:
        self._state = state
        self._store = store
        self.frames = 0
        self.evaluations = 0
        self.clean_since: datetime | None = None
        self.last_diff: RosterDiff | None = None
        self.last_roster: list[dict] | None = None

    def __call__(self, entries: list[dict]) -> None:
        self.frames += 1
        self.last_roster = list(entries)
        self._evaluate()

    def rediff(self) -> None:
        """Re-evaluate the last roster against the current store and FlipFix."""
        if self.last_roster is not None:
            self._evaluate()

    def _evaluate(self) -> None:
        assert self.last_roster is not None
        now = datetime.now(UTC)
        machines = self._state.flipfix_machines
        self.evaluations += 1
        diff = shadow_devices(self._store, self.last_roster, machines)
        self.last_diff = diff

        if not machines:
            # Not a verdict either way. Say so every time, so a rehearsal that
            # never reached FlipFix cannot read as a green gate.
            log.info(
                "tap shadow: %d outlets received; NO FlipFix roster yet, so assignments "
                "were not compared and the clock is not running%s",
                len(self.last_roster),
                f" ({diff.describe()})" if not diff.clean else "",
            )
            return

        if diff.clean:
            if self.clean_since is None:
                self.clean_since = now
            log.info(
                "tap shadow: roster agrees with the cloud recorder (%d outlets; clean since %s)",
                len(self.last_roster),
                self.clean_since.isoformat(timespec="seconds"),
            )
        else:
            self.clean_since = None
            log.warning("tap shadow: roster DISAGREES -- %s", diff.describe())


# Matches the FlipFix refresh cadence in `record()`, so a verdict is never more
# than a minute behind whichever side changed.
SHADOW_REDIFF_SECONDS = 60.0


async def shadow_loop(
    projector: ShadowProjector, *, interval: float = SHADOW_REDIFF_SECONDS
) -> None:
    """Re-diff the last roster on a timer, forever.

    Without this the verdict on a roster stands until tap next changes it, which
    for a healthy fleet is never -- a frame judged before FlipFix answered, or
    during a momentary disagreement, would be the verdict for the whole
    rehearsal.
    """
    while True:
        await asyncio.sleep(interval)
        try:
            projector.rediff()
        except Exception:  # noqa: BLE001 - a failed re-diff must not kill the server
            log.warning("tap shadow: re-diff failed", exc_info=True)


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
        # Per entry, and each one wrapped, exactly as `refresh_metadata` isolates
        # one device's failure from the rest: a single malformed entry must not
        # cost every later outlet its alias. tap re-sends the roster only when it
        # changes, so entries lost here would not come back on their own.
        try:
            _apply_entry(state, store, entry, machines, ts)
        except Exception:
            log.warning("tap roster: skipping an unusable entry %r", entry, exc_info=True)


def _apply_entry(
    state: RecorderState | None,
    store: Store,
    entry: dict,
    machines: Mapping[str, Any],
    ts: datetime,
) -> None:
    device_id = str(entry.get("device_id") or "")
    child_id = str(entry.get("child_id") or "")
    alias = str(entry.get("alias") or "")
    if not device_id:
        # Nothing can be keyed off a missing device id, and a plug created for
        # one would be unreachable forever.
        return
    # Absent means metered: `refresh_hourly_usage` filters on this, so the wrong
    # FALSE hides a real outlet from every energy chart. An older tap omits the
    # field entirely.
    has_emeter = _metered(entry)
    plug_id = store.ensure_plug(device_id, child_id, alias, has_emeter=has_emeter)

    if state is not None:
        state.plugs[plug_id] = (device_id, child_id, alias)
        state.plug_has_emeter[plug_id] = has_emeter
        # Only when tap sent one: an older tap omits it, and overwriting a known
        # strip name with "" would blank the dashboard's heading.
        device_alias = str(entry.get("device_alias") or "")
        if device_alias:
            state.strip_aliases[device_id] = device_alias

    if not machines:
        return

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
