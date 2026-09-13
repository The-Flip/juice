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

**`live` drives it, on juice's clock.** A live row is a present-tense claim, and
it is applied through the same three helpers the cloud recorder uses
(`_cache_reading`, `_update_buffer`, `check_overload`), so the dashboard cannot
tell which collector fed it. The one thing a live row's own timestamp is used for
is noticing that tap's clock is wrong: everything downstream -- command
reconciliation, status durations, the overload window -- is stamped with the
time juice admitted the row. See `LiveProjector`.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from juice.api.v2 import tap_wire as wire
from juice.collector import PlugReading
from juice.recorder import (
    _cache_reading,
    _update_buffer,
    check_overload,
    extract_asset_tag,
    mark_device_offline,
    note_device_ok,
)
from juice.state import OFF_WATTS
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

# The live frame's clock guard. Deliberately stricter than the durable channel's
# (`tap_wire.TS_CEILING_SLACK_MS` accepts an hour of drift for `readings`): a
# live row's timestamp is never *used* -- juice stamps its own admission time --
# so the only thing it can tell us is that the box's clock is wrong, and a tap
# three minutes fast should keep its history and lose its dashboard, loudly,
# because the fix is chrony on the box rather than a looser guard.
LIVE_MAX_SKEW_S = 120.0
# How long a device may be absent from live frames before its machines read as
# unreachable. tap drops a device from live rows the moment it parks it offline
# (three failed sweeps, a few seconds); the measured worst-case gap for a
# *reachable* outlet is 2.4 s on a reconnect. 15 s clears that with room and
# still beats the cloud recorder's own three-failure threshold.
LIVE_STALE_S = 15.0
LIVE_SWEEP_SECONDS = 1.0
# How often a dashboard is told. `_readings_snapshot` classifies every
# machine's full 3600-sample buffer -- measured at ~210 ms for 33 machines --
# so publishing on every 1 Hz frame would be a fifth of the event loop for as
# long as anyone has the floor open. Every other frame is a cadence no viewer
# can tell from the cloud recorder's 6-9 s, at a tenth of the cost. The real
# fix is a snapshot that reclassifies only the tail; noted in todo.md.
LIVE_PUBLISH_INTERVAL_S = 2.0
# Shadow mode: how long tap and the cloud recorder must disagree about an outlet
# before it is a finding. The cloud view is legitimately stale by up to
# `IDLE_RECHECK_SECONDS` (60 s): `poll_once` idle-skips an ON outlet drawing
# nothing and does not refresh its cached reading while it does, so every
# morning power-on disagrees for up to a minute. 90 s is that plus the cloud's
# own poll cadence.
LIVE_DISAGREE_S = 90.0

_IDX = {name: i for i, name in enumerate(wire.ROW_FIELDS)}


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

    def __init__(
        self,
        state: RecorderState,
        store: Store,
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._state = state
        self._store = store
        self._now = now or (lambda: datetime.now(UTC))
        self.frames = 0
        self.evaluations = 0
        self.clean_since: datetime | None = None
        self.last_diff: RosterDiff | None = None
        self.last_roster: list[dict] | None = None
        # The live half. `live_clean_since` is the second gate, beside
        # `clean_since`: when tap and the cloud recorder last *started* agreeing
        # about what every outlet is doing.
        self.live_frames = 0
        self.live_skewed = 0
        self.live_evaluations = 0
        self.live_clean_since: datetime | None = None
        self._known: dict[tuple[str, str], int] | None = None
        # (outlet, kind) -> (since, description) for every mismatch currently
        # open; one that has been open for LIVE_DISAGREE_S is a finding.
        self._mismatch_since: dict[tuple[tuple[str, str], str], tuple[datetime, str]] = {}
        # Outlets tap reports on a device the cloud recorder has parked offline:
        # a reachability difference, reported but never compared.
        self._tap_only: dict[tuple[str, str], datetime] = {}
        self._skewed = False
        # When the last admitted frame arrived and how many outlets it actually
        # compared: the two facts that separate "agrees" from "nothing to say".
        self._last_live_at: datetime | None = None
        self._compared = 0

    def __call__(self, entries: list[dict]) -> None:
        self.frames += 1
        self.last_roster = list(entries)
        self._evaluate()

    def rediff(self) -> None:
        """Re-evaluate the last roster and the live streak against the current
        store, FlipFix and cloud-driven state."""
        self._known = None  # a plug created since is a plug worth comparing
        if self.last_roster is not None:
            self._evaluate()
        self._evaluate_live(self._now())

    async def live(self, rows: list[list]) -> None:
        """The `app["tap_live"]` callable in shadow mode: compare, never apply.

        Each row is held against the cloud recorder's cached reading for the
        same outlet -- relay state, and whether the outlet is drawing at all.
        Not the wattage: two collectors sampling a pinball machine seconds
        apart will never agree on a number, and the question the rehearsal
        asks is whether the dashboard would *say something different*.
        """
        self.live_frames += 1
        now = self._now()
        offset = _skew_seconds(rows, now)
        if offset is None or abs(offset) > LIVE_MAX_SKEW_S:
            self.live_skewed += 1
            self._skewed = _log_skew_transition(self._skewed, offset, "shadow")
            return
        self._skewed = _log_skew_transition(self._skewed, offset, "shadow")

        self._last_live_at = now
        known = self._known_plugs()
        state = self._state
        compared: set[tuple[str, str]] = set()
        for row in rows:
            try:
                key = (str(row[_IDX["device_id"]]), str(row[_IDX["child_id"]] or ""))
                relay_on = bool(row[_IDX["relay_on"]])
                power_mw = row[_IDX["power_mw"]]
                watts = None if power_mw is None else float(power_mw) / 1000.0
            except IndexError, TypeError, ValueError:
                continue
            plug_id = known.get(key)
            if plug_id is None:
                continue  # the roster diff already names it
            if key[0] in state.offline_since:
                self._tap_only[key] = now
                continue
            self._tap_only.pop(key, None)
            cloud = state.plug_readings.get(plug_id)
            if cloud is None:
                continue  # the cloud has not read it yet this session
            compared.add(key)

            open_kinds: set[str] = set()
            if relay_on != cloud.is_on:
                open_kinds.add("relay")
                self._note_mismatch(
                    key,
                    "relay",
                    f"relay tap={_onoff(relay_on)} cloud={_onoff(cloud.is_on)}",
                    now,
                )
            if watts is not None and cloud.watts is not None:
                tap_drawing = watts >= OFF_WATTS
                cloud_drawing = cloud.watts >= OFF_WATTS
                if tap_drawing != cloud_drawing:
                    open_kinds.add("draw")
                    self._note_mismatch(
                        key,
                        "draw",
                        f"draw tap={_drawing(tap_drawing)} cloud={_drawing(cloud_drawing)}",
                        now,
                    )
            for kind in ("relay", "draw"):
                if kind not in open_kinds:
                    self._mismatch_since.pop((key, kind), None)

        # A mismatch is only open while the outlet is still being compared. One
        # that tap stopped reporting, or that the cloud parked, would otherwise
        # age into a finding on its own and keep the gate red forever.
        self._compared = len(compared)
        for open_key in list(self._mismatch_since):
            if open_key[0] not in compared:
                del self._mismatch_since[open_key]

    def _note_mismatch(self, key: tuple[str, str], kind: str, text: str, now: datetime) -> None:
        since = self._mismatch_since.get((key, kind), (now, text))[0]
        self._mismatch_since[(key, kind)] = (since, text)

    def _known_plugs(self) -> dict[tuple[str, str], int]:
        if self._known is None:
            self._known = {
                (device_id, child_id): plug_id
                for plug_id, device_id, child_id, _alias, _emeter in self._store.list_plugs()
            }
        return self._known

    def _evaluate_live(self, now: datetime) -> None:
        """The live verdict, on the 60 s tick.

        Three ways to have nothing to say, each said out loud so a silent or
        skewed channel can never read as green: no frame has arrived (tap
        suppresses live frames while catching up on backfill, which is how a
        fresh rehearsal starts), none has arrived for `LIVE_STALE_S` (tap is
        gone), or the last one compared no outlet at all (every row skewed,
        unknown, tap-only, or not yet read by the cloud). Each of those also
        stops the clock: the gate is about *continuous* agreement, and an hour
        nobody was watching is not an hour of agreement.
        """
        self.live_evaluations += 1
        not_running: str | None = None
        if self._last_live_at is None:
            not_running = "no live frames yet (tap suppresses them while catching up on backfill)"
        elif (now - self._last_live_at).total_seconds() > LIVE_STALE_S:
            not_running = f"no live frames for {(now - self._last_live_at).total_seconds():.0f}s"
        elif self._skewed:
            not_running = "live frames are being dropped for clock skew"
        elif self._compared == 0:
            not_running = "the last live frame compared no outlet"
        if not_running is not None:
            self.live_clean_since = None
            log.info("tap shadow: %s; the live gate is not running", not_running)
            return
        # Forget tap-only outlets tap has stopped reporting too.
        for key, seen in list(self._tap_only.items()):
            if (now - seen) > STALE_AFTER:
                del self._tap_only[key]
        persistent = [
            (key, since, text)
            for (key, _kind), (since, text) in self._mismatch_since.items()
            if (now - since).total_seconds() >= LIVE_DISAGREE_S
        ]
        if persistent:
            self.live_clean_since = None
            log.warning(
                "tap shadow: live DISAGREES -- %s",
                "; ".join(
                    f"outlet {key[0]}/{key[1]} {text} for {(now - since).total_seconds():.0f}s"
                    for key, since, text in sorted(persistent)
                ),
            )
            return
        if self.live_clean_since is None:
            self.live_clean_since = now
        tap_only = sorted(self._tap_only)
        log.info(
            "tap shadow: live agrees with the cloud recorder (%d outlets compared, %d frames; "
            "%d reachable by tap only%s; clean since %s)",
            self._compared,
            self.live_frames,
            len(tap_only),
            (" -- " + ", ".join(f"{d}/{c}" for d, c in tap_only[:6])) if tap_only else "",
            self.live_clean_since.isoformat(timespec="seconds"),
        )

    def _evaluate(self) -> None:
        assert self.last_roster is not None
        now = self._now()
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


# --- the live projection ------------------------------------------------------


def _onoff(on: bool) -> str:
    return "on" if on else "off"


def _drawing(drawing: bool) -> str:
    return "drawing" if drawing else "idle"


def _skew_seconds(rows: list[list], now: datetime) -> float | None:
    """tap's clock minus juice's, from the frame's synthesised timestamp.

    Every row in a live frame carries the same "now", so the first one speaks
    for the frame. None means the frame carries no usable timestamp at all.
    """
    if not rows:
        return 0.0
    try:
        ts_ms = rows[0][_IDX["ts_ms"]]
        return float(ts_ms) / 1000.0 - now.timestamp()
    except IndexError, TypeError, ValueError:
        return None


def _log_skew_transition(was_skewed: bool, offset: float | None, who: str) -> bool:
    """One line entering skew and one leaving it, never one per frame."""
    skewed = offset is None or abs(offset) > LIVE_MAX_SKEW_S
    if skewed and not was_skewed:
        if offset is None:
            log.error("tap live (%s): frames carry no usable timestamp; dropping them", who)
        else:
            log.error(
                "tap live (%s): clock skew of %+.0fs exceeds %.0fs; dropping live frames until "
                "it clears. Check the time on the tap box.",
                who,
                offset,
                LIVE_MAX_SKEW_S,
            )
    elif was_skewed and not skewed:
        log.warning(
            "tap live (%s): clock skew cleared (%+.0fs); applying live frames again",
            who,
            offset or 0.0,
        )
    return skewed


def live_reading(row: list, alias: str, has_emeter: bool) -> PlugReading:
    """A live row as the `PlugReading` the cloud recorder would have cached.

    `poll_once` has three shapes and this reproduces them: a metered outlet
    that is off is all zeros (a tap reads the meter regardless and may report a
    few milliwatts of nothing); a meterless outlet has no watts whichever way
    its relay is; an on, metered outlet carries what the meter said -- and null
    stays null, because since #101 an unmeasured reading is unknown, not zero.
    `current_ma` and `energy_wh` are always null on the live wire.
    """
    relay_on = bool(row[_IDX["relay_on"]])
    power_mw = row[_IDX["power_mw"]]
    voltage_mv = row[_IDX["voltage_mv"]]
    child_id = str(row[_IDX["child_id"]] or "")
    if not has_emeter:
        return PlugReading(child_id, alias, relay_on, None, None, None, None)
    if not relay_on:
        return PlugReading(child_id, alias, False, 0.0, 0.0, 0.0, 0.0)
    watts = None if power_mw is None else float(power_mw) / 1000.0
    voltage = None if voltage_mv is None else float(voltage_mv) / 1000.0
    return PlugReading(child_id, alias, True, watts, voltage, None, None)


@dataclass(slots=True)
class LiveOutcome:
    applied: int = 0
    unknown: int = 0
    bad: int = 0
    published: bool = False
    devices: set[str] = field(default_factory=set)


async def apply_live(
    state: RecorderState,
    store: Store,
    rows: list[list],
    *,
    now: datetime,
    publish: bool = True,
) -> LiveOutcome:
    """Project one live frame onto the floor's current state.

    Every device in the frame is marked reachable first -- presence is the
    proof, and doing it up front rather than row by row means a slow
    `check_overload` mid-frame cannot let the sweep take a device offline and
    then have a later row of the same frame bring it back with pre-outage data.
    Then per row: resolve the outlet to a plug juice already knows -- **never**
    `ensure_plug`, only the roster has an alias -- and the same three calls
    `poll_once` makes. `now` is juice's clock, and it is what every consumer
    sees; the row's own timestamp was only ever evidence about tap's clock, and
    `LiveProjector` has already judged it.
    """
    index = {
        (device_id, child_id): plug_id for plug_id, (device_id, child_id, _a) in state.plugs.items()
    }
    outcome = LiveOutcome()
    parsed: list[tuple[str, tuple[str, str], list]] = []
    for row in rows:
        try:
            device_id = str(row[_IDX["device_id"]])
            parsed.append((device_id, (device_id, str(row[_IDX["child_id"]] or "")), row))
        except IndexError, TypeError:
            outcome.bad += 1
    for device_id in {device_id for device_id, _key, _row in parsed}:
        note_device_ok(state, device_id)

    for device_id, key, row in parsed:
        plug_id = index.get(key)
        if plug_id is None:
            outcome.unknown += 1
            continue
        try:
            has_emeter = state.plug_has_emeter.get(plug_id, True)
            reading = live_reading(row, state.plugs[plug_id][2], has_emeter)
        except IndexError, TypeError, ValueError:
            outcome.bad += 1
            continue
        outcome.devices.add(device_id)
        _cache_reading(state, plug_id, reading, now, device_id)
        if reading.watts is not None:
            _update_buffer(state, plug_id, reading.watts)
            await check_overload(state, store, plug_id, now, reading.watts)
        outcome.applied += 1

    if publish and state.event_subscribers:
        from juice.server import _publish, _readings_snapshot

        _publish(state, {"type": "readings", "machines": _readings_snapshot(state)})
        outcome.published = True
    return outcome


class LiveProjector:
    """The `app["tap_live"]` callable for a tap-driven juice.

    Three rules, each the answer to a way this could quietly lie:

    - **The frame is judged by juice's clock and applied on it.** A frame whose
      timestamp is more than `LIVE_MAX_SKEW_S` from now is dropped and counted;
      the rows that are applied get `now` as their timestamp.
    - **A frame is applied in its own task, and the latest one wins.**
      `check_overload` can end in an actuation with a minute of retries;
      awaited from the receive loop that would hold every `readings` ack and
      make tap resend. A frame arriving while the previous one is still
      applying waits in a slot of one; a newer arrival replaces it, and the
      replaced frame is counted as dropped. Live frames are droppable by
      definition, and what matters is that the floor shows the newest one.
      An apply that has been running longer than `LIVE_STALE_S` is a hang
      (the cloud actuation path has no timeout); the sweep cancels it and
      says so, rather than letting the floor freeze as "current".
    - **Offline is absence.** tap omits a device it cannot reach from live rows
      entirely, so `sweep` -- run at 1 Hz by `live_loop` -- takes a device
      offline once it has been missing for `LIVE_STALE_S`. A dropped uplink
      therefore turns the whole floor unreachable within seconds, which is the
      honest answer.
    """

    def __init__(
        self,
        state: RecorderState,
        store: Store,
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._state = state
        self._store = store
        self._now = now or (lambda: datetime.now(UTC))
        self._started = self._now()
        self._inflight: asyncio.Task[None] | None = None
        self._inflight_since: datetime | None = None
        self._pending: tuple[list[list], datetime] | None = None
        self._last_published: datetime | None = None
        self._skewed = False
        self.frames = 0
        self.applied_frames = 0
        self.dropped_skew = 0
        self.dropped_busy = 0
        self.unknown_rows = 0
        self.sweeps = 0
        # device_id -> when it last appeared in an admitted frame.
        self.last_seen: dict[str, datetime] = {}

    async def __call__(self, rows: list[list]) -> None:
        self.frames += 1
        if not rows:
            return
        now = self._now()
        offset = _skew_seconds(rows, now)
        self._skewed = _log_skew_transition(self._skewed, offset, "collector")
        if self._skewed:
            self.dropped_skew += 1
            return
        # Presence in an admitted frame is proof of reachability whether or not
        # this frame gets applied.
        for row in rows:
            try:
                self.last_seen[str(row[_IDX["device_id"]])] = now
            except IndexError, TypeError:
                continue
        if self._inflight is not None and not self._inflight.done():
            if self._pending is not None:
                self.dropped_busy += 1
            self._pending = (rows, now)
            return
        self._start(rows, now)

    def _start(self, rows: list[list], now: datetime) -> None:
        self._inflight_since = now
        self._inflight = asyncio.create_task(self._apply(rows, now), name="tap-live-apply")

    async def _apply(self, rows: list[list], now: datetime) -> None:
        try:
            publish = (
                self._last_published is None
                or (now - self._last_published).total_seconds() >= LIVE_PUBLISH_INTERVAL_S
            )
            outcome = await apply_live(self._state, self._store, rows, now=now, publish=publish)
        except Exception:  # noqa: BLE001 - one bad frame must not stop the next
            log.warning("tap live: applying a frame failed", exc_info=True)
        else:
            self.applied_frames += 1
            self.unknown_rows += outcome.unknown
            if outcome.published:
                self._last_published = now
        finally:
            self._inflight_since = None
            if self._pending is not None:
                rows, now = self._pending
                self._pending = None
                self._start(rows, now)

    async def settle(self) -> None:
        """Wait for the in-flight apply and anything queued behind it. For
        tests and shutdown."""
        while self._inflight is not None and not self._inflight.done():
            try:
                await self._inflight
            except asyncio.CancelledError:
                if asyncio.current_task().cancelling():  # type: ignore[union-attr]
                    raise

    def sweep(self, now: datetime | None = None) -> bool:
        """Take devices unseen for `LIVE_STALE_S` offline. True if any changed."""
        now = now or self._now()
        self.sweeps += 1
        state = self._state
        if (
            self._inflight is not None
            and not self._inflight.done()
            and self._inflight_since is not None
            and (now - self._inflight_since).total_seconds() > LIVE_STALE_S
        ):
            log.error(
                "tap live: a frame has been applying for %.0fs; cancelling it. The floor "
                "was frozen on stale readings for that long.",
                (now - self._inflight_since).total_seconds(),
            )
            self._inflight.cancel()
        devices = {info[0] for info in state.plugs.values()} | set(self.last_seen)
        changed = False
        for device_id in sorted(devices):
            if device_id in state.offline_since:
                continue
            seen = self.last_seen.get(device_id, self._started)
            if (now - seen).total_seconds() > LIVE_STALE_S:
                mark_device_offline(
                    state, device_id, now, reason=f"no live rows for {LIVE_STALE_S:.0f}s"
                )
                changed = True
        if changed and state.event_subscribers:
            from juice.server import _publish, _readings_snapshot

            _publish(state, {"type": "readings", "machines": _readings_snapshot(state)})
        return changed


async def live_loop(projector: LiveProjector, *, interval: float = LIVE_SWEEP_SECONDS) -> None:
    """The 1 Hz housekeeping the cloud recorder's poll loop used to do.

    Two things, both of which must happen precisely when frames have *stopped*
    and so cannot ride on frame arrival: the staleness sweep, and
    `commands.sweep()`, which is what turns an unconfirmed command into
    `timed_out`.
    """
    while True:
        await asyncio.sleep(interval)
        try:
            projector.sweep()
        except Exception:  # noqa: BLE001 - a failed sweep must not kill the loop
            log.warning("tap live: staleness sweep failed", exc_info=True)
        try:
            projector._state.commands.sweep()
        except Exception:  # noqa: BLE001
            log.warning("tap live: command sweep failed", exc_info=True)
