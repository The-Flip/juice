"""The live and roster projections for the `tap` collector.

`juice/api/v2/ingest.py` is a protocol shim and stays one: it parses frames and
answers them. This module is what a frame *means* -- the floor's live state and
its roster, not an API concern. Keeping it out of `juice/api/v2/`
matters for a practical reason as well as a tidy one: the module that owns the
floor's live state should not be scheduled for deletion alongside the v1 API, and
`api_v2.md` describes `/api/v2/ingest` as a wire protocol, which stops being true
if the receiver also does machine assignment.

**`readings` never drives any of this.** That channel is replayable and allowed to
be days behind, so it goes to DuckDB and nowhere else -- feeding it here would run
overload detection across history and fire shutdowns for events that ended on
Tuesday. Only `devices` and `live` reach this module.

**`live` drives it, on juice's clock.** A live row is a present-tense claim, and
it is applied through the same helpers every reading has always gone through
(`cache_reading`, `update_buffer` here, `check_overload` in `juice/overload.py`), so the dashboard cannot tell how it was collected. The one thing a live row's own timestamp is used for
is noticing that tap's clock is wrong: everything downstream -- command
reconciliation, status durations, the overload window -- is stamped with the
time juice admitted the row. See `LiveProjector`.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
import uuid
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from juice.api.v2 import tap_wire as wire
from juice.commands import ATTEMPT_BUDGET_S
from juice.identity import extract_asset_tag
from juice.overload import (
    MAX_GAP_S,
    cancel_overload_shutdowns,
    check_overload,
    configure_overload_mode,
)
from juice.readings import PlugReading
from juice.store import Store

if TYPE_CHECKING:  # pragma: no cover - import cycle; RecorderState lives in server
    from juice.rollups import RollupWorker
    from juice.server import RecorderState

log = logging.getLogger(__name__)

# Said once per process rather than once per frame: tap re-sends its roster
# whenever it changes, and a FlipFix outage would otherwise produce this line
# every 60 seconds for its duration.
_warned_empty_roster = False

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
# still beats the three failed reads the cloud recorder used to wait for.
LIVE_STALE_S = 15.0
LIVE_SWEEP_SECONDS = 1.0
# The least time between two dashboard ticks. Every frame at tap's 1 Hz is a
# tick, and so is the out-of-cadence frame tap sends the moment a command
# moves a relay -- the one the operator's button is waiting on. A frame that
# lands inside the interval is held, not dropped: its tick goes out when the
# interval elapses, so a burst costs one snapshot and no state waits longer
# than this. Well under 1 s so jitter on the 1 Hz cadence never holds a
# regular frame; at ~2 ms a snapshot (`state.classify_last`, since #112) four
# a second is nothing, where the full classification's ~210 ms was a fifth
# of the event loop at 1 Hz and needed the old 1 s gate.
LIVE_PUBLISH_INTERVAL_S = 0.25
# How often the live channel's counters go to the log -- the same cadence as
# the ingest summary, so the two lines sit together.
LIVE_SUMMARY_SECONDS = 300.0
# ~3 minutes of arrivals at 55 outlets a second: enough for a p99 that means
# something, bounded so it never matters.
LIVE_GAP_SAMPLES = 10_000

# Power control. One attempt waits exactly the command contract's per-attempt
# budget for tap's `command_result` (see `juice.commands.ATTEMPT_BUDGET_S`);
# silence past that is a `TimeoutError`, which `call_with_retry` retries with
# the *same* command id, so tap answers from its cache or its in-flight task
# rather than throwing the relay again. The six retries are really juice
# polling tap for the outcome, for the 23.5 s the command contract promises.
# tap's own worst case on one device is about the same (4 attempts x 5 s plus
# backoff), so a device that answers on tap's last try can land its "ok" a
# second after juice has recorded `failed`: the late result is dropped, the
# relay moves anyway, and the next live reading shows the truth. Accepted --
# the alternative is a client told `timed_out` while the server still tries.
COMMAND_RESULT_TIMEOUT_S = ATTEMPT_BUDGET_S
# Errors tap reports that are the device's, not the command's: another attempt
# may find the strip answering. Matched on the exception name tap prefixes its
# error text with (`tap/uplink.py:_apply_command`). `ConnectionError` is the
# common one -- the poller raises it *before* its own retries whenever it has
# dropped the device, so without this a strip in a reconnect window gets one
# shot where the command contract promises 23.5 s of them.
RETRYABLE_TAP_ERRORS = ("ConnectionError:", "TimeoutError:", "TransientError:", "OSError:")
# `expires_at` on the wire: tap refuses a command it first sees after this, so
# a frame that sat in a dead socket cannot power a machine on later. Longer
# than the whole retry budget, so a redelivery is never refused as stale
# while the operator is still watching the spinner.
COMMAND_EXPIRES_S = 30.0
LATENCY_SAMPLES = 256

_IDX = {name: i for i, name in enumerate(wire.ROW_FIELDS)}


# The housekeeping cadence: FlipFix roster, operator state, reconciliation.
IDLE_RECHECK_SECONDS = 60


# ---------------------------------------------------------------------------
# What a reading does to RecorderState. Every collector's readings have gone
# through these; the live projection is what feeds them now.
# ---------------------------------------------------------------------------


def mark_device_offline(state: RecorderState, device_id: str, ts: datetime, *, reason: str) -> None:
    """Take a device offline: its machines render as unreachable from `ts`.

    Called when a device has stopped appearing in live frames
    (`collector_tap.LiveProjector.sweep`), so "offline" means one thing.
    """
    state.offline_since[device_id] = ts
    # Stamp the transition. track_status otherwise only runs after a
    # *successful* read, so these plugs would keep the timestamp from
    # whenever they were last reachable — and the floor would report
    # "unreachable" with a duration that is hours stale or minutes short.
    # A misleading duration is worse than none: it's what an operator
    # triages on.
    from juice.server import track_status

    for plug_id, info in state.plugs.items():
        if info[0] != device_id:
            continue
        track_status(
            state,
            plug_id,
            state.plug_readings.get(plug_id),
            has_emeter=state.plug_has_emeter.get(plug_id, True),
            offline=True,
            now=ts,
        )
    log.warning("Device %s offline (%s)", device_id, reason)


def note_device_ok(state: RecorderState | None, device_id: str) -> None:
    """Record a successful device read; clear offline status and log recovery."""
    if state is None:
        return
    if device_id in state.offline_since:
        log.info("Device %s back online", device_id)
    state.offline_since.pop(device_id, None)


def hydrate_assignments(state: RecorderState | None, store: Store) -> None:
    """Pre-fill in-memory assignment state from the DB's open assignments.

    On a cold start this makes every currently-assigned machine appear at once
    — before any roster frame has arrived, and including outlets tap has not
    seen for longer than its roster remembers. Live readings and re-assignments
    layer on top as frames arrive.
    `year` isn't persisted, so hydrated entries carry None.

    All known plugs hydrate too (not just assigned ones), so the strip outlet
    map shows every outlet of an offline-at-boot strip.
    """
    if state is None:
        return
    for plug_id, device_id, child_id, alias, has_emeter in store.list_plugs():
        state.plugs[plug_id] = (device_id, child_id, alias)
        state.plug_has_emeter[plug_id] = has_emeter
    for (
        plug_id,
        _device_id,
        _child_id,
        _alias,
        _has_emeter,
        asset_id,
        name,
    ) in store.list_open_assignments():
        state.assignments[plug_id] = (name, asset_id, None)
    state.lock_modes = store.get_lock_modes()
    state.power_baselines = store.get_power_baselines()
    state.strip_names = store.get_strip_names()
    state.strip_orders = store.get_strip_orders()
    state.circuit_devices = store.get_circuit_devices()
    state.circuits = {c["circuit_id"]: c for c in store.list_circuits()}


def cache_reading(
    recorder_state: RecorderState,
    plug_id: int,
    reading: PlugReading,
    ts: datetime,
    device_id: str = "",
) -> None:
    """Cache a fresh reading and offer it to any command awaiting confirmation.

    Kept as one helper so a reading can never reach `plug_readings` without its
    timestamp: command reconciliation depends on being able to tell a reading
    that postdates a command from one cached before it, and a missing timestamp
    would silently fall back to confirming against stale data.
    """
    recorder_state.plug_readings[plug_id] = reading
    recorder_state.plug_reading_ts[plug_id] = ts
    try:
        from juice.server import track_status

        recorder_state.commands.reconcile(plug_id, relay_on=reading.is_on, reading_ts=ts)
        # How long a machine has held its status is what makes the Problems
        # section triageable ("no draw for 4 min" vs a bare flag), and it has to
        # accumulate here rather than be derived when someone happens to look.
        track_status(
            recorder_state,
            plug_id,
            reading,
            has_emeter=recorder_state.plug_has_emeter.get(plug_id, True),
            offline=device_id in recorder_state.offline_since,
            now=ts,
        )
    except Exception:  # noqa: BLE001 — never let bookkeeping break the poll loop
        log.warning("Reading bookkeeping failed for plug %d", plug_id, exc_info=True)


def update_buffer(
    recorder_state: RecorderState,
    plug_id: int,
    watts: float,
) -> None:
    """Append a watts value to the ring buffer for a plug."""
    from juice.server import BUFFER_SIZE

    buf = recorder_state.watt_buffers.get(plug_id)
    if buf is None:
        buf = deque(maxlen=BUFFER_SIZE)
        recorder_state.watt_buffers[plug_id] = buf
    buf.append(watts)


def _metered(entry: dict) -> bool:
    """`has_emeter` with the safe default for both absent *and* null.

    `bool(None)` is False, and False is the wrong error here: an outlet wrongly
    marked unmetered vanishes from every energy chart. An older tap omits the
    field; a broken one might send null. Both mean "assume metered".
    """
    value = entry.get("has_emeter")
    return True if value is None else bool(value)


def apply_devices(
    state: RecorderState | None,
    store: Store,
    entries: list[dict],
    machines: Mapping[str, Any],
    ts: datetime,
    *,
    control: TapControl | None = None,
) -> None:
    """Project tap's roster onto plugs, machines and assignments.

    With `control`, every outlet in the roster also gets a `TapPlug` in
    `state.plug_objects`, the only place they come from. Without it (a bare
    `create_app`, or a store-only caller with no state) nothing is installed,
    so a power button never sends frames to nobody.

    Driven by a frame rather than a device poll. The alias is the whole point:
    ingest creates plugs
    for outlets it has never seen with an **empty** alias, deliberately, because
    it has no roster to write -- so until this runs, a tap-only juice shows those
    outlets unassigned however well their readings are stored.

    **An empty `machines` skips assignment entirely.** Assignment closes the
    assignment of every outlet whose tag is not in the roster, which was safe
    under the cloud recorder only by accident of ordering: it awaited FlipFix
    before its first refresh. A frame has no such ordering -- it arrives when tap connects,
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
        # Per entry, and each one wrapped, so one device's failure is isolated
        # from the rest: a single malformed entry must not
        # cost every later outlet its alias. tap re-sends the roster only when it
        # changes, so entries lost here would not come back on their own.
        try:
            _apply_entry(state, store, entry, machines, ts, control)
        except Exception:
            log.warning("tap roster: skipping an unusable entry %r", entry, exc_info=True)


def _apply_entry(
    state: RecorderState | None,
    store: Store,
    entry: dict,
    machines: Mapping[str, Any],
    ts: datetime,
    control: TapControl | None = None,
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
        if control is not None:
            # Renamed in place rather than replaced: tap re-sends the roster
            # on every change, and a command can be mid-flight on this object.
            existing = state.plug_objects.get(plug_id)
            if isinstance(existing, TapPlug) and existing.same_outlet(device_id, child_id):
                existing.alias = alias
            else:
                state.plug_objects[plug_id] = TapPlug(control, device_id, child_id, alias)

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


def _log_skew_transition(was_skewed: bool, offset: float | None) -> bool:
    """One line entering skew and one leaving it, never one per frame."""
    skewed = offset is None or abs(offset) > LIVE_MAX_SKEW_S
    if skewed and not was_skewed:
        if offset is None:
            log.error("tap live: frames carry no usable timestamp; dropping them")
        else:
            log.error(
                "tap live: clock skew of %+.0fs exceeds %.0fs; dropping live frames until "
                "it clears. Check the time on the tap box.",
                offset,
                LIVE_MAX_SKEW_S,
            )
    elif was_skewed and not skewed:
        log.warning(
            "tap live: clock skew cleared (%+.0fs); applying live frames again",
            offset or 0.0,
        )
    return skewed


def live_reading(row: list, alias: str, has_emeter: bool) -> PlugReading:
    """A live row as the `PlugReading` the floor caches.

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
    proof, and doing it up front rather than row by row means a slow apply
    (a `_readings_snapshot` publish mid-frame) cannot let the sweep take a
    device offline and then have a later row of the same frame bring it back
    with pre-outage data.
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
        cache_reading(state, plug_id, reading, now, device_id)
        if reading.watts is not None:
            update_buffer(state, plug_id, reading.watts)
            await check_overload(state, store, plug_id, now, reading.watts)
        outcome.applied += 1

    if publish:
        outcome.published = publish_readings(state)
    return outcome


def publish_readings(state: RecorderState) -> bool:
    """The SSE `readings` tick: every machine's snapshot, to whoever is
    listening. False when nobody is, and the snapshot is not built."""
    if not state.event_subscribers:
        return False
    from juice.server import _publish, _readings_snapshot

    _publish(state, {"type": "readings", "machines": _readings_snapshot(state)})
    return True


class GapMeter:
    """Per-outlet inter-arrival across consecutive live frames, against the
    overload gap bound.

    The bound (`overload.MAX_GAP_S`) was picked from a LAN measurement
    against a fake server; this is the same number on the real path, and the
    one to read before overload leaves `shadow` under tap. An outlet missing
    from intervening frames was *absent* -- its device parked -- which is the
    staleness sweep's business and says nothing about the bound; only a gap
    across consecutive frames is uplink latency, the thing the bound is for.
    """

    def __init__(self) -> None:
        # outlet -> (when, in which frame)
        self._seen: dict[tuple[str, str], tuple[datetime, int]] = {}
        self._gaps: deque[float] = deque(maxlen=LIVE_GAP_SAMPLES)
        self.frames = 0
        self.over_bound = 0
        self.absences = 0

    def observe(self, rows: list[list], now: datetime) -> None:
        self.frames += 1
        for row in rows:
            try:
                key = (str(row[_IDX["device_id"]]), str(row[_IDX["child_id"]]))
            except IndexError, TypeError:
                continue
            seen = self._seen.get(key)
            self._seen[key] = (now, self.frames)
            if seen is None:
                continue
            seen_at, seen_frame = seen
            if self.frames - seen_frame > 1:
                self.absences += 1
                continue
            gap = (now - seen_at).total_seconds()
            self._gaps.append(gap)
            if gap > MAX_GAP_S:
                self.over_bound += 1

    def describe(self) -> str:
        if not self._gaps:
            return "gaps: none measured yet"
        ordered = sorted(self._gaps)

        def pct(p: float) -> float:
            return ordered[min(len(ordered) - 1, int(round(p * (len(ordered) - 1))))]

        return (
            f"gaps p50 {pct(0.5):.2f}s p99 {pct(0.99):.2f}s max {ordered[-1]:.2f}s over "
            f"{len(ordered)} arrivals; {self.over_bound} ever over the {MAX_GAP_S:.0f}s "
            f"overload bound (cumulative); {self.absences} outlet absences (devices parked, "
            "not counted)"
        )


class LiveProjector:
    """The `app["tap_live"]` callable for a tap-driven juice.

    Three rules, each the answer to a way this could quietly lie:

    - **The frame is judged by juice's clock and applied on it.** A frame whose
      timestamp is more than `LIVE_MAX_SKEW_S` from now is dropped and counted;
      the rows that are applied get `now` as their timestamp.
    - **A frame is applied in its own task, and the latest one wins.**
      An apply publishes the SSE tick, which builds every machine's snapshot;
      awaited from the receive loop that would hold every `readings` ack and
      make tap resend. (An overload *shutdown* is not part of the
      apply at all: `check_overload` starts it on a task of its own, so its
      retries are neither in this task nor cancellable as its hang.) A frame
      arriving while the previous one is still
      applying waits in a slot of one; a newer arrival replaces it, and the
      replaced frame is counted as dropped. Live frames are droppable by
      definition, and what matters is that the floor shows the newest one.
      An apply that has been running longer than `LIVE_STALE_S` is a hang;
      the sweep cancels it and says so, rather than letting the floor freeze
      as "current".
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
        publish_interval_s: float = LIVE_PUBLISH_INTERVAL_S,
    ) -> None:
        self._state = state
        self._store = store
        self._now = now or (lambda: datetime.now(UTC))
        self._started = self._now()
        self._inflight: asyncio.Task[None] | None = None
        self._inflight_since: datetime | None = None
        self._pending: tuple[list[list], datetime] | None = None
        self._publish_interval = publish_interval_s
        self._last_published: datetime | None = None
        # The tick held for a frame that landed inside the interval, if one is
        # due. Never more than one: a burst is one snapshot.
        self._held_tick: asyncio.Task[None] | None = None
        self._skewed = False
        self.frames = 0
        self.applied_frames = 0
        self.dropped_skew = 0
        self.dropped_busy = 0
        self.unknown_rows = 0
        self.sweeps = 0
        # device_id -> when it last appeared in an admitted frame.
        self.last_seen: dict[str, datetime] = {}
        # When the last frame was admitted at all. A connected tap that sends
        # none is one catching up on backfill (it suppresses live frames while
        # far behind), and the floor should say that rather than "nine dead
        # strips" -- see `juice.api.v2.collector`.
        self.last_frame_at: datetime | None = None
        self.gaps = GapMeter()
        self._summarised_at = self._started

    async def __call__(self, rows: list[list]) -> None:
        self.frames += 1
        if not rows:
            return
        now = self._now()
        offset = _skew_seconds(rows, now)
        self._skewed = _log_skew_transition(self._skewed, offset)
        if self._skewed:
            self.dropped_skew += 1
            return
        self.last_frame_at = now
        self.gaps.observe(rows, now)
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
            since = (
                math.inf
                if self._last_published is None
                else (now - self._last_published).total_seconds()
            )
            publish = since >= self._publish_interval
            outcome = await apply_live(self._state, self._store, rows, now=now, publish=publish)
        except Exception:  # noqa: BLE001 - one bad frame must not stop the next
            log.warning("tap live: applying a frame failed", exc_info=True)
        else:
            self.applied_frames += 1
            self.unknown_rows += outcome.unknown
            if outcome.published:
                # Stamped when the tick went out, not when the frame was
                # admitted: a frame that waited in the slot behind another
                # apply is older than its tick.
                self._published(self._now())
            elif not publish:
                self._hold_tick()
        finally:
            self._inflight_since = None
            if self._pending is not None:
                rows, now = self._pending
                self._pending = None
                self._start(rows, now)

    def _published(self, now: datetime) -> None:
        """A tick just went out: the interval restarts, and a held one is moot."""
        self._last_published = now
        if self._held_tick is not None and not self._held_tick.done():
            self._held_tick.cancel()
        self._held_tick = None

    def _hold_tick(self) -> None:
        """Publish once the interval has elapsed, unless a frame does first.
        The remaining wait is measured now, after the apply, against the last
        tick's own stamp; an interval that elapsed during the apply is zero."""
        if self._held_tick is not None and not self._held_tick.done():
            return
        assert self._last_published is not None  # a tick is only held after one
        elapsed = (self._now() - self._last_published).total_seconds()
        delay = max(self._publish_interval - elapsed, 0.0)
        self._held_tick = asyncio.create_task(self._publish_held(delay), name="tap-live-tick")

    async def _publish_held(self, delay: float) -> None:
        await asyncio.sleep(delay)
        try:
            published = publish_readings(self._state)
        except Exception:  # noqa: BLE001 - same guard as the apply path
            log.warning("tap live: publishing a held tick failed", exc_info=True)
            return
        if published:
            self._last_published = self._now()

    def silent_since(self, now: datetime | None = None) -> datetime | None:
        """When live frames stopped, if they have been absent for
        `LIVE_STALE_S` -- or None while they are flowing. Before any frame at
        all, the silence dates from startup."""
        now = now or self._now()
        last = self.last_frame_at or self._started
        return last if (now - last).total_seconds() > LIVE_STALE_S else None

    def summarise(self) -> None:
        """One line on the live channel's health, for the log every so often."""
        log.info(
            "tap live: %d frames, %d applied, %d dropped for skew, %d superseded, "
            "%d rows for unknown outlets, %d devices offline; %s",
            self.frames,
            self.applied_frames,
            self.dropped_skew,
            self.dropped_busy,
            self.unknown_rows,
            len(self._state.offline_since),
            self.gaps.describe(),
        )

    async def settle(self) -> None:
        """Wait for the in-flight apply and anything queued behind it. For
        tests and shutdown."""
        while self._inflight is not None and not self._inflight.done():
            try:
                await self._inflight
            except asyncio.CancelledError:
                if asyncio.current_task().cancelling():  # type: ignore[union-attr]
                    raise

    def close(self) -> None:
        """Drop a held tick. After `settle`, at shutdown: nobody is listening."""
        if self._held_tick is not None and not self._held_tick.done():
            self._held_tick.cancel()
        self._held_tick = None

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
        if changed and publish_readings(state):
            self._published(now)
        return changed


async def live_loop(projector: LiveProjector, *, interval: float = LIVE_SWEEP_SECONDS) -> None:
    """The 1 Hz sweep: what has to happen even when no frame does.

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
        now = projector._now()
        if (now - projector._summarised_at).total_seconds() >= LIVE_SUMMARY_SECONDS:
            projector._summarised_at = now
            projector.summarise()


# --- power control ------------------------------------------------------------


class TapUnavailableError(RuntimeError):
    """No connected tap can carry this command. Refused, not retried: anything
    that is not a `TimeoutError` is exactly what `is_retryable` declines."""


class TapCommandFailedError(RuntimeError):
    """tap refused or the device said no in a way another attempt cannot fix:
    expired, unknown device, or a device error tap has already retried."""


class _SessionLostError(Exception):
    """The socket a command went down closed before its result came back."""


Sender = Callable[[dict], Awaitable[None]]


@dataclass(slots=True)
class _Pending:
    owner: str
    future: asyncio.Future[tuple[str, str | None]]


@dataclass(slots=True)
class _Session:
    tap_id: str
    send: Sender
    devices: set[str] = field(default_factory=set)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class TapControl:
    """The registry of connected taps, and the command round trip over them.

    `handle_ingest` registers a session on `hello`, feeds it the device ids
    each `devices` frame names, hands it every `command_result`, and
    unregisters on disconnect. `TapPlug.turn_on()` is `command()`: pick the
    session that reports the device, send a `command` frame, wait for the
    matching `command_result`.

    Keyed by `tap_id` so a second tap is a visible fact rather than a silent
    race, and routed by which tap *reported* the device, so a command can
    never land on a tap that cannot reach the strip. With exactly one tap
    connected -- the museum -- every device is its.
    """

    def __init__(
        self,
        *,
        now: Callable[[], datetime] | None = None,
        result_timeout: float = COMMAND_RESULT_TIMEOUT_S,
    ) -> None:
        self._now = now or (lambda: datetime.now(UTC))
        self._result_timeout = result_timeout
        self._sessions: dict[str, _Session] = {}
        self._pending: dict[str, _Pending] = {}
        self.commands_sent = 0
        self.commands_ok = 0
        self.commands_failed = 0
        self.commands_timed_out = 0
        # When the last session went away with none left -- the `since` of a
        # `collector_offline` entry. None while a tap is connected, and also
        # before any has ever been: the floor cannot tell "not yet" from
        # "gone" here, and `since: null` is the honest shape for both.
        self.disconnected_at: datetime | None = None
        # Send -> result, in ms, for every command tap answered (ok or error;
        # the wire cost is the same). Enough for a p95 that means something,
        # small enough to never matter. Silence is not a sample: a timeout is
        # a bound, not a measurement, and it is counted separately.
        self._latency_ms: deque[float] = deque(maxlen=LATENCY_SAMPLES)

    def latency(self) -> dict[str, float | int]:
        """Round-trip percentiles over the recent answered commands: what a
        button costs on the floor, which is the number the cutover gate
        wants beside 'agrees'."""
        samples = sorted(self._latency_ms)
        if not samples:
            return {"n": 0}

        def pct(p: float) -> float:
            return samples[min(len(samples) - 1, int(round(p * (len(samples) - 1))))]

        return {
            "n": len(samples),
            "p50_ms": round(pct(0.5), 1),
            "p95_ms": round(pct(0.95), 1),
            "max_ms": round(samples[-1], 1),
        }

    def snapshot(self) -> dict[str, Any]:
        """One dict for a status view: who is connected, what they report,
        how commands have gone, how long they take."""
        return {
            "connected": [
                {"tap_id": s.tap_id, "devices": sorted(s.devices)} for s in self._sessions.values()
            ],
            "pending": len(self._pending),
            "commands": {
                "sent": self.commands_sent,
                "ok": self.commands_ok,
                "failed": self.commands_failed,
                "timed_out": self.commands_timed_out,
            },
            "latency": self.latency(),
        }

    # -- what the receiver tells us --------------------------------------------

    def connect(self, tap_id: str, send: Sender) -> None:
        if tap_id in self._sessions:
            log.warning("tap control: %s connected again; the newer socket wins", tap_id)
        self._sessions[tap_id] = _Session(tap_id, send)
        self.disconnected_at = None
        log.info("tap control: %s can now take commands", tap_id)

    def disconnect(self, tap_id: str, send: Sender) -> None:
        """Forget a session -- only if it is still the current one for that
        id, since a reconnect's handler can outlive the old socket's."""
        session = self._sessions.get(tap_id)
        if session is None or session.send is not send:
            return
        del self._sessions[tap_id]
        if not self._sessions:
            self.disconnected_at = self._now()
        log.info("tap control: %s disconnected", tap_id)
        for pending in list(self._pending.values()):
            if pending.owner == tap_id and not pending.future.done():
                pending.future.set_exception(_SessionLostError(tap_id))

    def note_devices(self, tap_id: str, device_ids: set[str]) -> None:
        """The frame is the full roster, so this replaces: a strip that moved
        to another tap must stop being routed to the one that lost it."""
        session = self._sessions.get(tap_id)
        if session is not None:
            session.devices = set(device_ids)

    def resolve(self, result: tuple[str, str, str | None]) -> None:
        command_id, status, error = result
        pending = self._pending.pop(command_id, None)
        if pending is None or pending.future.done():
            # A result for a wait that already timed out; the retry will ask
            # again and tap will answer from its cache.
            log.debug("tap control: late result for %s ignored", command_id)
            return
        pending.future.set_result((status, error))

    @property
    def connected(self) -> list[str]:
        return sorted(self._sessions)

    @property
    def pending(self) -> int:
        return len(self._pending)

    def owner_of(self, device_id: str) -> str | None:
        for session in self._sessions.values():
            if device_id in session.devices:
                return session.tap_id
        if len(self._sessions) == 1:
            return next(iter(self._sessions))
        return None

    # -- the round trip --------------------------------------------------------

    async def command(
        self,
        kind: str,
        device_id: str,
        child_id: str,
        *,
        command_id: str | None = None,
        expires_at: datetime | None = None,
    ) -> str:
        """Send one command and wait for its result. Returns the command id.

        Raises `TapUnavailableError` (no tap to send to; refuse),
        `TapCommandFailedError` (tap said an error another try cannot fix;
        refuse) or `TimeoutError` (retry -- with the same `command_id`, or tap
        will treat the retry as a new command and throw the relay again). The
        last covers more than silence: a socket that closes under the command
        is retried too, because tap reconnects within a second and its cache
        still holds the answer, and so is a device error tap names as
        transient (`RETRYABLE_TAP_ERRORS`), because failures are not cached
        and the next ask actuates again.
        """
        owner = self.owner_of(device_id)
        if owner is None:
            raise TapUnavailableError(
                "no connected tap reports this device"
                if self._sessions
                else "no tap is connected; the collector is offline"
            )
        session = self._sessions[owner]
        command_id = command_id or uuid.uuid4().hex
        expires_at = expires_at or self._now() + timedelta(seconds=COMMAND_EXPIRES_S)
        frame = wire.command(command_id, kind, device_id, child_id, expires_at)
        # The wire id is not the registry's command id (`TapPlug` cannot see
        # that one), so name both halves here or the two logs cannot be joined.
        log.info(
            "tap control: %s %s/%s -> %s as %s",
            kind,
            device_id[:12],
            child_id,
            owner,
            command_id[:8],
        )

        loop = asyncio.get_running_loop()
        future: asyncio.Future[tuple[str, str | None]] = loop.create_future()
        self._pending[command_id] = _Pending(owner, future)
        self.commands_sent += 1
        started = time.monotonic()
        try:
            async with session.lock:  # one writer per socket
                await session.send(frame)
            status, error = await asyncio.wait_for(future, timeout=self._result_timeout)
        except TimeoutError:
            self.commands_timed_out += 1
            raise TimeoutError(
                f"tap {owner} gave no answer for {kind} within {self._result_timeout:.0f}s"
            ) from None
        except _SessionLostError:
            self.commands_timed_out += 1
            raise TimeoutError(f"tap {owner} disconnected before answering {kind}") from None
        except Exception as exc:
            # The socket died under the send; the handler's disconnect follows.
            # Retryable for the same reason: tap is a reconnect away.
            future.cancel()
            self.commands_timed_out += 1
            raise TimeoutError(f"tap {owner}: {type(exc).__name__}: {exc}") from exc
        finally:
            self._pending.pop(command_id, None)

        elapsed_ms = (time.monotonic() - started) * 1000
        self._latency_ms.append(elapsed_ms)
        log.info(
            "tap control: %s %s from %s in %.0f ms%s",
            command_id[:8],
            status,
            owner,
            elapsed_ms,
            f" ({error})" if error else "",
        )
        if status != "ok":
            text = error or "tap reported an error"
            if text.startswith(RETRYABLE_TAP_ERRORS):
                self.commands_timed_out += 1
                raise TimeoutError(f"tap {owner}: {text}")
            self.commands_failed += 1
            if text == "expired":
                text = (
                    "tap refused the command as expired -- if this keeps happening, "
                    "check the clock on the tap box"
                )
            raise TapCommandFailedError(text)
        self.commands_ok += 1
        return command_id


class TapPlug:
    """A `plug_objects` entry whose relay lives behind a tap.

    The `Controllable` the power handlers actuate through: they only ever call
    `turn_on()` / `turn_off()` and read `.alias`, and everything they do around
    that -- the command lifecycle, `call_with_retry`, confirmation from the
    next reading -- is unchanged. The one thing this adds is the redelivery
    id: a retry after silence re-sends the *same* command_id, so tap's cache
    answers a command it has already applied instead of throwing the relay
    twice. A result of either kind ends the reuse; so does the expiry, and so
    does a command of the opposite kind.
    """

    def __init__(self, control: TapControl, device_id: str, child_id: str, alias: str) -> None:
        self._control = control
        self.device_id = device_id
        self.child_id = child_id
        self.alias = alias
        # kind -> (command_id, expires_at) for a command sent but unanswered.
        self._open: dict[str, tuple[str, datetime]] = {}

    def same_outlet(self, device_id: str, child_id: str) -> bool:
        return self.device_id == device_id and self.child_id == child_id

    async def turn_on(self) -> None:
        await self._command("turn_on")

    async def turn_off(self) -> None:
        await self._command("turn_off")

    async def _command(self, kind: str) -> None:
        now = self._control._now()
        # Asking for the opposite makes any open id for it stale: replaying a
        # cached "ok" for a turn_on after a turn_off has happened in between
        # would leave the command awaiting a relay that never moves.
        for other in [k for k in self._open if k != kind]:
            del self._open[other]
        reuse = self._open.get(kind)
        if reuse is not None and reuse[1] <= now:
            reuse = None
        if reuse is None:
            reuse = (uuid.uuid4().hex, now + timedelta(seconds=COMMAND_EXPIRES_S))
        command_id, expires_at = reuse
        try:
            await self._control.command(
                kind, self.device_id, self.child_id, command_id=command_id, expires_at=expires_at
            )
        except TimeoutError:
            self._open[kind] = (command_id, expires_at)
            raise
        except Exception:
            # Answered (with an error) or unsendable: the id has served.
            self._open.pop(kind, None)
            raise
        else:
            self._open.pop(kind, None)

    def __repr__(self) -> str:
        return f"TapPlug({self.device_id}/{self.child_id} {self.alias!r})"


# --- the collector itself -------------------------------------------------------


def reconcile_from_store(
    state: RecorderState,
    store: Store,
    machines: Mapping[str, Any],
    ts: datetime,
    *,
    control: TapControl,
) -> None:
    """Re-run assignment over every outlet the store knows, from its alias.

    It reads aliases from the **store** rather than from a device or a frame: tap's
    roster frames have already written them there (`apply_devices`), and tap
    re-sends a roster only when an *outlet* changes -- so a machine renamed or
    added in FlipFix would otherwise sit unassigned until someone relabelled a
    plug. Same guard as the frame path: an empty roster assigns nothing away.
    """
    entries = [
        {"device_id": device_id, "child_id": child_id, "alias": alias, "has_emeter": has_emeter}
        for _plug_id, device_id, child_id, alias, has_emeter in store.list_plugs()
    ]
    apply_devices(state, store, entries, machines, ts, control=control)


def roster_projection(
    state: RecorderState, store: Store, control: TapControl
) -> Callable[[list[dict]], None]:
    """The `tap_devices` callable for a tap-driven server.

    A closure rather than a bound `apply_devices` so the frame is judged
    against whatever FlipFix said *most recently* -- `state.flipfix_machines`
    is refreshed every minute by the housekeeping loop -- and not against the
    roster that happened to be current when the app was built.
    """

    def project(entries: list[dict]) -> None:
        apply_devices(
            state, store, entries, state.flipfix_machines, datetime.now(UTC), control=control
        )

    return project


async def _fetch_roster(state: RecorderState, flipfix_url: str, flipfix_key: str) -> None:
    """Refresh `state.flipfix_machines`, keeping the last good one on a blip.

    `get_machines` answers `{}` for *any* failure. Until #103 the cloud path
    handed that straight to `refresh_metadata` and unassigned the floor on
    every 500; here the guard is the same and the reason is the same.
    """
    from juice.flipfix import get_machines

    fresh = await get_machines(flipfix_url, flipfix_key)
    if fresh:
        state.flipfix_machines = fresh
    elif state.flipfix_machines:
        log.warning(
            "FlipFix returned no machines; keeping the last roster of %d rather than "
            "unassigning the floor",
            len(state.flipfix_machines),
        )


async def tap_collector_startup(
    state: RecorderState,
    store: Store,
    rollups: RollupWorker,
    *,
    flipfix_url: str | None,
    flipfix_key: str | None,
    public_url: str | None,
    control: TapControl,
) -> None:
    """The startup: everything that has to be true before the first frame.

    Hydrate from the store so the floor renders at once; resolve the overload
    mode; recompute the baselines; fetch FlipFix; reconcile assignments from
    the store's aliases (which also hands out the `TapPlug`s); seed the
    sparklines; run the retro migration and one rollup pass so `/usage` is
    populated at boot rather than a rollup interval later.
    """
    from juice.rollups import refresh_baselines_into
    from juice.server import seed_buffers

    hydrate_assignments(state, store)
    configure_overload_mode(state)
    # No window can exist yet -- `check_overload` needs an assignment and a
    # baseline, both of which `hydrate_assignments` just set with no await
    # between it and here -- so this clears nothing; it states the intent.
    state.overload_windows.clear()
    state.flipfix_url = flipfix_url
    state.flipfix_key = flipfix_key
    state.public_url = (public_url or "").rstrip("/") or None
    await refresh_baselines_into(store, rollups, state)
    if flipfix_url and flipfix_key:
        await _fetch_roster(state, flipfix_url, flipfix_key)
    reconcile_from_store(state, store, state.flipfix_machines, datetime.now(UTC), control=control)
    seed_buffers(state, store)
    await rollups.apply_retro_migration()
    await rollups.refresh()
    log.info(
        "tap collector: %d plugs, %d assigned, %d FlipFix machines; waiting for a tap",
        len(state.plugs),
        len(state.assignments),
        len(state.flipfix_machines),
    )


async def housekeeping_pass(
    state: RecorderState,
    store: Store,
    *,
    flipfix_url: str | None,
    flipfix_key: str | None,
    control: TapControl,
    now: datetime | None = None,
) -> None:
    """One housekeeping tick, every `IDLE_RECHECK_SECONDS`.

    The FlipFix roster, the operator-set state the endpoints also update
    synchronously (locks, strip names and order, circuits -- re-read wholesale
    so it self-heals), and assignment reconciliation from the store.
    """
    if flipfix_url and flipfix_key:
        await _fetch_roster(state, flipfix_url, flipfix_key)
    state.lock_modes = store.get_lock_modes()
    state.strip_names = store.get_strip_names()
    state.strip_orders = store.get_strip_orders()
    state.circuit_devices = store.get_circuit_devices()
    state.circuits = {c["circuit_id"]: c for c in store.list_circuits()}
    reconcile_from_store(
        state, store, state.flipfix_machines, now or datetime.now(UTC), control=control
    )


async def housekeeping_loop(
    state: RecorderState,
    store: Store,
    *,
    flipfix_url: str | None,
    flipfix_key: str | None,
    control: TapControl,
    interval: float = IDLE_RECHECK_SECONDS,
) -> None:
    while True:
        await asyncio.sleep(interval)
        try:
            await housekeeping_pass(
                state, store, flipfix_url=flipfix_url, flipfix_key=flipfix_key, control=control
            )
        except Exception:  # noqa: BLE001 - a failed pass must not kill the collector
            log.warning("tap collector: housekeeping pass failed", exc_info=True)


async def run_tap_collector(
    state: RecorderState,
    store: Store,
    rollups: RollupWorker,
    control: TapControl,
    projector: LiveProjector,
    *,
    flipfix_url: str | None,
    flipfix_key: str | None,
    public_url: str | None,
    interval: float = IDLE_RECHECK_SECONDS,
) -> None:
    """The tap-driven floor's `record()`: start up, then keep house forever.

    What is *not* here says what tap took over: no device poll, no failure
    counting. Readings arrive on the socket and are stored by ingest; the live
    frame drives the state through `projector`; commands go back through
    `control`. This coroutine owns the two loops that must run whether or not
    a tap is talking -- the 1 Hz staleness/command sweep and the minute-cadence
    housekeeping -- and the rollups and retention run beside it as they
    already did.
    """

    async def startup_then_housekeeping() -> None:
        await tap_collector_startup(
            state,
            store,
            rollups,
            flipfix_url=flipfix_url,
            flipfix_key=flipfix_key,
            public_url=public_url,
            control=control,
        )
        await housekeeping_loop(
            state,
            store,
            flipfix_url=flipfix_url,
            flipfix_key=flipfix_key,
            control=control,
            interval=interval,
        )

    # The sweep runs from the first second, not from the end of startup: the
    # server is already up, frames are already being applied and commands can
    # already be issued while a retro migration takes its minutes, and nothing
    # else times a command out or notices a device has gone quiet.
    try:
        await asyncio.gather(live_loop(projector), startup_then_housekeeping())
    finally:
        await cancel_overload_shutdowns(state)
