"""Sustained-overload detection.

A stuck solenoid (or similar fault) holds a coil energized, so the machine draws
an abnormally high load *continuously* for minutes — unlike normal gameplay,
which only spikes briefly as individual solenoids fire. We detect the former by
watching a trailing time-window of power readings and firing when the *average*
over the whole window exceeds a per-machine threshold.

The threshold is relative to each machine's own baseline (machines vary widely —
some normally sustain 200W+), with an absolute floor so low-baseline machines
aren't tripped by a modest bump.

The window and its threshold are pure (no I/O, no clock): the caller supplies
timestamps and the baseline. The live projection feeds every reading through
them, and the `overload-report` CLI replays historical readings through the
*same* logic, so the backtest matches production exactly.

Below the "Acting on the window" banner is the guard that is not pure: it reads
`JUICE_OVERLOAD_PROTECTION`, actuates the plug, writes the audit row and files
the FlipFix report. Kept in one module because the two halves share a
vocabulary, but the line between them is the line `overload-report` sits on.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections import deque
from datetime import datetime
from typing import TYPE_CHECKING

from juice.control import call_with_retry
from juice.flipfix import add_log_entry, report_unplayable

if TYPE_CHECKING:
    from juice.floor_state import FloorState
    from juice.store import Store

log = logging.getLogger(__name__)

# Fire when the trailing-window average exceeds REL_MULTIPLIER x the machine's
# baseline, but never below FLOOR_WATTS. Validated against production data: real
# incidents ran at 3.6-3.9x baseline; the highest sustained level of any healthy
# machine was ~2.0x, so 2.5x sits in open space with zero historical false
# positives.
REL_MULTIPLIER = 2.5
FLOOR_WATTS = 80.0

# The load must stay high for this long (seconds) before we act, so transient
# solenoid spikes and power-on inrush never trigger a shutdown.
SUSTAIN_SECONDS = 120

# How long a machine whose shutdown *failed* waits before the window may fire
# it again. Without this it re-arms on the next full window, ~120 s later,
# and every attempt is six `turn_off` commands to a strip that just refused
# six, an ERROR line and an audit row -- on the collector's command channel,
# which under tap is the same socket the readings ride. (The actuation runs
# on its own task, so the retries no longer stall the collector itself; the
# cooldown is about not hammering.) A strip that would not answer six times
# in a row is not going to answer 120 s later either; ten minutes is long
# enough for it to reboot or for an operator to reach it, and the window
# keeps filling meanwhile so the retry is prompt once the cooldown ends.
OVERLOAD_RETRY_COOLDOWN_S = 600.0

# The largest hole a window may have and still be believed. A window that
# spans two minutes but *observed* six seconds of them is not evidence of a
# sustained load; the verdict is refused until the hole has aged out, which
# delays a real overload by the window at most -- provided holes come rarer
# than one per window; a link that hiccups every minute keeps it refused, and
# that is the honest answer to a link like that. The bound is a fact about
# the collector's cadence: tap sends live frames at 1 Hz, and the frame
# re-stamps a device's last values for the one or two timed-out sweeps (~15 s)
# before it parks the device and the outlets vanish, so no juice-side bound
# sees meter staleness below that; what this bounds is the uplink. Measured on
# the museum LAN against a fake server (49 outlets, 11 min, n=28,244):
# inter-arrival p50 1.001 s, p99 1.003 s, max 1.006 s; a dropped socket and
# reconnect 2.38 s. The real path adds a WAN reconnect on the second attempt
# (backoff, TLS, hello, one live interval: ~5-6 s) and any event-loop stall
# here, so 10 s. At 10 s one held sample is 8% of the window: to fire alone on
# the lowest threshold on the floor it would have to read ~900 W, which
# nothing draws. `collector_tap.GapMeter` measures the real path and prints it
# on the `tap live:` summary; read it before overload leaves shadow.
#
# History before the cutover (2026-09-16) was collected by the cloud recorder
# at p50 6.9 s / p99.9 21.6 s between readings; a window over those rows wants
# `max_gap_seconds=30` or it refuses most of them (`overload-report --max-gap`).
MAX_GAP_S = 10.0

# Baseline = this quantile of per-minute average watts over the trailing window
# of days. Minute-averaging removes transient spikes; the high quantile absorbs
# brief past incidents. A machine needs at least MIN_BASELINE_MINUTES of "on"
# history before it's armed (otherwise it's never auto-shut-down — fail-safe).
BASELINE_DAYS = 30
BASELINE_QUANTILE = 0.99
MIN_BASELINE_MINUTES = 500

# Auto-shutdown behavior, set via JUICE_OVERLOAD_PROTECTION:
#   'live'   — detect and shut machines down (default)
#   'shadow' — detect and log/audit only, no power action
#   'off'    — disable detection entirely
OVERLOAD_MODES = ("live", "shadow", "off")


def threshold_for(baseline: float) -> float:
    """Watts above which a sustained load is an overload for this machine."""
    return max(REL_MULTIPLIER * baseline, FLOOR_WATTS)


def resolve_overload_mode(raw: str | None) -> str:
    """Normalize a JUICE_OVERLOAD_PROTECTION value to a valid mode.

    Unrecognized values (typos) fall back to 'live' rather than silently
    disabling protection — the safety feature fails toward protecting machines.
    """
    mode = (raw or "live").lower()
    return mode if mode in OVERLOAD_MODES else "live"


class OverloadWindow:
    """Trailing time-window of (timestamp, watts) for one plug.

    `verdict` fires only once the window covers a full SUSTAIN_SECONDS with no
    hole wider than `max_gap_seconds` *and* the time-weighted mean watts over it
    exceeds the machine's threshold — so it can't fire on a partially-filled
    window right after power-on, nor on a handful of samples straddling a gap.

    The rule is a *mean* over the window, not a minimum, so a load well above
    the threshold fires before it has lasted the whole window: a machine that
    jumps to 4x baseline from a normal-draw start crosses a 2.5x mean after
    about 60 s of the 120. That is the intended shape — the worse the fault,
    the sooner the cut — and worth knowing when reading a shutdown's timing.
    """

    def __init__(
        self,
        sustain_seconds: float = SUSTAIN_SECONDS,
        max_gap_seconds: float = MAX_GAP_S,
    ) -> None:
        self._sustain = sustain_seconds
        self.max_gap_seconds = max_gap_seconds
        self._samples: deque[tuple[datetime, float]] = deque()

    def add(self, ts: datetime, watts: float) -> None:
        """Append a reading and trim to just cover the trailing sustain window.

        Keeps one sample at/just before the cutoff (the "straddler") so the
        retained samples actually *bracket* a full SUSTAIN_SECONDS of history —
        otherwise the span would always fall just short of the window and never
        satisfy `verdict`'s coverage check on real, unaligned timestamps.
        """
        # A gap longer than the window means we have no idea what the load did in
        # between — start fresh rather than bridging stale watts across the gap
        # (which could look "full" with only a couple of samples and misfire).
        if self._samples and ts.timestamp() - self._samples[-1][0].timestamp() > self._sustain:
            self._samples.clear()
        self._samples.append((ts, watts))
        cutoff = ts.timestamp() - self._sustain
        while len(self._samples) >= 2 and self._samples[1][0].timestamp() <= cutoff:
            self._samples.popleft()

    def reset(self) -> None:
        """Forget all buffered samples (e.g. after acting on an overload)."""
        self._samples.clear()

    def peak(self) -> float:
        """Highest watts currently in the window (0 if empty)."""
        return max((w for _, w in self._samples), default=0.0)

    def verdict(self, baseline: float) -> tuple[bool, float]:
        """Return (fire, window_mean_watts) for the current window.

        Fires when the buffered samples span at least the full sustain window,
        no two consecutive samples are further apart than `max_gap_seconds`,
        and the time-weighted mean exceeds `threshold_for(baseline)`. The mean
        weights each sample by how long it held -- until the next one -- so
        uneven sampling cannot bias it: five readings inside one second of a
        spike are one second of spike, not five samples' worth.
        """
        if len(self._samples) < 2:
            return False, 0.0
        times = [t.timestamp() for t, _ in self._samples]
        span = times[-1] - times[0]
        if span < self._sustain:
            return False, 0.0
        # Each sample holds until the next; the last one holds nothing yet. The
        # first sample is the straddler `add()` keeps at or before the cutoff,
        # and only the part of its hold *inside* the window counts -- weighting
        # all of it would let a high reading from before the window push the
        # mean over the threshold from outside the two minutes it claims.
        start = times[-1] - self._sustain
        held = [b - a for a, b in zip(times[:-1], times[1:], strict=True)]
        watts = [w for _, w in self._samples]
        inside = [times[1] - max(times[0], start), *held[1:]]
        mean = sum(w * h for w, h in zip(watts[:-1], inside, strict=True)) / (times[-1] - start)
        if max(held) > self.max_gap_seconds:
            return False, mean
        return mean > threshold_for(baseline), mean


# ---------------------------------------------------------------------------
# Acting on the window: the guard the live projection feeds every reading
# to, and the shutdown it starts when one fires.
# ---------------------------------------------------------------------------


def configure_overload_mode(state: FloorState) -> None:
    """Resolve `JUICE_OVERLOAD_PROTECTION` onto the state, loudly.

    Part of the collector's startup, because `FloorState.overload_mode`
    defaults to `"live"` and `hydrate_assignments` loads real baselines: a
    startup that skipped this would be armed for real with nobody having
    asked.
    """
    raw_mode = os.environ.get("JUICE_OVERLOAD_PROTECTION")
    state.overload_mode = resolve_overload_mode(raw_mode)
    if raw_mode and raw_mode.lower() not in OVERLOAD_MODES:
        # Unrecognized value (typo): fail safe toward protection, not silently
        # off, and make the misconfiguration loud rather than implicit.
        log.warning(
            "Invalid JUICE_OVERLOAD_PROTECTION=%r; expected one of %s — using 'live'",
            raw_mode,
            ", ".join(OVERLOAD_MODES),
        )
    log.info("Overload protection: %s", state.overload_mode)


async def check_overload(
    state: FloorState,
    store: Store,
    plug_id: int,
    ts: datetime,
    watts: float,
) -> None:
    """Feed a fresh reading to the plug's overload window; act if it fires.

    A machine is only armed once it has a baseline (enough history). Machines
    already locked off are skipped — they're powered down and can't be re-armed
    until a human clears them. Honors `state.overload_mode` ('off'/'shadow'/'live').

    Acting means *starting* the shutdown, not finishing it: the actuation runs
    on its own task (`state.overload_shutdowns`) and this returns at once. The
    caller is the poll loop or the live-frame apply, and six `turn_off`
    retries awaited there (~24 s) would hole every other machine's window —
    and under tap, outlive the projector's hang-cancel. While a plug's
    shutdown is in flight, or for `OVERLOAD_RETRY_COOLDOWN_S` after one has
    failed, the window keeps filling but is not asked for a verdict.
    """
    if state.overload_mode == "off":
        return
    assignment = state.assignments.get(plug_id)
    if assignment is None:
        return
    name, asset_id, _year = assignment
    baseline = state.power_baselines.get(asset_id)
    if baseline is None:  # not enough history -> not armed
        return
    if state.lock_modes.get(asset_id) == "off":  # already shut down for overload
        return

    # Track the start of the current above-threshold streak so we can report how
    # long the machine was actually overloading before we cut it.
    if watts > threshold_for(baseline):
        state.overload_onsets.setdefault(plug_id, ts)
    else:
        state.overload_onsets.pop(plug_id, None)

    window = state.overload_windows.get(plug_id)
    if window is None:
        window = OverloadWindow()
        state.overload_windows[plug_id] = window
    window.add(ts, watts)
    inflight = state.overload_shutdowns.get(plug_id)
    if inflight is not None and not inflight.done():
        return
    failed_at = state.overload_failed_at.get(plug_id)
    if failed_at is not None:
        if (ts - failed_at).total_seconds() < OVERLOAD_RETRY_COOLDOWN_S:
            return
        del state.overload_failed_at[plug_id]
    fire, mean_w = window.verdict(baseline)
    if not fire:
        return

    # Capture peak + duration before the reset clears the window.
    peak_w = window.peak()
    onset = state.overload_onsets.pop(plug_id, ts)
    duration_s = max(0.0, (ts - onset).total_seconds())
    window.reset()  # don't re-fire on the same sustained load
    if state.overload_mode == "shadow":
        log.warning(
            "OVERLOAD (shadow) %s (%s): %.0fW sustained vs %.0fW baseline — would shut down",
            name,
            asset_id,
            mean_w,
            baseline,
        )
        try:
            store.record_power_event(
                ts,
                plug_id,
                "turn_off",
                "overload",
                "system",
                "shadow",
                error=f"{mean_w:.0f}W sustained vs {baseline:.0f}W baseline",
            )
        except Exception as e:
            log.warning("Audit write failed for plug %d: %s", plug_id, e)
        _publish_overload(state, plug_id, name, asset_id, mean_w, baseline, shadow=True)
        return

    task = asyncio.create_task(
        _trigger_overload_shutdown(
            state, store, plug_id, name, asset_id, ts, mean_w, peak_w, baseline, duration_s
        ),
        name=f"overload-shutdown-{asset_id}",
    )
    state.overload_shutdowns[plug_id] = task
    task.add_done_callback(lambda t: _forget_shutdown(state, plug_id, t))


def _forget_shutdown(state: FloorState, plug_id: int, task: asyncio.Task) -> None:
    if state.overload_shutdowns.get(plug_id) is task:
        del state.overload_shutdowns[plug_id]
    if task.cancelled():
        return
    if (exc := task.exception()) is not None:
        # Every failure the shutdown expects is handled inside it; anything
        # reaching here is a bug, and a task's exception unobserved is a bug
        # that logs nothing.
        log.error("Overload shutdown task for plug %d crashed", plug_id, exc_info=exc)


async def cancel_overload_shutdowns(state: FloorState) -> None:
    """Cancel every in-flight shutdown and wait for it to end -- for the
    collector's own shutdown, so nothing is still mid-actuation when the
    store closes under it."""
    tasks = list(state.overload_shutdowns.values())
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def _format_duration(seconds: float) -> str:
    secs = int(round(seconds))
    if secs < 60:
        return f"{secs}s"
    return f"{secs // 60}m {secs % 60:02d}s"


async def _trigger_overload_shutdown(
    state: FloorState,
    store: Store,
    plug_id: int,
    name: str,
    asset_id: str,
    ts: datetime,
    mean_w: float,
    peak_w: float,
    baseline: float,
    duration_s: float,
) -> None:
    """Power the machine off and lock it off after a confirmed overload."""
    plug = state.plug_objects.get(plug_id)
    if plug is None:
        # A failure like any other: without the cooldown this would re-fire
        # and ERROR every window for as long as the object is missing.
        state.overload_failed_at[plug_id] = ts
        log.error("Overload on %s (%s) but no controllable plug %d", name, asset_id, plug_id)
        return
    log.warning(
        "OVERLOAD %s (%s): %.0fW sustained vs %.0fW baseline — shutting down + locking off",
        name,
        asset_id,
        mean_w,
        baseline,
    )
    try:
        await call_with_retry(plug.turn_off, max_attempts=6)
    except Exception as e:
        # Keyed off the firing reading's clock, which is the one `check_overload`
        # compares against -- the same clock under both collectors, and the
        # only one a test can drive.
        state.overload_failed_at[plug_id] = ts
        log.error(
            "Overload shutdown FAILED for %s (%s): %s -- still overloading and still ON; "
            "not retrying for %.0f s",
            name,
            asset_id,
            e,
            OVERLOAD_RETRY_COOLDOWN_S,
        )
        try:
            store.record_power_event(
                ts, plug_id, "turn_off", "overload", "system", "error", error=str(e)
            )
        except Exception as ae:
            log.warning("Audit write failed for plug %d: %s", plug_id, ae)
        return

    # The relay is off, which is the part that matters. What follows is
    # bookkeeping *about a machine*, and this task captured which machine
    # before it started: a relabel that landed during the actuation would
    # have the lock, the audit row and the FlipFix report name a machine that
    # is no longer on this outlet. Rare (an operator relabelling an outlet in
    # the seconds it is overloading), but wrong, so say so and stop here.
    current = state.assignments.get(plug_id)
    if current is None or current[1] != asset_id:
        log.warning(
            "Overload shutdown of plug %d: %s (%s) was reassigned mid-actuation to %s; "
            "outlet is off, not locking or reporting",
            plug_id,
            name,
            asset_id,
            "nothing" if current is None else f"{current[0]} ({current[1]})",
        )
        return

    try:
        store.record_power_event(
            ts,
            plug_id,
            "turn_off",
            "overload",
            "system",
            "ok",
            error=f"{mean_w:.0f}W sustained vs {baseline:.0f}W baseline",
        )
    except Exception as e:
        log.warning("Audit write failed for plug %d: %s", plug_id, e)

    # Lock the machine off so it can't be re-powered until a human inspects it.
    # Set the in-memory lock first so it holds even if the DB write fails, and
    # don't let a persistence error escape (it would kill the recording loop).
    state.lock_modes[asset_id] = "off"
    try:
        machine_id = store.ensure_machine(asset_id, name)
        store.set_machine_lock_mode(machine_id, "off")
    except Exception as e:
        log.warning("Lock persistence failed for %s (%s): %s", name, asset_id, e)

    _publish_overload(state, plug_id, name, asset_id, mean_w, baseline, shadow=False)

    await _report_overload_to_flipfix(
        state, store, plug_id, name, asset_id, ts, mean_w, peak_w, baseline, duration_s
    )


async def _report_overload_to_flipfix(
    state: FloorState,
    store: Store,
    plug_id: int,
    name: str,
    asset_id: str,
    ts: datetime,
    mean_w: float,
    peak_w: float,
    baseline: float,
    duration_s: float,
) -> None:
    """Record the overload in FlipFix and audit the outcome.

    Files an 'unplayable' problem report; if one is already open (200), appends a
    log entry to it so the recurrence is still recorded — "saving the machine from
    fire is always worth noting". Best-effort; the result is written to the power
    audit log (visible on the dashboard) so a skip/failure isn't invisible.
    """
    link = f" {state.public_url}/machine/{plug_id}" if state.public_url else ""
    text = (
        f"Juice auto-shut-down {name} after a sustained power overload — "
        f"~{mean_w:.0f}W (peak {peak_w:.0f}W) for {_format_duration(duration_s)}, "
        f"vs {baseline:.0f}W normal draw. Powered off and locked to prevent overheating.{link}"
    )
    occurred = ts.isoformat()

    if not (state.flipfix_url and state.flipfix_key):
        _audit_flipfix(store, ts, plug_id, "error", "FlipFix not configured")
        return

    res = await report_unplayable(
        state.flipfix_url, state.flipfix_key, asset_id, text, occurred_at=occurred
    )
    if res.created:
        _audit_flipfix(
            store, ts, plug_id, "ok", f"FlipFix: filed unplayable report #{res.report_id}"
        )
    elif res.ok and res.report_id is not None:
        # An unplayable report was already open — log the recurrence onto it.
        logged = await add_log_entry(
            state.flipfix_url, state.flipfix_key, res.report_id, text, occurred_at=occurred
        )
        result = "ok" if logged else "error"
        verb = "appended to" if logged else "FAILED to append to"
        _audit_flipfix(store, ts, plug_id, result, f"FlipFix: {verb} open report #{res.report_id}")
    else:
        detail = f"status {res.status}" if res.status is not None else "no response"
        _audit_flipfix(store, ts, plug_id, "error", f"FlipFix report failed ({detail})")


def _audit_flipfix(store: Store, ts: datetime, plug_id: int, result: str, note: str) -> None:
    """Best-effort audit row for a FlipFix report outcome."""
    try:
        store.record_power_event(ts, plug_id, "report", "flipfix", "system", result, error=note)
    except Exception as e:
        log.warning("FlipFix audit write failed for plug %d: %s", plug_id, e)


def _publish_overload(
    state: FloorState,
    plug_id: int,
    name: str,
    asset_id: str,
    mean_w: float,
    baseline: float,
    *,
    shadow: bool,
) -> None:
    """Notify dashboard clients of an overload event (no-op without a publisher)."""
    from juice.floor_state import publish

    publish(
        state,
        {
            "type": "overload_shutdown",
            "plug_id": plug_id,
            "asset_id": asset_id,
            "machine_name": name,
            "watts": round(mean_w),
            "baseline": round(baseline),
            "shadow": shadow,
            "actor": "system",
            "source": "overload",
        },
    )
