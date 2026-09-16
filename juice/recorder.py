"""What the collector keeps in RecorderState between frames.

Assignments and their hydration, the live-reading cache and buffers, the
overload window and its shutdown, and the air-monitor poll. The tap
projection (`juice/collector_tap.py`) drives all of it; nothing here polls.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from collections import deque
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from juice.air_collector import AirAccount, AirReading, AirSensor
from juice.control import call_with_retry
from juice.flipfix import add_log_entry, report_unplayable
from juice.overload import (
    OVERLOAD_MODES,
    OVERLOAD_RETRY_COOLDOWN_S,
    OverloadWindow,
    resolve_overload_mode,
    threshold_for,
)
from juice.readings import PlugReading
from juice.store import Store

if TYPE_CHECKING:
    from juice.server import RecorderState

log = logging.getLogger(__name__)

ASSET_TAG_RE = re.compile(r"M\d+")
IDLE_RECHECK_SECONDS = 60

# Air monitors report ~every 15 min, so polling them at the 1 Hz power cadence
# would be wasteful (and ON CONFLICT-deduped anyway). 5 min keeps the dashboard
# fresh without hammering the Qingping cloud.
AIR_POLL_SECONDS = 300
# First-deploy history lookback (no stored readings yet). Subsequent backfills
# start from the last stored reading, so this only applies once per sensor.
AIR_BACKFILL_DAYS = 30
# How often to re-run the (cheap, gap-only) history backfill while running, to
# recover readings missed during a device outage that the forward poll can't see.
AIR_BACKFILL_INTERVAL_SECONDS = 6 * 3600

# One-off data migration: reapply all current calibrations to the historical
# hourly_play_seconds rollup (which was frozen under older calibrations). Runs
# once per DB, guarded by the applied_migrations marker.


def extract_asset_tag(alias: str) -> str | None:
    """Extract asset tag like M0013 from a plug alias."""
    m = ASSET_TAG_RE.search(alias)
    return m.group(0) if m else None


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
    — including machines whose plug is offline, which metadata refresh would
    otherwise skip and drop. Live readings and re-assignments layer on top as
    the recorder polls. `year` isn't persisted, so hydrated entries carry None.

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


def _cache_reading(
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


def _update_buffer(
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


async def check_overload(
    state: RecorderState,
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
        window = OverloadWindow(max_gap_seconds=state.overload_max_gap_s)
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


def _forget_shutdown(state: RecorderState, plug_id: int, task: asyncio.Task) -> None:
    if state.overload_shutdowns.get(plug_id) is task:
        del state.overload_shutdowns[plug_id]
    if task.cancelled():
        return
    if (exc := task.exception()) is not None:
        # Every failure the shutdown expects is handled inside it; anything
        # reaching here is a bug, and a task's exception unobserved is a bug
        # that logs nothing.
        log.error("Overload shutdown task for plug %d crashed", plug_id, exc_info=exc)


async def cancel_overload_shutdowns(state: RecorderState) -> None:
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
    state: RecorderState,
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
    state: RecorderState,
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
    state: RecorderState,
    plug_id: int,
    name: str,
    asset_id: str,
    mean_w: float,
    baseline: float,
    *,
    shadow: bool,
) -> None:
    """Notify dashboard clients of an overload event (no-op without a publisher)."""
    from juice.server import _publish

    _publish(
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


def _air_row(reading: AirReading) -> tuple:
    """Flatten an AirReading into an insert_air_readings row tuple."""
    return (
        reading.ts,
        reading.mac,
        reading.temperature,
        reading.humidity,
        reading.co2,
        reading.pm25,
        reading.pm10,
        reading.tvoc,
        reading.noise,
        reading.battery,
    )


async def air_poll_once(air_account: AirAccount, store: Store, ts: datetime) -> int:
    """Fetch every air monitor's latest snapshot and persist it.

    Returns the number of sensors seen. Reading inserts are deduped on
    (ts, mac) in the store, so re-polling within a device's report interval is
    a no-op. Air data is independent of the power path — no FlipFix lookup, no
    assignment, no overload logic.
    """
    pairs = await air_account.devices()
    rows = []
    for sensor, reading in pairs:
        store.ensure_air_sensor(sensor.mac, sensor.name, sensor.online, ts)
        rows.append(_air_row(reading))
    if rows:
        store.insert_air_readings(rows)
    return len(pairs)


async def air_backfill(
    air_account: AirAccount,
    store: Store,
    sensors: list[AirSensor],
    now: datetime,
    default_days: int = AIR_BACKFILL_DAYS,
) -> int:
    """Pull historical readings from Qingping and persist them; returns the count.

    Per sensor, the window starts just after the latest reading we already have
    (gap-fill across a restart or device outage), or `default_days` back when we
    have none (first deploy). Inserts dedupe on (ts, mac), so running this every
    startup — and periodically — is safe and idempotent. A per-sensor failure is
    logged and skipped rather than aborting the whole backfill.
    """
    end_unix = int(now.timestamp())
    total = 0
    for sensor in sensors:
        last = store.air_last_ts(sensor.mac)
        if last is not None:
            start_unix = int(last.replace(tzinfo=UTC).timestamp()) + 1
        else:
            start_unix = end_unix - default_days * 86_400
        if start_unix >= end_unix:
            continue
        try:
            readings = await air_account.history(sensor.mac, start_unix, end_unix)
        except Exception:
            log.warning("Air backfill failed for %s", sensor.mac, exc_info=True)
            continue
        rows = [_air_row(r) for r in readings]
        if rows:
            store.insert_air_readings(rows)
            total += len(rows)
    return total


async def _air_backfill_safe(air_account: AirAccount, store: Store) -> None:
    """Discover sensors and backfill their history, swallowing+logging errors."""
    try:
        now = datetime.now(UTC)
        sensors = [s for s, _ in await air_account.devices()]
        n = await air_backfill(air_account, store, sensors, now)
        log.info("Air backfill: %d historical readings across %d sensors", n, len(sensors))
    except Exception:
        log.warning("Air backfill failed", exc_info=True)


async def air_record(
    air_account: AirAccount, store: Store, interval: float = AIR_POLL_SECONDS
) -> None:
    """Poll Qingping air monitors forever, persisting each cycle.

    Runs as a separate task alongside the power recorder. On startup, and every
    AIR_BACKFILL_INTERVAL_SECONDS thereafter, it backfills history from the cloud
    so the dashboard is populated immediately and gaps (restarts, device
    outages) are filled — `/devices` only returns the latest snapshot, so the
    forward poll alone can't recover missed readings. A failed cycle logs and is
    retried next interval rather than killing the loop.
    """
    log.info("Air monitoring: polling Qingping every %.0fs", interval)
    await _air_backfill_safe(air_account, store)
    backfill_every = max(1, round(AIR_BACKFILL_INTERVAL_SECONDS / interval))
    polls_since_backfill = 0
    while True:
        start = asyncio.get_running_loop().time()
        ts = datetime.now(UTC)
        try:
            count = await air_poll_once(air_account, store, ts)
            log.debug("Air poll: %d sensors", count)
        except Exception:
            log.warning("Air poll failed", exc_info=True)
        polls_since_backfill += 1
        if polls_since_backfill >= backfill_every:
            await _air_backfill_safe(air_account, store)
            polls_since_backfill = 0
        elapsed = asyncio.get_running_loop().time() - start
        await asyncio.sleep(max(0, interval - elapsed))


def configure_overload_mode(state: RecorderState) -> None:
    """Resolve `JUICE_OVERLOAD_PROTECTION` onto the state, loudly.

    Part of the collector's startup, because `RecorderState.overload_mode`
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
