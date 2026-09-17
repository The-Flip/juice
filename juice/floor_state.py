"""The floor as the collector last saw it.

One `FloorState` per server, mutated by the live and roster projections
(`juice/collector_tap.py`) and read by every handler and the SSE publisher.
Nothing here is persisted: `hydrate_assignments` rebuilds the durable parts
from the store at startup, and the rest -- readings, buffers, offline devices,
in-flight commands and operations -- is whatever has happened since.

Its own module, rather than a corner of `juice/server.py`, so the modules that
mutate it need not reach into the HTTP server for the type of the thing they
mutate. `publish` lives here too: it is the fan-out over
`FloorState.event_subscribers`, and `CommandRegistry` needs it at construction.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime

from juice.commands import CommandRegistry
from juice.control import Controllable
from juice.flipfix import MachineInfo
from juice.overload import OverloadWindow
from juice.readings import PlugReading
from juice.state import Calibration

log = logging.getLogger(__name__)


@dataclass
class Operation:
    """An in-flight bulk power operation (All On / All Off)."""

    id: str
    kind: str  # 'all_on' | 'all_off'
    started_at: datetime
    started_by: str
    targets: list[int]
    current_machine: str | None = None
    completed: list[int] = field(default_factory=list)
    failed: list[tuple[int, str]] = field(default_factory=list)
    index: int = 0
    state: str = "running"  # 'running' | 'complete' | 'cancelled'
    cancel_requested: bool = False
    label: str | None = (
        None  # human scope label for the banner (e.g. "Backline strip"); None for global
    )


@dataclass
class FloorState:
    """The floor as the collector last saw it, shared with the HTTP API."""

    commands: CommandRegistry = field(init=False)

    def __post_init__(self) -> None:
        # Bound to this state's own fan-out so command progress reaches the same
        # SSE subscribers as everything else, with no extra wiring at call sites.
        self.commands = CommandRegistry(publish=lambda event: publish(self, event))

    plug_readings: dict[int, PlugReading] = field(default_factory=dict)
    # When each plug's cached reading was taken. PlugReading carries no
    # timestamp, and command reconciliation must be able to tell a reading that
    # postdates a command from a stale one cached before it — see
    # juice.commands.CommandRegistry.reconcile.
    plug_reading_ts: dict[int, datetime] = field(default_factory=dict)
    # plug_id -> (physical status, when it started). Tracked continuously rather
    # than derived on read: a value computed at request time would report a
    # duration of zero for a machine nobody had looked at in an hour.
    status_since: dict[int, tuple[str, datetime]] = field(default_factory=dict)
    watt_buffers: dict[int, deque] = field(default_factory=dict)
    assignments: dict[int, tuple[str, str, int | None]] = field(
        default_factory=dict
    )  # plug_id -> (name, asset_id, year)
    plugs: dict[int, tuple[str, str, str]] = field(
        default_factory=dict
    )  # plug_id -> (device_id, child_id, alias)
    calibrations: dict[int, Calibration] = field(default_factory=dict)  # plug_id -> Calibration
    strip_aliases: dict[str, str] = field(default_factory=dict)  # device_id -> strip alias
    # Operator-set strip names (device_id -> name). Display falls back to the
    # Kasa alias when no override is set.
    strip_names: dict[str, str] = field(default_factory=dict)
    # Operator-set dashboard order (device_id -> position). Strips without a
    # position sort after positioned ones, by display name.
    strip_orders: dict[str, int] = field(default_factory=dict)
    # Circuit membership and metadata, hydrated from the store.
    circuit_devices: dict[str, int] = field(default_factory=dict)  # device_id -> circuit_id
    circuits: dict[int, dict] = field(default_factory=dict)  # circuit_id -> circuit row dict
    plug_objects: dict[int, Controllable] = field(
        default_factory=dict
    )  # plug_id -> whatever the collector on duty controls it with
    plug_has_emeter: dict[int, bool] = field(default_factory=dict)  # plug_id -> has_emeter
    # Locked machines by asset_id (the lock follows the machine across outlet
    # moves). 'on' = locked-on (refuse off; skipped by all-off); 'off' =
    # locked-off (refuse on; skipped by all-on). Unlocked machines are absent.
    lock_modes: dict[str, str] = field(default_factory=dict)
    # Overload detection. Per-machine "normal" sustained power (asset_id -> watts,
    # absent until enough history) and a trailing-window watt accumulator per plug.
    power_baselines: dict[str, float] = field(default_factory=dict)
    overload_windows: dict[int, OverloadWindow] = field(default_factory=dict)
    # When each plug's current above-threshold streak began (plug_id -> ts), so a
    # shutdown can report how long the machine was actually overloading.
    overload_onsets: dict[int, datetime] = field(default_factory=dict)
    # Auto-shutdown behavior: 'live' acts, 'shadow' only logs/audits, 'off' disables.
    overload_mode: str = "live"
    # Shutdowns in flight (plug_id -> the task actuating them). The actuation
    # runs off the collector's loop so its retries stall nobody; while a
    # plug's task is here the window does not fire it again.
    overload_shutdowns: dict[int, asyncio.Task] = field(default_factory=dict)
    # When a plug's last shutdown *failed* (plug_id -> the firing reading's
    # ts), so the window waits `OVERLOAD_RETRY_COOLDOWN_S` before re-firing.
    overload_failed_at: dict[int, datetime] = field(default_factory=dict)
    # FlipFix creds, so an overload shutdown can file a problem report + mark the
    # machine broken. None when FlipFix isn't configured (reporting skipped).
    flipfix_url: str | None = None
    flipfix_key: str | None = None
    # The FlipFix machine roster as last fetched (asset_id -> {name, year}), kept
    # here so anything that assigns from an alias -- the roster projection or
    # the housekeeping pass -- resolves against the same, current answer. Empty until the
    # first successful fetch, and `collector_tap.apply_devices` treats empty as
    # "do not unassign anything", which is what makes a frame arriving before
    # FlipFix has answered safe.
    flipfix_machines: dict[str, MachineInfo] = field(default_factory=dict)
    # Juice's own public base URL (e.g. https://juice.theflip.museum), used to deep
    # link from a FlipFix report back to the machine page. None -> link omitted.
    public_url: str | None = None
    current_operation: Operation | None = None
    event_subscribers: set[asyncio.Queue] = field(default_factory=set)
    # Device health: a device is "offline" once it has been absent from tap's
    # live frames for `collector_tap.LIVE_STALE_S`. Its machines render as
    # OFFLINE rather than vanishing.
    offline_since: dict[str, datetime] = field(default_factory=dict)  # device_id -> marked-at


def publish(state: FloorState, event: dict) -> None:
    """Fan-out a single event to every SSE subscriber.

    A subscriber that can't keep up used to have its events dropped silently, so
    a client could fall arbitrarily far behind without ever knowing — which is
    why every page also blind-polls. Instead we now **drain the queue and
    collapse it to a single `resync_required`**: the client learns within one
    event that it has a gap, and the queue is freed in the process. The
    discarded events are overwhelmingly `readings` (idempotent snapshots), and
    anything order-sensitive is recovered by the resync.

    Never blocks and never raises — the publisher runs on the live-frame
    apply, and one wedged browser tab must not stall it.
    """
    for q in list(state.event_subscribers):
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            log.warning("SSE subscriber fell behind; collapsing queue to a resync")
            while not q.empty():
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:  # pragma: no cover - racing consumer
                    break
            try:
                q.put_nowait({"type": "resync_required"})
            except asyncio.QueueFull:  # pragma: no cover - racing consumer
                pass
