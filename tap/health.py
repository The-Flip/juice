"""One mutable record of what tap is doing, read by everything that asks.

The watchdog and the status page read the *same* object. That is the whole
point: the failure mode where the dashboard is green while the process is
wedged requires two sources of truth, so there is one.

Nothing here touches the database. The status page has to render when the disk
is stuck, because that is exactly when somebody is looking at it.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime

from tap.device import DeviceState

# Enough samples for a stable p95 at 1 Hz without holding a sweep's worth of
# history per device forever.
_LATENCY_SAMPLES = 300


def _pct(values: list[float], q: float) -> float | None:
    """Nearest-rank percentile. Small samples, no interpolation needed."""
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, round(q * len(ordered)) - 1))
    return round(ordered[idx], 1)


def describe_failure(
    exc: BaseException,
    *,
    phase: str = "",
    duration_ms: float | None = None,
) -> str:
    """A failure line that says something.

    `asyncio.timeout` raises a bare `TimeoutError` whose `str()` is empty, so
    the obvious `f"{type(exc).__name__}: {exc}"` renders as `"TimeoutError: "` —
    which is what tap reported for every one of 132 real failures. Where the
    exception has nothing to say, the phase and the elapsed time do.
    """
    kind = type(exc).__name__
    detail = str(exc).strip()
    parts = [kind]
    if detail:
        parts.append(f": {detail}")
    if duration_ms is not None:
        parts.append(f" after {duration_ms:.0f}ms")
    if phase:
        parts.append(f" in {phase}")
    return "".join(parts)


def _iso(ts: datetime | None) -> str | None:
    """RFC 3339 with a real offset — never a bare Z bolted onto a naive value."""
    return None if ts is None else ts.astimezone(UTC).isoformat()


@dataclass
class OutletHealth:
    child_id: str
    alias: str = ""
    relay_on: bool = False
    power_mw: int | None = None
    voltage_mv: int | None = None
    overcurrent: bool = False


@dataclass
class DeviceHealth:
    device_id: str
    host: str
    model: str = ""
    family: str = ""
    pinned: bool = False
    state: DeviceState = DeviceState.STARTING
    consecutive_failures: int = 0
    last_ok: datetime | None = None
    last_error: str = ""
    sweeps_ok: int = 0
    sweeps_failed: int = 0
    last_error_at: datetime | None = None
    last_error_phase: str = ""
    failures_by_kind: dict[str, int] = field(default_factory=dict)
    outlets: dict[str, OutletHealth] = field(default_factory=dict)
    _latency: deque[float] = field(default_factory=lambda: deque(maxlen=_LATENCY_SAMPLES))
    # Failed attempts are timed too, in their own deque. Folding them into
    # `_latency` would flatter a fleet that fails fast; dropping them — which is
    # what this did until eight hours of real data showed 132 timeouts and a
    # p95 of 616 ms against an 800 ms budget — censors precisely the tail that
    # causes the failures out of the percentile you would use to size it.
    _fail_latency: deque[float] = field(default_factory=lambda: deque(maxlen=_LATENCY_SAMPLES))

    def record_sweep(self, duration_ms: float) -> None:
        self._latency.append(duration_ms)
        self.sweeps_ok += 1
        self.last_ok = datetime.now(UTC)
        self.consecutive_failures = 0

    def record_failure(
        self,
        exc: BaseException,
        *,
        phase: str = "",
        duration_ms: float | None = None,
    ) -> None:
        self.sweeps_failed += 1
        self.consecutive_failures += 1
        kind = type(exc).__name__
        self.failures_by_kind[kind] = self.failures_by_kind.get(kind, 0) + 1
        self.last_error = describe_failure(exc, phase=phase, duration_ms=duration_ms)
        self.last_error_phase = phase
        self.last_error_at = datetime.now(UTC)
        # Sweep-phase attempts only. A connect gets a 15 s budget against the
        # sweep's 0.8 s, and an offline device re-probing every 60 s would fill
        # a 300-sample deque with 15 s connect timeouts inside five hours —
        # burying the 800 ms sweep timeouts these percentiles exist to expose.
        if duration_ms is not None and phase.startswith("sweep"):
            self._fail_latency.append(duration_ms)

    def snapshot(self) -> dict:
        samples = list(self._latency)
        failed = list(self._fail_latency)
        age = None if self.last_ok is None else (datetime.now(UTC) - self.last_ok).total_seconds()
        return {
            "device_id": self.device_id,
            "host": self.host,
            "model": self.model,
            "family": self.family,
            "pinned": self.pinned,
            "state": str(self.state),
            "consecutive_failures": self.consecutive_failures,
            "last_ok": _iso(self.last_ok),
            "last_ok_age_s": None if age is None else round(age, 1),
            "last_error": self.last_error,
            "last_error_at": _iso(self.last_error_at),
            "last_error_phase": self.last_error_phase,
            "failures_by_kind": dict(self.failures_by_kind),
            "sweeps_ok": self.sweeps_ok,
            "sweeps_failed": self.sweeps_failed,
            "sweep_p50_ms": _pct(samples, 0.50),
            "sweep_p95_ms": _pct(samples, 0.95),
            "sweep_fail_p50_ms": _pct(failed, 0.50),
            "sweep_fail_p95_ms": _pct(failed, 0.95),
            "outlets": [
                {
                    "child_id": o.child_id,
                    "alias": o.alias,
                    "relay_on": o.relay_on,
                    "watts": None if o.power_mw is None else round(o.power_mw / 1000, 2),
                    "volts": None if o.voltage_mv is None else round(o.voltage_mv / 1000, 1),
                    "overcurrent": o.overcurrent,
                }
                for o in sorted(self.outlets.values(), key=lambda x: x.child_id)
            ],
        }


@dataclass
class BufferHealth:
    rows_written: int = 0
    rows_dropped: int = 0
    batches_committed: int = 0
    last_write: datetime | None = None
    oldest_ts: datetime | None = None
    newest_ts: datetime | None = None
    queue_depth: int = 0
    days: list[dict] = field(default_factory=list)
    total_bytes: int = 0
    retention_days: int = 0
    last_commit_ms: float | None = None

    def snapshot(self) -> dict:
        return {
            "rows_written": self.rows_written,
            "rows_dropped": self.rows_dropped,
            "batches_committed": self.batches_committed,
            "last_write": _iso(self.last_write),
            "oldest_ts": _iso(self.oldest_ts),
            "newest_ts": _iso(self.newest_ts),
            "queue_depth": self.queue_depth,
            "days": self.days,
            "total_bytes": self.total_bytes,
            "retention_days": self.retention_days,
            "last_commit_ms": self.last_commit_ms,
        }


@dataclass
class UplinkHealth:
    """`enabled` is False when no URL is configured — tap runs standalone happily."""

    enabled: bool = False
    url: str = ""
    connected: bool = False
    since: datetime | None = None
    last_error: str = ""
    backoff_s: float = 0.0
    reconnects: int = 0
    sent_cursor: str | None = None
    acked_cursor: str | None = None
    lag_rows: int = 0
    lag_seconds: float | None = None
    batches_sent: int = 0
    batches_acked: int = 0
    batches_nacked: int = 0
    batches_poisoned: int = 0
    rows_acked: int = 0
    commands_received: int = 0
    commands_failed: int = 0
    live_suppressed: bool = False

    def snapshot(self) -> dict:
        return {
            "enabled": self.enabled,
            "url": self.url,
            "connected": self.connected,
            "since": _iso(self.since),
            "last_error": self.last_error,
            "backoff_s": round(self.backoff_s, 1),
            "reconnects": self.reconnects,
            "sent_cursor": self.sent_cursor,
            "acked_cursor": self.acked_cursor,
            "lag_rows": self.lag_rows,
            "lag_seconds": None if self.lag_seconds is None else round(self.lag_seconds, 1),
            "batches_sent": self.batches_sent,
            "batches_acked": self.batches_acked,
            "batches_nacked": self.batches_nacked,
            "batches_poisoned": self.batches_poisoned,
            "rows_acked": self.rows_acked,
            "commands_received": self.commands_received,
            "commands_failed": self.commands_failed,
            "live_suppressed": self.live_suppressed,
        }


@dataclass
class Health:
    tap_id: str = "tap"
    version: str = ""
    config_path: str | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    devices: dict[str, DeviceHealth] = field(default_factory=dict)
    buffer: BufferHealth = field(default_factory=BufferHealth)
    uplink: UplinkHealth = field(default_factory=UplinkHealth)
    discovery_last: datetime | None = None
    discovery_found: int = 0
    # Non-fatal complaints the watchdog is currently making. Shown on the status
    # page so a degraded-but-alive daemon explains itself.
    warnings: list[str] = field(default_factory=list)

    def device(self, device_id: str, *, host: str = "") -> DeviceHealth:
        entry = self.devices.get(device_id)
        if entry is None:
            entry = DeviceHealth(device_id=device_id, host=host)
            self.devices[device_id] = entry
        elif host:
            entry.host = host
        return entry

    def forget_device(self, device_id: str) -> None:
        self.devices.pop(device_id, None)

    def rename_device(self, old_key: str, new_key: str) -> None:
        """Re-key a device's health without losing what it has already recorded.

        A poller starts under a `host:` placeholder and re-keys to the real
        device_id the moment it connects. Dropping the placeholder threw away
        every failure recorded before that — which is exactly the connect
        failures, the only ones whose phase would have said `connect`. The
        entry is the same physical device; move it.
        """
        entry = self.devices.pop(old_key, None)
        if entry is None or new_key in self.devices:
            # Nothing to carry, or the destination is already established and
            # is the authority. Either way the placeholder is gone.
            return
        entry.device_id = new_key
        self.devices[new_key] = entry

    @property
    def uptime_seconds(self) -> float:
        return (datetime.now(UTC) - self.started_at).total_seconds()

    def any_device_online(self) -> bool:
        return any(
            d.state in (DeviceState.ONLINE, DeviceState.DEGRADED) for d in self.devices.values()
        )

    def last_successful_sweep(self) -> datetime | None:
        stamps = [d.last_ok for d in self.devices.values() if d.last_ok is not None]
        return max(stamps) if stamps else None

    def snapshot(self) -> dict:
        return {
            "tap_id": self.tap_id,
            "version": self.version,
            "config_path": self.config_path,
            "started_at": _iso(self.started_at),
            "uptime_seconds": round(self.uptime_seconds, 1),
            "now": _iso(datetime.now(UTC)),
            "monotonic": round(time.monotonic(), 3),
            "discovery_last": _iso(self.discovery_last),
            "discovery_found": self.discovery_found,
            "warnings": list(self.warnings),
            "devices": [d.snapshot() for d in sorted(self.devices.values(), key=lambda d: d.host)],
            "buffer": self.buffer.snapshot(),
            "uplink": self.uplink.snapshot(),
        }
