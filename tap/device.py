"""The device seam: what `tap` needs from a smart plug, and nothing more.

`PowerDevice` is a `Protocol` rather than a base class so the test fakes are
real implementations rather than mocks — they typecheck, and they can inject
faults (hang, refuse, reject credentials) that a mock would only pretend to.

Two deliberate choices in the data shapes:

**One timestamp per sweep, not per outlet.** A strip's six outlets are read
~90 ms apart, but they are one observation of one strip. A shared timestamp is
what makes "what was this strip drawing at time t" a sum rather than an
approximation.

**Raw integers, in the units the device reports** (milliwatts, millivolts,
milliamps). Converting to float watts in the hot path costs precision, costs
bytes in the buffer — SQLite varint-encodes a zero into one byte and a float
into eight — and buys nothing, because whoever wants watts can divide.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Protocol, runtime_checkable


class Family(StrEnum):
    """Which protocol a device speaks.

    `SMART` is KLAP/AES over HTTP (Tapo P316M, EP25, KP125M); `IOT` is the
    legacy TCP-9999 protocol (HS300, EP10). `AUTO` asks discovery to decide.
    """

    SMART = "smart"
    IOT = "iot"
    AUTO = "auto"


class DeviceState(StrEnum):
    """Where a device sits in the poller's offline state machine."""

    ONLINE = "online"
    DEGRADED = "degraded"  # 1-2 consecutive failures; still polled at full rate
    OFFLINE = "offline"  # parked, slow re-probe
    UNAUTHORIZED = "unauthorized"  # credentials rejected; needs a human
    STARTING = "starting"  # constructed, not yet connected
    EXCLUDED = "excluded"  # config refuses it; the poller has stopped


@dataclass(frozen=True, slots=True)
class OutletReading:
    """One outlet at one instant.

    `child_id` is the device's own identifier for the outlet and is empty for a
    single-outlet device. Power fields are `None` when the outlet has no energy
    meter (an EP10) or when that outlet's read failed while the rest of the
    sweep succeeded — `None` means "unmeasured", never zero.
    """

    child_id: str
    alias: str
    relay_on: bool
    power_mw: int | None = None
    voltage_mv: int | None = None
    current_ma: int | None = None
    energy_wh: int | None = None
    # Hardware-side protection status, when the device reports it. The P316M
    # trips its own overcurrent cutoff independently of any software rule, and
    # without this a hardware trip is indistinguishable from someone pulling
    # the plug.
    overcurrent: bool = False
    protection_tripped: bool = False


@dataclass(frozen=True, slots=True)
class Sweep:
    """One full read of one device.

    The three timing fields split `duration_ms` into the parts that can be
    acted on separately. A slow sweep is either uniformly slow — every round
    trip inflated, which is a network or firmware problem — or one outlet
    stalling, which is a plug problem, and `duration_ms` alone cannot tell
    them apart. With `n` outlets, `emeter_max_ms` near `emeter_total_ms / n`
    is the first; near `emeter_total_ms` is the second.

    `None` means the adapter did not time that phase, never zero.
    """

    device_id: str
    ts: datetime
    outlets: list[OutletReading] = field(default_factory=list)
    duration_ms: float = 0.0
    # The single call that enumerates the outlets: `get_child_device_list` on
    # SMART, `get_sysinfo` on IOT.
    listing_ms: float | None = None
    # Sum of the per-outlet meter reads, and the slowest single one.
    emeter_total_ms: float | None = None
    emeter_max_ms: float | None = None


@runtime_checkable
class PowerDevice(Protocol):
    """A pollable, switchable power device.

    Implementations translate their library's exceptions into `tap.errors`:
    `TransientError` for anything worth retrying, `DeviceAuthError` for rejected
    credentials. Nothing above this seam should import a device library.
    """

    device_id: str
    host: str
    model: str
    family: Family
    # Which round trip a sweep is on, for a failure to be attributed to. The
    # sweep budget cancels from outside, so the exception that reaches the
    # poller cannot say where it was; this can.
    phase: str

    async def open(self) -> None:
        """Connect and learn the device's identity. Safe to call again to reconnect."""
        ...

    async def sweep(self) -> Sweep:
        """Read every outlet once."""
        ...

    async def set_relay(self, child_id: str, on: bool) -> None:
        """Switch one outlet. `child_id` is empty for a single-outlet device."""
        ...

    async def close(self) -> None:
        """Release the connection. Must be safe to call when never opened."""
        ...
