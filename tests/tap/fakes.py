"""Fault-injecting fakes for the device seam.

`PowerDevice` is a Protocol, so these are real implementations rather than
mocks: they typecheck, they run the production code paths, and they can hang or
refuse in ways a `MagicMock` can only pretend to. Nothing in this file imports
python-kasa, which is the point — the vast majority of tap's behaviour is
testable without a device library or a device.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from tap.device import Family, OutletReading, Sweep
from tap.errors import DeviceAuthError, TransientError


class FakeDevice:
    """A PowerDevice that can be told to misbehave."""

    def __init__(
        self,
        device_id: str = "FAKE0001",
        host: str = "10.0.0.1",
        *,
        outlets: int = 2,
        model: str = "FAKE100",
        family: Family = Family.SMART,
        sweep_ms: float = 0.0,
        hang: bool = False,
        fail_with: BaseException | None = None,
        auth_error: bool = False,
        open_fail: BaseException | None = None,
        watts: int = 42_000,
    ) -> None:
        self.device_id = device_id
        self.host = host
        self.model = model
        self.family = family
        self.phase = ""
        self._outlets = outlets
        self._sweep_ms = sweep_ms
        self.hang = hang
        self.fail_with = fail_with
        self.auth_error = auth_error
        self.open_fail = open_fail
        self.watts = watts
        # Observable behaviour, for assertions.
        self.sweeps = 0
        self.opens = 0
        self.closes = 0
        self.relay_calls: list[tuple[str, bool]] = []
        self.relay_state: dict[str, bool] = {}

    async def open(self) -> None:
        self.opens += 1
        if self.open_fail is not None:
            raise self.open_fail

    async def close(self) -> None:
        self.closes += 1

    async def sweep(self) -> Sweep:
        if self.hang:
            await asyncio.Event().wait()  # never returns; the budget must cancel us
        if self.auth_error:
            raise DeviceAuthError("fake credentials rejected")
        if self.fail_with is not None:
            raise self.fail_with
        if self._sweep_ms:
            await asyncio.sleep(self._sweep_ms / 1000)
        self.sweeps += 1
        ts = datetime.now(UTC)
        return Sweep(
            device_id=self.device_id,
            ts=ts,
            outlets=[
                OutletReading(
                    child_id=f"{self.device_id}{i:02d}",
                    alias=f"outlet {i}",
                    relay_on=self.relay_state.get(f"{self.device_id}{i:02d}", True),
                    power_mw=self.watts,
                    voltage_mv=119_000,
                    current_ma=350,
                    energy_wh=1,
                )
                for i in range(self._outlets)
            ],
            duration_ms=self._sweep_ms,
        )

    async def set_relay(self, child_id: str, on: bool) -> None:
        self.relay_calls.append((child_id, on))
        if self.fail_with is not None:
            raise self.fail_with
        self.relay_state[child_id] = on


class FakeProtocol:
    """Replays canned protocol responses, and records what was asked.

    Used to pin the exact request dicts the adapters build against payloads
    captured from real hardware.
    """

    def __init__(self, responses: dict | None = None) -> None:
        self.responses = responses or {}
        self.requests: list[dict] = []
        self.fail_with: BaseException | None = None

    async def query(self, payload: dict, retry_count: int = 3) -> dict:
        self.requests.append(payload)
        if self.fail_with is not None:
            raise self.fail_with
        key = next(iter(payload))
        if key == "control_child":
            method = payload["control_child"]["requestData"]["method"]
            key = f"control_child:{method}"
        if key not in self.responses:
            raise TransientError(f"fake protocol has no response for {key}")
        return self.responses[key]
