"""SMART / KLAP devices — the Tapo P316M and its relatives.

The whole reason this adapter exists rather than using `python-kasa`'s device
objects: a full `Device.update()` on a P316M costs **812 ms** because it
refreshes every module on the parent and all six children. tap wants four
integers per outlet, and the raw calls give them in a fraction of that:

    get_child_device_list                        76 ms   relay state + aliases
    control_child -> get_emeter_data       6 x   14 ms   the actual measurements
                                          ------------
                                                160 ms   per six-outlet strip

`get_energy_usage` (67 ms) returns cumulative counters we do not need;
`get_emeter_data` is the cheap one. The six children are read one at a time
because **the firmware rejects `control_child` nested inside a
`multipleRequest`** — every sub-request comes back `error_code: -1001` — and
because `SmartProtocol` serialises on its own lock regardless.

Relay state comes from `device_on` in the child list rather than being inferred
from `power_mw > 0`. Those are different questions: a machine in attract mode
draws power with the relay on, and a relay can be on with nothing plugged in.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import Any

from tap.config import Credentials
from tap.device import Family, OutletReading, Sweep
from tap.errors import DeviceAuthError, TransientError
from tap.kasa_common import NO_RETRY, connect, decode_alias, translate

log = logging.getLogger(__name__)


class SmartPowerDevice:
    """A KLAP/AES device, polled through raw protocol calls."""

    family = Family.SMART

    def __init__(
        self, host: str, *, credentials: Credentials | None = None, device_id: str = ""
    ) -> None:
        self.host = host
        self.device_id = device_id
        self.model = ""
        self._credentials = credentials
        self._device: Any = None
        self._proto: Any = None
        self._children: list[str] = []

    async def open(self) -> None:
        await self.close()
        device = await connect(self.host, family=Family.SMART, credentials=self._credentials)
        self._device = device
        self._proto = device.protocol
        try:
            info = (await self._query({"get_device_info": None}))["get_device_info"]
        except BaseException as e:
            raise translate(e) from e
        self.device_id = info.get("device_id", "") or self.device_id
        self.model = info.get("model", "") or device.model or ""
        if not self.device_id:
            raise TransientError(f"{self.host}: device reported no device_id")

    async def close(self) -> None:
        device, self._device, self._proto = self._device, None, None
        if device is not None:
            try:
                await device.disconnect()
            except Exception as e:  # noqa: BLE001 — closing must never raise
                log.debug("%s: disconnect failed: %s", self.host, e)

    async def _query(self, payload: dict) -> dict:
        if self._proto is None:
            raise TransientError(f"{self.host}: not connected")
        return await self._proto.query(payload, retry_count=NO_RETRY)

    async def _child_query(self, child_id: str, method: str, params=None) -> dict:
        """One `control_child` round trip, unwrapped."""
        response = await self._query(
            {
                "control_child": {
                    "device_id": child_id,
                    "requestData": {"method": method, "params": params},
                }
            }
        )
        data = response.get("control_child", {}).get("responseData", {})
        code = data.get("error_code", 0)
        if code:
            raise TransientError(f"{self.host}/{child_id}: {method} returned error_code {code}")
        return data.get("result") or {}

    async def sweep(self) -> Sweep:
        started = time.perf_counter()
        # One timestamp for the whole device: the outlets are read ~14ms apart
        # but they are one observation of one strip.
        ts = datetime.now(UTC)
        try:
            children = await self._child_list()
            outlets = []
            for child in children:
                outlets.append(await self._read_outlet(child))
        except BaseException as e:
            raise translate(e) from e
        return Sweep(
            device_id=self.device_id,
            ts=ts,
            outlets=outlets,
            duration_ms=round((time.perf_counter() - started) * 1000, 2),
        )

    async def _child_list(self) -> list[dict]:
        """The children, or a synthetic single child for a one-outlet device."""
        response = await self._query({"get_child_device_list": None})
        listing = response.get("get_child_device_list") or {}
        children = listing.get("child_device_list")
        if children:
            self._children = [c["device_id"] for c in children]
            return children
        # A single-outlet SMART plug has no child list; present it as one
        # outlet with an empty child_id, matching how the server keys plugs.
        info = (await self._query({"get_device_info": None}))["get_device_info"]
        self._children = []
        return [dict(info, device_id="")]

    async def _read_outlet(self, child: dict) -> OutletReading:
        child_id = child.get("device_id", "")
        alias = decode_alias(child.get("nickname"))
        relay_on = bool(child.get("device_on", False))
        overcurrent = child.get("overcurrent_status", "normal") != "normal"
        protection = child.get("power_protection_status", "normal") != "normal"
        try:
            if child_id:
                emeter = await self._child_query(child_id, "get_emeter_data")
            else:
                emeter = (await self._query({"get_emeter_data": None})).get("get_emeter_data") or {}
        except BaseException as e:
            failure = translate(e)
            # A rejected credential is about the device, not this outlet, and
            # must reach the poller so it can park rather than retry at 1 Hz.
            if isinstance(failure, DeviceAuthError):
                raise failure from e
            # Anything else: one outlet failing must not lose the rest of the
            # sweep. Power fields stay None — unmeasured, not zero.
            log.debug("%s/%s: emeter read failed: %s", self.host, child_id, failure)
            return OutletReading(
                child_id=child_id,
                alias=alias,
                relay_on=relay_on,
                overcurrent=overcurrent,
                protection_tripped=protection,
            )
        return OutletReading(
            child_id=child_id,
            alias=alias,
            relay_on=relay_on,
            power_mw=emeter.get("power_mw"),
            voltage_mv=emeter.get("voltage_mv"),
            current_ma=emeter.get("current_ma"),
            energy_wh=emeter.get("energy_wh"),
            overcurrent=overcurrent,
            protection_tripped=protection,
        )

    async def set_relay(self, child_id: str, on: bool) -> None:
        try:
            if child_id:
                await self._child_query(child_id, "set_device_info", {"device_on": on})
            else:
                await self._query({"set_device_info": {"device_on": on}})
        except BaseException as e:
            raise translate(e) from e
