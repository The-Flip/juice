"""Legacy IOT devices — the HS300 strip and the EP10 single plug.

Same call shapes juice already sends through the cloud passthrough
(`juice/collector.py`), issued straight to the device on the LAN instead. One
`get_sysinfo` returns relay state and alias for every outlet at once, then one
`get_realtime` per metered outlet.

**Identity is the load-bearing detail here.** juice keys plugs on the TP-Link
*cloud* `deviceId` (40 hex) plus a `child_id` (that string plus `00`..`05`), and
the same values come back from a local `get_sysinfo` — `deviceId` at the top and
`children[i]["id"]` per outlet. python-kasa's own properties are **not** those
values: `IotDevice.device_id` returns the MAC address
(`kasa/iot/iotdevice.py:653`) and `IotStripPlug.device_id` returns
`f"{mac}_{child_id}"` (`kasa/iot/iotstrip.py:435-440`). Using either would fork
every outlet into a duplicate plug on the server, so this adapter reads the raw
sysinfo dict and never touches those properties.

Untested against real hardware at the time of writing: the HS300s live on the
museum LAN, and only a P316M was reachable from the development network. The
call shapes are the ones juice has been using in production for months, and the
fixture test pins them, but first contact with a real strip is the real check.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime
from typing import Any

from tap.config import Credentials
from tap.device import Family, OutletReading, Sweep
from tap.errors import DeviceAuthError, TransientError
from tap.kasa_common import NO_RETRY, connect, translate

log = logging.getLogger(__name__)

_SYSINFO: dict[str, Any] = {"system": {"get_sysinfo": {}}}
_REALTIME: dict[str, Any] = {"emeter": {"get_realtime": {}}}


class IotPowerDevice:
    """An HS300/EP10-class device, polled through raw protocol calls."""

    family = Family.IOT

    def __init__(
        self, host: str, *, credentials: Credentials | None = None, device_id: str = ""
    ) -> None:
        self.host = host
        self.device_id = device_id
        self.model = ""
        self.phase = ""
        self._roster: list[dict] | None = None
        self.roster_age = 0
        self.has_emeter = True
        self._credentials = credentials
        self._device: Any = None
        self._proto: Any = None

    async def open(self) -> None:
        await self.close()
        # A reconnect must not serve relay state from before the outage. Not
        # reachable through DevicePoller today, which builds a fresh device on
        # every connect, but `open()` promises it is safe to call again.
        self._roster = None
        self.roster_age = 0
        device = await connect(self.host, family=Family.IOT, credentials=self._credentials)
        self._device = device
        self._proto = device.protocol
        await self.refresh_identity()

    async def refresh_identity(self) -> None:
        """Learn device_id, model and metering support from one sysinfo call.

        Kept separate from `open` so the identity mapping — the part that
        decides whether readings land on the right plug — is testable without a
        device on the other end.
        """
        sysinfo = await self._sysinfo()
        # NOT python-kasa's device_id property — see the module docstring.
        self.device_id = sysinfo.get("deviceId", "") or self.device_id
        self.model = sysinfo.get("model", "") or ""
        # "ENE" in the feature string is how these devices advertise energy
        # monitoring; an EP10 has none and reports NULL power forever.
        self.has_emeter = "ENE" in (sysinfo.get("feature") or "")
        if not self.device_id:
            raise TransientError(f"{self.host}: sysinfo carried no deviceId")

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

    async def _sysinfo(self) -> dict:
        try:
            response = await self._query(_SYSINFO)
        except BaseException as e:
            raise translate(e) from e
        sysinfo = response.get("system", {}).get("get_sysinfo") or {}
        if not sysinfo:
            raise TransientError(f"{self.host}: empty get_sysinfo response")
        return sysinfo

    @staticmethod
    def _outlets_of(sysinfo: dict) -> list[dict]:
        """Normalise strip children and a bare single plug into one shape."""
        children = sysinfo.get("children")
        if children:
            return [
                {"id": c.get("id", ""), "alias": c.get("alias", ""), "state": c.get("state", 0)}
                for c in children
            ]
        return [
            {
                "id": "",  # single-outlet devices carry an empty child_id
                "alias": sysinfo.get("alias", ""),
                "state": sysinfo.get("relay_state", 0),
            }
        ]

    async def sweep(self) -> Sweep:
        started = time.perf_counter()
        ts = datetime.now(UTC)
        # See kasa_smart.sweep: the phase is what a cancelled sweep leaves
        # behind to say where it was.
        listing_ms: float | None = None
        if self._roster is None:
            listing_start = time.perf_counter()
            await self.refresh_roster()
            listing_ms = (time.perf_counter() - listing_start) * 1000
        children = self._roster
        if children is None:  # refresh_roster either sets it or raises
            raise TransientError(f"{self.host}: no outlet roster after refresh")
        outlets = []
        emeter_total = 0.0
        emeter_max = 0.0
        for i, child in enumerate(children, 1):
            self.phase = f"emeter[{i}/{len(children)}]"
            outlet_start = time.perf_counter()
            outlets.append(await self._read_outlet(child))
            took = (time.perf_counter() - outlet_start) * 1000
            emeter_total += took
            emeter_max = max(emeter_max, took)
        self.phase = ""
        age = self.roster_age
        self.roster_age += 1
        return Sweep(
            device_id=self.device_id,
            ts=ts,
            outlets=outlets,
            duration_ms=round((time.perf_counter() - started) * 1000, 2),
            # None, not 0.0: a sweep that did not fetch a roster did not time
            # one, and zeros would drag the listing percentile to the floor.
            listing_ms=None if listing_ms is None else round(listing_ms, 2),
            # None, not 0.0, when there was nothing to read: the field means
            # "not timed". An EP10 has no energy meter, so `_read_outlet`
            # issues no request at all and what we timed is this loop's own
            # overhead — reporting that as emeter latency would be a fiction.
            emeter_total_ms=round(emeter_total, 2) if (outlets and self.has_emeter) else None,
            emeter_max_ms=round(emeter_max, 2) if (outlets and self.has_emeter) else None,
            roster_age=age,
        )

    def note_relay(self, child_id: str, on: bool) -> None:
        """Patch the cached roster after we switch an outlet ourselves."""
        for child in self._roster or ():
            if child.get("id", "") == child_id:
                child["state"] = int(on)
                break

    async def refresh_roster(self) -> None:
        """Re-read the outlet roster. See kasa_smart.refresh_roster."""
        # See kasa_smart.refresh_roster: the phase survives a failure.
        self.phase = "sysinfo"
        self._roster = self._outlets_of(await self._sysinfo())
        self.phase = ""
        self.roster_age = 0

    async def _read_outlet(self, child: dict) -> OutletReading:
        child_id = child.get("id", "")
        base = OutletReading(
            child_id=child_id,
            alias=child.get("alias", ""),
            relay_on=bool(child.get("state", 0)),
        )
        if not self.has_emeter:
            return base
        # A deep-ish copy: dict(_REALTIME) would share the module-level inner
        # dict with every other call.
        payload: dict[str, Any] = {"emeter": {"get_realtime": {}}}
        if child_id:
            payload = {"context": {"child_ids": [child_id]}, **_REALTIME}
        try:
            response = await self._query(payload)
            realtime = response.get("emeter", {}).get("get_realtime") or {}
        except asyncio.CancelledError, KeyboardInterrupt, SystemExit:
            # See kasa_smart._read_outlet: cancellation must never be treated as
            # a bad outlet, or the sweep budget cannot cancel anything.
            raise
        except Exception as e:
            failure = translate(e)
            # A rejected credential is about the device, not this outlet, and
            # must reach the poller so it can park rather than retry at 1 Hz.
            if isinstance(failure, DeviceAuthError):
                raise failure from e
            # Anything else: one outlet failing must not lose the rest of the
            # sweep. Power fields stay None — unmeasured, not zero.
            log.debug("%s/%s: emeter read failed: %s", self.host, child_id, failure)
            return base
        return OutletReading(
            child_id=child_id,
            alias=base.alias,
            relay_on=base.relay_on,
            power_mw=_scaled(realtime, "power_mw", "power", 1000),
            voltage_mv=_scaled(realtime, "voltage_mv", "voltage", 1000),
            current_ma=_scaled(realtime, "current_ma", "current", 1000),
            # Some firmware reports the lifetime counter as energy_wh instead.
            energy_wh=_scaled(realtime, "total_wh", "total", 1000, "energy_wh"),
        )

    async def set_relay(self, child_id: str, on: bool) -> None:
        state: dict[str, Any] = {"system": {"set_relay_state": {"state": 1 if on else 0}}}
        payload: dict[str, Any] = (
            {"context": {"child_ids": [child_id]}, **state} if child_id else state
        )
        try:
            await self._query(payload)
        except BaseException as e:
            raise translate(e) from e


def _scaled(
    realtime: dict, milli_key: str, base_key: str, factor: int, *alt_keys: str
) -> int | None:
    """Prefer the milli-unit field; fall back to the older base-unit one.

    HS300 firmware reports `power_mw`/`voltage_mv`/`current_ma`/`total_wh`.
    Older IOT firmware reports `power`/`voltage`/`current`/`total` in watts,
    volts, amps and **kilowatt-hours**, which we scale up so the buffer only
    ever holds one unit per column. (python-kasa's `EmeterStatus` documents the
    legacy `total` as kWh; the x1000 turns it into watt-hours to match.)
    """
    for key in (milli_key, *alt_keys):
        value = realtime.get(key)
        if value is not None:
            return int(value)
    value = realtime.get(base_key)
    if value is None:
        return None
    return int(round(float(value) * factor))
