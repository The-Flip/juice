"""Collect power data from Kasa HS300 smart power strips via TP-Link cloud."""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

import aiohttp

CLOUD_URL = "https://wap.tplinkcloud.com"


@dataclass
class PlugReading:
    child_id: str
    alias: str
    is_on: bool
    watts: float
    voltage: float
    amps: float
    total_kwh: float


@dataclass
class StripReading:
    alias: str
    device_id: str
    plugs: list[PlugReading]


def _plug_reading(child: dict, emeter: dict) -> PlugReading:
    """Build a PlugReading from raw sysinfo child and emeter dicts."""
    return PlugReading(
        child_id=child["id"],
        alias=child["alias"],
        is_on=bool(child["state"]),
        watts=emeter["power_mw"] / 1000,
        voltage=emeter["voltage_mv"] / 1000,
        amps=emeter["current_ma"] / 1000,
        total_kwh=emeter["total_wh"] / 1000,
    )


class Plug:
    """A single outlet on a power strip."""

    def __init__(self, child_id: str, alias: str, strip: Strip) -> None:
        self.child_id = child_id
        self.alias = alias
        self._strip = strip

    async def turn_on(self) -> None:
        """Turn this plug on."""
        await self._strip._passthrough(
            {
                "context": {"child_ids": [self.child_id]},
                "system": {"set_relay_state": {"state": 1}},
            }
        )

    async def turn_off(self) -> None:
        """Turn this plug off."""
        await self._strip._passthrough(
            {
                "context": {"child_ids": [self.child_id]},
                "system": {"set_relay_state": {"state": 0}},
            }
        )

    async def read(self) -> PlugReading:
        """Read power data for this plug."""
        emeter_resp, sysinfo_resp = await asyncio.gather(
            self._strip._passthrough(
                {
                    "context": {"child_ids": [self.child_id]},
                    "emeter": {"get_realtime": {}},
                }
            ),
            self._strip._passthrough(
                {
                    "context": {"child_ids": [self.child_id]},
                    "system": {"get_sysinfo": {}},
                }
            ),
        )
        child = sysinfo_resp["system"]["get_sysinfo"]["children"][0]
        em = emeter_resp["emeter"]["get_realtime"]
        return _plug_reading(child, em)

    def __repr__(self) -> str:
        return f"Plug({self.alias!r})"


class Strip:
    """A Kasa HS300 power strip."""

    def __init__(
        self,
        device_id: str,
        alias: str,
        model: str,
        server_url: str,
        account: Account,
    ) -> None:
        self.device_id = device_id
        self.alias = alias
        self.model = model
        self._server_url = server_url
        self._account = account
        self._plugs: list[Plug] | None = None

    async def plugs(self) -> list[Plug]:
        """Return plug objects, fetching sysinfo if needed."""
        if self._plugs is None:
            await self._sysinfo()
        return self._plugs

    async def _sysinfo(self) -> dict:
        """Fetch sysinfo, caching plug objects as a side effect."""
        resp = await self._passthrough({"system": {"get_sysinfo": {}}})
        sysinfo = resp["system"]["get_sysinfo"]
        self._plugs = [
            Plug(child_id=c["id"], alias=c["alias"], strip=self) for c in sysinfo["children"]
        ]
        return sysinfo

    async def _passthrough(self, request: dict) -> dict:
        """Send a passthrough request to this strip."""
        return await self._account._passthrough(
            self._server_url,
            self.device_id,
            request,
        )

    async def read(self) -> StripReading:
        """Read power data from all plugs on this strip."""
        sysinfo = await self._sysinfo()
        children = sysinfo["children"]

        async def _read_plug(child: dict) -> PlugReading:
            emeter_resp = await self._passthrough(
                {
                    "context": {"child_ids": [child["id"]]},
                    "emeter": {"get_realtime": {}},
                },
            )
            return _plug_reading(child, emeter_resp["emeter"]["get_realtime"])

        plug_readings = await asyncio.gather(*[_read_plug(c) for c in children])

        return StripReading(
            alias=self.alias,
            device_id=self.device_id,
            plugs=list(plug_readings),
        )

    def __repr__(self) -> str:
        return f"Strip({self.alias!r}, {self.device_id[:12]}...)"


class Account:
    """TP-Link cloud account — owns the session and token."""

    def __init__(self, session: aiohttp.ClientSession, token: str) -> None:
        self._session = session
        self._token = token

    async def strips(self) -> list[Strip]:
        """List HS300 power strips on the account."""
        resp = await self._session.post(
            f"{CLOUD_URL}?token={self._token}",
            json={"method": "getDeviceList"},
        )
        data = await resp.json()
        result = []
        for dev in data["result"]["deviceList"]:
            if "HS300" in dev.get("deviceModel", ""):
                result.append(
                    Strip(
                        device_id=dev["deviceId"],
                        alias=dev["alias"],
                        model=dev["deviceModel"],
                        server_url=dev["appServerUrl"],
                        account=self,
                    )
                )
        return result

    async def strip(self, device_id: str) -> Strip:
        """Find a strip by full or prefix device ID."""
        strips = await self.strips()
        for s in strips:
            if s.device_id.startswith(device_id):
                return s
        raise LookupError(f"No strip found matching '{device_id}'")

    async def _passthrough(
        self,
        server_url: str,
        device_id: str,
        request: dict,
    ) -> dict:
        resp = await self._session.post(
            f"{server_url}?token={self._token}",
            json={
                "method": "passthrough",
                "params": {
                    "deviceId": device_id,
                    "requestData": json.dumps(request),
                },
            },
        )
        data = await resp.json()
        if data.get("error_code", -1) != 0:
            raise RuntimeError(f"Passthrough failed: {data.get('msg', data)}")
        return json.loads(data["result"]["responseData"])


@asynccontextmanager
async def connect(username: str, password: str) -> AsyncIterator[Account]:
    """Connect to the TP-Link cloud and yield an Account."""
    async with aiohttp.ClientSession() as session:
        resp = await session.post(
            CLOUD_URL,
            json={
                "method": "login",
                "params": {
                    "appType": "Tapo_Android",
                    "cloudUserName": username,
                    "cloudPassword": password,
                    "terminalUUID": str(uuid.uuid4()),
                },
            },
        )
        data = await resp.json()
        if data.get("error_code", -1) != 0:
            raise RuntimeError(f"Cloud login failed: {data.get('msg', data)}")
        token = data["result"]["token"]
        yield Account(session, token)
