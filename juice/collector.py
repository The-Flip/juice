"""Collect power data from Kasa HS300 smart power strips via TP-Link cloud."""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
import re
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass

import aiohttp

log = logging.getLogger(__name__)

CLOUD_URL = "https://wap.tplinkcloud.com"

# Transient cloud-API messages worth retrying. Matches the three error patterns
# observed in production audit logs (the second covers "Device is offline" and
# "Device is offline during processing").
_RETRYABLE_PASSTHROUGH_MESSAGES = ("Request timeout", "Device is offline")

# Backoff schedule for call_with_retry: 0.5, 1, 2, 4, 4, 4, ... capped at _MAX_DELAY.
_RETRY_BASE_DELAY = 0.5
_RETRY_MAX_DELAY = 4.0
# Granularity at which an in-flight backoff polls should_stop(). Lower = more
# responsive cancel, higher = fewer wakeups.
_RETRY_SLEEP_TICK = 0.1


def is_retryable_passthrough_error(exc: BaseException) -> bool:
    """True for transient power-control failures that deserve another attempt."""
    if isinstance(exc, asyncio.TimeoutError | aiohttp.ClientError):
        return True
    if isinstance(exc, RuntimeError):
        msg = str(exc)
        if msg.startswith("Passthrough failed: "):
            return any(m in msg for m in _RETRYABLE_PASSTHROUGH_MESSAGES)
    return False


async def call_with_retry[T](
    fn: Callable[[], Awaitable[T]],
    *,
    should_stop: Callable[[], bool] | None = None,
    max_attempts: int | None = None,
    on_retry: Callable[[int, BaseException, float], None] | None = None,
) -> T:
    """Call fn() with retries on transient passthrough errors.

    Delays double each attempt up to _RETRY_MAX_DELAY. Each backoff is chunked
    into _RETRY_SLEEP_TICK slices so should_stop() is polled while sleeping.
    Re-raises the last exception when should_stop returns True or max_attempts
    is exhausted; non-retryable errors propagate immediately.

    on_retry(attempt, exc, delay) is invoked between attempts so callers can
    observe progress (logging, SSE events). `attempt` is the just-failed
    attempt (1-based); the next attempt about to run is `attempt + 1`.
    """
    attempt = 0
    last_exc: BaseException | None = None
    while True:
        attempt += 1
        try:
            return await fn()
        except BaseException as e:
            if not is_retryable_passthrough_error(e):
                raise
            last_exc = e
            if max_attempts is not None and attempt >= max_attempts:
                raise
            if should_stop is not None and should_stop():
                raise

            delay = min(_RETRY_BASE_DELAY * (2 ** (attempt - 1)), _RETRY_MAX_DELAY)
            if on_retry is not None:
                on_retry(attempt, e, delay)

            # Interruptible sleep: wake every _RETRY_SLEEP_TICK to check should_stop.
            remaining = delay
            while remaining > 0:
                if should_stop is not None and should_stop():
                    raise last_exc from None
                step = min(_RETRY_SLEEP_TICK, remaining)
                await asyncio.sleep(step)
                remaining -= step


@dataclass
class PlugReading:
    child_id: str
    alias: str
    is_on: bool
    watts: float | None
    voltage: float | None
    amps: float | None
    total_kwh: float | None


@dataclass
class StripReading:
    alias: str
    device_id: str
    plugs: list[PlugReading]


def outlet_number(child_id: str) -> int | None:
    """1-based physical outlet position from an HS300 child_id.

    HS300 child IDs are the device_id plus a two-digit 0-based outlet index
    ("00".."05"). Single-outlet devices (EP10 _SelfPlug) use "" — no position.
    """
    if len(child_id) < 2 or not child_id[-2:].isdigit():
        return None
    return int(child_id[-2:]) + 1


def _plug_reading(child: dict, emeter: dict | None) -> PlugReading:
    """Build a PlugReading from raw sysinfo child and emeter dicts.

    Pass emeter=None for devices without energy monitoring (e.g. EP10),
    in which case all power fields are set to None.
    """
    if emeter is None:
        return PlugReading(
            child_id=child["id"],
            alias=child["alias"],
            is_on=bool(child["state"]),
            watts=None,
            voltage=None,
            amps=None,
            total_kwh=None,
        )
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
    """A Kasa HS300 power strip (multi-outlet, per-outlet energy monitoring)."""

    has_emeter = True

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

    async def child_states(self) -> list[dict]:
        """Return per-child state dicts ({id, alias, state}) from one sysinfo call."""
        sysinfo = await self._sysinfo()
        return sysinfo["children"]

    async def read_emeter(self, child_id: str) -> dict | None:
        """Fetch raw emeter realtime dict for one child plug."""
        resp = await self._passthrough(
            {
                "context": {"child_ids": [child_id]},
                "emeter": {"get_realtime": {}},
            }
        )
        return resp["emeter"]["get_realtime"]

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


class _SelfPlug:
    """A single-outlet device's only plug — delegates to the parent Outlet."""

    child_id = ""

    def __init__(self, outlet: Outlet) -> None:
        self._outlet = outlet
        self.alias = outlet.alias

    async def turn_on(self) -> None:
        await self._outlet._passthrough({"system": {"set_relay_state": {"state": 1}}})

    async def turn_off(self) -> None:
        await self._outlet._passthrough({"system": {"set_relay_state": {"state": 0}}})

    async def read(self) -> PlugReading:
        return (await self._outlet.read()).plugs[0]

    def __repr__(self) -> str:
        return f"Plug({self.alias!r})"


class Outlet:
    """A single-outlet Kasa device (e.g. EP10).

    Has no children; the device itself is the outlet. May or may not have
    energy monitoring (`has_emeter`) — EP10 does not; EP25/KP115/KP125 do.
    """

    def __init__(
        self,
        device_id: str,
        alias: str,
        model: str,
        server_url: str,
        account: Account,
        has_emeter: bool = False,
    ) -> None:
        self.device_id = device_id
        self.alias = alias
        self.model = model
        self.has_emeter = has_emeter
        self._server_url = server_url
        self._account = account
        self._plug: _SelfPlug | None = None

    async def plugs(self) -> list[_SelfPlug]:
        if self._plug is None:
            self._plug = _SelfPlug(self)
        return [self._plug]

    async def _sysinfo(self) -> dict:
        """Fetch sysinfo, returning the inner get_sysinfo dict."""
        resp = await self._passthrough({"system": {"get_sysinfo": {}}})
        return resp["system"]["get_sysinfo"]

    async def _passthrough(self, request: dict) -> dict:
        return await self._account._passthrough(
            self._server_url,
            self.device_id,
            request,
        )

    async def child_states(self) -> list[dict]:
        """Return a single 'child' dict synthesized from the device's relay_state."""
        sysinfo = await self._sysinfo()
        return [
            {
                "id": "",
                "alias": self.alias,
                "state": sysinfo.get("relay_state", 0),
            }
        ]

    async def read_emeter(self, child_id: str) -> dict | None:
        """Fetch raw emeter realtime dict, or None if device has no energy monitoring."""
        if not self.has_emeter:
            return None
        resp = await self._passthrough({"emeter": {"get_realtime": {}}})
        return resp["emeter"]["get_realtime"]

    async def read(self) -> StripReading:
        sysinfo = await self._sysinfo()
        is_on = bool(sysinfo.get("relay_state", 0))
        emeter: dict | None
        if self.has_emeter:
            emeter_resp = await self._passthrough({"emeter": {"get_realtime": {}}})
            emeter = emeter_resp["emeter"]["get_realtime"]
        else:
            emeter = None
        child = {"id": "", "alias": self.alias, "state": 1 if is_on else 0}
        plug_reading = _plug_reading(child, emeter)
        return StripReading(
            alias=self.alias,
            device_id=self.device_id,
            plugs=[plug_reading],
        )

    def __repr__(self) -> str:
        return f"Outlet({self.alias!r}, {self.device_id[:12]}...)"


# Discovery: map TP-Link deviceModel substring → (class, constructor kwargs).
# A future EP25/KP115/KP125 with emeter would be one extra entry, e.g.
#   "EP25": (Outlet, {"has_emeter": True}).
_DEVICE_DISPATCH: list[tuple[str, type, dict]] = [
    ("HS300", Strip, {}),
    ("EP10", Outlet, {"has_emeter": False}),
]
# Newer Kasa devices (EP25, KP125M, …) use the SMART/KLAP protocol and don't
# respond to the legacy `wap.tplinkcloud.com` passthrough this collector is
# built on — they appear in getDeviceList but every read returns "Device is
# offline". `discover` flags them as [UNSUPPORTED MODEL] so operators can
# move tracked machines onto an HS300 outlet (or an EP10 for on/off-only).

# A base64-looking alias: only the base64 alphabet, length a multiple of 4.
_B64_ALIAS_RE = re.compile(r"^[A-Za-z0-9+/]{4,}={0,2}$")


def _decode_alias(raw: str) -> str:
    """Decode a base64-encoded device alias when it is unambiguously one.

    Newer Kasa models (e.g. EP25) report the alias base64-encoded in the cloud
    device list, while HS300/EP10 report plaintext. Real aliases here contain
    spaces/hyphens (e.g. "Star Trip - M0009"), so they never satisfy the strict
    base64 test below — only genuinely-encoded values are decoded; anything
    else is returned unchanged.
    """
    if len(raw) % 4 != 0 or not _B64_ALIAS_RE.match(raw):
        return raw
    try:
        decoded = base64.b64decode(raw, validate=True).decode("utf-8")
    except binascii.Error, ValueError:
        return raw
    # Guard against plaintext that happens to be valid base64 but decodes to
    # control bytes/garbage.
    return decoded if decoded.isprintable() and decoded.strip() else raw


def _build_device(dev: dict, account: Account) -> Strip | Outlet | None:
    model = dev.get("deviceModel", "")
    for needle, cls, extra in _DEVICE_DISPATCH:
        if needle in model:
            return cls(
                device_id=dev["deviceId"],
                alias=_decode_alias(dev["alias"]),
                model=model,
                server_url=dev["appServerUrl"],
                account=account,
                **extra,
            )
    return None


class Account:
    """TP-Link cloud account — owns the session and token."""

    def __init__(self, session: aiohttp.ClientSession, token: str) -> None:
        self._session = session
        self._token = token
        # device_ids we've already logged as unsupported; keeps the recorder's
        # 60s refresh from re-warning about the same device forever.
        self._logged_unsupported: set[str] = set()

    async def raw_devices(self) -> list[dict]:
        """Return the raw device dicts from the cloud, including unsupported
        models. Each dict has at least deviceId, alias, deviceModel, status
        (1 = online, 0 = offline), appServerUrl."""
        resp = await self._session.post(
            f"{CLOUD_URL}?token={self._token}",
            json={"method": "getDeviceList"},
        )
        data = await resp.json()
        return data["result"]["deviceList"]

    async def devices(self) -> list[Strip | Outlet]:
        """List all supported Kasa devices on the account (strips + outlets).

        Devices whose model isn't supported are logged and skipped — otherwise
        a swapped-in plug of an unknown model disappears from juice with no
        trace even though it's healthy in the Kasa app.
        """
        result: list[Strip | Outlet] = []
        for dev in await self.raw_devices():
            built = _build_device(dev, self)
            if built is None:
                dev_id = dev.get("deviceId", "")
                if dev_id not in self._logged_unsupported:
                    self._logged_unsupported.add(dev_id)
                    log.warning(
                        "Ignoring unsupported Kasa device: alias=%r model=%r id=%s",
                        _decode_alias(dev.get("alias", "")),
                        dev.get("deviceModel"),
                        dev_id[:12],
                    )
            else:
                result.append(built)
        return result

    async def strips(self) -> list[Strip]:
        """List HS300 power strips on the account."""
        return [d for d in await self.devices() if isinstance(d, Strip)]

    async def device(self, device_id: str) -> Strip | Outlet:
        """Find any device (strip or outlet) by full or prefix device ID."""
        for d in await self.devices():
            if d.device_id.startswith(device_id):
                return d
        raise LookupError(f"No device found matching '{device_id}'")

    async def strip(self, device_id: str) -> Strip:
        """Find a strip by full or prefix device ID."""
        for s in await self.strips():
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
