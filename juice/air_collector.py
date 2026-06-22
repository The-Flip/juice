"""Collect air-quality data from Qingping IoT monitors via the Qingping cloud.

Qingping's cloud is a separate world from the Kasa/TP-Link cloud the power
collector talks to, but the shape mirrors `juice.collector`: an `AirAccount`
owns the aiohttp session + a cached OAuth token, and `connect()` yields one.

Auth is OAuth2 client-credentials (App Key / App Secret from
developer.qingping.co) against `oauth.cleargrass.com`; data comes from
`apis.cleargrass.com`. The access token lives ~2h, so it's cached and
refreshed lazily (on expiry, or on a 401).

These monitors are *room/zone*-scoped, not machine-scoped — there's no FlipFix
asset tag and no on/off control — so this stays deliberately separate from the
power pipeline (readings/rollups/state classification).
"""

from __future__ import annotations

import base64
import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime

import aiohttp

from juice.collector import call_with_retry

log = logging.getLogger(__name__)

OAUTH_URL = "https://oauth.cleargrass.com/oauth2/token"
API_BASE = "https://apis.cleargrass.com"

# Refresh the token this many seconds before its stated expiry, so a request
# never races the TTL boundary.
_TOKEN_SAFETY_MARGIN = 60.0

# Bound per-request retries so a persistent 401/403/5xx surfaces to the recorder
# loop (logged, retried next cycle) instead of spinning forever — call_with_retry
# is otherwise unbounded.
_MAX_REQUEST_ATTEMPTS = 4

# The /data endpoint caps a response at `limit` rows, so history() pages through
# by advancing the start cursor. This bounds the page count as a runaway guard
# (30 days at 15-min cadence ≈ 2880 rows ≈ 15 pages of 200).
_MAX_HISTORY_PAGES = 200

# Metric keys in the Qingping `data` payload that map onto AirReading fields.
# Each value in the payload is an object like {"value": 22.5}. Field
# availability varies by model — absent metrics stay None.
_METRIC_FIELDS = ("temperature", "humidity", "co2", "pm25", "pm10", "tvoc", "noise", "battery")


@dataclass
class AirSensor:
    """A Qingping air monitor (identified by MAC, named in the Qingping+ app)."""

    mac: str
    name: str
    online: bool


@dataclass
class AirReading:
    """One snapshot of a monitor's metrics. Missing metrics are None."""

    mac: str
    ts: datetime
    temperature: float | None = None
    humidity: float | None = None
    co2: float | None = None
    pm25: float | None = None
    pm10: float | None = None
    tvoc: float | None = None
    noise: float | None = None
    battery: float | None = None


def _parse_online(status: object) -> bool:
    """Interpret a device's `info.status` as an online boolean, defensively.

    The cloud has reported status as an int (1/0), a string, or a dict with an
    `offline` flag across firmware/model variants; an absent status is treated
    as online (a listed device is, by default, reachable).
    """
    if status is None:
        return True
    if isinstance(status, bool):
        return status
    if isinstance(status, int | float):
        return bool(status)
    if isinstance(status, str):
        return status.strip().lower() in {"online", "1", "true", "on"}
    if isinstance(status, dict):
        if "offline" in status:
            return not status["offline"]
        if "online" in status:
            return bool(status["online"])
    return True


def _num(data: dict, key: str) -> float | None:
    """Pull data[key]['value'] as a float, or None if absent/unparseable."""
    item = data.get(key)
    if not isinstance(item, dict):
        return None
    try:
        return float(item["value"])
    except KeyError, TypeError, ValueError:
        return None


def _reading_ts(data: dict) -> datetime:
    """The reading's own timestamp (unix secs in `data.timestamp.value`), UTC.

    Falls back to now() when the payload omits or mangles it.
    """
    item = data.get("timestamp")
    if isinstance(item, dict):
        try:
            return datetime.fromtimestamp(float(item["value"]), UTC)
        except KeyError, TypeError, ValueError, OSError, OverflowError:
            pass
    return datetime.now(UTC)


def _parse_reading(mac: str, data: dict) -> AirReading:
    return AirReading(
        mac=mac,
        ts=_reading_ts(data),
        **{field: _num(data, field) for field in _METRIC_FIELDS},
    )


class AirAccount:
    """A Qingping cloud account — owns the session and a cached OAuth token."""

    def __init__(self, session: aiohttp.ClientSession, app_key: str, app_secret: str) -> None:
        self._session = session
        self._app_key = app_key
        self._app_secret = app_secret
        self._token: str | None = None
        self._token_expiry: float = 0.0  # monotonic deadline; 0 = no token

    def _expire_token_now(self) -> None:
        """Force the next request to re-fetch a token (used in tests)."""
        self._token_expiry = 0.0

    async def _fetch_token(self) -> str:
        basic = base64.b64encode(f"{self._app_key}:{self._app_secret}".encode()).decode()
        async with self._session.post(
            OAUTH_URL,
            headers={"Authorization": f"Basic {basic}"},
            data={"grant_type": "client_credentials", "scope": "device_full_access"},
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
        token = data.get("access_token")
        if not token:
            raise RuntimeError(f"Qingping token request failed: {data}")
        expires_in = float(data.get("expires_in", 7200))
        self._token = token
        self._token_expiry = time.monotonic() + max(0.0, expires_in - _TOKEN_SAFETY_MARGIN)
        return token

    async def _ensure_token(self) -> str:
        if self._token is not None and time.monotonic() < self._token_expiry:
            return self._token
        return await self._fetch_token()

    async def _get(self, path: str, params: dict[str, str | int]) -> dict:
        """Authenticated GET returning parsed JSON; refreshes once on a 401."""

        async def _do() -> dict:
            token = await self._ensure_token()
            query: dict[str, str | int] = {**params, "timestamp": int(time.time())}
            async with self._session.get(
                f"{API_BASE}{path}",
                params=query,
                headers={"Authorization": f"Bearer {token}"},
            ) as resp:
                if resp.status == 401:
                    # Token rejected (e.g. revoked before its stated TTL) — drop it
                    # and let call_with_retry's next attempt mint a fresh one.
                    self._expire_token_now()
                    raise aiohttp.ClientResponseError(
                        resp.request_info, resp.history, status=401, message="unauthorized"
                    )
                # Surface 4xx/5xx (403/429/5xx) as a ClientError before JSON
                # parsing — call_with_retry will back off and retry transient ones.
                resp.raise_for_status()
                return await resp.json()

        return await call_with_retry(_do, max_attempts=_MAX_REQUEST_ATTEMPTS)

    async def devices(self) -> list[tuple[AirSensor, AirReading]]:
        """List bound monitors, each with its latest snapshot reading."""
        data = await self._get("/v1/apis/devices", {})
        pairs: list[tuple[AirSensor, AirReading]] = []
        for dev in data.get("devices", []):
            info = dev.get("info", {})
            mac = info.get("mac")
            if not mac:
                continue
            sensor = AirSensor(
                mac=mac,
                name=info.get("name") or "",
                online=_parse_online(info.get("status")),
            )
            pairs.append((sensor, _parse_reading(mac, dev.get("data", {}))))
        return pairs

    async def history(
        self, mac: str, start_time: int, end_time: int, limit: int = 200
    ) -> list[AirReading]:
        """All historical readings for one monitor in [start_time, end_time) (unix secs).

        The endpoint returns at most `limit` rows per call, so this pages by
        advancing the start cursor past the last reading until a short (or
        empty) page arrives. Cursor advancement also guarantees forward
        progress, so a misbehaving API can't spin forever.
        """
        out: list[AirReading] = []
        cursor = start_time
        for _ in range(_MAX_HISTORY_PAGES):
            data = await self._get(
                "/v1/apis/devices/data",
                {"mac": mac, "start_time": cursor, "end_time": end_time, "limit": limit},
            )
            batch = [_parse_reading(mac, row) for row in data.get("data", [])]
            if not batch:
                break
            out.extend(batch)
            if len(batch) < limit:
                break
            next_cursor = int(batch[-1].ts.timestamp()) + 1
            if next_cursor <= cursor:  # no forward progress — stop defensively
                break
            cursor = next_cursor
        return out


@asynccontextmanager
async def connect(app_key: str, app_secret: str) -> AsyncIterator[AirAccount]:
    """Open a Qingping cloud session and yield an AirAccount.

    The token is fetched lazily on first use, so opening the session is cheap
    and never fails on bad credentials until the first request.
    """
    async with aiohttp.ClientSession() as session:
        yield AirAccount(session, app_key, app_secret)
