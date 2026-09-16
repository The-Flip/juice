"""Collect air-quality data from Qingping IoT monitors via the Qingping cloud.

Qingping's cloud is its own world -- nothing to do with the Kasa plugs tap
reads on the LAN. An `AirAccount` owns the aiohttp session + a cached OAuth
token, and `connect()` yields one.

Auth is OAuth2 client-credentials (App Key / App Secret from
developer.qingping.co) against `oauth.cleargrass.com`; data comes from
`apis.cleargrass.com`. The access token lives ~2h, so it's cached and
refreshed lazily (on expiry, or on a 401).

These monitors are *room/zone*-scoped, not machine-scoped — there's no FlipFix
asset tag and no on/off control — so this stays deliberately separate from the
power pipeline (readings/rollups/state classification).
"""

from __future__ import annotations

import asyncio
import base64
import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import aiohttp

from juice.control import call_with_retry

if TYPE_CHECKING:
    from juice.store import Store

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


# ---------------------------------------------------------------------------
# The poll loop: what `serve` runs beside the collector when the Qingping
# credentials are set.
# ---------------------------------------------------------------------------

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
