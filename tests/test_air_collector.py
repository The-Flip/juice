"""Tests for juice.air_collector — the Qingping cloud API wrapper."""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest
from aioresponses import aioresponses

from juice.air_collector import (
    OAUTH_URL,
    AirReading,
    AirSensor,
    _parse_online,
    _reading_ts,
    connect,
)
from juice.store import Store

# aioresponses matches the full URL incl. query; both endpoints carry a
# per-request `timestamp`, so match them by pattern.
_DEVICES_RE = re.compile(r"^https://apis\.cleargrass\.com/v1/apis/devices\?.*$")
_DATA_RE = re.compile(r"^https://apis\.cleargrass\.com/v1/apis/devices/data\?.*$")

FAKE_TOKEN = "fake-access-token"
MAC = "582D34AABBCC"


def _token_response(token: str = FAKE_TOKEN, expires_in: int = 7200) -> dict:
    return {"access_token": token, "expires_in": expires_in, "token_type": "bearer"}


def _metric(value: float) -> dict:
    return {"value": value}


def _device(
    mac: str = MAC,
    name: str = "Main Floor",
    status: object = 1,
    *,
    ts: int = 1_700_000_000,
    full: bool = True,
) -> dict:
    data: dict = {"timestamp": _metric(ts), "temperature": _metric(22.5), "humidity": _metric(45)}
    if full:
        data.update(
            {
                "co2": _metric(620),
                "pm25": _metric(8),
                "pm10": _metric(12),
                "tvoc": _metric(130),
                "battery": _metric(88),
            }
        )
    return {"info": {"mac": mac, "name": name, "status": status}, "data": data}


def _devices_response(*devices: dict) -> dict:
    return {"total": len(devices), "devices": list(devices)}


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (1, True),
        (0, False),
        (True, True),
        (False, False),
        ("online", True),
        ("offline", False),
        (None, True),  # absent status -> assume online
        ({"offline": False}, True),
        ({"offline": True}, False),
    ],
)
def test_parse_online(status: object, expected: bool) -> None:
    assert _parse_online(status) is expected


def test_reading_ts_from_unix() -> None:
    ts = _reading_ts({"timestamp": {"value": 1_700_000_000}})
    assert ts.year == 2023
    assert ts.tzinfo is not None


def test_reading_ts_falls_back_to_now_when_missing() -> None:
    ts = _reading_ts({})  # no timestamp key
    assert ts.tzinfo is not None


# ---------------------------------------------------------------------------
# AirAccount.devices()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_devices_parses_sensor_and_reading() -> None:
    with aioresponses() as m:
        m.post(OAUTH_URL, payload=_token_response())
        m.get(_DEVICES_RE, payload=_devices_response(_device()))
        async with connect("app-key", "app-secret") as account:
            pairs = await account.devices()

    assert len(pairs) == 1
    sensor, reading = pairs[0]
    assert sensor == AirSensor(mac=MAC, name="Main Floor", online=True)
    assert reading.mac == MAC
    assert reading.temperature == 22.5
    assert reading.humidity == 45.0
    assert reading.co2 == 620.0
    assert reading.pm25 == 8.0
    assert reading.pm10 == 12.0
    assert reading.tvoc == 130.0
    assert reading.battery == 88.0
    assert reading.ts.year == 2023


@pytest.mark.asyncio
async def test_devices_missing_metrics_become_none() -> None:
    with aioresponses() as m:
        m.post(OAUTH_URL, payload=_token_response())
        m.get(_DEVICES_RE, payload=_devices_response(_device(full=False)))
        async with connect("k", "s") as account:
            pairs = await account.devices()

    _, reading = pairs[0]
    assert reading.temperature == 22.5
    assert reading.co2 is None
    assert reading.pm25 is None
    assert reading.battery is None
    assert reading.noise is None


@pytest.mark.asyncio
async def test_devices_marks_offline_sensor() -> None:
    with aioresponses() as m:
        m.post(OAUTH_URL, payload=_token_response())
        m.get(_DEVICES_RE, payload=_devices_response(_device(status=0)))
        async with connect("k", "s") as account:
            pairs = await account.devices()

    sensor, _ = pairs[0]
    assert sensor.online is False


@pytest.mark.asyncio
async def test_token_is_cached_across_calls() -> None:
    with aioresponses() as m:
        m.post(OAUTH_URL, payload=_token_response())  # registered ONCE
        m.get(_DEVICES_RE, payload=_devices_response(_device()))
        m.get(_DEVICES_RE, payload=_devices_response(_device()))
        async with connect("k", "s") as account:
            await account.devices()
            await account.devices()
            # Only one token request was registered; a second would 500 if hit.
            assert account._token == FAKE_TOKEN


@pytest.mark.asyncio
async def test_token_refreshes_when_expired() -> None:
    with aioresponses() as m:
        m.post(OAUTH_URL, payload=_token_response(token="first"))
        m.get(_DEVICES_RE, payload=_devices_response(_device()))
        m.post(OAUTH_URL, payload=_token_response(token="second"))
        m.get(_DEVICES_RE, payload=_devices_response(_device()))
        async with connect("k", "s") as account:
            await account.devices()
            assert account._token == "first"
            account._expire_token_now()  # simulate the ~2h TTL elapsing
            await account.devices()
            assert account._token == "second"


@pytest.mark.asyncio
async def test_get_refreshes_token_on_401_then_succeeds() -> None:
    # A 401 drops the cached token; call_with_retry's next attempt mints a fresh
    # one and the request succeeds.
    with aioresponses() as m:
        m.post(OAUTH_URL, payload=_token_response(token="t1"))
        m.get(_DEVICES_RE, status=401)
        m.post(OAUTH_URL, payload=_token_response(token="t2"))
        m.get(_DEVICES_RE, payload=_devices_response(_device()))
        async with connect("k", "s") as account:
            pairs = await account.devices()
    assert len(pairs) == 1
    assert account._token == "t2"


@pytest.mark.asyncio
async def test_get_raises_on_persistent_server_error() -> None:
    # A non-2xx surfaces as a ClientResponseError (not silently parsed) and is
    # bounded by max_attempts rather than retrying forever.
    with aioresponses() as m:
        m.post(OAUTH_URL, payload=_token_response())
        m.get(_DEVICES_RE, status=500, repeat=True)
        async with connect("k", "s") as account:
            with pytest.raises(aiohttp.ClientResponseError):
                await account.devices()


@pytest.mark.asyncio
async def test_history_parses_series() -> None:
    history_payload = {
        "total": 2,
        "data": [
            {"timestamp": _metric(1_700_000_000), "co2": _metric(600)},
            {"timestamp": _metric(1_700_000_900), "co2": _metric(640)},
        ],
    }
    with aioresponses() as m:
        m.post(OAUTH_URL, payload=_token_response())
        m.get(_DATA_RE, payload=history_payload)
        async with connect("k", "s") as account:
            rows = await account.history(MAC, 1_700_000_000, 1_700_001_000)

    assert len(rows) == 2
    assert all(isinstance(r, AirReading) for r in rows)
    assert rows[0].co2 == 600.0
    assert rows[1].co2 == 640.0


@pytest.mark.asyncio
async def test_history_paginates_until_short_page() -> None:
    # A full page (== limit) triggers a follow-up fetch; a short page ends it.
    page1 = {
        "data": [
            {"timestamp": _metric(1000), "co2": _metric(1)},
            {"timestamp": _metric(2000), "co2": _metric(2)},
        ]
    }
    page2 = {"data": [{"timestamp": _metric(3000), "co2": _metric(3)}]}
    with aioresponses() as m:
        m.post(OAUTH_URL, payload=_token_response())
        m.get(_DATA_RE, payload=page1)
        m.get(_DATA_RE, payload=page2)
        async with connect("k", "s") as account:
            rows = await account.history(MAC, 1000, 100_000, limit=2)

    assert [r.co2 for r in rows] == [1.0, 2.0, 3.0]


class TestAirPollOnce:
    @pytest.mark.asyncio
    async def test_persists_sensors_and_readings(self) -> None:
        from juice.air_collector import AirReading, AirSensor, air_poll_once

        ts = datetime(2026, 6, 20, 12, 0, 0, tzinfo=UTC)
        reading = AirReading(
            mac="MAC1",
            ts=ts,
            temperature=22.5,
            humidity=45.0,
            co2=620.0,
            pm25=8.0,
            pm10=12.0,
            tvoc=130.0,
            noise=None,
            battery=88.0,
        )
        air_account = MagicMock()
        air_account.devices = AsyncMock(
            return_value=[(AirSensor(mac="MAC1", name="Main Floor", online=True), reading)]
        )

        with Store(":memory:") as store:
            count = await air_poll_once(air_account, store, ts)
            assert count == 1
            sensors = store.list_air_sensors()
            assert sensors[0]["mac"] == "MAC1"
            assert sensors[0]["name"] == "Main Floor"
            latest = store.air_latest()
            assert latest["MAC1"]["co2"] == 620.0

    @pytest.mark.asyncio
    async def test_no_sensors_is_a_noop(self) -> None:
        from juice.air_collector import air_poll_once

        air_account = MagicMock()
        air_account.devices = AsyncMock(return_value=[])
        with Store(":memory:") as store:
            count = await air_poll_once(air_account, store, datetime.now(UTC))
            assert count == 0
            assert store.list_air_sensors() == []


class TestAirBackfill:
    @pytest.mark.asyncio
    async def test_inserts_history(self) -> None:
        from juice.air_collector import AirReading, AirSensor, air_backfill

        t = datetime(2026, 6, 20, 12, 0, 0, tzinfo=UTC)
        hist = [
            AirReading(mac="MAC1", ts=t, co2=600.0),
            AirReading(mac="MAC1", ts=t + timedelta(minutes=15), co2=620.0),
        ]
        acct = MagicMock()
        acct.history = AsyncMock(return_value=hist)
        with Store(":memory:") as store:
            n = await air_backfill(
                acct, store, [AirSensor("MAC1", "Main", True)], datetime(2026, 6, 21, tzinfo=UTC)
            )
            assert n == 2
            assert store.air_latest()["MAC1"]["co2"] == 620.0

    @pytest.mark.asyncio
    async def test_first_run_uses_default_lookback(self) -> None:
        from juice.air_collector import AirSensor, air_backfill

        acct = MagicMock()
        acct.history = AsyncMock(return_value=[])
        now = datetime(2026, 6, 21, 0, 0, 0, tzinfo=UTC)
        with Store(":memory:") as store:
            await air_backfill(acct, store, [AirSensor("MAC1", "Main", True)], now, default_days=30)
        _mac, start_unix, end_unix = acct.history.call_args.args[:3]
        assert end_unix == int(now.timestamp())
        assert start_unix == end_unix - 30 * 86_400

    @pytest.mark.asyncio
    async def test_gap_fill_starts_after_last_stored(self) -> None:
        from juice.air_collector import AirSensor, air_backfill

        last = datetime(2026, 6, 20, 12, 0, 0, tzinfo=UTC)
        acct = MagicMock()
        acct.history = AsyncMock(return_value=[])
        with Store(":memory:") as store:
            # (ts, mac, temperature, humidity, co2, pm25, pm10, tvoc, noise, battery)
            store.insert_air_readings(
                [(last, "MAC1", 22.0, 44.0, 600.0, 7.0, 10.0, 120.0, None, 90.0)]
            )
            await air_backfill(
                acct, store, [AirSensor("MAC1", "Main", True)], datetime(2026, 6, 21, tzinfo=UTC)
            )
        _mac, start_unix, _end = acct.history.call_args.args[:3]
        assert start_unix == int(last.timestamp()) + 1

    @pytest.mark.asyncio
    async def test_one_sensor_failure_does_not_abort_others(self) -> None:
        from juice.air_collector import AirReading, AirSensor, air_backfill

        t = datetime(2026, 6, 20, 12, 0, 0, tzinfo=UTC)

        async def _history(mac, *_args, **_kw):
            if mac == "BAD":
                raise RuntimeError("boom")
            return [AirReading(mac="GOOD", ts=t, co2=500.0)]

        acct = MagicMock()
        acct.history = AsyncMock(side_effect=_history)
        with Store(":memory:") as store:
            n = await air_backfill(
                acct,
                store,
                [AirSensor("BAD", "Bad", True), AirSensor("GOOD", "Good", True)],
                datetime(2026, 6, 21, tzinfo=UTC),
            )
            assert n == 1
            assert "GOOD" in store.air_latest()
