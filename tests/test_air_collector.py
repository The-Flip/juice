"""Tests for juice.air_collector — the Qingping cloud API wrapper."""

from __future__ import annotations

import re

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
