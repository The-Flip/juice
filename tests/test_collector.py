"""Tests for juice.collector — the OO cloud API wrapper."""

from __future__ import annotations

import json

import pytest
from aioresponses import aioresponses

from juice.collector import (
    CLOUD_URL,
    Account,
    Outlet,
    Plug,
    PlugReading,
    Strip,
    StripReading,
    _plug_reading,
    connect,
)

# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

FAKE_TOKEN = "fake-token-abc123"
DEVICE_ID = "8006B6174F130000AABBCCDD"
SERVER_URL = "https://use1-wap.tplinkcloud.com"


def _login_response(token: str = FAKE_TOKEN) -> dict:
    return {"error_code": 0, "result": {"token": token}}


def _login_failure() -> dict:
    return {"error_code": -20601, "msg": "Invalid credentials"}


def _device_list(*devices: dict) -> dict:
    return {"error_code": 0, "result": {"deviceList": list(devices)}}


def _hs300_device(
    device_id: str = DEVICE_ID,
    alias: str = "TP-LINK_Power Strip_98E1",
    model: str = "HS300(US)",
) -> dict:
    return {
        "deviceId": device_id,
        "alias": alias,
        "deviceModel": model,
        "appServerUrl": SERVER_URL,
    }


def _sysinfo_response(children: list[dict] | None = None) -> dict:
    if children is None:
        children = [
            {"id": "child01", "alias": "Plug 1", "state": 1},
            {"id": "child02", "alias": "Plug 2", "state": 0},
        ]
    inner = json.dumps({"system": {"get_sysinfo": {"children": children}}})
    return {"error_code": 0, "result": {"responseData": inner}}


def _emeter_response(
    power_mw: int = 100_000,
    voltage_mv: int = 120_000,
    current_ma: int = 833,
    total_wh: int = 5_000,
) -> dict:
    inner = json.dumps(
        {
            "emeter": {
                "get_realtime": {
                    "power_mw": power_mw,
                    "voltage_mv": voltage_mv,
                    "current_ma": current_ma,
                    "total_wh": total_wh,
                },
            },
        }
    )
    return {"error_code": 0, "result": {"responseData": inner}}


def _passthrough_error(msg: str = "device offline") -> dict:
    return {"error_code": -20571, "msg": msg}


def _ep10_device(
    device_id: str = "EP10AABBCCDD",
    alias: str = "Snack Machine",
    model: str = "EP10(US)",
) -> dict:
    return {
        "deviceId": device_id,
        "alias": alias,
        "deviceModel": model,
        "appServerUrl": SERVER_URL,
    }


def _ep10_sysinfo_response(relay_state: int = 1, alias: str = "Snack Machine") -> dict:
    inner = json.dumps(
        {
            "system": {
                "get_sysinfo": {
                    "model": "EP10(US)",
                    "alias": alias,
                    "feature": "TIM",
                    "relay_state": relay_state,
                }
            }
        }
    )
    return {"error_code": 0, "result": {"responseData": inner}}


def _set_relay_state_response() -> dict:
    inner = json.dumps({"system": {"set_relay_state": {"err_code": 0}}})
    return {"error_code": 0, "result": {"responseData": inner}}


@pytest.fixture
def mock_api():
    with aioresponses() as m:
        yield m


def _stub_login(mock_api, token: str = FAKE_TOKEN) -> None:
    mock_api.post(CLOUD_URL, payload=_login_response(token))


def _stub_device_list(mock_api, *devices: dict, token: str = FAKE_TOKEN) -> None:
    mock_api.post(f"{CLOUD_URL}?token={token}", payload=_device_list(*devices))


def _stub_passthrough(mock_api, payload: dict, token: str = FAKE_TOKEN) -> None:
    mock_api.post(f"{SERVER_URL}?token={token}", payload=payload)


# ---------------------------------------------------------------------------
# connect()
# ---------------------------------------------------------------------------


class TestConnect:
    @pytest.mark.asyncio
    async def test_yields_account(self, mock_api) -> None:
        _stub_login(mock_api)
        async with connect("user@example.com", "secret") as account:
            assert isinstance(account, Account)

    @pytest.mark.asyncio
    async def test_login_failure_raises(self, mock_api) -> None:
        mock_api.post(CLOUD_URL, payload=_login_failure())
        with pytest.raises(RuntimeError, match="Cloud login failed"):
            async with connect("user@example.com", "wrong"):
                pass


# ---------------------------------------------------------------------------
# Account.strips()
# ---------------------------------------------------------------------------


class TestAccountStrips:
    @pytest.mark.asyncio
    async def test_returns_hs300_strips(self, mock_api) -> None:
        _stub_login(mock_api)
        _stub_device_list(
            mock_api,
            _hs300_device(),
            _hs300_device(
                device_id="SECOND_DEVICE",
                alias="Strip 2",
            ),
        )

        async with connect("u", "p") as account:
            strips = await account.strips()
            assert len(strips) == 2
            assert strips[0].alias == "TP-LINK_Power Strip_98E1"
            assert strips[0].device_id == DEVICE_ID
            assert strips[1].device_id == "SECOND_DEVICE"

    @pytest.mark.asyncio
    async def test_filters_non_hs300(self, mock_api) -> None:
        _stub_login(mock_api)
        _stub_device_list(
            mock_api,
            _hs300_device(),
            {
                "deviceId": "OTHER",
                "alias": "Bulb",
                "deviceModel": "LB100",
                "appServerUrl": SERVER_URL,
            },
        )

        async with connect("u", "p") as account:
            strips = await account.strips()
            assert len(strips) == 1
            assert strips[0].device_id == DEVICE_ID

    @pytest.mark.asyncio
    async def test_empty_list(self, mock_api) -> None:
        _stub_login(mock_api)
        _stub_device_list(mock_api)

        async with connect("u", "p") as account:
            strips = await account.strips()
            assert strips == []


# ---------------------------------------------------------------------------
# Account.strip()
# ---------------------------------------------------------------------------


class TestAccountStrip:
    @pytest.mark.asyncio
    async def test_find_by_full_id(self, mock_api) -> None:
        _stub_login(mock_api)
        _stub_device_list(mock_api, _hs300_device())

        async with connect("u", "p") as account:
            strip = await account.strip(DEVICE_ID)
            assert strip.device_id == DEVICE_ID

    @pytest.mark.asyncio
    async def test_find_by_prefix(self, mock_api) -> None:
        _stub_login(mock_api)
        _stub_device_list(mock_api, _hs300_device())

        async with connect("u", "p") as account:
            strip = await account.strip("8006B6")
            assert strip.device_id == DEVICE_ID

    @pytest.mark.asyncio
    async def test_not_found_raises(self, mock_api) -> None:
        _stub_login(mock_api)
        _stub_device_list(mock_api, _hs300_device())

        async with connect("u", "p") as account:
            with pytest.raises(LookupError, match="No strip found"):
                await account.strip("XXXXXX")


# ---------------------------------------------------------------------------
# Strip.plugs()
# ---------------------------------------------------------------------------


class TestStripPlugs:
    @pytest.mark.asyncio
    async def test_fetches_and_returns_plugs(self, mock_api) -> None:
        _stub_login(mock_api)
        _stub_device_list(mock_api, _hs300_device())
        _stub_passthrough(mock_api, _sysinfo_response())

        async with connect("u", "p") as account:
            strip = await account.strip(DEVICE_ID)
            plugs = await strip.plugs()
            assert len(plugs) == 2
            assert plugs[0].alias == "Plug 1"
            assert plugs[0].child_id == "child01"
            assert plugs[1].alias == "Plug 2"

    @pytest.mark.asyncio
    async def test_caches_after_first_call(self, mock_api) -> None:
        _stub_login(mock_api)
        _stub_device_list(mock_api, _hs300_device())
        # Only one sysinfo response stubbed — a second network call would fail
        _stub_passthrough(mock_api, _sysinfo_response())

        async with connect("u", "p") as account:
            strip = await account.strip(DEVICE_ID)
            first = await strip.plugs()
            second = await strip.plugs()
            assert first is second


# ---------------------------------------------------------------------------
# Strip.read()
# ---------------------------------------------------------------------------


class TestStripRead:
    @pytest.mark.asyncio
    async def test_returns_strip_reading(self, mock_api) -> None:
        children = [
            {"id": "child01", "alias": "Pinball 1", "state": 1},
            {"id": "child02", "alias": "Pinball 2", "state": 0},
        ]
        _stub_login(mock_api)
        _stub_device_list(mock_api, _hs300_device())
        _stub_passthrough(mock_api, _sysinfo_response(children))
        # One emeter response per plug
        _stub_passthrough(mock_api, _emeter_response(power_mw=800_000, current_ma=6667))
        _stub_passthrough(mock_api, _emeter_response(power_mw=0, current_ma=0))

        async with connect("u", "p") as account:
            strip = await account.strip(DEVICE_ID)
            reading = await strip.read()

            assert isinstance(reading, StripReading)
            assert reading.alias == "TP-LINK_Power Strip_98E1"
            assert reading.device_id == DEVICE_ID
            assert len(reading.plugs) == 2

            p1 = reading.plugs[0]
            assert isinstance(p1, PlugReading)
            assert p1.child_id == "child01"
            assert p1.alias == "Pinball 1"
            assert p1.is_on is True
            assert p1.watts == pytest.approx(800.0)
            assert p1.amps == pytest.approx(6.667)

            p2 = reading.plugs[1]
            assert p2.child_id == "child02"
            assert p2.alias == "Pinball 2"
            assert p2.is_on is False
            assert p2.watts == pytest.approx(0.0)

    @pytest.mark.asyncio
    async def test_read_populates_plugs(self, mock_api) -> None:
        _stub_login(mock_api)
        _stub_device_list(mock_api, _hs300_device())
        _stub_passthrough(mock_api, _sysinfo_response())
        _stub_passthrough(mock_api, _emeter_response())
        _stub_passthrough(mock_api, _emeter_response())

        async with connect("u", "p") as account:
            strip = await account.strip(DEVICE_ID)
            await strip.read()
            # plugs() should return cached data without another network call
            plugs = await strip.plugs()
            assert len(plugs) == 2


# ---------------------------------------------------------------------------
# Plug.read()
# ---------------------------------------------------------------------------


class TestPlugRead:
    @pytest.mark.asyncio
    async def test_returns_plug_reading(self, mock_api) -> None:
        children = [{"id": "child01", "alias": "Pinball 1", "state": 1}]
        _stub_login(mock_api)
        _stub_device_list(mock_api, _hs300_device())
        _stub_passthrough(mock_api, _sysinfo_response(children))
        # Plug.read() makes two passthrough calls: emeter then sysinfo
        _stub_passthrough(
            mock_api,
            _emeter_response(
                power_mw=500_000,
                voltage_mv=121_500,
                current_ma=4115,
                total_wh=12_345,
            ),
        )
        _stub_passthrough(mock_api, _sysinfo_response(children))

        async with connect("u", "p") as account:
            strip = await account.strip(DEVICE_ID)
            plugs = await strip.plugs()
            reading = await plugs[0].read()

            assert isinstance(reading, PlugReading)
            assert reading.child_id == "child01"
            assert reading.alias == "Pinball 1"
            assert reading.is_on is True
            assert reading.watts == pytest.approx(500.0)
            assert reading.voltage == pytest.approx(121.5)
            assert reading.amps == pytest.approx(4.115)
            assert reading.total_kwh == pytest.approx(12.345)


# ---------------------------------------------------------------------------
# Passthrough error handling
# ---------------------------------------------------------------------------


class TestPassthroughErrors:
    @pytest.mark.asyncio
    async def test_passthrough_error_raises(self, mock_api) -> None:
        _stub_login(mock_api)
        _stub_device_list(mock_api, _hs300_device())
        _stub_passthrough(mock_api, _passthrough_error("device offline"))

        async with connect("u", "p") as account:
            strip = await account.strip(DEVICE_ID)
            with pytest.raises(RuntimeError, match="Passthrough failed"):
                await strip.read()


# ---------------------------------------------------------------------------
# Repr
# ---------------------------------------------------------------------------


class TestPlugReadingOptionalEmeter:
    def test_plug_reading_with_no_emeter_yields_none_power_fields(self) -> None:
        child = {"id": "self", "alias": "Snack", "state": 1}
        reading = _plug_reading(child, None)
        assert reading.is_on is True
        assert reading.watts is None
        assert reading.voltage is None
        assert reading.amps is None
        assert reading.total_kwh is None


# ---------------------------------------------------------------------------
# Account.devices() — mixed discovery
# ---------------------------------------------------------------------------


class TestAccountDevices:
    @pytest.mark.asyncio
    async def test_discovers_hs300_and_ep10(self, mock_api) -> None:
        _stub_login(mock_api)
        _stub_device_list(mock_api, _hs300_device(), _ep10_device())

        async with connect("u", "p") as account:
            devices = await account.devices()
            assert len(devices) == 2
            kinds = {type(d).__name__ for d in devices}
            assert kinds == {"Strip", "Outlet"}
            outlet = next(d for d in devices if isinstance(d, Outlet))
            assert outlet.has_emeter is False
            assert outlet.model == "EP10(US)"
            strip = next(d for d in devices if isinstance(d, Strip))
            assert strip.has_emeter is True

    @pytest.mark.asyncio
    async def test_filters_unknown_models(self, mock_api) -> None:
        _stub_login(mock_api)
        _stub_device_list(
            mock_api,
            _hs300_device(),
            _ep10_device(),
            {
                "deviceId": "OTHER",
                "alias": "Bulb",
                "deviceModel": "LB100",
                "appServerUrl": SERVER_URL,
            },
        )

        async with connect("u", "p") as account:
            devices = await account.devices()
            assert len(devices) == 2

    @pytest.mark.asyncio
    async def test_account_device_finds_outlet_by_prefix(self, mock_api) -> None:
        _stub_login(mock_api)
        _stub_device_list(mock_api, _ep10_device())

        async with connect("u", "p") as account:
            device = await account.device("EP10")
            assert isinstance(device, Outlet)
            assert device.device_id == "EP10AABBCCDD"


# ---------------------------------------------------------------------------
# Outlet — single-outlet device (EP10)
# ---------------------------------------------------------------------------


class TestOutlet:
    @pytest.mark.asyncio
    async def test_plugs_returns_single_self_plug(self, mock_api) -> None:
        _stub_login(mock_api)
        _stub_device_list(mock_api, _ep10_device())
        _stub_passthrough(mock_api, _ep10_sysinfo_response(relay_state=1))

        async with connect("u", "p") as account:
            outlet = await account.device("EP10")
            plugs = await outlet.plugs()
            assert len(plugs) == 1
            assert plugs[0].alias == "Snack Machine"
            # child_id is empty/sentinel for a single-outlet device
            assert plugs[0].child_id == ""

    @pytest.mark.asyncio
    async def test_read_returns_strip_reading_with_one_plug_no_emeter(self, mock_api) -> None:
        _stub_login(mock_api)
        _stub_device_list(mock_api, _ep10_device())
        _stub_passthrough(mock_api, _ep10_sysinfo_response(relay_state=1))

        async with connect("u", "p") as account:
            outlet = await account.device("EP10")
            reading = await outlet.read()

            assert isinstance(reading, StripReading)
            assert reading.alias == "Snack Machine"
            assert reading.device_id == "EP10AABBCCDD"
            assert len(reading.plugs) == 1
            p = reading.plugs[0]
            assert p.child_id == ""
            assert p.alias == "Snack Machine"
            assert p.is_on is True
            assert p.watts is None
            assert p.voltage is None
            assert p.amps is None
            assert p.total_kwh is None

    @pytest.mark.asyncio
    async def test_read_off_state(self, mock_api) -> None:
        _stub_login(mock_api)
        _stub_device_list(mock_api, _ep10_device())
        _stub_passthrough(mock_api, _ep10_sysinfo_response(relay_state=0))

        async with connect("u", "p") as account:
            outlet = await account.device("EP10")
            reading = await outlet.read()
            assert reading.plugs[0].is_on is False
            assert reading.plugs[0].watts is None

    @pytest.mark.asyncio
    async def test_turn_on_uses_bare_set_relay_state(self, mock_api) -> None:
        _stub_login(mock_api)
        _stub_device_list(mock_api, _ep10_device())
        _stub_passthrough(mock_api, _ep10_sysinfo_response(relay_state=0))
        _stub_passthrough(mock_api, _set_relay_state_response())

        async with connect("u", "p") as account:
            outlet = await account.device("EP10")
            plugs = await outlet.plugs()
            await plugs[0].turn_on()

            # Inspect the last passthrough call: should be set_relay_state with
            # NO context.child_ids wrapper (EP10 has no children).
            sent_calls = [
                json.loads(c.kwargs["json"]["params"]["requestData"])
                for c in mock_api.requests[("POST", _yarl(f"{SERVER_URL}?token={FAKE_TOKEN}"))]
                if "params" in c.kwargs.get("json", {})
            ]
            relay_calls = [
                c for c in sent_calls if "system" in c and "set_relay_state" in c.get("system", {})
            ]
            assert len(relay_calls) == 1
            call = relay_calls[0]
            assert "context" not in call
            assert call == {"system": {"set_relay_state": {"state": 1}}}


def _yarl(url: str):
    """Build a yarl.URL for indexing into aioresponses.requests."""
    from yarl import URL

    return URL(url)


# ---------------------------------------------------------------------------
# Repr
# ---------------------------------------------------------------------------


class TestRepr:
    def test_strip_repr(self) -> None:
        strip = Strip.__new__(Strip)
        strip.alias = "My Strip"
        strip.device_id = "AABBCCDD11223344"
        assert repr(strip) == "Strip('My Strip', AABBCCDD1122...)"

    def test_plug_repr(self) -> None:
        plug = Plug.__new__(Plug)
        plug.alias = "Outlet 3"
        assert repr(plug) == "Plug('Outlet 3')"
