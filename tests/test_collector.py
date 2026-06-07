"""Tests for juice.collector — the OO cloud API wrapper."""

from __future__ import annotations

import json

import aiohttp
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
    _decode_alias,
    _plug_reading,
    call_with_retry,
    connect,
    is_retryable_passthrough_error,
    outlet_number,
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


def _ep25_device(
    device_id: str = "EP25AABBCCDD",
    alias: str = "U3RhciBUcmlwIC0gTTAwMDk=",  # base64("Star Trip - M0009")
    model: str = "EP25(US)",
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
# outlet_number()
# ---------------------------------------------------------------------------


class TestOutletNumber:
    def test_hs300_child_id_suffix_maps_one_based(self) -> None:
        base = "8006188258AD5449B36256BD70827E8C25536CB8"
        assert outlet_number(base + "00") == 1
        assert outlet_number(base + "03") == 4
        assert outlet_number(base + "05") == 6

    def test_empty_child_id_returns_none(self) -> None:
        # Single-outlet devices (EP10 _SelfPlug) use "" as their child_id.
        assert outlet_number("") is None

    def test_non_numeric_suffix_returns_none(self) -> None:
        assert outlet_number("8006188258AD5449B36256BD70827E8C25536CBXY") is None

    def test_short_child_id_returns_none(self) -> None:
        assert outlet_number("3") is None


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

    @pytest.mark.asyncio
    async def test_ep25_is_unsupported_but_visible_in_raw(self, mock_api) -> None:
        # EP25 (and other SMART/KLAP devices) can't be reached by the legacy
        # passthrough — devices() filters them out, but raw_devices() still
        # shows them so `discover` can flag them for the operator.
        _stub_login(mock_api)
        # devices() and raw_devices() each call the cloud once.
        _stub_device_list(mock_api, _hs300_device(), _ep25_device())
        _stub_device_list(mock_api, _hs300_device(), _ep25_device())

        async with connect("u", "p") as account:
            assert len(await account.devices()) == 1  # only HS300
            raw = await account.raw_devices()
            assert {d["deviceModel"] for d in raw} == {"HS300(US)", "EP25(US)"}

    @pytest.mark.asyncio
    async def test_unsupported_warning_logs_once_per_device(self, mock_api, caplog) -> None:
        _stub_login(mock_api)
        # Two device-list calls, same unsupported device — warning should only
        # appear once so the recorder's 60s refresh can't flood the console.
        _stub_device_list(mock_api, _ep25_device())
        _stub_device_list(mock_api, _ep25_device())

        async with connect("u", "p") as account:
            with caplog.at_level("WARNING", logger="juice.collector"):
                await account.devices()
                await account.devices()
            unsupported = [r for r in caplog.records if "unsupported" in r.message]
            assert len(unsupported) == 1

    @pytest.mark.asyncio
    async def test_raw_devices_includes_unsupported(self, mock_api) -> None:
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
            raw = await account.raw_devices()
            assert {d["deviceModel"] for d in raw} == {"HS300(US)", "LB100"}


class TestDecodeAlias:
    def test_plaintext_passthrough(self) -> None:
        assert _decode_alias("Star Trip - M0009") == "Star Trip - M0009"
        assert _decode_alias("Duck Locker - M0037") == "Duck Locker - M0037"
        assert _decode_alias("unused 1") == "unused 1"

    def test_decodes_base64(self) -> None:
        assert _decode_alias("U3RhciBUcmlwIC0gTTAwMDk=") == "Star Trip - M0009"
        assert _decode_alias("QmxhY2tvdXQgLSBNMDAxMw==") == "Blackout - M0013"


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


# ---------------------------------------------------------------------------
# Retry helpers
# ---------------------------------------------------------------------------


class TestIsRetryablePassthroughError:
    def test_request_timeout(self) -> None:
        assert is_retryable_passthrough_error(RuntimeError("Passthrough failed: Request timeout"))

    def test_device_offline(self) -> None:
        assert is_retryable_passthrough_error(RuntimeError("Passthrough failed: Device is offline"))

    def test_device_offline_during_processing(self) -> None:
        assert is_retryable_passthrough_error(
            RuntimeError("Passthrough failed: Device is offline during processing")
        )

    def test_asyncio_timeout(self) -> None:
        assert is_retryable_passthrough_error(TimeoutError())

    def test_aiohttp_client_error(self) -> None:
        assert is_retryable_passthrough_error(aiohttp.ClientError())

    def test_token_invalid_not_retryable(self) -> None:
        assert not is_retryable_passthrough_error(RuntimeError("Passthrough failed: Token invalid"))

    def test_unrelated_runtime_error_not_retryable(self) -> None:
        assert not is_retryable_passthrough_error(RuntimeError("something else"))

    def test_value_error_not_retryable(self) -> None:
        assert not is_retryable_passthrough_error(ValueError("nope"))


@pytest.fixture
def fast_sleep(monkeypatch):
    """Replace asyncio.sleep with a no-op for retry tests (kept awaitable)."""

    async def _noop(_):
        return None

    monkeypatch.setattr("juice.collector.asyncio.sleep", _noop)


class TestCallWithRetry:
    @pytest.mark.asyncio
    async def test_succeeds_first_try(self, fast_sleep) -> None:
        calls = []

        async def fn():
            calls.append(1)
            return "ok"

        retries: list = []
        result = await call_with_retry(fn, on_retry=lambda a, e, d: retries.append((a, e, d)))
        assert result == "ok"
        assert calls == [1]
        assert retries == []

    @pytest.mark.asyncio
    async def test_two_transient_failures_then_success(self, fast_sleep) -> None:
        attempts = {"n": 0}

        async def fn():
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise RuntimeError("Passthrough failed: Request timeout")
            return "ok"

        retries: list = []
        result = await call_with_retry(fn, on_retry=lambda a, e, d: retries.append((a, d)))
        assert result == "ok"
        assert attempts["n"] == 3
        assert [a for a, _ in retries] == [1, 2]

    @pytest.mark.asyncio
    async def test_non_retryable_raises_immediately(self, fast_sleep) -> None:
        attempts = {"n": 0}

        async def fn():
            attempts["n"] += 1
            raise RuntimeError("Passthrough failed: Token invalid")

        with pytest.raises(RuntimeError, match="Token invalid"):
            await call_with_retry(fn)
        assert attempts["n"] == 1

    @pytest.mark.asyncio
    async def test_should_stop_raises_last_error(self, fast_sleep) -> None:
        attempts = {"n": 0}
        should_stop_after = 2

        async def fn():
            attempts["n"] += 1
            raise RuntimeError("Passthrough failed: Device is offline")

        # should_stop returns True after the first retry's backoff.
        def should_stop():
            return attempts["n"] >= should_stop_after

        with pytest.raises(RuntimeError, match="Device is offline"):
            await call_with_retry(fn, should_stop=should_stop)
        # Stopped before completing many attempts.
        assert attempts["n"] <= 3

    @pytest.mark.asyncio
    async def test_max_attempts_bounds(self, fast_sleep) -> None:
        attempts = {"n": 0}

        async def fn():
            attempts["n"] += 1
            raise RuntimeError("Passthrough failed: Request timeout")

        with pytest.raises(RuntimeError, match="Request timeout"):
            await call_with_retry(fn, max_attempts=3)
        assert attempts["n"] == 3

    @pytest.mark.asyncio
    async def test_backoff_schedule(self, fast_sleep) -> None:
        async def fn():
            raise RuntimeError("Passthrough failed: Request timeout")

        delays: list[float] = []
        with pytest.raises(RuntimeError):
            await call_with_retry(
                fn,
                max_attempts=6,
                on_retry=lambda a, e, d: delays.append(d),
            )
        # 5 retries after 6 attempts: 0.5, 1.0, 2.0, 4.0, 4.0
        assert delays == [0.5, 1.0, 2.0, 4.0, 4.0]
