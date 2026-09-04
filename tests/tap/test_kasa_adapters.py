"""The one place in the suite that knows what a TP-Link protocol frame looks like.

The SMART fixtures are real payloads captured from a P316M at 192.168.4.38
(`tests/tap/fixtures/p316m.json`, with identifiers and location scrubbed). These
tests assert two things and nothing else: the request dict we build, and how we
unwrap the response. They are what fails loudly if python-kasa or a firmware
update changes shape underneath us.

The IOT payloads are synthetic — the HS300s live on the museum LAN and were not
reachable while this was written — but the shapes are the ones juice has been
sending through the cloud passthrough in production for months.
"""

from __future__ import annotations

import asyncio
import json
import pathlib
from datetime import UTC, datetime, timedelta

import pytest

# python-kasa is an optional extra (`uv sync --extra tap`). Everything else
# in tests/tap/ runs without it; only this module needs the real library.
pytest.importorskip("kasa", reason="install with: uv sync --extra tap")

from tap.device import Family
from tap.errors import DeviceAuthError, TransientError
from tap.kasa_common import decode_alias, translate
from tap.kasa_iot import IotPowerDevice
from tap.kasa_smart import SmartPowerDevice
from tests.tap.fakes import FakeProtocol

FIXTURES = pathlib.Path(__file__).parent / "fixtures"
P316M = json.loads((FIXTURES / "p316m.json").read_text())


def _smart_responses() -> dict:
    return {
        "get_child_device_list": P316M["get_child_device_list"],
        "get_device_info": P316M["get_device_info"],
        "control_child:get_emeter_data": P316M["get_emeter_data"],
        "control_child:set_device_info": {
            "control_child": {"responseData": {"result": {}, "error_code": 0}}
        },
    }


def _smart_device() -> tuple[SmartPowerDevice, FakeProtocol]:
    device = SmartPowerDevice("192.168.4.38", device_id="DEVICE1")
    proto = FakeProtocol(_smart_responses())
    # The protocol is the seam; open() would otherwise need a real network.
    device._proto = proto
    return device, proto


class TestSmartSweep:
    async def test_reads_every_outlet_from_the_real_payload(self):
        device, _ = _smart_device()
        sweep = await device.sweep()
        assert len(sweep.outlets) == 6
        assert sweep.device_id == "DEVICE1"
        first = sweep.outlets[0]
        assert first.alias == "Tapo P316M_1"  # base64 'VGFwbyBQMzE2TV8x'
        assert first.relay_on is True
        assert first.power_mw is not None
        assert first.voltage_mv > 100_000  # ~120 V in millivolts

    async def test_the_timestamp_is_taken_before_the_outlets_are_read(self):
        """Six outlets read 14ms apart are still one observation of one strip.

        The sweep timestamp must be stamped once, up front — not after the last
        outlet — or a slow strip's readings are attributed to the moment it
        finished rather than the moment it was sampled.
        """
        device, proto = _smart_device()

        # Make the reads slow enough that "before" and "after" are far apart.
        real_query = proto.query

        async def slow(payload, retry_count=3):
            await asyncio.sleep(0.02)
            return await real_query(payload, retry_count)

        proto.query = slow
        before = datetime.now(UTC)
        sweep = await device.sweep()
        after = datetime.now(UTC)

        assert (after - before).total_seconds() > 0.1  # 7 calls x 20ms
        # The stamp belongs at the start, not the end.
        assert (sweep.ts - before).total_seconds() < 0.05
        assert sweep.ts < after - timedelta(seconds=0.05)
        assert sweep.duration_ms > 100

    async def test_it_never_calls_the_expensive_update_path(self):
        """`get_energy_usage` is 67ms and cumulative; `get_emeter_data` is 14ms."""
        device, proto = _smart_device()
        await device.sweep()
        methods = [
            r["control_child"]["requestData"]["method"]
            for r in proto.requests
            if "control_child" in r
        ]
        assert set(methods) == {"get_emeter_data"}
        assert "get_energy_usage" not in methods

    async def test_child_requests_are_never_batched(self):
        """The firmware rejects control_child inside multipleRequest (-1001)."""
        device, proto = _smart_device()
        await device.sweep()
        assert not any("multipleRequest" in r for r in proto.requests)
        # One list call plus one per child.
        assert len(proto.requests) == 7

    async def test_a_failed_outlet_degrades_to_null_without_losing_the_sweep(self):
        device, proto = _smart_device()
        del proto.responses["control_child:get_emeter_data"]
        sweep = await device.sweep()
        assert len(sweep.outlets) == 6
        assert all(o.power_mw is None for o in sweep.outlets)
        # Relay state still came from the child list, so it survives.
        assert all(o.relay_on for o in sweep.outlets)

    async def test_an_auth_failure_is_not_swallowed_as_a_bad_outlet(self):
        """A rejected credential is about the device, and must reach the poller."""
        device, proto = _smart_device()
        proto.fail_with = DeviceAuthError("nope")
        with pytest.raises(DeviceAuthError):
            await device.sweep()


class TestSmartRelay:
    async def test_set_relay_uses_control_child_set_device_info(self):
        device, proto = _smart_device()
        await device.set_relay("CHILD01", False)
        (request,) = proto.requests
        assert request["control_child"]["device_id"] == "CHILD01"
        assert request["control_child"]["requestData"] == {
            "method": "set_device_info",
            "params": {"device_on": False},
        }


# --- IOT ---------------------------------------------------------------------

HS300_SYSINFO = {
    "system": {
        "get_sysinfo": {
            "sw_ver": "1.0.6",
            "model": "HS300(US)",
            "deviceId": "8006B6174F1315B851DB345FE0CB5472251BCFDC",
            "alias": "TP-LINK_Power Strip_98E1",
            "feature": "TIM:ENE",
            "children": [
                {
                    "id": "8006B6174F1315B851DB345FE0CB5472251BCFDC00",
                    "alias": "Star Trip - M0009",
                    "state": 1,
                },
                {"id": "8006B6174F1315B851DB345FE0CB5472251BCFDC01", "alias": "Spare", "state": 0},
            ],
        }
    }
}
HS300_REALTIME = {
    "emeter": {
        "get_realtime": {
            "current_ma": 812,
            "voltage_mv": 119_674,
            "power_mw": 96_413,
            "total_wh": 41_233,
            "err_code": 0,
        }
    }
}
EP10_SYSINFO = {
    "system": {
        "get_sysinfo": {
            "model": "EP10(US)",
            "deviceId": "8006376B10F8AAAA",
            "alias": "Duck Locker - M0037",
            "feature": "TIM",
            "relay_state": 1,
        }
    }
}


def _iot_device(sysinfo: dict) -> tuple[IotPowerDevice, FakeProtocol]:
    device = IotPowerDevice("192.168.4.51")
    proto = FakeProtocol({"system": sysinfo, "emeter": HS300_REALTIME, "context": HS300_REALTIME})
    device._proto = proto
    return device, proto


class TestIotSweep:
    async def test_strip_children_become_outlets(self):
        device, _ = _iot_device(HS300_SYSINFO)
        device.has_emeter = True
        sweep = await device.sweep()
        assert [o.child_id[-2:] for o in sweep.outlets] == ["00", "01"]
        assert sweep.outlets[0].alias == "Star Trip - M0009"
        assert sweep.outlets[0].relay_on is True
        assert sweep.outlets[1].relay_on is False
        assert sweep.outlets[0].power_mw == 96_413
        assert sweep.outlets[0].energy_wh == 41_233

    async def test_child_id_is_the_cloud_id_not_the_mac(self):
        """python-kasa's device_id property returns a MAC; juice keys on this."""
        device, _ = _iot_device(HS300_SYSINFO)
        await device.refresh_identity()
        assert device.device_id == "8006B6174F1315B851DB345FE0CB5472251BCFDC"
        sweep = await device.sweep()
        assert sweep.outlets[0].child_id.startswith(device.device_id)

    async def test_single_outlet_device_uses_an_empty_child_id(self):
        device, _ = _iot_device(EP10_SYSINFO)
        await device.refresh_identity()
        assert device.has_emeter is False  # feature is "TIM", no "ENE"
        sweep = await device.sweep()
        assert len(sweep.outlets) == 1
        assert sweep.outlets[0].child_id == ""
        assert sweep.outlets[0].alias == "Duck Locker - M0037"
        # No meter means NULL power, which is not the same as zero.
        assert sweep.outlets[0].power_mw is None

    async def test_emeter_request_is_scoped_to_the_child(self):
        device, proto = _iot_device(HS300_SYSINFO)
        device.has_emeter = True
        await device.sweep()
        emeter_requests = [r for r in proto.requests if "emeter" in r]
        assert emeter_requests
        assert emeter_requests[0]["context"]["child_ids"] == [
            "8006B6174F1315B851DB345FE0CB5472251BCFDC00"
        ]


class TestIotRelay:
    async def test_set_relay_scopes_to_the_child(self):
        device, proto = _iot_device(HS300_SYSINFO)
        await device.set_relay("CHILD00", True)
        (request,) = proto.requests
        assert request["context"]["child_ids"] == ["CHILD00"]
        assert request["system"]["set_relay_state"] == {"state": 1}

    async def test_single_outlet_relay_has_no_context(self):
        device, proto = _iot_device(EP10_SYSINFO)
        await device.set_relay("", False)
        (request,) = proto.requests
        assert "context" not in request
        assert request["system"]["set_relay_state"] == {"state": 0}


class TestUnitFallback:
    async def test_older_firmware_base_units_are_scaled_up(self):
        """Older IOT firmware reports watts/volts/amps rather than milli-units."""
        device = IotPowerDevice("10.0.0.1")
        device.has_emeter = True
        device._proto = FakeProtocol(
            {
                "system": HS300_SYSINFO,
                "context": {
                    "emeter": {
                        "get_realtime": {
                            "current": 0.812,
                            "voltage": 119.674,
                            "power": 96.413,
                            "total": 41.233,
                        }
                    }
                },
            }
        )
        sweep = await device.sweep()
        assert sweep.outlets[0].power_mw == 96_413
        assert sweep.outlets[0].voltage_mv == 119_674
        assert sweep.outlets[0].current_ma == 812


class TestAliasDecoding:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("VGFwbyBQMzE2TV8x", "Tapo P316M_1"),
            ("", ""),
            (None, ""),
            # A plain alias must survive untouched even if it looks like base64.
            ("Star Trip - M0009", "Star Trip - M0009"),
            ("not base64!!", "not base64!!"),
        ],
    )
    def test_decode_alias(self, raw, expected):
        assert decode_alias(raw) == expected


class TestErrorTranslation:
    def test_auth_errors_get_their_own_type(self):
        from kasa.exceptions import AuthenticationError

        assert isinstance(translate(AuthenticationError("bad")), DeviceAuthError)

    def test_transport_errors_are_transient(self):
        assert isinstance(translate(TimeoutError()), TransientError)
        assert isinstance(translate(OSError("refused")), TransientError)

    def test_family_enum_round_trips(self):
        assert Family("smart") is Family.SMART
        assert Family("iot") is Family.IOT


class TestAliasDecodingIsFamilyScoped:
    """`decode_alias` is for SMART `nickname` only, and these show why."""

    @pytest.mark.parametrize("tag", ["M000", "M009", "M014"])
    def test_plain_asset_tags_are_valid_base64_and_would_be_mangled(self, tag):
        """The reason the caller must key on family, not on the string.

        juice extracts `M\\d+` from the alias to assign machines, and these tags
        happen to be decodable base64, so passing an IOT alias through here
        would silently rename the machine. Only *some* tags decode -- `M0009999`
        does not -- which makes it worse, not better: the corruption would be
        intermittent and look like a device problem.
        """
        assert decode_alias(tag) != tag

    def test_the_iot_adapter_never_decodes(self):
        """An IOT alias must reach the buffer byte for byte."""
        device, _ = _iot_device(
            {
                "system": {
                    "get_sysinfo": {
                        "deviceId": "D" * 40,
                        "model": "HS300(US)",
                        "feature": "TIM",
                        "children": [{"id": "D" * 40 + "00", "alias": "M000", "state": 1}],
                    }
                }
            }
        )
        device.has_emeter = False
        import asyncio

        sweep = asyncio.run(device.sweep())
        assert sweep.outlets[0].alias == "M000"

    def test_the_round_trip_guard_rejects_non_canonical_base64(self):
        """Deleting the guard must break something, or it is decoration."""
        # 'YQ==' is canonical for b'a'; 'YR==' decodes to the same bytes but is
        # not what b64encode would emit, so it must come back untouched.
        assert decode_alias("YQ==") == "a"
        assert decode_alias("YR==") == "YR=="


class TestCancellationPropagates:
    """The sweep budget only exists if cancellation actually escapes a sweep.

    Both adapters used to catch `BaseException` per outlet, which caught the
    `CancelledError` that `asyncio.timeout` delivers. The timeout then saw
    nothing propagating and did not fire, so a sweep ran to completion however
    long it took -- and `stop()` never returned. The fakes could not catch it:
    `FakeDevice.hang` awaits an Event, which propagates cancellation cleanly.
    """

    @staticmethod
    async def _hanging(device, proto):
        async def never(payload, retry_count=3):
            await asyncio.Event().wait()

        proto.query = never
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.05):
                await device.sweep()

    async def test_smart_sweep_is_cancellable(self):
        device, proto = _smart_device()
        await self._hanging(device, proto)

    async def test_iot_sweep_is_cancellable(self):
        device, proto = _iot_device(HS300_SYSINFO)
        device.has_emeter = True
        await self._hanging(device, proto)

    async def test_a_cancelled_outlet_does_not_come_back_as_a_null_reading(self):
        """The failure mode: cancellation quietly degraded one outlet instead."""
        device, proto = _smart_device()
        calls = {"n": 0}
        real = proto.query

        async def slow_after_first(payload, retry_count=3):
            calls["n"] += 1
            if calls["n"] > 2:
                await asyncio.Event().wait()
            return await real(payload, retry_count)

        proto.query = slow_after_first
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.05):
                await device.sweep()
        # It stopped where it was cancelled rather than nulling the rest.
        assert calls["n"] == 3
