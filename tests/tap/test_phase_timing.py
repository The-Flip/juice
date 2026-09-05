"""Where a slow sweep spends its time.

Production log, six consecutive timeouts against a real P316M:

    emeter[5/6]  emeter[6/6]  emeter[6/6]  emeter[5/6]  emeter[3/6]  emeter[3/6]

Always the back half, never outlets 1 or 2 — which is the signature of a budget
running out, not of an outlet hanging. If one outlet stalled, the phase would
pin to that index every time. So `last_error_phase` says where the clock ran
out, and near the end is nearly always the answer; it cannot distinguish "every
round trip is slow" from "outlet 5 stalls".

Timing each phase on a *successful* sweep can. With six outlets, an
`emeter_max` near `emeter_total / 6` is a uniformly slow sweep; an
`emeter_max` near `emeter_total` is one outlet stalling. That is the difference
between raising the budget and replacing a plug.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from tap.device import Sweep

pytest.importorskip("kasa", reason="install with: uv sync --extra tap")

from tests.tap.test_kasa_adapters import (
    HS300_SYSINFO,
    _iot_device,
    _smart_device,
)


class TestSweepCarriesItsBreakdown:
    def test_a_sweep_defaults_to_no_timing_rather_than_fake_zeros(self):
        s = Sweep(device_id="D", ts=datetime.now(UTC))
        assert s.listing_ms is None
        assert s.emeter_total_ms is None
        assert s.emeter_max_ms is None

    async def test_smart_reports_the_listing_and_the_outlet_reads_separately(self):
        device, proto = _smart_device()
        inner = proto.query

        async def slow(payload, retry_count=3):
            # The listing is cheap; one outlet is not.
            await asyncio.sleep(0.03 if "get_child_device_list" in payload else 0.005)
            return await inner(payload, retry_count)

        proto.query = slow
        sweep = await device.sweep()

        assert sweep.listing_ms >= 25
        assert sweep.emeter_total_ms >= 25  # six outlets at ~5ms
        assert sweep.emeter_max_ms is not None
        assert sweep.emeter_max_ms <= sweep.emeter_total_ms
        # The parts must not exceed the whole.
        assert sweep.listing_ms + sweep.emeter_total_ms <= sweep.duration_ms + 1

    async def test_one_stalling_outlet_shows_up_as_max_near_total(self):
        """The case `last_error_phase` could not distinguish."""
        device, proto = _smart_device()
        inner = proto.query
        calls = {"n": 0}

        async def one_slow(payload, retry_count=3):
            if "control_child" in payload:
                calls["n"] += 1
                if calls["n"] == 4:
                    await asyncio.sleep(0.05)
            return await inner(payload, retry_count)

        proto.query = one_slow
        sweep = await device.sweep()

        assert sweep.emeter_max_ms >= 45
        # Nearly all of the outlet time went to one outlet.
        assert sweep.emeter_max_ms > sweep.emeter_total_ms * 0.7

    async def test_a_uniformly_slow_sweep_shows_max_near_the_average(self):
        device, proto = _smart_device()
        inner = proto.query

        async def all_slow(payload, retry_count=3):
            if "control_child" in payload:
                await asyncio.sleep(0.01)
            return await inner(payload, retry_count)

        proto.query = all_slow
        sweep = await device.sweep()

        assert sweep.emeter_max_ms < sweep.emeter_total_ms * 0.4, (
            "six comparable reads should not look like one stall"
        )

    async def test_iot_reports_the_same_breakdown(self):
        device, proto = _iot_device(HS300_SYSINFO)
        sweep = await device.sweep()
        assert sweep.listing_ms is not None
        assert sweep.emeter_total_ms is not None
        assert sweep.emeter_max_ms is not None
