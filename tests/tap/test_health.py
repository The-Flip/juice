"""`DeviceHealth` aggregation — no device library needed, so never skipped.

These live apart from `test_phase_timing.py` on purpose: that module is gated
behind `pytest.importorskip("kasa")` because it drives the real adapters, and a
bare `uv run` in this repo silently re-syncs the venv without the `tap` extra.
Health arithmetic has no such dependency and must not vanish with it.
"""

from __future__ import annotations

from tap.health import DeviceHealth


class TestHealthAggregatesThePhases:
    def test_percentiles_are_reported_per_phase(self):
        h = DeviceHealth(device_id="D", host="10.0.0.1")
        for listing, total, mx in ((10.0, 60.0, 12.0), (20.0, 120.0, 25.0), (30.0, 180.0, 35.0)):
            h.record_sweep(
                total + listing, listing_ms=listing, emeter_total_ms=total, emeter_max_ms=mx
            )

        snap = h.snapshot()
        assert snap["listing_p50_ms"] == 20.0
        assert snap["emeter_total_p50_ms"] == 120.0
        assert snap["emeter_max_p50_ms"] == 25.0
        assert snap["listing_p95_ms"] == 30.0

    def test_a_sweep_with_no_breakdown_leaves_the_phases_null(self):
        """An adapter that does not report timings must not read as zero."""
        h = DeviceHealth(device_id="D", host="10.0.0.1")
        h.record_sweep(120.0)
        snap = h.snapshot()
        assert snap["sweep_p50_ms"] == 120.0
        assert snap["listing_p50_ms"] is None
        assert snap["emeter_total_p50_ms"] is None
        assert snap["emeter_max_p50_ms"] is None

    def test_the_breakdown_answers_uniform_versus_stalled(self):
        """The question the production log could not settle, made a number."""
        uniform = DeviceHealth(device_id="U", host="10.0.0.1")
        uniform.record_sweep(660.0, listing_ms=60.0, emeter_total_ms=600.0, emeter_max_ms=105.0)
        stalled = DeviceHealth(device_id="S", host="10.0.0.2")
        stalled.record_sweep(660.0, listing_ms=60.0, emeter_total_ms=600.0, emeter_max_ms=520.0)

        u, s = uniform.snapshot(), stalled.snapshot()
        assert u["sweep_p50_ms"] == s["sweep_p50_ms"]  # indistinguishable before
        assert u["emeter_max_p50_ms"] < s["emeter_max_p50_ms"]  # and distinguishable now


class TestTheSlowestOutletShare:
    """The share is recorded per sweep, not divided out of two percentiles."""

    def test_share_comes_from_each_sweep_not_from_the_percentiles(self):
        h = DeviceHealth(device_id="D", host="10.0.0.1")
        # Some sweeps uniformly slow, a disjoint set with one stalled outlet.
        for _ in range(18):
            h.record_sweep(660.0, listing_ms=60.0, emeter_total_ms=600.0, emeter_max_ms=100.0)
        for _ in range(2):
            h.record_sweep(180.0, listing_ms=60.0, emeter_total_ms=120.0, emeter_max_ms=100.0)

        snap = h.snapshot()
        # p95(max)/p95(total) would be 100/600 = 0.17 and hide the stall entirely.
        assert snap["emeter_max_p95_ms"] / snap["emeter_total_p95_ms"] < 0.2
        # The real per-sweep share reaches 0.83.
        assert snap["emeter_share_p95"] > 0.8

    def test_share_is_null_without_timings(self):
        h = DeviceHealth(device_id="D", host="10.0.0.1")
        h.record_sweep(120.0)
        assert h.snapshot()["emeter_share_p50"] is None

    def test_a_zero_total_does_not_divide(self):
        """An unmetered device reports zero outlet time; that is not a share."""
        h = DeviceHealth(device_id="D", host="10.0.0.1")
        h.record_sweep(5.0, listing_ms=5.0, emeter_total_ms=0.0, emeter_max_ms=0.0)
        assert h.snapshot()["emeter_share_p50"] is None
