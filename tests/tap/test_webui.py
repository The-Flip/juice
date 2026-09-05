"""The status page: served from memory, and honest about being unhealthy."""

from __future__ import annotations

import pytest
from aiohttp.test_utils import TestClient, TestServer

from tap.device import DeviceState
from tap.errors import TransientError
from tap.health import Health, OutletHealth
from tap.webui import create_app


@pytest.fixture
async def client():
    health = Health(tap_id="test-tap", version="0.1.0")
    async with TestClient(TestServer(create_app(health))) as c:
        c.health = health
        yield c


class TestStatus:
    async def test_renders_with_no_devices_at_all(self, client):
        response = await client.get("/api/status")
        assert response.status == 200
        body = await response.json()
        assert body["tap_id"] == "test-tap"
        assert body["devices"] == []
        assert body["uplink"]["enabled"] is False

    async def test_reports_devices_and_live_outlets(self, client):
        entry = client.health.device("DEV1", host="10.0.0.1")
        entry.model = "P316M"
        entry.state = DeviceState.ONLINE
        entry.record_sweep(120.0)
        entry.outlets["DEV100"] = OutletHealth(
            child_id="DEV100", alias="Star Trip", relay_on=True, power_mw=96_413
        )
        body = await (await client.get("/api/status")).json()
        (device,) = body["devices"]
        assert device["model"] == "P316M"
        assert device["state"] == "online"
        assert device["sweep_p50_ms"] == 120.0
        assert device["outlets"][0]["watts"] == 96.41

    async def test_the_page_itself_is_html(self, client):
        response = await client.get("/")
        assert response.status == 200
        assert response.content_type == "text/html"
        assert "tap" in await response.text()


class TestHealthEndpoint:
    async def test_ok_when_there_are_no_warnings(self, client):
        response = await client.get("/api/health")
        assert response.status == 200
        assert (await response.json())["ok"] is True

    async def test_503_while_the_watchdog_is_complaining(self, client):
        """A container healthcheck must be able to see this without parsing."""
        client.health.warnings = ["credentials rejected by 10.0.0.1"]
        response = await client.get("/api/health")
        assert response.status == 503
        body = await response.json()
        assert body["ok"] is False
        assert body["reasons"] == ["credentials rejected by 10.0.0.1"]


class TestNoDatabaseOnTheRequestPath:
    async def test_status_works_with_no_buffer_at_all(self, client):
        """The page has to render when the disk is wedged — that's when it's read."""
        body = await (await client.get("/api/status")).json()
        assert body["buffer"]["rows_written"] == 0
        assert body["buffer"]["days"] == []


class TestFailuresAreVisibleOnTheStatusPage:
    """`sweeps_failed: 132` and `last_error: "TimeoutError: "` was the whole
    report after eight hours against real hardware. The snapshot now carries
    enough to act on."""

    async def test_snapshot_carries_the_failure_breakdown(self, client):
        health = client.health
        entry = health.device("D1", host="10.0.0.5")
        entry.record_failure(TimeoutError(), phase="sweep:emeter[3/6]", duration_ms=800.0)
        entry.record_failure(TimeoutError(), phase="sweep:emeter[1/6]", duration_ms=805.0)
        entry.record_failure(TransientError("child list came back empty"), phase="sweep")

        body = await (await client.get("/api/status")).json()
        (dev,) = body["devices"]
        assert dev["failures_by_kind"] == {"TimeoutError": 2, "TransientError": 1}
        assert dev["last_error_phase"] == "sweep"
        assert dev["last_error_at"] is not None
        assert dev["sweep_fail_p95_ms"] is not None

    async def test_a_clean_device_reports_no_failure_detail(self, client):
        health = client.health
        health.device("D1", host="10.0.0.5").record_sweep(120.0)

        body = await (await client.get("/api/status")).json()
        (dev,) = body["devices"]
        assert dev["failures_by_kind"] == {}
        assert dev["last_error"] == ""
        assert dev["sweep_fail_p95_ms"] is None
