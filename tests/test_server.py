"""Tests for juice.server — API endpoints relevant to outlet support."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from juice.collector import PlugReading
from juice.server import RecorderState, create_app, handle_machines, handle_outlets
from juice.store import Store


@pytest.fixture
def store():
    with Store(":memory:") as s:
        yield s


def _make_request(app, app_state, app_store):
    """Minimal request-like object whose .app exposes the registered keys."""

    class _App:
        def __init__(self):
            self._d = {"recorder_state": app_state, "store": app_store}

        def __getitem__(self, key):
            return self._d[key]

    class _Req:
        pass

    req = _Req()
    req.app = _App()
    return req


class TestHandleMachinesHasEmeter:
    @pytest.mark.asyncio
    async def test_emeter_machine_emits_power_and_state(self, store: Store) -> None:
        state = RecorderState()
        plug_id = store.ensure_plug("hs300", "c01", "Blackout - M0013", has_emeter=True)
        state.assignments[plug_id] = ("Blackout", "M0013", 1980)
        state.plugs[plug_id] = ("hs300", "c01", "Blackout - M0013")
        state.plug_has_emeter[plug_id] = True
        state.strip_aliases["hs300"] = "Strip 1"
        state.plug_readings[plug_id] = PlugReading(
            child_id="c01",
            alias="Blackout - M0013",
            is_on=True,
            watts=325.4,
            voltage=120.1,
            amps=2.712,
            total_kwh=12.5,
        )

        req = _make_request(None, state, store)
        resp = await handle_machines(req)
        body = await _json(resp)
        machines = body["machines"]
        assert len(machines) == 1
        m = machines[0]
        assert m["has_emeter"] is True
        assert m["is_on"] is True
        assert m["power"]["watts"] == 325.4
        assert m["power"]["voltage"] == 120.1

    @pytest.mark.asyncio
    async def test_no_emeter_machine_emits_is_on_no_power(self, store: Store) -> None:
        state = RecorderState()
        plug_id = store.ensure_plug("ep10-1", "", "Snack M9999", has_emeter=False)
        state.assignments[plug_id] = ("Snack", "M9999", None)
        state.plugs[plug_id] = ("ep10-1", "", "Snack M9999")
        state.plug_has_emeter[plug_id] = False
        state.strip_aliases["ep10-1"] = "Snack M9999"
        state.plug_readings[plug_id] = PlugReading(
            child_id="",
            alias="Snack M9999",
            is_on=True,
            watts=None,
            voltage=None,
            amps=None,
            total_kwh=None,
        )

        req = _make_request(None, state, store)
        resp = await handle_machines(req)
        body = await _json(resp)
        m = body["machines"][0]
        assert m["has_emeter"] is False
        assert m["is_on"] is True
        assert m["power"] is None
        assert m["state"] is None
        assert m["sparkline"] == []


class TestHandleOutlets:
    @pytest.mark.asyncio
    async def test_returns_only_unassigned_no_emeter(self, store: Store) -> None:
        # An assigned HS300 plug — should not appear
        store.ensure_plug("hs300", "c01", "Blackout - M0013", has_emeter=True)
        # An unassigned EP10 — should appear
        ep10_unassigned = store.ensure_plug("ep10-a", "", "Garage Plug", has_emeter=False)
        # An assigned EP10 — should NOT appear
        ep10_assigned = store.ensure_plug("ep10-b", "", "Tagged M9999", has_emeter=False)
        mid = store.ensure_machine("M9999", "Tagged")
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        store.update_assignment(ep10_assigned, mid, ts)

        state = RecorderState()

        req = _make_request(None, state, store)
        resp = await handle_outlets(req)
        body = await _json(resp)
        outlets = body["outlets"]
        assert len(outlets) == 1
        assert outlets[0]["plug_id"] == ep10_unassigned
        assert outlets[0]["alias"] == "Garage Plug"
        assert outlets[0]["is_on"] is None

    @pytest.mark.asyncio
    async def test_prefers_live_reading_for_is_on(self, store: Store) -> None:
        pid = store.ensure_plug("ep10-c", "", "Live", has_emeter=False)
        state = RecorderState()
        state.plug_readings[pid] = PlugReading(
            child_id="",
            alias="Live",
            is_on=True,
            watts=None,
            voltage=None,
            amps=None,
            total_kwh=None,
        )
        req = _make_request(None, state, store)
        resp = await handle_outlets(req)
        body = await _json(resp)
        assert body["outlets"][0]["is_on"] is True


class TestRouter:
    def test_outlets_and_plug_power_routes_registered(self, store: Store) -> None:
        state = RecorderState()
        app = create_app(state, store)
        routes = {(r.method, r.resource.canonical) for r in app.router.routes()}
        assert ("GET", "/api/outlets") in routes
        assert ("POST", "/api/plugs/{plug_id}/power") in routes


async def _json(resp):
    """Extract JSON body from an aiohttp web.Response."""
    import json

    return json.loads(resp.body.decode())
