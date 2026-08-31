"""Tests for the /api/v2 foundation: access gating, identity, serialization.

The access matrix here is **derived from the route table**, not hand-listed. v1's
equivalent (`_PRIVATE_ROUTES` / `_PUBLIC_ROUTES` in test_server.py) is a manually
maintained list, so a route nobody remembered to add is silently untested — which
is how `handle_calibrate` shipped without a capability check. A generated matrix
cannot have that gap.
"""

from __future__ import annotations

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from juice.api.access import Access, access_of
from juice.api.v2 import ROUTES
from juice.collector import PlugReading
from juice.server import RecorderState, create_app
from juice.store import Store

DEV = "DEVICE_A"

# Values to substitute into dynamic path segments, keyed by segment name.
_PLACEHOLDERS = {"asset_id": "M0001", "plug_id": "1", "device_id": DEV, "mac": "AA:BB"}


def _concrete(path: str) -> str:
    for name, value in _PLACEHOLDERS.items():
        path = path.replace("{" + name + "}", value)
    return path


@pytest.fixture
def store():
    with Store(":memory:") as s:
        yield s


def _state() -> RecorderState:
    state = RecorderState()
    state.plugs[1] = (DEV, DEV + "00", "Godzilla - M0001")
    state.plug_has_emeter[1] = True
    state.assignments[1] = ("Godzilla", "M0001", 2021)
    state.plug_readings[1] = PlugReading(
        child_id=DEV + "00",
        alias="Godzilla",
        is_on=True,
        watts=210.0,
        voltage=120.0,
        amps=1.8,
        total_kwh=5.0,
    )
    return state


def _app(state: RecorderState, store: Store) -> web.Application:
    # dev_auth installs the real gating middleware, so these exercise production
    # auth behaviour rather than the no-auth test shortcut.
    return create_app(state, store, dev_auth=True)


class TestAccessDeclarations:
    def test_every_route_declares_an_audience(self) -> None:
        """register_v2 refuses an undeclared route; this says so at test time
        too, with a readable failure."""
        undeclared = [f"{r.method} {r.path}" for r in ROUTES if access_of(r.handler) is None]
        assert undeclared == []

    @pytest.mark.asyncio
    async def test_anonymous_matches_the_declared_level(self, store: Store) -> None:
        """The generated matrix. Each route's real anonymous behaviour must match
        what it declared — no hand-maintained list to fall out of date."""
        async with TestClient(TestServer(_app(_state(), store))) as client:
            for route in ROUTES:
                level = access_of(route.handler)
                path = _concrete(route.path)
                resp = await client.request(route.method, path)

                if level is Access.ANON_READ and route.method == "GET":
                    assert resp.status != 401, f"{path} declared anon-readable but 401d"
                else:
                    assert resp.status == 401, f"{path} declared {level} but allowed anon"

    @pytest.mark.asyncio
    async def test_an_unknown_v2_path_does_not_leak_existence(self, store: Store) -> None:
        """No v2 path matches v1's regexes, so an unrouted one fails closed."""
        async with TestClient(TestServer(_app(_state(), store))) as client:
            resp = await client.get("/api/v2/does-not-exist")
            assert resp.status == 401


class TestMachinesCollection:
    @pytest.mark.asyncio
    async def test_returns_machines_with_a_derived_status(self, store: Store) -> None:
        async with TestClient(TestServer(_app(_state(), store))) as client:
            body = await (await client.get("/api/v2/machines")).json()

        assert len(body["machines"]) == 1
        machine = body["machines"][0]
        assert machine["asset_id"] == "M0001"
        assert machine["status"] in {"powered", "attract", "playing", "abandoned"}
        assert machine["relay"] == "on"

    @pytest.mark.asyncio
    async def test_a_moved_machine_appears_once_on_its_live_outlet(self, store: Store) -> None:
        """The stale assignment on the old offline outlet must not double it."""
        from datetime import UTC, datetime

        state = _state()
        state.plugs[9] = ("DEVICE_B", "DEVICE_B00", "Godzilla - M0001")
        state.plug_has_emeter[9] = True
        state.assignments[9] = ("Godzilla", "M0001", 2021)
        state.offline_since[DEV] = datetime.now(UTC)

        async with TestClient(TestServer(_app(state, store))) as client:
            await client.get("/login")  # plug_id is operator-only
            body = await (await client.get("/api/v2/machines")).json()

            assert len(body["machines"]) == 1
            assert body["machines"][0]["plug_id"] == 9


class TestSingleMachine:
    @pytest.mark.asyncio
    async def test_addressed_by_asset_id(self, store: Store) -> None:
        async with TestClient(TestServer(_app(_state(), store))) as client:
            resp = await client.get("/api/v2/machines/M0001")
            assert resp.status == 200
            assert (await resp.json())["asset_id"] == "M0001"

    @pytest.mark.asyncio
    async def test_unknown_asset_is_a_coded_404(self, store: Store) -> None:
        async with TestClient(TestServer(_app(_state(), store))) as client:
            resp = await client.get("/api/v2/machines/M9999")
            assert resp.status == 404
            assert (await resp.json())["error"]["code"] == "unknown_machine"

    @pytest.mark.asyncio
    async def test_two_online_claimants_is_a_coded_409_naming_both(self, store: Store) -> None:
        """A Kasa label typo. Guessing would act on the wrong machine."""
        state = _state()
        state.plugs[9] = ("DEVICE_B", "DEVICE_B00", "Godzilla - M0001")
        state.assignments[9] = ("Godzilla", "M0001", 2021)

        async with TestClient(TestServer(_app(state, store))) as client:
            resp = await client.get("/api/v2/machines/M0001")
            assert resp.status == 409
            body = await resp.json()

        assert body["error"]["code"] == "ambiguous_assignment"
        assert sorted(body["error"]["detail"]["candidates"]) == [1, 9]


class TestRedaction:
    @pytest.mark.asyncio
    async def test_anonymous_viewers_do_not_see_operational_detail(self, store: Store) -> None:
        """One redaction boundary, asserted by walking the operator payload —
        so a newly added operator-only field can't leak by omission."""
        from juice.api.v2.views import OPERATOR_ONLY_KEYS

        async with TestClient(TestServer(_app(_state(), store))) as client:
            anon = (await (await client.get("/api/v2/machines")).json())["machines"][0]
            await client.get("/login")  # dev shim: one-click operator session
            operator = (await (await client.get("/api/v2/machines")).json())["machines"][0]

        assert OPERATOR_ONLY_KEYS & set(operator), "test would pass vacuously"
        for key in OPERATOR_ONLY_KEYS:
            assert key not in anon, f"{key} leaked to an anonymous viewer"
        # The floor is still legible logged-out — that's the point of the view.
        assert anon["name"] == "Godzilla"
        assert anon["status"] == operator["status"]
