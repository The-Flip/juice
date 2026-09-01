"""Tests for /api/v2 power writes.

The contract: **202 means the device call has started, not that the request was
accepted.** Anything refusable without a WAN round-trip stays synchronous. If a
lock or an in-flight command became a `failed` phase on the stream, the operator
would tap, get an optimistic pending state, and learn 300ms later on a different
channel that nothing was ever going to happen — strictly worse than v1's 409.
"""

from __future__ import annotations

import pytest
from aiohttp.test_utils import TestClient, TestServer

from juice.collector import PlugReading
from juice.server import RecorderState, create_app
from juice.store import Store

DEV = "DEVICE_A"


@pytest.fixture
def store():
    with Store(":memory:") as s:
        yield s


class _FakePlug:
    def __init__(self, alias: str = "Godzilla") -> None:
        self.alias = alias
        self.calls = 0

    async def turn_on(self) -> None:
        self.calls += 1

    async def turn_off(self) -> None:
        self.calls += 1


def _state(*, relay_on: bool = True, controllable: bool = True) -> RecorderState:
    state = RecorderState()
    state.plugs[1] = (DEV, DEV + "00", "Godzilla - M0001")
    state.plug_has_emeter[1] = True
    state.assignments[1] = ("Godzilla", "M0001", 2021)
    state.plug_readings[1] = PlugReading(
        child_id=DEV + "00",
        alias="Godzilla",
        is_on=relay_on,
        watts=210.0 if relay_on else 0.0,
        voltage=120.0,
        amps=1.8,
        total_kwh=5.0,
    )
    if controllable:
        state.plug_objects[1] = _FakePlug()
    return state


async def _client(state: RecorderState, store: Store) -> TestClient:
    return TestClient(TestServer(create_app(state, store, dev_auth=True)))


class TestAccepted:
    @pytest.mark.asyncio
    async def test_power_off_returns_202_with_a_command_to_follow(self, store: Store) -> None:
        state = _state(relay_on=True)
        async with await _client(state, store) as client:
            await client.get("/login")
            resp = await client.post("/api/v2/machines/M0001/power", json={"on": False})
            assert resp.status == 202
            body = await resp.json()

        assert body["command_id"]
        assert body["kind"] == "turn_off"
        assert body["expect"] == {"relay": "off"}
        assert body["stream"] == "/api/v2/stream"
        assert body["timeout_ms"] > 0
        assert "confirmed" in body["terminal_phases"]

    @pytest.mark.asyncio
    async def test_reboot_returns_202(self, store: Store) -> None:
        state = _state(relay_on=True)
        async with await _client(state, store) as client:
            await client.get("/login")
            resp = await client.post("/api/v2/machines/M0001/reboot")
            assert resp.status == 202
            body = await resp.json()

        assert body["kind"] == "reboot"
        assert body["expect"] == {"relay": "on"}

    @pytest.mark.asyncio
    async def test_the_timeout_is_the_servers_not_the_clients(self, store: Store) -> None:
        """A client that gives up before the server stops retrying is how a
        machine comes up after the UI declared failure."""
        from juice.commands import POWER_TIMEOUT_MS, REBOOT_TIMEOUT_MS

        state = _state(relay_on=True)
        async with await _client(state, store) as client:
            await client.get("/login")
            power = await (
                await client.post("/api/v2/machines/M0001/power", json={"on": False})
            ).json()

        assert power["timeout_ms"] == POWER_TIMEOUT_MS
        assert REBOOT_TIMEOUT_MS > POWER_TIMEOUT_MS


class TestSynchronousRefusals:
    """The point of the phase: nothing refusable offline becomes a stream event."""

    @pytest.mark.asyncio
    async def test_unknown_machine_is_a_synchronous_404(self, store: Store) -> None:
        async with await _client(_state(), store) as client:
            await client.get("/login")
            resp = await client.post("/api/v2/machines/M9999/power", json={"on": True})
            assert resp.status == 404
            assert (await resp.json())["error"]["code"] == "unknown_machine"

    @pytest.mark.asyncio
    async def test_a_locked_machine_is_refused_synchronously(self, store: Store) -> None:
        state = _state(relay_on=True)
        state.lock_modes["M0001"] = "on"  # locked ON: turning off is forbidden

        async with await _client(state, store) as client:
            await client.get("/login")
            resp = await client.post("/api/v2/machines/M0001/power", json={"on": False})
            assert resp.status == 409
            body = await resp.json()

        assert body["error"]["code"] == "machine_locked"
        assert body["error"]["detail"]["lock_mode"] == "on"
        assert state.plug_objects[1].calls == 0, "a locked machine must not reach the device"

    @pytest.mark.asyncio
    async def test_a_reboot_is_refused_by_a_lock_in_either_direction(self, store: Store) -> None:
        state = _state(relay_on=True)
        state.lock_modes["M0001"] = "off"

        async with await _client(state, store) as client:
            await client.get("/login")
            resp = await client.post("/api/v2/machines/M0001/reboot")
            assert resp.status == 409
            assert (await resp.json())["error"]["code"] == "machine_locked"

    @pytest.mark.asyncio
    async def test_an_uncontrollable_outlet_is_refused_synchronously(self, store: Store) -> None:
        state = _state(controllable=False)
        async with await _client(state, store) as client:
            await client.get("/login")
            resp = await client.post("/api/v2/machines/M0001/power", json={"on": True})
            assert resp.status == 409
            assert (await resp.json())["error"]["code"] == "not_controllable"

    @pytest.mark.asyncio
    async def test_a_conflicting_command_names_who_holds_it(self, store: Store) -> None:
        """user_needs J6: two people converge on a smoking machine. The second
        must be told who is already acting, not silently race them."""
        state = _state(relay_on=True)
        state.commands.open(
            kind="reboot", plug_id=1, actor="dana", source="reboot", asset_id="M0001"
        )

        async with await _client(state, store) as client:
            await client.get("/login")
            resp = await client.post("/api/v2/machines/M0001/power", json={"on": False})
            assert resp.status == 409
            body = await resp.json()

        assert body["error"]["code"] == "command_in_flight"
        assert body["error"]["detail"]["command"]["kind"] == "reboot"
        assert body["error"]["detail"]["command"]["actor"] == "dana"

    @pytest.mark.asyncio
    async def test_ambiguous_asset_is_refused_rather_than_guessed(self, store: Store) -> None:
        state = _state(relay_on=True)
        state.plugs[9] = ("DEVICE_B", "DEVICE_B00", "Godzilla - M0001")
        state.assignments[9] = ("Godzilla", "M0001", 2021)

        async with await _client(state, store) as client:
            await client.get("/login")
            resp = await client.post("/api/v2/machines/M0001/power", json={"on": True})
            assert resp.status == 409
            assert (await resp.json())["error"]["code"] == "ambiguous_assignment"


class TestAccessControl:
    @pytest.mark.asyncio
    async def test_anonymous_cannot_write(self, store: Store) -> None:
        async with await _client(_state(), store) as client:
            resp = await client.post("/api/v2/machines/M0001/power", json={"on": True})
            assert resp.status == 401

    @pytest.mark.asyncio
    async def test_a_bad_body_is_rejected(self, store: Store) -> None:
        async with await _client(_state(), store) as client:
            await client.get("/login")
            resp = await client.post("/api/v2/machines/M0001/power", json={"on": "yes"})
            assert resp.status == 400
