"""Tests for /api/v2 collections — outlets, strips, circuits, power-events.

The physical hierarchy a technician navigates, plus the record of who did what.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from aiohttp.test_utils import TestClient, TestServer

from juice.collector import PlugReading
from juice.server import RecorderState, create_app
from juice.store import Store

DEV_A = "DEVICE_A"
DEV_B = "DEVICE_B"
T0 = datetime(2026, 9, 1, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def store():
    with Store(":memory:") as s:
        yield s


def _plug(
    state: RecorderState,
    plug_id: int,
    device_id: str,
    *,
    asset_id: str | None = None,
    is_on: bool = True,
    watts: float | None = 200.0,
    has_emeter: bool = True,
) -> None:
    alias = f"Outlet {plug_id}" if asset_id is None else f"Machine - {asset_id}"
    state.plugs[plug_id] = (device_id, f"{device_id}{plug_id:02d}", alias)
    state.plug_has_emeter[plug_id] = has_emeter
    if asset_id:
        state.assignments[plug_id] = (f"Machine {asset_id}", asset_id, 1980)
    state.plug_readings[plug_id] = PlugReading(
        child_id=f"{device_id}{plug_id:02d}",
        alias=alias,
        is_on=is_on,
        watts=watts,
        voltage=120.0,
        amps=1.0,
        total_kwh=1.0,
    )


def _state() -> RecorderState:
    state = RecorderState()
    _plug(state, 1, DEV_A, asset_id="M0001")
    _plug(state, 2, DEV_A, watts=0.0)  # unassigned, relay on, no draw
    _plug(state, 3, DEV_B, has_emeter=False, watts=None)
    state.strip_orders[DEV_A] = 0
    return state


async def _get(state: RecorderState, store: Store, path: str, *, login: bool = True):
    async with TestClient(TestServer(create_app(state, store, dev_auth=True))) as client:
        if login:
            await client.get("/login")
        resp = await client.get(path)
        return resp.status, (await resp.json() if resp.status < 500 else None)


class TestOutlets:
    @pytest.mark.asyncio
    async def test_unassigned_outlets_share_the_machine_status_vocabulary(
        self, store: Store
    ) -> None:
        """An outlet with no machine still has a relay, a draw and a
        reachability, so no_draw means the same thing there. v1 gave outlets
        their own shape, which is why the Problems filter couldn't span both."""
        status, body = await _get(_state(), store, "/api/v2/outlets")
        assert status == 200

        by_id = {o["plug_id"]: o for o in body["outlets"]}
        assert by_id[2]["machine"] is None
        assert by_id[2]["status"] == "no_draw"
        assert by_id[1]["machine"]["asset_id"] == "M0001"

    @pytest.mark.asyncio
    async def test_an_unmetered_outlet_reports_powered_not_a_fake_zero(self, store: Store) -> None:
        status, body = await _get(_state(), store, "/api/v2/outlets")
        outlet = next(o for o in body["outlets"] if o["plug_id"] == 3)
        assert outlet["draw_watts"] is None
        assert outlet["status"] == "powered"

    @pytest.mark.asyncio
    async def test_single_outlet_by_id(self, store: Store) -> None:
        status, body = await _get(_state(), store, "/api/v2/outlets/1")
        assert status == 200
        assert body["plug_id"] == 1

    @pytest.mark.asyncio
    async def test_a_bad_id_is_a_400_not_a_500(self, store: Store) -> None:
        """v1 lets a malformed id raise into aiohttp's plaintext 500."""
        status, body = await _get(_state(), store, "/api/v2/outlets/not-a-number")
        assert status == 400
        assert body["error"]["code"] == "bad_request"

    @pytest.mark.asyncio
    async def test_unknown_outlet_is_a_coded_404(self, store: Store) -> None:
        status, body = await _get(_state(), store, "/api/v2/outlets/999")
        assert status == 404
        assert body["error"]["code"] == "unknown_outlet"

    @pytest.mark.asyncio
    async def test_outlets_are_operator_only(self, store: Store) -> None:
        """This is the wiring of the building."""
        status, _ = await _get(_state(), store, "/api/v2/outlets", login=False)
        assert status == 401


class TestStrips:
    @pytest.mark.asyncio
    async def test_strips_carry_their_outlets_and_ordering(self, store: Store) -> None:
        status, body = await _get(_state(), store, "/api/v2/strips")
        assert status == 200

        strips = {s["device_id"]: s for s in body["strips"]}
        assert len(strips[DEV_A]["outlets"]) == 2
        # Positioned strips first — the operator's arrangement, not alphabetical.
        assert body["strips"][0]["device_id"] == DEV_A

    @pytest.mark.asyncio
    async def test_draw_total_says_how_much_it_could_not_measure(self, store: Store) -> None:
        """A strip with an unmetered outlet must not report a total that
        silently omits it."""
        status, body = await _get(_state(), store, "/api/v2/strips")
        strips = {s["device_id"]: s for s in body["strips"]}

        assert strips[DEV_A]["draw_watts"] == 200.0
        assert strips[DEV_A]["unmeasured_outlets"] == 0
        assert strips[DEV_B]["draw_watts"] is None
        assert strips[DEV_B]["unmeasured_outlets"] == 1

    @pytest.mark.asyncio
    async def test_a_nameless_device_still_gets_a_usable_label(self, store: Store) -> None:
        """Single-outlet plugs aren't cloud-discovered strips, so they have
        neither an operator name nor a Kasa alias and _strip_display_name returns
        "". Emitting a blank row is bad; sorting it to the top of the list
        because "" sorts first is worse."""
        state = RecorderState()
        # The derived label sorts alphabetically FIRST, so only the `named` flag
        # can produce the right order — an earlier version of this test passed
        # by alphabetical accident while the ordering was in fact broken.
        _plug(state, 7, "LONELY_DEVICE_ID", asset_id="A0001")
        _plug(state, 8, DEV_A)
        state.strip_names[DEV_A] = "Zebra Row"

        status, body = await _get(state, store, "/api/v2/strips")
        assert status == 200

        names = [s["name"] for s in body["strips"]]
        assert all(n for n in names), f"a strip has no label: {names}"
        assert "A0001" in names[1], f"expected the derived label second: {names}"
        assert names[0] == "Zebra Row", f"an operator-named strip must lead: {names}"

    @pytest.mark.asyncio
    async def test_an_unreachable_strip_says_since_when(self, store: Store) -> None:
        state = _state()
        state.offline_since[DEV_B] = T0

        status, body = await _get(state, store, f"/api/v2/strips/{DEV_B}")
        assert body["status"] == "unreachable"
        assert body["since"] == T0.isoformat()

    @pytest.mark.asyncio
    async def test_unknown_strip_is_a_coded_404(self, store: Store) -> None:
        status, body = await _get(_state(), store, "/api/v2/strips/NOPE")
        assert status == 404
        assert body["error"]["code"] == "unknown_strip"


class TestCircuits:
    @pytest.mark.asyncio
    async def test_circuits_carry_capacity_and_members(self, store: Store) -> None:
        circuit_id = store.create_circuit("Panel A", "12", "Backline", 20.0)
        state = _state()
        state.circuit_devices[DEV_A] = circuit_id

        status, body = await _get(state, store, "/api/v2/circuits")
        assert status == 200

        circuit = body["circuits"][0]
        assert circuit["label"] == "Panel A 12"
        assert circuit["capacity_watts"] == 20.0 * 120
        assert [s["device_id"] for s in circuit["strips"]] == [DEV_A]

    @pytest.mark.asyncio
    async def test_a_circuit_with_no_amps_has_no_capacity(self, store: Store) -> None:
        """Better than inventing a number the breaker never promised."""
        store.create_circuit("Panel A", "13", "Unknown rating", None)
        status, body = await _get(_state(), store, "/api/v2/circuits")
        assert body["circuits"][0]["capacity_watts"] is None


class TestPowerEvents:
    @staticmethod
    def _seed(store: Store, n: int) -> None:
        for i in range(n):
            store.record_power_event(
                datetime(2026, 9, 1, 12, i, 0, tzinfo=UTC),
                1,
                "turn_on",
                "individual",
                "dana@theflip.museum",
                "ok",
            )

    @pytest.mark.asyncio
    async def test_returns_newest_first_with_a_cursor(self, store: Store) -> None:
        self._seed(store, 5)
        status, body = await _get(_state(), store, "/api/v2/power-events?limit=2")
        assert status == 200
        assert len(body["events"]) == 2
        assert body["next_before"] == body["events"][-1]["event_id"]

    @pytest.mark.asyncio
    async def test_no_cursor_when_history_is_exhausted(self, store: Store) -> None:
        """A cursor on a short page would send the client back for nothing."""
        self._seed(store, 2)
        status, body = await _get(_state(), store, "/api/v2/power-events?limit=50")
        assert body["next_before"] is None

    @pytest.mark.asyncio
    async def test_filtering_by_asset_id_not_plug_id(self, store: Store) -> None:
        """J6/J10 both start from a machine, and asset_id survives outlet moves."""
        self._seed(store, 3)
        status, body = await _get(_state(), store, "/api/v2/power-events?asset_id=M0001")
        assert status == 200
        assert len(body["events"]) == 3

    @pytest.mark.asyncio
    async def test_unknown_asset_filter_is_a_coded_404(self, store: Store) -> None:
        status, body = await _get(_state(), store, "/api/v2/power-events?asset_id=M9999")
        assert status == 404
        assert body["error"]["code"] == "unknown_machine"

    @pytest.mark.asyncio
    async def test_an_oversized_limit_is_rejected_not_clamped(self, store: Store) -> None:
        """v1 silently clamps. A client asking for 5000 and getting 200 has no
        way to know its request was altered."""
        status, body = await _get(_state(), store, "/api/v2/power-events?limit=5000")
        assert status == 400
        assert body["error"]["detail"]["max"] == 200

    @pytest.mark.asyncio
    async def test_a_bad_cursor_is_rejected(self, store: Store) -> None:
        status, body = await _get(_state(), store, "/api/v2/power-events?before=abc")
        assert status == 400

    @pytest.mark.asyncio
    async def test_the_audit_log_is_operator_only(self, store: Store) -> None:
        """Every row names an actor."""
        status, _ = await _get(_state(), store, "/api/v2/power-events", login=False)
        assert status == 401
