"""Tests for GET /api/v2/floor — the Tier-1 view.

Opening and closing the museum and reading the floor at a glance are ~90% of
what juice is used for (user_needs.md), so this is one request rather than four,
and it leads with what's wrong.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from aiohttp.test_utils import TestClient, TestServer

from juice.collector import PlugReading
from juice.server import RecorderState, create_app, track_status
from juice.state import Calibration
from juice.store import Store

DEV_A = "DEVICE_A"
DEV_B = "DEVICE_B"
T0 = datetime(2026, 8, 31, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
def store():
    with Store(":memory:") as s:
        yield s


def _add(
    state: RecorderState,
    plug_id: int,
    device_id: str,
    asset_id: str,
    *,
    is_on: bool = True,
    watts: float | None = 200.0,
    now: datetime = T0,
) -> None:
    state.plugs[plug_id] = (device_id, f"{device_id}{plug_id:02d}", f"X - {asset_id}")
    state.plug_has_emeter[plug_id] = True
    state.assignments[plug_id] = (f"Machine {asset_id}", asset_id, 1980)
    reading = PlugReading(
        child_id=f"{device_id}{plug_id:02d}",
        alias=f"Machine {asset_id}",
        is_on=is_on,
        watts=watts,
        voltage=120.0,
        amps=1.0,
        total_kwh=1.0,
    )
    state.plug_readings[plug_id] = reading
    track_status(
        state,
        plug_id,
        reading,
        has_emeter=True,
        offline=device_id in state.offline_since,
        now=now,
    )


async def _floor(state: RecorderState, store: Store, *, login: bool = True) -> dict:
    async with TestClient(TestServer(create_app(state, store, dev_auth=True))) as client:
        if login:
            await client.get("/login")
        return await (await client.get("/api/v2/floor")).json()


class TestProblems:
    @pytest.mark.asyncio
    async def test_no_draw_and_abandoned_are_problems(self, store: Store) -> None:
        state = RecorderState()
        _add(state, 1, DEV_A, "M0001", watts=200.0)  # healthy
        _add(state, 2, DEV_A, "M0002", watts=0.0)  # relay on, nothing drawn

        body = await _floor(state, store)

        assert [p["asset_id"] for p in body["problems"]] == ["M0002"]
        assert body["problems"][0]["status"] == "no_draw"

    @pytest.mark.asyncio
    async def test_problems_carry_how_long_it_has_been_wrong(self, store: Store) -> None:
        """'no draw for 4 min' is what makes the panel actionable; a bare flag
        doesn't distinguish a real fault from a machine still starting up."""
        state = RecorderState()
        _add(state, 1, DEV_A, "M0001", watts=0.0, now=T0)

        body = await _floor(state, store)

        assert body["problems"][0]["since"] == T0.isoformat()

    @pytest.mark.asyncio
    async def test_a_machine_with_a_command_in_flight_is_not_a_problem(self, store: Store) -> None:
        """A machine five seconds into a reboot is genuinely no_draw. Without
        this the panel fills with machines that are merely still starting, every
        time someone opens the museum — exactly when it matters most."""
        state = RecorderState()
        _add(state, 1, DEV_A, "M0001", watts=0.0)
        state.commands.open(
            kind="reboot", plug_id=1, actor="dana", source="reboot", asset_id="M0001"
        )

        body = await _floor(state, store)

        assert body["problems"] == []
        machine = body["groups"][0]["machines"][0]
        assert machine["status"] == "no_draw"  # still honestly reported...
        assert machine["pending_command"]["kind"] == "reboot"  # ...with the intent alongside

    @pytest.mark.asyncio
    async def test_problems_are_a_filter_not_a_separate_list(self, store: Store) -> None:
        """Every problem must also appear among the machines with the same
        status — otherwise the panel and the tiles can drift apart."""
        state = RecorderState()
        _add(state, 1, DEV_A, "M0001", watts=0.0)
        _add(state, 2, DEV_A, "M0002", watts=200.0)

        body = await _floor(state, store)
        by_asset = {m["asset_id"]: m for g in body["groups"] for m in g["machines"]}
        for problem in body["problems"]:
            assert by_asset[problem["asset_id"]]["status"] == problem["status"]


class TestInfrastructure:
    @pytest.mark.asyncio
    async def test_an_unreachable_device_is_one_entry_not_one_per_machine(
        self, store: Store
    ) -> None:
        """A dead six-outlet strip is one thing to go and look at, not six."""
        state = RecorderState()
        state.offline_since[DEV_B] = T0
        for plug_id, asset in ((5, "M0005"), (6, "M0006"), (7, "M0007")):
            _add(state, plug_id, DEV_B, asset)

        body = await _floor(state, store)

        assert len(body["infrastructure"]) == 1
        entry = body["infrastructure"][0]
        assert entry["kind"] == "unreachable_device"
        assert sorted(entry["affects"]) == ["M0005", "M0006", "M0007"]
        assert entry["since"] == T0.isoformat()

    @pytest.mark.asyncio
    async def test_unreachable_machines_are_not_in_problems(self, store: Store) -> None:
        state = RecorderState()
        state.offline_since[DEV_B] = T0
        _add(state, 5, DEV_B, "M0005")

        body = await _floor(state, store)

        assert body["problems"] == []
        assert len(body["infrastructure"]) == 1


class TestCounts:
    @pytest.mark.asyncio
    async def test_counts_summarise_the_floor(self, store: Store) -> None:
        state = RecorderState()
        _add(state, 1, DEV_A, "M0001", watts=200.0)
        _add(state, 2, DEV_A, "M0002", watts=0.0)
        _add(state, 3, DEV_A, "M0003", is_on=False, watts=0.0)

        body = await _floor(state, store)

        assert body["counts"]["total"] == 3
        assert body["counts"]["powered"] == 1
        assert body["counts"]["problems"] == 1


class TestPayloadWeight:
    @pytest.mark.asyncio
    async def test_no_sparklines(self, store: Store) -> None:
        """This is what the front-desk tablet re-fetches on every resync and
        holds all day; sparkline floats dominated v1's payload."""
        state = RecorderState()
        _add(state, 1, DEV_A, "M0001")
        state.calibrations[1] = Calibration(idle_max_rsd=None, play_min_rsd=10.0)

        body = await _floor(state, store)
        machine = body["groups"][0]["machines"][0]

        assert "sparkline" not in machine
        assert "sparkline_states" not in machine


class TestPublicView:
    @pytest.mark.asyncio
    async def test_anonymous_sees_the_floor_without_operational_detail(self, store: Store) -> None:
        state = RecorderState()
        _add(state, 1, DEV_A, "M0001")

        body = await _floor(state, store, login=False)
        machine = body["groups"][0]["machines"][0]

        assert machine["name"] == "Machine M0001"
        assert "plug_id" not in machine
        assert "device_id" not in machine
        assert body["infrastructure"] == []  # device ids are operational detail

    @pytest.mark.asyncio
    async def test_no_operator_identity_leaks_anywhere_in_the_payload(self, store: Store) -> None:
        """redact() strips top-level keys, so a *nested* payload can slip past it.

        pending_command carries the acting operator, and _actor() resolves that
        to an OAuth email. This walks the entire anonymous response rather than
        checking a key list, so a leak introduced by any future nested field is
        caught too — a top-level-only assertion gave false confidence here once
        already.
        """
        state = RecorderState()
        _add(state, 1, DEV_A, "M0001", watts=0.0)
        state.commands.open(
            kind="reboot",
            plug_id=1,
            actor="dana@theflip.museum",
            source="reboot",
            asset_id="M0001",
        )

        body = await _floor(state, store, login=False)

        assert "@" not in json.dumps(body), "an operator identity reached a public viewer"
        # The pending state itself is fine to show — it's who did it that isn't.
        machine = body["groups"][0]["machines"][0]
        assert machine["pending_command"]["kind"] == "reboot"
        assert "actor" not in machine["pending_command"]

    @pytest.mark.asyncio
    async def test_operators_still_see_who_is_acting(self, store: Store) -> None:
        """The point of showing it: two people converging on one machine need to
        know who is already on it (user_needs J6)."""
        state = RecorderState()
        _add(state, 1, DEV_A, "M0001", watts=0.0)
        state.commands.open(
            kind="reboot",
            plug_id=1,
            actor="dana@theflip.museum",
            source="reboot",
            asset_id="M0001",
        )

        body = await _floor(state, store, login=True)
        machine = body["groups"][0]["machines"][0]

        assert machine["pending_command"]["actor"] == "dana@theflip.museum"

    @pytest.mark.asyncio
    async def test_anonymous_never_sees_who_is_running_an_operation(self, store: Store) -> None:
        """`started_by` is an email address. Mirrors v1's public SSE behaviour."""
        from juice.server import Operation

        state = RecorderState()
        _add(state, 1, DEV_A, "M0001")
        state.current_operation = Operation(
            id="op1",
            kind="all_on",
            started_at=datetime.now(UTC) - timedelta(seconds=5),
            started_by="dana@theflip.museum",
            targets=[1],
        )

        anon = await _floor(state, store, login=False)
        operator = await _floor(state, store, login=True)

        assert anon["operation"] is None
        assert operator["operation"]["started_by"] == "dana@theflip.museum"


class TestUnreachableIsNotStaleData:
    """§3 defines `unreachable` as "we know nothing current", so the payload
    must not keep serving the last values it saw as though they were live."""

    @pytest.mark.asyncio
    async def test_an_unreachable_machine_reports_no_relay_and_no_draw(self, store: Store) -> None:
        state = RecorderState()
        state.offline_since[DEV_A] = T0
        # Added while already offline, so status_since is stamped by the
        # unreachable transition itself. Setting offline_since afterwards would
        # leave the earlier `powered` stamp in place and the duration assertion
        # below would hold whether or not offline transitions are tracked.
        _add(state, 1, DEV_A, "M0001", is_on=True, watts=127.4)

        body = await _floor(state, store)
        machine = body["groups"][0]["machines"][0]

        assert machine["status"] == "unreachable"
        assert machine["relay"] is None
        assert machine["draw_watts"] is None
        # How long we have known nothing is still reported.
        assert machine["status_since"] == T0.isoformat()

    @pytest.mark.asyncio
    async def test_a_reachable_machine_still_reports_both(self, store: Store) -> None:
        state = RecorderState()
        _add(state, 1, DEV_A, "M0001", is_on=True, watts=127.4)

        body = await _floor(state, store)
        machine = body["groups"][0]["machines"][0]

        assert machine["relay"] == "on"
        assert machine["draw_watts"] == 127.4


class TestCollectorOffline:
    """On a tap-driven floor a dropped uplink silences every device at once.
    Nine `unreachable_device` entries would be nine wrong diagnoses: the
    strips are fine, juice has lost its collector. One entry says so."""

    @pytest.mark.asyncio
    async def test_no_tap_connected_is_one_entry_and_hides_the_per_device_ones(
        self, store: Store
    ) -> None:
        from juice.collector_tap import TapControl

        state = RecorderState()
        state.offline_since[DEV_A] = T0
        state.offline_since[DEV_B] = T0
        _add(state, 1, DEV_A, "M0001")
        _add(state, 5, DEV_B, "M0005")
        control = TapControl()

        app = create_app(state, store, dev_auth=True, tap_control=control)
        async with TestClient(TestServer(app)) as client:
            await client.get("/login")
            body = await (await client.get("/api/v2/floor")).json()

        assert len(body["infrastructure"]) == 1
        entry = body["infrastructure"][0]
        assert entry["kind"] == "collector_offline"
        assert sorted(entry["affects"]) == ["M0001", "M0005"]
        assert entry["device_id"] is None

    @pytest.mark.asyncio
    async def test_with_a_tap_connected_devices_are_reported_as_usual(self, store: Store) -> None:
        from juice.collector_tap import TapControl

        state = RecorderState()
        state.offline_since[DEV_B] = T0
        _add(state, 5, DEV_B, "M0005")
        control = TapControl()

        async def send(_frame):
            pass

        control.connect("bumper", send)
        app = create_app(state, store, dev_auth=True, tap_control=control)
        async with TestClient(TestServer(app)) as client:
            await client.get("/login")
            body = await (await client.get("/api/v2/floor")).json()

        assert [e["kind"] for e in body["infrastructure"]] == ["unreachable_device"]

    @pytest.mark.asyncio
    async def test_since_is_when_the_last_tap_left_not_when_strips_died(self, store: Store) -> None:
        from juice.collector_tap import TapControl

        state = RecorderState()
        state.offline_since[DEV_B] = T0  # dead for weeks
        _add(state, 5, DEV_B, "M0005")
        left = datetime(2026, 9, 13, 16, 0, 0, tzinfo=UTC)
        control = TapControl(now=lambda: left)

        async def send(_frame):
            pass

        control.connect("bumper", send)
        control.disconnect("bumper", send)
        app = create_app(state, store, dev_auth=True, tap_control=control)
        async with TestClient(TestServer(app)) as client:
            await client.get("/login")
            body = await (await client.get("/api/v2/floor")).json()

        assert body["infrastructure"][0]["since"] == left.isoformat()

    @pytest.mark.asyncio
    async def test_a_connected_but_silent_tap_is_reported_as_such(self, store: Store) -> None:
        """tap suppresses live frames while catching up on backfill. The
        socket is up, commands work, and the tiles do not move: neither
        offline nor nine dead strips."""
        from datetime import timedelta

        from juice.collector_tap import LIVE_STALE_S, LiveProjector, TapControl

        state = RecorderState()
        state.offline_since[DEV_A] = T0
        _add(state, 1, DEV_A, "M0001")
        control = TapControl()

        async def send(_frame):
            pass

        control.connect("bumper", send)
        started = datetime(2026, 9, 13, 16, 0, 0, tzinfo=UTC)
        clock = [started]
        projector = LiveProjector(state, store, now=lambda: clock[0])
        clock[0] = started + timedelta(seconds=LIVE_STALE_S + 1)
        app = create_app(state, store, dev_auth=True, tap_control=control, tap_live=projector)
        async with TestClient(TestServer(app)) as client:
            await client.get("/login")
            body = await (await client.get("/api/v2/floor")).json()
            # An operation is still allowed: the tap answers commands even
            # while it is not sending live frames.
            resp = await client.post("/api/v2/operations", json={"kind": "all_off"})

        assert [e["kind"] for e in body["infrastructure"]] == ["collector_silent"]
        assert body["infrastructure"][0]["since"] == started.isoformat()
        assert resp.status != 409

    @pytest.mark.asyncio
    async def test_anonymous_callers_still_get_nothing(self, store: Store) -> None:
        from juice.collector_tap import TapControl

        state = RecorderState()
        state.offline_since[DEV_A] = T0
        _add(state, 1, DEV_A, "M0001")
        app = create_app(state, store, dev_auth=True, tap_control=TapControl())
        async with TestClient(TestServer(app)) as client:
            body = await (await client.get("/api/v2/floor")).json()
        assert body["infrastructure"] == []
