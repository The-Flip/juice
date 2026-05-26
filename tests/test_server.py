"""Tests for juice.server — API endpoints relevant to outlet support."""

from __future__ import annotations

import asyncio
from collections import deque
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from juice.collector import PlugReading
from juice.server import (
    Operation,
    RecorderState,
    _build_targets,
    _publish,
    _sse_stream,
    create_app,
    handle_all_off,
    handle_all_on,
    handle_cancel_operation,
    handle_current_operation,
    handle_machines,
    handle_outlets,
    handle_power,
    handle_power_events,
    run_operation,
)
from juice.state import Calibration
from juice.store import Store


@pytest.fixture
def store():
    with Store(":memory:") as s:
        yield s


def _make_request(
    app,
    app_state,
    app_store,
    *,
    match_info: dict | None = None,
    body: dict | None = None,
    user: dict | None = None,
):
    """Minimal request-like object whose .app exposes the registered keys.

    Optional kwargs let handlers that need `match_info`, JSON `body`, or
    a logged-in `user` exercise the same code paths as aiohttp.
    """

    class _App:
        def __init__(self):
            self._d = {"recorder_state": app_state, "store": app_store}

        def __getitem__(self, key):
            return self._d[key]

        def __contains__(self, key):
            return key in self._d

    class _Req:
        def __init__(self):
            self._attrs = {"user": user} if user is not None else {}

        def get(self, key, default=None):
            return self._attrs.get(key, default)

        def __getitem__(self, key):
            return self._attrs[key]

        async def json(self):
            return body or {}

    req = _Req()
    req.app = _App()
    req.match_info = match_info or {}
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


class _FakePlug:
    """Minimal stand-in for collector.Plug for handle_power tests."""

    def __init__(self, alias: str = "Test", fail: bool = False) -> None:
        self.alias = alias
        self._fail = fail
        self.turn_on = AsyncMock(side_effect=self._maybe_fail)
        self.turn_off = AsyncMock(side_effect=self._maybe_fail)

    async def _maybe_fail(self):
        if self._fail:
            raise RuntimeError("device offline")


class TestHandlePowerAudit:
    @pytest.mark.asyncio
    async def test_success_writes_audit_row_and_publishes_event(self, store: Store) -> None:
        state = RecorderState()
        plug_id = store.ensure_plug("hs300", "c01", "Blackout")
        state.plug_objects[plug_id] = _FakePlug(alias="Blackout")

        # SSE subscriber to verify publish.
        import asyncio as _aio

        q = _aio.Queue(maxsize=8)
        state.event_subscribers.add(q)

        req = _make_request(
            None,
            state,
            store,
            match_info={"plug_id": str(plug_id)},
            body={"on": True},
            user={"email": "william@theflip.museum"},
        )
        resp = await handle_power(req)
        assert resp.status == 200
        body = await _json(resp)
        assert body == {"ok": True, "on": True}

        # Audit row
        rows = store.recent_power_events(limit=10)
        assert len(rows) == 1
        assert rows[0]["actor"] == "william@theflip.museum"
        assert rows[0]["action"] == "turn_on"
        assert rows[0]["source"] == "individual"
        assert rows[0]["result"] == "ok"

        # Event delivered
        assert q.qsize() == 1
        ev = q.get_nowait()
        assert ev["type"] == "power_change"
        assert ev["plug_id"] == plug_id
        assert ev["on"] is True
        assert ev["actor"] == "william@theflip.museum"

    @pytest.mark.asyncio
    async def test_failure_writes_error_audit_no_publish(self, store: Store) -> None:
        state = RecorderState()
        plug_id = store.ensure_plug("hs300", "c01", "Blackout")
        state.plug_objects[plug_id] = _FakePlug(fail=True)

        import asyncio as _aio

        q = _aio.Queue(maxsize=8)
        state.event_subscribers.add(q)

        req = _make_request(
            None,
            state,
            store,
            match_info={"plug_id": str(plug_id)},
            body={"on": False},
            user={"email": "william@theflip.museum"},
        )
        resp = await handle_power(req)
        assert resp.status == 500

        rows = store.recent_power_events(limit=10)
        assert len(rows) == 1
        assert rows[0]["result"] == "error"
        assert rows[0]["error"] == "device offline"
        assert rows[0]["action"] == "turn_off"
        # No power_change event published on failure.
        assert q.qsize() == 0

    @pytest.mark.asyncio
    async def test_success_response_survives_audit_write_failure(
        self, store: Store, monkeypatch
    ) -> None:
        state = RecorderState()
        plug_id = store.ensure_plug("hs300", "c01", "Blackout")
        state.plug_objects[plug_id] = _FakePlug(alias="Blackout")

        # Make the success-path audit write blow up.
        original = store.record_power_event
        calls: list[tuple] = []

        def _flaky(*args, **kwargs):
            calls.append((args, kwargs))
            # Only the "ok" write fails; errors still record (defence in depth).
            if "ok" in args or kwargs.get("result") == "ok":
                raise RuntimeError("disk full")
            return original(*args, **kwargs)

        monkeypatch.setattr(store, "record_power_event", _flaky)

        req = _make_request(
            None,
            state,
            store,
            match_info={"plug_id": str(plug_id)},
            body={"on": True},
            user={"email": "w"},
        )
        resp = await handle_power(req)
        # The toggle succeeded, so the API call must succeed too.
        assert resp.status == 200

    @pytest.mark.asyncio
    async def test_anonymous_actor_when_no_user(self, store: Store) -> None:
        state = RecorderState()
        plug_id = store.ensure_plug("hs300", "c01", "Blackout")
        state.plug_objects[plug_id] = _FakePlug()

        req = _make_request(
            None,
            state,
            store,
            match_info={"plug_id": str(plug_id)},
            body={"on": True},
            user=None,
        )
        resp = await handle_power(req)
        assert resp.status == 200

        rows = store.recent_power_events(limit=10)
        assert rows[0]["actor"] == "anonymous"


def _seed_machine(
    store: Store,
    state: RecorderState,
    plug_id_seed: tuple[str, str, str],
    asset_id: str,
    name: str,
    year: int | None,
    *,
    has_emeter: bool = True,
    watts: float | None = None,
) -> int:
    """Insert a plug + machine + assignment and register them in RecorderState."""
    device_id, child_id, alias = plug_id_seed
    plug_id = store.ensure_plug(device_id, child_id, alias, has_emeter=has_emeter)
    store.ensure_machine(asset_id, name)
    ts = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
    store.update_assignment(plug_id, store._machine_cache[asset_id][0], ts)
    state.assignments[plug_id] = (name, asset_id, year)
    state.plugs[plug_id] = (device_id, child_id, alias)
    state.plug_has_emeter[plug_id] = has_emeter
    if watts is not None:
        state.plug_readings[plug_id] = PlugReading(
            child_id=child_id,
            alias=alias,
            is_on=watts > 0,
            watts=watts if has_emeter else None,
            voltage=120.0 if has_emeter else None,
            amps=watts / 120.0 if has_emeter else None,
            total_kwh=0.0 if has_emeter else None,
        )
    return plug_id


class TestBuildTargets:
    def test_sorts_by_year_ascending_nulls_first(self, store: Store) -> None:
        state = RecorderState()
        a = _seed_machine(store, state, ("hs", "c01", "A"), "M1", "A", 1990, watts=0)
        b = _seed_machine(store, state, ("hs", "c02", "B"), "M2", "B", 1980, watts=0)
        c = _seed_machine(store, state, ("hs", "c03", "C"), "M3", "C", None, watts=0)
        targets = _build_targets(state, "all_on")
        assert targets == [c, b, a]

    def test_skips_already_on_when_turning_on(self, store: Store) -> None:
        state = RecorderState()
        on_pid = _seed_machine(store, state, ("hs", "c01", "On"), "M1", "On", 1980, watts=200)
        off_pid = _seed_machine(store, state, ("hs", "c02", "Off"), "M2", "Off", 1985, watts=0)
        targets = _build_targets(state, "all_on")
        assert targets == [off_pid]
        assert on_pid not in targets

    def test_skips_already_off_when_turning_off(self, store: Store) -> None:
        state = RecorderState()
        on_pid = _seed_machine(store, state, ("hs", "c01", "On"), "M1", "On", 1980, watts=200)
        _ = _seed_machine(store, state, ("hs", "c02", "Off"), "M2", "Off", 1985, watts=0)
        targets = _build_targets(state, "all_off")
        assert targets == [on_pid]

    def test_skips_playing_when_turning_off(self, store: Store) -> None:
        state = RecorderState()
        playing = _seed_machine(
            store, state, ("hs", "c01", "Playing"), "M1", "Playing", 1980, watts=300
        )
        idle = _seed_machine(store, state, ("hs", "c02", "Idle"), "M2", "Idle", 1985, watts=200)
        # Calibrate + buffer so classify() returns PLAYING for the playing plug
        # and not-PLAYING for the idle plug.
        cal = Calibration(idle_max_rsd=1.0, play_min_rsd=5.0)
        state.calibrations[playing] = cal
        state.calibrations[idle] = cal
        # PLAYING: high variance buffer
        state.watt_buffers[playing] = deque(
            [200.0, 280.0, 220.0, 320.0, 260.0, 310.0, 230.0, 290.0] * 6,
            maxlen=64,
        )
        # IDLE: low variance buffer
        state.watt_buffers[idle] = deque([200.0] * 64, maxlen=64)
        targets = _build_targets(state, "all_off")
        assert playing not in targets
        assert idle in targets

    def test_no_reading_excluded_from_all_off(self, store: Store) -> None:
        state = RecorderState()
        nr_pid = _seed_machine(store, state, ("hs", "c01", "NR"), "M1", "NR", 1980)  # no watts
        targets_off = _build_targets(state, "all_off")
        assert nr_pid not in targets_off  # can't be sure it's on, so leave alone

    def test_no_reading_included_in_all_on(self, store: Store) -> None:
        state = RecorderState()
        nr_pid = _seed_machine(store, state, ("hs", "c01", "NR"), "M1", "NR", 1980)
        targets_on = _build_targets(state, "all_on")
        assert nr_pid in targets_on

    def test_no_emeter_plug_uses_is_on(self, store: Store) -> None:
        state = RecorderState()
        on_pid = _seed_machine(
            store,
            state,
            ("ep10", "", "EP10-on"),
            "M1",
            "EP10-on",
            1980,
            has_emeter=False,
            watts=1.0,  # watts>0 sets is_on=True
        )
        off_pid = _seed_machine(
            store,
            state,
            ("ep10", "x", "EP10-off"),
            "M2",
            "EP10-off",
            1990,
            has_emeter=False,
            watts=0.0,
        )
        # has_emeter=False sets watts=None; but is_on follows watts>0 in our helper.
        # For has_emeter=False the reading.watts is None, reading.is_on=True for "on_pid".
        # Force the readings directly to make intent clear.
        state.plug_readings[on_pid] = PlugReading(
            child_id="",
            alias="EP10-on",
            is_on=True,
            watts=None,
            voltage=None,
            amps=None,
            total_kwh=None,
        )
        state.plug_readings[off_pid] = PlugReading(
            child_id="x",
            alias="EP10-off",
            is_on=False,
            watts=None,
            voltage=None,
            amps=None,
            total_kwh=None,
        )
        assert _build_targets(state, "all_on") == [off_pid]
        assert _build_targets(state, "all_off") == [on_pid]


class TestRunOperation:
    @pytest.mark.asyncio
    async def test_runs_steps_and_publishes_events(self, store: Store) -> None:
        state = RecorderState()
        a = _seed_machine(store, state, ("hs", "c01", "A"), "M1", "A", 1980, watts=0)
        b = _seed_machine(store, state, ("hs", "c02", "B"), "M2", "B", 1990, watts=0)
        fake_a = _FakePlug(alias="A")
        fake_b = _FakePlug(alias="B")
        state.plug_objects[a] = fake_a
        state.plug_objects[b] = fake_b

        q: asyncio.Queue = asyncio.Queue(maxsize=64)
        state.event_subscribers.add(q)

        op = Operation(
            id="op1",
            kind="all_on",
            started_at=datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC),
            started_by="william@theflip.museum",
            targets=[a, b],
        )
        state.current_operation = op
        await run_operation(state, store, op, on=True, sleep=0.0)

        # Both plugs were turned on
        fake_a.turn_on.assert_awaited_once()
        fake_b.turn_on.assert_awaited_once()
        assert op.state == "complete"
        assert op.completed == [a, b]
        assert op.failed == []
        assert state.current_operation is None

        # Audit rows
        rows = store.recent_power_events(limit=10)
        actors = [r["actor"] for r in rows]
        results = [r["result"] for r in rows]
        op_ids = [r["operation_id"] for r in rows]
        sources = [r["source"] for r in rows]
        assert actors == ["william@theflip.museum"] * 2
        assert results == ["ok", "ok"]
        assert op_ids == ["op1", "op1"]
        assert sources == ["all_on", "all_on"]

        # Events: started + 2 step + 2 power_change + complete = 6
        events = []
        while not q.empty():
            events.append(q.get_nowait())
        types = [e["type"] for e in events]
        assert types[0] == "operation_started"
        assert types[-1] == "operation_complete"
        assert types.count("operation_step") == 2
        assert types.count("power_change") == 2

    @pytest.mark.asyncio
    async def test_cancellation_between_steps(self, store: Store) -> None:
        state = RecorderState()
        a = _seed_machine(store, state, ("hs", "c01", "A"), "M1", "A", 1980, watts=0)
        b = _seed_machine(store, state, ("hs", "c02", "B"), "M2", "B", 1990, watts=0)

        op = Operation(
            id="op1",
            kind="all_on",
            started_at=datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC),
            started_by="w",
            targets=[a, b],
        )
        state.current_operation = op

        # First plug's turn_on flips cancel_requested; second plug should never run.
        fake_a = _FakePlug(alias="A")
        fake_b = _FakePlug(alias="B")

        async def _set_cancel():
            op.cancel_requested = True

        fake_a.turn_on = AsyncMock(side_effect=_set_cancel)
        state.plug_objects[a] = fake_a
        state.plug_objects[b] = fake_b

        await run_operation(state, store, op, on=True, sleep=0.0)

        fake_a.turn_on.assert_awaited_once()
        fake_b.turn_on.assert_not_awaited()
        assert op.state == "cancelled"
        assert op.completed == [a]
        # The completed step was recorded; the un-attempted one was not.
        rows = store.recent_power_events(limit=10)
        assert len(rows) == 1
        assert rows[0]["plug_id"] == a

    @pytest.mark.asyncio
    async def test_failure_recorded_and_op_continues(self, store: Store) -> None:
        state = RecorderState()
        a = _seed_machine(store, state, ("hs", "c01", "A"), "M1", "A", 1980, watts=200)
        b = _seed_machine(store, state, ("hs", "c02", "B"), "M2", "B", 1990, watts=200)

        op = Operation(
            id="op2",
            kind="all_off",
            started_at=datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC),
            started_by="w",
            targets=[a, b],
        )
        state.current_operation = op
        fake_a = _FakePlug(alias="A", fail=True)
        fake_b = _FakePlug(alias="B")
        state.plug_objects[a] = fake_a
        state.plug_objects[b] = fake_b

        await run_operation(state, store, op, on=False, sleep=0.0)

        assert op.state == "complete"
        assert op.completed == [b]
        assert [pid for pid, _ in op.failed] == [a]

        rows = store.recent_power_events(limit=10)
        results = {r["plug_id"]: r["result"] for r in rows}
        assert results[a] == "error"
        assert results[b] == "ok"
        # Failed row carries the error message
        err_rows = [r for r in rows if r["result"] == "error"]
        assert err_rows[0]["error"] == "device offline"

    @pytest.mark.asyncio
    async def test_skipped_when_plug_object_missing(self, store: Store) -> None:
        state = RecorderState()
        a = _seed_machine(store, state, ("hs", "c01", "A"), "M1", "A", 1980, watts=0)
        # No plug_object registered — orchestrator records failure and moves on.

        op = Operation(
            id="op3",
            kind="all_on",
            started_at=datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC),
            started_by="w",
            targets=[a],
        )
        state.current_operation = op
        await run_operation(state, store, op, on=True, sleep=0.0)

        assert op.state == "complete"
        assert op.completed == []
        assert [pid for pid, _ in op.failed] == [a]
        rows = store.recent_power_events(limit=10)
        assert rows[0]["result"] == "error"


class TestBulkEndpoints:
    @pytest.mark.asyncio
    async def test_all_on_returns_409_when_operation_running(self, store: Store) -> None:
        state = RecorderState()
        # Stub an in-flight operation
        state.current_operation = Operation(
            id="in-flight",
            kind="all_on",
            started_at=datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC),
            started_by="w",
            targets=[1, 2],
        )
        req = _make_request(None, state, store, body={}, user={"email": "w"})
        resp = await handle_all_on(req)
        assert resp.status == 409
        body = await _json(resp)
        assert body["operation_id"] == "in-flight"

    @pytest.mark.asyncio
    async def test_all_off_starts_operation_and_returns_id(self, store: Store) -> None:
        state = RecorderState()
        a = _seed_machine(store, state, ("hs", "c01", "A"), "M1", "A", 1980, watts=200)
        state.plug_objects[a] = _FakePlug(alias="A")

        req = _make_request(None, state, store, body={}, user={"email": "w"})
        resp = await handle_all_off(req)
        assert resp.status == 200
        body = await _json(resp)
        assert "operation_id" in body
        assert body["targets"] == 1

        # The task is running in the background; let it complete.
        await asyncio.sleep(0)
        for _ in range(20):
            if state.current_operation is None:
                break
            await asyncio.sleep(0.01)
        assert state.current_operation is None

    @pytest.mark.asyncio
    async def test_cancel_sets_flag(self, store: Store) -> None:
        state = RecorderState()
        state.current_operation = Operation(
            id="op-x",
            kind="all_on",
            started_at=datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC),
            started_by="w",
            targets=[1],
        )
        req = _make_request(None, state, store, match_info={"id": "op-x"}, user={"email": "w"})
        resp = await handle_cancel_operation(req)
        assert resp.status == 200
        assert state.current_operation.cancel_requested is True

    @pytest.mark.asyncio
    async def test_cancel_404_for_unknown_id(self, store: Store) -> None:
        state = RecorderState()
        req = _make_request(None, state, store, match_info={"id": "nope"}, user={"email": "w"})
        resp = await handle_cancel_operation(req)
        assert resp.status == 404

    @pytest.mark.asyncio
    async def test_current_returns_null_when_idle(self, store: Store) -> None:
        state = RecorderState()
        req = _make_request(None, state, store)
        resp = await handle_current_operation(req)
        body = await _json(resp)
        assert body is None

    @pytest.mark.asyncio
    async def test_current_returns_running_op(self, store: Store) -> None:
        state = RecorderState()
        state.current_operation = Operation(
            id="op-x",
            kind="all_off",
            started_at=datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC),
            started_by="w",
            targets=[1, 2, 3],
        )
        req = _make_request(None, state, store)
        resp = await handle_current_operation(req)
        body = await _json(resp)
        assert body["id"] == "op-x"
        assert body["total"] == 3
        assert body["state"] == "running"


class TestPowerEventsAPI:
    @pytest.mark.asyncio
    async def test_returns_recent_events_newest_first(self, store: Store) -> None:
        state = RecorderState()
        pid = store.ensure_plug("d1", "c01", "P1")
        mid = store.ensure_machine("M0001", "Blackout")
        ts = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        store.update_assignment(pid, mid, ts)
        for i in range(3):
            store.record_power_event(
                datetime(2026, 5, 25, 12, i, 0, tzinfo=UTC),
                pid,
                "turn_on",
                "individual",
                f"user{i}",
                "ok",
            )

        req = _make_request(None, state, store)
        # Mimic query string access — aiohttp's request.query is a multidict;
        # for the test mock we just expose a dict-like .query.
        req.query = {}
        resp = await handle_power_events(req)
        body = await _json(resp)
        events = body["events"]
        assert len(events) == 3
        assert [e["actor"] for e in events] == ["user2", "user1", "user0"]
        assert events[0]["machine_name"] == "Blackout"
        # ts serialized as ISO string
        assert isinstance(events[0]["ts"], str)
        assert events[0]["ts"].startswith("2026-05-25")

    @pytest.mark.asyncio
    async def test_respects_limit(self, store: Store) -> None:
        state = RecorderState()
        pid = store.ensure_plug("d1", "c01", "P1")
        for i in range(5):
            store.record_power_event(
                datetime(2026, 5, 25, 12, i, 0, tzinfo=UTC),
                pid,
                "turn_on",
                "individual",
                "u",
                "ok",
            )
        req = _make_request(None, state, store)
        req.query = {"limit": "2"}
        resp = await handle_power_events(req)
        body = await _json(resp)
        assert len(body["events"]) == 2

    @pytest.mark.asyncio
    async def test_pagination_with_before(self, store: Store) -> None:
        state = RecorderState()
        pid = store.ensure_plug("d1", "c01", "P1")
        ids = []
        for i in range(5):
            ids.append(
                store.record_power_event(
                    datetime(2026, 5, 25, 12, i, 0, tzinfo=UTC),
                    pid,
                    "turn_on",
                    "individual",
                    "u",
                    "ok",
                )
            )
        req = _make_request(None, state, store)
        req.query = {"limit": "10", "before": str(ids[2])}
        resp = await handle_power_events(req)
        body = await _json(resp)
        assert [e["event_id"] for e in body["events"]] == [ids[1], ids[0]]

    @pytest.mark.asyncio
    async def test_ts_serialized_with_utc_offset(self, store: Store) -> None:
        """Naive UTC datetimes from DuckDB must be qualified so `new Date()` works."""
        state = RecorderState()
        pid = store.ensure_plug("d1", "c01", "P1")
        store.record_power_event(
            datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC),
            pid,
            "turn_on",
            "individual",
            "u",
            "ok",
        )
        req = _make_request(None, state, store)
        req.query = {}
        resp = await handle_power_events(req)
        body = await _json(resp)
        ts = body["events"][0]["ts"]
        # Must end with a timezone marker (Z or +HH:MM), not a naive ISO string.
        assert ts.endswith("+00:00") or ts.endswith("Z"), ts

    @pytest.mark.asyncio
    async def test_caps_limit_at_max(self, store: Store) -> None:
        state = RecorderState()
        pid = store.ensure_plug("d1", "c01", "P1")
        for i in range(5):
            store.record_power_event(
                datetime(2026, 5, 25, 12, i, 0, tzinfo=UTC),
                pid,
                "turn_on",
                "individual",
                "u",
                "ok",
            )
        req = _make_request(None, state, store)
        req.query = {"limit": "99999"}
        resp = await handle_power_events(req)
        # Doesn't error; cap is enforced internally.
        assert resp.status == 200


class TestSSEStream:
    @pytest.mark.asyncio
    async def test_emits_hello_then_queued_events(self, store: Store) -> None:
        state = RecorderState()
        captured: list[dict] = []

        async def write(ev: dict) -> None:
            captured.append(ev)

        task = asyncio.create_task(_sse_stream(state, write))
        # Let the stream register its queue + emit hello.
        await asyncio.sleep(0)
        _publish(
            state,
            {
                "type": "power_change",
                "plug_id": 7,
                "on": True,
                "actor": "w",
                "source": "individual",
            },
        )
        # Let the queue drain into write().
        for _ in range(5):
            await asyncio.sleep(0)
            if len(captured) >= 2:
                break

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert captured[0]["type"] == "hello"
        assert captured[0]["current_operation"] is None
        assert captured[1]["type"] == "power_change"
        assert captured[1]["plug_id"] == 7
        # Queue is cleaned up on disconnect.
        assert len(state.event_subscribers) == 0

    @pytest.mark.asyncio
    async def test_hello_carries_current_operation(self, store: Store) -> None:
        state = RecorderState()
        state.current_operation = Operation(
            id="op-x",
            kind="all_off",
            started_at=datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC),
            started_by="w",
            targets=[1, 2, 3],
        )
        captured: list[dict] = []

        async def write(ev: dict) -> None:
            captured.append(ev)

        task = asyncio.create_task(_sse_stream(state, write))
        await asyncio.sleep(0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert captured[0]["type"] == "hello"
        assert captured[0]["current_operation"]["id"] == "op-x"
        assert captured[0]["current_operation"]["total"] == 3


async def _json(resp):
    """Extract JSON body from an aiohttp web.Response."""
    import json

    return json.loads(resp.body.decode())
