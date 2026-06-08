"""Tests for juice.server — API endpoints relevant to outlet support."""

from __future__ import annotations

import asyncio
from collections import deque
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from juice.collector import PlugReading
from juice.server import (
    BULK_OP_MAX_ATTEMPTS,
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
    handle_lock,
    handle_machine_peak,
    handle_machines,
    handle_outlets,
    handle_play_hours,
    handle_power,
    handle_power_events,
    handle_strip_detail,
    handle_strip_name,
    handle_strip_peaks,
    handle_strip_usage,
    handle_usage,
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
    oauth_configured: bool = False,
):
    """Minimal request-like object whose .app exposes the registered keys.

    Optional kwargs let handlers that need `match_info`, JSON `body`, or
    a logged-in `user` exercise the same code paths as aiohttp. Default
    is `oauth_configured=False` — matches the dev-mode-no-OAuth path
    where capability checks are skipped. Public-readable tests opt in by
    passing `oauth_configured=True` so the handler sees a configured-but-
    anonymous request.
    """
    from juice.auth import oauth_config_key

    class _App:
        def __init__(self):
            self._d = {"recorder_state": app_state, "store": app_store}
            if oauth_configured:
                self._d[oauth_config_key] = {"dev": True}

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


def _make_authed_request(*args, **kwargs):
    """Helper: build a request with a non-empty `user` for handler-level tests
    of the authed branch (we don't go through the auth middleware here)."""
    return _make_request(*args, user={"email": "w@theflip.museum"}, **kwargs)


class TestHandleMachinesPublicFiltering:
    def _seed_machine_with_plug(self, store, state):
        plug_id = store.ensure_plug("hs300", "c01", "Blackout - M0013", has_emeter=True)
        state.assignments[plug_id] = ("Blackout", "M0013", 1980)
        state.plugs[plug_id] = ("hs300", "c01", "Blackout - M0013")
        state.plug_has_emeter[plug_id] = True
        state.strip_aliases["hs300"] = "Main Strip"
        state.plug_readings[plug_id] = PlugReading(
            child_id="c01",
            alias="Blackout - M0013",
            is_on=True,
            watts=300.0,
            voltage=120.0,
            amps=2.5,
            total_kwh=10.0,
        )
        return plug_id

    @pytest.mark.asyncio
    async def test_unauthenticated_omits_strip_and_plug_aliases(self, store: Store) -> None:
        state = RecorderState()
        plug_id = self._seed_machine_with_plug(store, state)

        # OAuth configured, but no logged-in user — the public-readable path.
        req = _make_request(None, state, store, oauth_configured=True)
        resp = await handle_machines(req)
        body = await _json(resp)
        m = body["machines"][0]

        # Machine name + state + power data remain visible.
        assert m["name"] == "Blackout"
        assert m["is_on"] is True
        assert m["power"]["watts"] == 300.0

        # plug_id is needed for the detail-page link.
        assert m["plug"]["plug_id"] == plug_id
        # But nothing that names a plug, strip, or device ID is exposed.
        assert "alias" not in m["plug"]
        assert "device_id" not in m["plug"]
        assert "child_id" not in m["plug"]
        assert m["strip_alias"] == ""
        assert m["strip_device_id"] == ""

    @pytest.mark.asyncio
    async def test_authenticated_keeps_aliases(self, store: Store) -> None:
        state = RecorderState()
        plug_id = self._seed_machine_with_plug(store, state)

        req = _make_authed_request(None, state, store)
        resp = await handle_machines(req)
        body = await _json(resp)
        m = body["machines"][0]

        assert m["plug"]["plug_id"] == plug_id
        assert m["plug"]["alias"] == "Blackout - M0013"
        assert m["plug"]["device_id"] == "hs300"
        assert m["plug"]["child_id"] == "c01"
        assert m["strip_alias"] == "Main Strip"
        assert m["strip_device_id"] == "hs300"


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


class TestHandleMachinesOffline:
    @pytest.mark.asyncio
    async def test_offline_device_marks_machine_offline(self, store: Store) -> None:
        state = RecorderState()
        _seed_machine(
            store,
            state,
            ("ep10-dead", "", "Blackout - M0013"),
            "M0013",
            "Blackout",
            None,
            has_emeter=False,
        )
        state.offline_since["ep10-dead"] = datetime(2026, 5, 27, 1, 15, 0, tzinfo=UTC)

        req = _make_request(None, state, store)
        body = await _json(await handle_machines(req))
        assert len(body["machines"]) == 1
        m = body["machines"][0]
        assert m["offline"] is True
        assert m["state"] == "OFFLINE"

    @pytest.mark.asyncio
    async def test_moved_machine_dedupes_offline_copy(self, store: Store) -> None:
        # Same machine on its old (offline) outlet and its new (online) outlet:
        # only the online copy should be returned.
        state = RecorderState()
        _seed_machine(
            store,
            state,
            ("ep10-old", "", "Star Trip - M0009"),
            "M0009",
            "Star Trip",
            None,
            has_emeter=False,
        )
        _seed_machine(
            store,
            state,
            ("hs300", "c02", "Star Trip - M0009"),
            "M0009",
            "Star Trip",
            None,
            watts=180.0,
        )
        state.offline_since["ep10-old"] = datetime(2026, 5, 27, 1, 15, 0, tzinfo=UTC)

        req = _make_authed_request(None, state, store)
        body = await _json(await handle_machines(req))
        assert len(body["machines"]) == 1
        m = body["machines"][0]
        assert m["offline"] is False
        assert m["plug"]["device_id"] == "hs300"


class TestHandleOutlets:
    @pytest.mark.asyncio
    async def test_returns_only_unassigned_no_emeter(self, store: Store) -> None:
        now = datetime.now(UTC)
        # An assigned HS300 plug — should not appear
        store.ensure_plug("hs300", "c01", "Blackout - M0013", has_emeter=True)
        # An unassigned EP10 that recently drew power — should appear
        ep10_unassigned = store.ensure_plug("ep10-a", "", "Garage Plug", has_emeter=False)
        store.insert_readings([(now, ep10_unassigned, None, None, None, None)])
        # An assigned EP10 — should NOT appear
        ep10_assigned = store.ensure_plug("ep10-b", "", "Tagged M9999", has_emeter=False)
        mid = store.ensure_machine("M9999", "Tagged")
        store.update_assignment(ep10_assigned, mid, now)
        store.insert_readings([(now, ep10_assigned, None, None, None, None)])

        state = RecorderState()

        req = _make_request(None, state, store)
        resp = await handle_outlets(req)
        body = await _json(resp)
        outlets = body["outlets"]
        assert len(outlets) == 1
        assert outlets[0]["plug_id"] == ep10_unassigned
        assert outlets[0]["alias"] == "Garage Plug"
        assert outlets[0]["is_on"] is True

    @pytest.mark.asyncio
    async def test_prefers_live_reading_for_is_on(self, store: Store) -> None:
        pid = store.ensure_plug("ep10-c", "", "Live", has_emeter=False)
        # Recent power draw so the outlet qualifies; live reading drives is_on.
        store.insert_readings([(datetime.now(UTC), pid, None, None, None, None)])
        state = RecorderState()
        state.plug_has_emeter[pid] = False  # no-emeter → is_on drives the tile
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

    @pytest.mark.asyncio
    async def test_emeter_outlet_uses_watts_for_is_on(self, store: Store) -> None:
        # An emeter outlet drawing ~0W reads OFF even if its relay flag is set,
        # so the tile agrees with _build_targets / an all-off sweep.
        pid = store.ensure_plug("hs300", "c06", "Sign", has_emeter=True)
        store.insert_readings([(datetime.now(UTC), pid, 30.0, 120.0, 0.25, 1.0)])
        state = RecorderState()
        state.plug_has_emeter[pid] = True
        state.plug_readings[pid] = PlugReading(
            child_id="c06",
            alias="Sign",
            is_on=True,  # relay flag on …
            watts=0.0,  # … but no power draw
            voltage=120.0,
            amps=0.0,
            total_kwh=1.0,
        )
        req = _make_request(None, state, store)
        resp = await handle_outlets(req)
        body = await _json(resp)
        assert body["outlets"][0]["is_on"] is False


class TestRouter:
    def test_outlets_and_plug_power_routes_registered(self, store: Store) -> None:
        state = RecorderState()
        app = create_app(state, store)
        routes = {(r.method, r.resource.canonical) for r in app.router.routes()}
        assert ("GET", "/api/outlets") in routes
        assert ("POST", "/api/plugs/{plug_id}/power") in routes
        assert ("POST", "/api/machines/{plug_id}/lock") in routes


class _FakePlug:
    """Minimal stand-in for collector.Plug for handle_power / run_operation tests.

    fail=True             — every call raises with the legacy "device offline"
                            message (non-retryable per the new classifier, so
                            pre-retry tests still see a single-attempt failure).
    fail_count=N          — first N calls raise; the rest succeed. Uses the
                            retryable `fail_error` message so retries engage.
    fail_error="…"        — message of the RuntimeError raised on fail_count
                            attempts; default is a retryable Passthrough error.
    """

    def __init__(
        self,
        alias: str = "Test",
        fail: bool = False,
        fail_count: int = 0,
        fail_error: str = "Passthrough failed: Request timeout",
    ) -> None:
        self.alias = alias
        self._fail_forever = fail
        self._fail_remaining = fail_count
        self._error_msg = "device offline" if fail else fail_error
        self.calls = 0
        self.turn_on = AsyncMock(side_effect=self._next)
        self.turn_off = AsyncMock(side_effect=self._next)

    async def _next(self):
        self.calls += 1
        if self._fail_forever:
            raise RuntimeError(self._error_msg)
        if self._fail_remaining > 0:
            self._fail_remaining -= 1
            raise RuntimeError(self._error_msg)


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
    async def test_retries_then_succeeds(self, store: Store, monkeypatch) -> None:
        async def _noop(_):
            return None

        monkeypatch.setattr("juice.collector.asyncio.sleep", _noop)

        state = RecorderState()
        plug_id = store.ensure_plug("hs300", "c01", "Blackout")
        state.plug_objects[plug_id] = _FakePlug(alias="Blackout", fail_count=5)

        req = _make_request(
            None,
            state,
            store,
            match_info={"plug_id": str(plug_id)},
            body={"on": True},
            user={"email": "w"},
        )
        resp = await handle_power(req)
        assert resp.status == 200
        rows = store.recent_power_events(limit=10)
        assert len(rows) == 1
        assert rows[0]["result"] == "ok"

    @pytest.mark.asyncio
    async def test_exhausts_max_attempts(self, store: Store, monkeypatch) -> None:
        async def _noop(_):
            return None

        monkeypatch.setattr("juice.collector.asyncio.sleep", _noop)

        state = RecorderState()
        plug_id = store.ensure_plug("hs300", "c01", "Blackout")
        # Fails on every attempt with retryable error.
        fake = _FakePlug(alias="Blackout")
        fake.turn_on = AsyncMock(side_effect=RuntimeError("Passthrough failed: Device is offline"))
        state.plug_objects[plug_id] = fake

        req = _make_request(
            None,
            state,
            store,
            match_info={"plug_id": str(plug_id)},
            body={"on": True},
            user={"email": "w"},
        )
        resp = await handle_power(req)
        assert resp.status == 500
        rows = store.recent_power_events(limit=10)
        assert rows[0]["result"] == "error"
        assert "attempts" in rows[0]["error"]
        # 6 attempts: 1 initial + 5 retries.
        assert fake.turn_on.call_count == 6

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


class TestHandleMachinesLock:
    @pytest.mark.asyncio
    async def test_includes_locked_field(self, store: Store) -> None:
        state = RecorderState()
        _seed_machine(store, state, ("hs", "c01", "Lck - M1"), "M1", "Lck", 1980, watts=200)
        _seed_machine(store, state, ("hs", "c02", "Free - M2"), "M2", "Free", 1985, watts=200)
        state.locked_assets.add("M1")

        req = _make_request(None, state, store)
        resp = await handle_machines(req)
        body = await _json(resp)
        locked_by_asset = {m["asset_id"]: m["locked"] for m in body["machines"]}
        assert locked_by_asset == {"M1": True, "M2": False}

    @pytest.mark.asyncio
    async def test_locked_visible_to_public(self, store: Store) -> None:
        state = RecorderState()
        _seed_machine(store, state, ("hs", "c01", "Lck - M1"), "M1", "Lck", 1980, watts=200)
        state.locked_assets.add("M1")

        # OAuth configured, no user — the public-readable path.
        req = _make_request(None, state, store, oauth_configured=True)
        resp = await handle_machines(req)
        body = await _json(resp)
        assert body["machines"][0]["locked"] is True


class TestHandlePowerLock:
    @pytest.mark.asyncio
    async def test_turn_off_locked_machine_409(self, store: Store) -> None:
        state = RecorderState()
        plug_id = _seed_machine(
            store, state, ("hs", "c01", "Blackout - M0013"), "M0013", "Blackout", 1980
        )
        fake = _FakePlug(alias="Blackout - M0013")
        state.plug_objects[plug_id] = fake
        state.locked_assets.add("M0013")
        q = asyncio.Queue(maxsize=8)
        state.event_subscribers.add(q)

        req = _make_request(
            None,
            state,
            store,
            match_info={"plug_id": str(plug_id)},
            body={"on": False},
            user={"email": "w@theflip.museum"},
        )
        resp = await handle_power(req)
        assert resp.status == 409
        body = await _json(resp)
        assert "locked" in body["error"]
        fake.turn_off.assert_not_called()
        # Refusal audited; no power_change published.
        rows = store.recent_power_events(limit=10)
        assert len(rows) == 1
        assert rows[0]["result"] == "refused"
        assert rows[0]["action"] == "turn_off"
        assert q.qsize() == 0

    @pytest.mark.asyncio
    async def test_turn_on_locked_machine_allowed(self, store: Store) -> None:
        state = RecorderState()
        plug_id = _seed_machine(
            store, state, ("hs", "c01", "Blackout - M0013"), "M0013", "Blackout", 1980
        )
        fake = _FakePlug(alias="Blackout - M0013")
        state.plug_objects[plug_id] = fake
        state.locked_assets.add("M0013")

        req = _make_request(
            None,
            state,
            store,
            match_info={"plug_id": str(plug_id)},
            body={"on": True},
            user={"email": "w@theflip.museum"},
        )
        resp = await handle_power(req)
        assert resp.status == 200
        fake.turn_on.assert_called_once()

    @pytest.mark.asyncio
    async def test_unassigned_outlet_off_unaffected(self, store: Store) -> None:
        state = RecorderState()
        plug_id = store.ensure_plug("hs", "c01", "Outlet")
        fake = _FakePlug(alias="Outlet")
        state.plug_objects[plug_id] = fake
        state.locked_assets.add("M0013")  # some other machine locked

        req = _make_request(
            None,
            state,
            store,
            match_info={"plug_id": str(plug_id)},
            body={"on": False},
            user={"email": "w@theflip.museum"},
        )
        resp = await handle_power(req)
        assert resp.status == 200
        fake.turn_off.assert_called_once()


def _seed_strip_plug(
    store: Store,
    state: RecorderState,
    device_id: str,
    child_id: str,
    alias: str,
    *,
    has_emeter: bool = True,
    watts: float | None = None,
) -> int:
    """Insert a bare plug (no machine) and register it in RecorderState."""
    plug_id = store.ensure_plug(device_id, child_id, alias, has_emeter=has_emeter)
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


DEV = "8006188258AD5449B36256BD70827E8C25536CB8"


class TestMachinePeakAPI:
    @pytest.mark.asyncio
    async def test_returns_peak_within_window(self, store: Store) -> None:
        state = RecorderState()
        pid = _seed_strip_plug(store, state, DEV, DEV[:38] + "00", "A")
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        store.insert_readings([(h, pid, 100.0, 120.0, 0.8, 0.0)])
        store.insert_readings([(h.replace(second=30), pid, 312.34, 120.0, 2.6, 0.0)])
        store.refresh_hourly_usage()

        req = _make_request(None, state, store, match_info={"plug_id": str(pid)})
        req.query = {
            "start": h.isoformat(),
            "end": (h + timedelta(hours=1)).isoformat(),
        }
        resp = await handle_machine_peak(req)
        assert resp.status == 200
        body = await _json(resp)
        assert body["plug_id"] == pid
        assert body["peak_watts"] == 312.3

    @pytest.mark.asyncio
    async def test_null_when_no_data(self, store: Store) -> None:
        state = RecorderState()
        req = _make_request(None, state, store, match_info={"plug_id": "999"})
        req.query = {}
        body = await _json(await handle_machine_peak(req))
        assert body["peak_watts"] is None

    @pytest.mark.asyncio
    async def test_days_param_narrows_window(self, store: Store) -> None:
        state = RecorderState()
        pid = _seed_strip_plug(store, state, DEV, DEV[:38] + "00", "A")
        old = datetime.now(UTC) - timedelta(days=45)
        store.insert_readings([(old, pid, 100.0, 120.0, 0.8, 0.0)])
        store.insert_readings([(old + timedelta(seconds=30), pid, 800.0, 120.0, 6.7, 0.0)])
        store.refresh_hourly_usage()

        req = _make_request(None, state, store, match_info={"plug_id": str(pid)})
        req.query = {"days": "30"}
        body = await _json(await handle_machine_peak(req))
        # The 800W spike is 45 days old — outside the 30-day window.
        assert body["peak_watts"] is None


class TestStripPeaksAPI:
    @pytest.mark.asyncio
    async def test_shape_and_values(self, store: Store) -> None:
        state = RecorderState()
        state.strip_aliases[DEV] = "Kasa Strip"
        state.strip_names[DEV] = "Back Wall"
        p1 = _seed_strip_plug(store, state, DEV, DEV[:38] + "00", "A", watts=120.0)
        p2 = _seed_strip_plug(store, state, DEV, DEV[:38] + "01", "B", watts=80.55)

        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        # Anchor rows, then staggered peaks: actual simultaneous max 450,
        # theoretical 250 + 400 = 650.
        store.insert_readings([(h, p1, 200.0, 120.0, 1.7, 0.0)])
        store.insert_readings([(h, p2, 100.0, 120.0, 0.8, 0.0)])
        ts2 = h.replace(second=30)
        store.insert_readings([(ts2, p1, 50.0, 120.0, 0.4, 0.0)])
        store.insert_readings([(ts2, p2, 400.0, 120.0, 3.3, 0.0)])
        ts3 = h.replace(minute=1)
        store.insert_readings([(ts3, p1, 250.0, 120.0, 2.1, 0.0)])
        store.insert_readings([(ts3, p2, 200.0, 120.0, 1.7, 0.0)])
        store.refresh_hourly_usage()
        store.refresh_hourly_strip_peak()

        req = _make_authed_request(None, state, store)
        req.query = {
            "start": h.isoformat(),
            "end": (h + timedelta(hours=1)).isoformat(),
        }
        resp = await handle_strip_peaks(req)
        assert resp.status == 200
        body = await _json(resp)
        assert len(body["strips"]) == 1
        s = body["strips"][0]
        assert s["device_id"] == DEV
        assert s["display_name"] == "Back Wall"
        # 120.0 + round(80.55, 1) — binary float 80.55 rounds down to 80.5.
        assert s["current_watts"] == pytest.approx(200.5)
        assert s["peak_watts_actual"] == pytest.approx(450.0)
        assert s["peak_watts_theoretical"] == pytest.approx(650.0)

    @pytest.mark.asyncio
    async def test_sorted_by_display_name(self, store: Store) -> None:
        state = RecorderState()
        state.strip_aliases["dev-b"] = "Zebra"
        state.strip_aliases["dev-a"] = "Alpha"
        _seed_strip_plug(store, state, "dev-b", "devbchild00", "B1")
        _seed_strip_plug(store, state, "dev-a", "devachild00", "A1")

        req = _make_authed_request(None, state, store)
        req.query = {}
        body = await _json(await handle_strip_peaks(req))
        assert [s["display_name"] for s in body["strips"]] == ["Alpha", "Zebra"]

    @pytest.mark.asyncio
    async def test_excludes_devices_with_no_emeter_plugs(self, store: Store) -> None:
        state = RecorderState()
        state.strip_aliases["ep"] = "Snack Corner"
        _seed_strip_plug(store, state, "ep", "", "Snack", has_emeter=False)

        req = _make_authed_request(None, state, store)
        req.query = {}
        body = await _json(await handle_strip_peaks(req))
        assert body["strips"] == []

    @pytest.mark.asyncio
    async def test_current_watts_null_when_no_readings(self, store: Store) -> None:
        state = RecorderState()
        state.strip_aliases[DEV] = "Kasa Strip"
        _seed_strip_plug(store, state, DEV, DEV[:38] + "00", "Silent")

        req = _make_authed_request(None, state, store)
        req.query = {}
        body = await _json(await handle_strip_peaks(req))
        assert body["strips"][0]["current_watts"] is None
        assert body["strips"][0]["peak_watts_actual"] is None
        assert body["strips"][0]["peak_watts_theoretical"] is None


class TestStripUsageAPI:
    @pytest.mark.asyncio
    async def test_shape_and_totals(self, store: Store) -> None:
        state = RecorderState()
        state.strip_aliases[DEV] = "Kasa Strip"
        p1 = _seed_strip_plug(store, state, DEV, DEV[:38] + "00", "A")
        p2 = _seed_strip_plug(store, state, DEV, DEV[:38] + "01", "B")

        # Two hours of data on both plugs: 200W and 100W, 30s apart.
        for h in (12, 13):
            base = datetime(2026, 5, 25, h, 0, 0, tzinfo=UTC)
            for sec in (0, 30):
                ts = base.replace(second=sec)
                store.insert_readings([(ts, p1, 200.0, 120.0, 1.7, 0.0)])
                store.insert_readings([(ts, p2, 100.0, 120.0, 0.8, 0.0)])
        store.refresh_hourly_usage()

        start = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        end = datetime(2026, 5, 25, 14, 0, 0, tzinfo=UTC)
        req = _make_authed_request(None, state, store, match_info={"device_id": DEV})
        req.query = {"start": start.isoformat(), "end": end.isoformat()}
        resp = await handle_strip_usage(req)
        assert resp.status == 200
        body = await _json(resp)

        assert body["device_id"] == DEV
        assert body["start"].startswith("2026-05-25T12:00")
        assert body["end"].startswith("2026-05-25T14:00")
        assert len(body["hours"]) == 2
        assert len(body["hourly_kwh"]) == 2
        # Nonzero in BOTH buckets — this is the naive-timestamp regression
        # guard: a tz mismatch silently yields all zeros.
        # Hour 12: each plug's :30 reading carries dt=30s → (200+100)W × 30s.
        h12 = round(300.0 * 30 / 3600 / 1000, 4)
        # Hour 13: the :00 reading's gap from 12:00:30 caps at 60s, plus the
        # :30 reading's 30s → (200+100)W × 90s.
        h13 = round(300.0 * 90 / 3600 / 1000, 4)
        assert body["hourly_kwh"] == [h12, h13]
        assert body["total_kwh"] == pytest.approx(sum(body["hourly_kwh"]))

    @pytest.mark.asyncio
    async def test_includes_actual_and_theoretical_peaks(self, store: Store) -> None:
        state = RecorderState()
        state.strip_aliases[DEV] = "Kasa Strip"
        p1 = _seed_strip_plug(store, state, DEV, DEV[:38] + "00", "A")
        p2 = _seed_strip_plug(store, state, DEV, DEV[:38] + "01", "B")

        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        store.insert_readings([(h, p1, 200.0, 120.0, 1.7, 0.0)])
        store.insert_readings([(h, p2, 100.0, 120.0, 0.8, 0.0)])
        ts2 = h.replace(second=30)
        store.insert_readings([(ts2, p1, 50.0, 120.0, 0.4, 0.0)])
        store.insert_readings([(ts2, p2, 400.0, 120.0, 3.3, 0.0)])
        ts3 = h.replace(minute=1)
        store.insert_readings([(ts3, p1, 250.0, 120.0, 2.1, 0.0)])
        store.insert_readings([(ts3, p2, 200.0, 120.0, 1.7, 0.0)])
        store.refresh_hourly_usage()
        store.refresh_hourly_strip_peak()

        req = _make_authed_request(None, state, store, match_info={"device_id": DEV})
        req.query = {
            "start": h.isoformat(),
            "end": (h + timedelta(hours=1)).isoformat(),
        }
        body = await _json(await handle_strip_usage(req))
        # Actual: simultaneous max sum (50+400=450). Theoretical: 250+400=650.
        assert body["peak_watts_actual"] == pytest.approx(450.0)
        assert body["peak_watts_theoretical"] == pytest.approx(650.0)

    @pytest.mark.asyncio
    async def test_peaks_null_when_no_rollup_data(self, store: Store) -> None:
        state = RecorderState()
        state.strip_aliases[DEV] = "Kasa Strip"

        req = _make_authed_request(None, state, store, match_info={"device_id": DEV})
        req.query = {}
        body = await _json(await handle_strip_usage(req))
        assert body["peak_watts_actual"] is None
        assert body["peak_watts_theoretical"] is None

    @pytest.mark.asyncio
    async def test_excludes_other_device_plugs(self, store: Store) -> None:
        state = RecorderState()
        state.strip_aliases[DEV] = "Kasa Strip"
        _seed_strip_plug(store, state, DEV, DEV[:38] + "00", "Mine")
        other = store.ensure_plug("other-dev", "c00", "Other", has_emeter=True)
        state.plugs[other] = ("other-dev", "c00", "Other")

        base = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        for sec in (0, 30):
            store.insert_readings([(base.replace(second=sec), other, 500.0, 120.0, 4.2, 0.0)])
        store.refresh_hourly_usage()

        req = _make_authed_request(None, state, store, match_info={"device_id": DEV})
        req.query = {
            "start": base.isoformat(),
            "end": base.replace(hour=13).isoformat(),
        }
        body = await _json(await handle_strip_usage(req))
        assert body["hourly_kwh"] == [0.0]
        assert body["total_kwh"] == 0.0

    @pytest.mark.asyncio
    async def test_unknown_device_404(self, store: Store) -> None:
        state = RecorderState()
        req = _make_authed_request(None, state, store, match_info={"device_id": "nope"})
        req.query = {}
        resp = await handle_strip_usage(req)
        assert resp.status == 404

    @pytest.mark.asyncio
    async def test_known_strip_with_no_usage_returns_zeros(self, store: Store) -> None:
        state = RecorderState()
        state.strip_aliases[DEV] = "Kasa Strip"

        base = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        req = _make_authed_request(None, state, store, match_info={"device_id": DEV})
        req.query = {
            "start": base.isoformat(),
            "end": base.replace(hour=14).isoformat(),
        }
        resp = await handle_strip_usage(req)
        assert resp.status == 200
        body = await _json(resp)
        assert body["hourly_kwh"] == [0.0, 0.0]
        assert body["total_kwh"] == 0.0

    @pytest.mark.asyncio
    async def test_default_window_is_30_days(self, store: Store) -> None:
        state = RecorderState()
        state.strip_aliases[DEV] = "Kasa Strip"

        req = _make_authed_request(None, state, store, match_info={"device_id": DEV})
        req.query = {}
        body = await _json(await handle_strip_usage(req))
        assert len(body["hours"]) == 30 * 24
        assert len(body["hourly_kwh"]) == 30 * 24


class TestHandleStripDetail:
    @pytest.mark.asyncio
    async def test_outlets_sorted_by_outlet_number(self, store: Store) -> None:
        state = RecorderState()
        state.strip_aliases[DEV] = "Kasa Strip"
        # Seed out of physical order: surrogate plug_ids won't match outlet order.
        p3 = _seed_strip_plug(store, state, DEV, DEV + "03", "Outlet D")
        p0 = _seed_strip_plug(store, state, DEV, DEV + "00", "Outlet A")
        p5 = _seed_strip_plug(store, state, DEV, DEV + "05", "Outlet F")

        req = _make_authed_request(None, state, store, match_info={"device_id": DEV})
        resp = await handle_strip_detail(req)
        assert resp.status == 200
        body = await _json(resp)
        assert [o["plug_id"] for o in body["outlets"]] == [p0, p3, p5]
        assert [o["outlet_number"] for o in body["outlets"]] == [1, 4, 6]

    @pytest.mark.asyncio
    async def test_machine_attribution_and_null_for_unassigned(self, store: Store) -> None:
        state = RecorderState()
        state.strip_aliases[DEV] = "Kasa Strip"
        assigned = _seed_machine(
            store, state, (DEV, DEV + "00", "Blackout - M0013"), "M0013", "Blackout", 1980
        )
        bare = _seed_strip_plug(store, state, DEV, DEV + "01", "Unused 1")

        req = _make_authed_request(None, state, store, match_info={"device_id": DEV})
        body = await _json(await handle_strip_detail(req))
        by_id = {o["plug_id"]: o for o in body["outlets"]}
        assert by_id[assigned]["machine"] == {"name": "Blackout", "asset_id": "M0013"}
        assert by_id[bare]["machine"] is None
        assert by_id[bare]["alias"] == "Unused 1"

    @pytest.mark.asyncio
    async def test_watts_and_is_on_from_live_readings(self, store: Store) -> None:
        state = RecorderState()
        state.strip_aliases[DEV] = "Kasa Strip"
        on = _seed_strip_plug(store, state, DEV, DEV + "00", "On", watts=212.34)
        off = _seed_strip_plug(store, state, DEV, DEV + "01", "Off", watts=0.0)
        silent = _seed_strip_plug(store, state, DEV, DEV + "02", "NoReading")

        req = _make_authed_request(None, state, store, match_info={"device_id": DEV})
        body = await _json(await handle_strip_detail(req))
        by_id = {o["plug_id"]: o for o in body["outlets"]}
        assert by_id[on]["is_on"] is True
        assert by_id[on]["watts"] == 212.3
        assert by_id[off]["is_on"] is False
        assert by_id[silent]["is_on"] is False
        assert by_id[silent]["watts"] is None

    @pytest.mark.asyncio
    async def test_custom_name_alias_and_display_name(self, store: Store) -> None:
        state = RecorderState()
        state.strip_aliases[DEV] = "Kasa Strip"
        _seed_strip_plug(store, state, DEV, DEV + "00", "Outlet A")

        req = _make_authed_request(None, state, store, match_info={"device_id": DEV})
        body = await _json(await handle_strip_detail(req))
        assert body["alias"] == "Kasa Strip"
        assert body["name"] == ""
        assert body["display_name"] == "Kasa Strip"

        state.strip_names[DEV] = "Back Wall"
        body = await _json(await handle_strip_detail(req))
        assert body["name"] == "Back Wall"
        assert body["display_name"] == "Back Wall"
        assert body["alias"] == "Kasa Strip"

    @pytest.mark.asyncio
    async def test_offline_device_flagged_but_renders(self, store: Store) -> None:
        state = RecorderState()
        # Hydrated from DB only — no cloud refresh yet, so no strip_aliases entry.
        _seed_strip_plug(store, state, DEV, DEV + "00", "Outlet A")
        state.offline_since[DEV] = datetime(2026, 6, 5, 12, 0, 0, tzinfo=UTC)

        req = _make_authed_request(None, state, store, match_info={"device_id": DEV})
        resp = await handle_strip_detail(req)
        assert resp.status == 200
        body = await _json(resp)
        assert body["offline"] is True
        assert len(body["outlets"]) == 1

    @pytest.mark.asyncio
    async def test_total_watts_sums_live_readings(self, store: Store) -> None:
        state = RecorderState()
        state.strip_aliases[DEV] = "Kasa Strip"
        _seed_strip_plug(store, state, DEV, DEV + "00", "A", watts=212.34)
        _seed_strip_plug(store, state, DEV, DEV + "01", "B", watts=100.0)

        req = _make_authed_request(None, state, store, match_info={"device_id": DEV})
        body = await _json(await handle_strip_detail(req))
        # Sum of the rounded per-outlet values (212.3 + 100.0), so the
        # headline always equals the sum of the visible rows.
        assert body["total_watts"] == 312.3

    @pytest.mark.asyncio
    async def test_total_watts_ignores_missing_readings(self, store: Store) -> None:
        state = RecorderState()
        state.strip_aliases[DEV] = "Kasa Strip"
        _seed_strip_plug(store, state, DEV, DEV + "00", "A", watts=50.0)
        _seed_strip_plug(store, state, DEV, DEV + "01", "Silent")

        req = _make_authed_request(None, state, store, match_info={"device_id": DEV})
        body = await _json(await handle_strip_detail(req))
        assert body["total_watts"] == 50.0

    @pytest.mark.asyncio
    async def test_total_watts_null_when_no_readings(self, store: Store) -> None:
        state = RecorderState()
        state.strip_aliases[DEV] = "Kasa Strip"
        _seed_strip_plug(store, state, DEV, DEV + "00", "Silent")

        req = _make_authed_request(None, state, store, match_info={"device_id": DEV})
        body = await _json(await handle_strip_detail(req))
        assert body["total_watts"] is None

    @pytest.mark.asyncio
    async def test_unknown_device_404(self, store: Store) -> None:
        state = RecorderState()
        req = _make_authed_request(None, state, store, match_info={"device_id": "nope"})
        resp = await handle_strip_detail(req)
        assert resp.status == 404

    @pytest.mark.asyncio
    async def test_ep10_outlet_number_null(self, store: Store) -> None:
        state = RecorderState()
        state.strip_aliases["d-ep10"] = "Snack Plug"
        _seed_strip_plug(store, state, "d-ep10", "", "Snack", has_emeter=False)

        req = _make_authed_request(None, state, store, match_info={"device_id": "d-ep10"})
        body = await _json(await handle_strip_detail(req))
        assert body["outlets"][0]["outlet_number"] is None


class TestHandleMachinesOutletInfo:
    @pytest.mark.asyncio
    async def test_authed_includes_outlet_number(self, store: Store) -> None:
        state = RecorderState()
        _seed_machine(
            store, state, (DEV, DEV + "03", "Blackout - M0013"), "M0013", "Blackout", 1980
        )

        req = _make_request(None, state, store)
        body = await _json(await handle_machines(req))
        assert body["machines"][0]["plug"]["outlet_number"] == 4

    @pytest.mark.asyncio
    async def test_public_omits_outlet_number(self, store: Store) -> None:
        state = RecorderState()
        _seed_machine(
            store, state, (DEV, DEV + "03", "Blackout - M0013"), "M0013", "Blackout", 1980
        )

        req = _make_request(None, state, store, oauth_configured=True)
        body = await _json(await handle_machines(req))
        assert "outlet_number" not in body["machines"][0]["plug"]

    @pytest.mark.asyncio
    async def test_sorted_by_outlet_number_within_strip(self, store: Store) -> None:
        state = RecorderState()
        # Seed in reverse physical order so plug_id order != outlet order.
        _seed_machine(store, state, (DEV, DEV + "04", "B - M2"), "M2", "B", 1985)
        _seed_machine(store, state, (DEV, DEV + "01", "A - M1"), "M1", "A", 1980)

        req = _make_request(None, state, store)
        body = await _json(await handle_machines(req))
        assert [m["asset_id"] for m in body["machines"]] == ["M1", "M2"]

    @pytest.mark.asyncio
    async def test_strip_alias_resolves_custom_name(self, store: Store) -> None:
        state = RecorderState()
        _seed_machine(
            store, state, (DEV, DEV + "00", "Blackout - M0013"), "M0013", "Blackout", 1980
        )
        state.strip_aliases[DEV] = "Kasa Strip"
        state.strip_names[DEV] = "Back Wall"

        req = _make_request(None, state, store)
        body = await _json(await handle_machines(req))
        assert body["machines"][0]["strip_alias"] == "Back Wall"


class TestHandleStripName:
    def _seed(self, store: Store, state: RecorderState) -> None:
        state.strip_aliases[DEV] = "Kasa Strip"
        _seed_strip_plug(store, state, DEV, DEV + "00", "Outlet A")

    @pytest.mark.asyncio
    async def test_sets_name_persists_store_and_state(self, store: Store) -> None:
        state = RecorderState()
        self._seed(store, state)

        req = _make_request(
            None, state, store, match_info={"device_id": DEV}, body={"name": "Back Wall"}
        )
        resp = await handle_strip_name(req)
        assert resp.status == 200
        body = await _json(resp)
        assert body == {"ok": True, "name": "Back Wall", "display_name": "Back Wall"}
        assert state.strip_names == {DEV: "Back Wall"}
        assert store.get_strip_names() == {DEV: "Back Wall"}

    @pytest.mark.asyncio
    async def test_empty_name_clears_override(self, store: Store) -> None:
        state = RecorderState()
        self._seed(store, state)
        store.set_strip_name(DEV, "Back Wall")
        state.strip_names[DEV] = "Back Wall"

        req = _make_request(None, state, store, match_info={"device_id": DEV}, body={"name": "  "})
        resp = await handle_strip_name(req)
        assert resp.status == 200
        body = await _json(resp)
        assert body == {"ok": True, "name": "", "display_name": "Kasa Strip"}
        assert state.strip_names == {}
        assert store.get_strip_names() == {}

    @pytest.mark.asyncio
    async def test_non_string_name_400(self, store: Store) -> None:
        state = RecorderState()
        self._seed(store, state)

        for bad in (True, 123, None, ["x"]):
            req = _make_request(
                None, state, store, match_info={"device_id": DEV}, body={"name": bad}
            )
            resp = await handle_strip_name(req)
            assert resp.status == 400, f"expected 400 for {bad!r}"
        assert state.strip_names == {}

    @pytest.mark.asyncio
    async def test_non_object_body_400(self, store: Store) -> None:
        state = RecorderState()
        self._seed(store, state)

        for bad in ("just a string", [1, 2], 42):
            req = _make_request(None, state, store, match_info={"device_id": DEV})

            async def _json_body(value=bad):
                return value

            req.json = _json_body
            resp = await handle_strip_name(req)
            assert resp.status == 400, f"expected 400 for body {bad!r}"
        assert state.strip_names == {}

    @pytest.mark.asyncio
    async def test_too_long_name_400(self, store: Store) -> None:
        state = RecorderState()
        self._seed(store, state)

        req = _make_request(
            None, state, store, match_info={"device_id": DEV}, body={"name": "x" * 101}
        )
        resp = await handle_strip_name(req)
        assert resp.status == 400

    @pytest.mark.asyncio
    async def test_requires_capability_403(self, store: Store) -> None:
        state = RecorderState()
        self._seed(store, state)

        req = _make_authed_request(
            None,
            state,
            store,
            match_info={"device_id": DEV},
            body={"name": "Back Wall"},
            oauth_configured=True,
        )
        resp = await handle_strip_name(req)
        assert resp.status == 403
        assert state.strip_names == {}

    @pytest.mark.asyncio
    async def test_unauthenticated_401(self, store: Store) -> None:
        state = RecorderState()
        self._seed(store, state)

        req = _make_request(
            None,
            state,
            store,
            match_info={"device_id": DEV},
            body={"name": "Back Wall"},
            oauth_configured=True,
        )
        resp = await handle_strip_name(req)
        assert resp.status == 401

    @pytest.mark.asyncio
    async def test_unknown_device_404(self, store: Store) -> None:
        state = RecorderState()
        req = _make_request(
            None, state, store, match_info={"device_id": "nope"}, body={"name": "X"}
        )
        resp = await handle_strip_name(req)
        assert resp.status == 404

    @pytest.mark.asyncio
    async def test_publishes_strip_name_change_event(self, store: Store) -> None:
        state = RecorderState()
        self._seed(store, state)
        q = asyncio.Queue(maxsize=8)
        state.event_subscribers.add(q)

        req = _make_request(
            None,
            state,
            store,
            match_info={"device_id": DEV},
            body={"name": "Back Wall"},
            user={"email": "w@theflip.museum"},
        )
        await handle_strip_name(req)

        ev = q.get_nowait()
        assert ev == {
            "type": "strip_name_change",
            "device_id": DEV,
            "name": "Back Wall",
            "actor": "w@theflip.museum",
        }


class TestHandleLock:
    @pytest.mark.asyncio
    async def test_lock_updates_store_and_state(self, store: Store) -> None:
        state = RecorderState()
        plug_id = _seed_machine(
            store, state, ("hs", "c01", "Blackout - M0013"), "M0013", "Blackout", 1980
        )

        req = _make_request(
            None, state, store, match_info={"plug_id": str(plug_id)}, body={"locked": True}
        )
        resp = await handle_lock(req)
        assert resp.status == 200
        body = await _json(resp)
        assert body == {"ok": True, "locked": True}
        assert state.locked_assets == {"M0013"}
        assert store.get_locked_asset_ids() == {"M0013"}

    @pytest.mark.asyncio
    async def test_unlock_roundtrip(self, store: Store) -> None:
        state = RecorderState()
        plug_id = _seed_machine(
            store, state, ("hs", "c01", "Blackout - M0013"), "M0013", "Blackout", 1980
        )

        for locked in (True, False):
            req = _make_request(
                None, state, store, match_info={"plug_id": str(plug_id)}, body={"locked": locked}
            )
            resp = await handle_lock(req)
            assert resp.status == 200
        assert state.locked_assets == set()
        assert store.get_locked_asset_ids() == set()

    @pytest.mark.asyncio
    async def test_requires_capability(self, store: Store) -> None:
        state = RecorderState()
        plug_id = _seed_machine(
            store, state, ("hs", "c01", "Blackout - M0013"), "M0013", "Blackout", 1980
        )

        req = _make_authed_request(
            None,
            state,
            store,
            match_info={"plug_id": str(plug_id)},
            body={"locked": True},
            oauth_configured=True,
        )
        resp = await handle_lock(req)
        assert resp.status == 403
        assert state.locked_assets == set()

    @pytest.mark.asyncio
    async def test_unauthenticated_401(self, store: Store) -> None:
        state = RecorderState()
        plug_id = _seed_machine(
            store, state, ("hs", "c01", "Blackout - M0013"), "M0013", "Blackout", 1980
        )

        req = _make_request(
            None,
            state,
            store,
            match_info={"plug_id": str(plug_id)},
            body={"locked": True},
            oauth_configured=True,
        )
        resp = await handle_lock(req)
        assert resp.status == 401

    @pytest.mark.asyncio
    async def test_non_boolean_locked_400(self, store: Store) -> None:
        state = RecorderState()
        plug_id = _seed_machine(
            store, state, ("hs", "c01", "Blackout - M0013"), "M0013", "Blackout", 1980
        )

        req = _make_request(
            None, state, store, match_info={"plug_id": str(plug_id)}, body={"locked": "false"}
        )
        resp = await handle_lock(req)
        assert resp.status == 400
        assert state.locked_assets == set()
        assert store.get_locked_asset_ids() == set()

    @pytest.mark.asyncio
    async def test_unassigned_plug_400(self, store: Store) -> None:
        state = RecorderState()
        plug_id = store.ensure_plug("hs", "c01", "Outlet")

        req = _make_request(
            None, state, store, match_info={"plug_id": str(plug_id)}, body={"locked": True}
        )
        resp = await handle_lock(req)
        assert resp.status == 400

    @pytest.mark.asyncio
    async def test_publishes_lock_change_event(self, store: Store) -> None:
        state = RecorderState()
        plug_id = _seed_machine(
            store, state, ("hs", "c01", "Blackout - M0013"), "M0013", "Blackout", 1980
        )
        q = asyncio.Queue(maxsize=8)
        state.event_subscribers.add(q)

        req = _make_request(
            None,
            state,
            store,
            match_info={"plug_id": str(plug_id)},
            body={"locked": True},
            user={"email": "w@theflip.museum"},
        )
        await handle_lock(req)

        ev = q.get_nowait()
        assert ev == {
            "type": "lock_change",
            "plug_id": plug_id,
            "asset_id": "M0013",
            "locked": True,
            "actor": "w@theflip.museum",
        }


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

    def test_skips_locked_when_turning_off(self, store: Store) -> None:
        state = RecorderState()
        locked = _seed_machine(store, state, ("hs", "c01", "Lck"), "M1", "Lck", 1980, watts=200)
        free = _seed_machine(store, state, ("hs", "c02", "Free"), "M2", "Free", 1985, watts=200)
        state.locked_assets.add("M1")
        targets = _build_targets(state, "all_off")
        assert targets == [free]
        assert locked not in targets

    def test_locked_included_in_all_on(self, store: Store) -> None:
        state = RecorderState()
        locked = _seed_machine(store, state, ("hs", "c01", "Lck"), "M1", "Lck", 1980, watts=0)
        state.locked_assets.add("M1")
        targets = _build_targets(state, "all_on")
        assert targets == [locked]

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


def _register_outlet(
    store: Store,
    state: RecorderState,
    seed: tuple[str, str, str],
    *,
    has_emeter: bool = True,
    is_on: bool | None = None,
) -> int:
    """Register an unassigned (non-machine) outlet in RecorderState."""
    device_id, child_id, alias = seed
    plug_id = store.ensure_plug(device_id, child_id, alias, has_emeter=has_emeter)
    state.plugs[plug_id] = (device_id, child_id, alias)
    state.plug_has_emeter[plug_id] = has_emeter
    if is_on is not None:
        state.plug_readings[plug_id] = PlugReading(
            child_id=child_id,
            alias=alias,
            is_on=is_on,
            watts=(1.0 if is_on else 0.0) if has_emeter else None,
            voltage=120.0 if has_emeter else None,
            amps=None,
            total_kwh=None,
        )
    return plug_id


class TestBuildTargetsOutlets:
    def test_outlets_appended_after_machines_all_on(self, store: Store) -> None:
        state = RecorderState()
        m_new = _seed_machine(store, state, ("hs", "c01", "New"), "M1", "New", 1990, watts=0)
        m_old = _seed_machine(store, state, ("hs", "c02", "Old"), "M2", "Old", 1980, watts=0)
        outlet = _register_outlet(state=state, store=store, seed=("hs", "c06", "Sign"), is_on=False)
        targets = _build_targets(state, "all_on", [outlet])
        # Machines first (year asc), outlet last.
        assert targets == [m_old, m_new, outlet]

    def test_outlet_already_on_skipped_on_all_on(self, store: Store) -> None:
        state = RecorderState()
        outlet = _register_outlet(state=state, store=store, seed=("hs", "c06", "Sign"), is_on=True)
        assert _build_targets(state, "all_on", [outlet]) == []

    def test_outlet_already_off_skipped_on_all_off(self, store: Store) -> None:
        state = RecorderState()
        outlet = _register_outlet(state=state, store=store, seed=("hs", "c06", "Sign"), is_on=False)
        assert _build_targets(state, "all_off", [outlet]) == []

    def test_outlet_no_reading_included_on_all_on_excluded_on_all_off(self, store: Store) -> None:
        state = RecorderState()
        outlet = _register_outlet(state=state, store=store, seed=("hs", "c06", "Sign"))
        assert _build_targets(state, "all_on", [outlet]) == [outlet]
        assert _build_targets(state, "all_off", [outlet]) == []

    def test_outlet_no_playing_check(self, store: Store) -> None:
        # An on outlet has no calibration/buffer, so it's swept on all_off
        # (no PLAYING gate the way machines have).
        state = RecorderState()
        outlet = _register_outlet(state=state, store=store, seed=("hs", "c06", "Sign"), is_on=True)
        assert _build_targets(state, "all_off", [outlet]) == [outlet]


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
    async def test_outlet_uses_alias_as_machine_name(self, store: Store) -> None:
        # A non-machine outlet has no assignment; the step event should carry
        # its alias so the progress UI shows something meaningful.
        state = RecorderState()
        outlet = _register_outlet(
            state=state, store=store, seed=("hs", "c06", "Snack Machine"), is_on=False
        )
        fake = _FakePlug(alias="Snack Machine")
        state.plug_objects[outlet] = fake

        q: asyncio.Queue = asyncio.Queue(maxsize=64)
        state.event_subscribers.add(q)

        op = Operation(
            id="op-outlet",
            kind="all_on",
            started_at=datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC),
            started_by="w",
            targets=[outlet],
        )
        state.current_operation = op
        await run_operation(state, store, op, on=True, sleep=0.0)

        events = []
        while not q.empty():
            events.append(q.get_nowait())
        step = next(e for e in events if e["type"] == "operation_step")
        assert step["machine_name"] == "Snack Machine"

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

    @pytest.mark.asyncio
    async def test_retries_transient_failure_then_succeeds(self, store: Store, monkeypatch) -> None:
        # No-op sleeps so retries run instantly.
        async def _noop(_):
            return None

        monkeypatch.setattr("juice.collector.asyncio.sleep", _noop)

        state = RecorderState()
        a = _seed_machine(store, state, ("hs", "c01", "A"), "M1", "A", 1980, watts=0)
        fake = _FakePlug(alias="A", fail_count=2)  # fails twice with retryable error
        state.plug_objects[a] = fake

        q: asyncio.Queue = asyncio.Queue(maxsize=64)
        state.event_subscribers.add(q)

        op = Operation(
            id="op-retry",
            kind="all_on",
            started_at=datetime(2026, 5, 26, 12, 0, 0, tzinfo=UTC),
            started_by="w",
            targets=[a],
        )
        state.current_operation = op
        await run_operation(state, store, op, on=True, sleep=0.0)

        # Final outcome is success.
        assert op.state == "complete"
        assert op.completed == [a]
        assert op.failed == []
        assert fake.calls == 3

        # Audit log records a single success (no intermediate error rows).
        rows = store.recent_power_events(limit=10)
        assert len(rows) == 1
        assert rows[0]["result"] == "ok"

        # SSE: 1 started, 2 retry events, 1 step ok, 1 power_change, 1 complete.
        events = []
        while not q.empty():
            events.append(q.get_nowait())
        types = [e["type"] for e in events]
        assert types[0] == "operation_started"
        assert types.count("operation_step_retry") == 2
        # The retry events carry attempt number and the failing error message.
        retries = [e for e in events if e["type"] == "operation_step_retry"]
        assert [r["attempt"] for r in retries] == [1, 2]
        assert all(r["machine_name"] == "A" for r in retries)
        assert all("Request timeout" in r["error"] for r in retries)
        assert types.count("operation_step") == 1
        assert types[-1] == "operation_complete"

    @pytest.mark.asyncio
    async def test_bounded_retry_gives_up_on_persistent_failure(
        self, store: Store, monkeypatch
    ) -> None:
        # Without a bound, a permanently-unreachable plug spins forever during
        # an "all off" — fight a transient blip, then move on to the next plug.
        async def _noop(_):
            return None

        monkeypatch.setattr("juice.collector.asyncio.sleep", _noop)

        state = RecorderState()
        a = _seed_machine(store, state, ("hs", "c01", "A"), "M1", "A", 1980, watts=0)
        b = _seed_machine(store, state, ("hs", "c02", "B"), "M2", "B", 1985, watts=0)
        # `a` is the dead plug — fails every attempt with a retryable error.
        state.plug_objects[a] = _FakePlug(alias="A", fail_count=999)
        # `b` is healthy — must still get serviced after `a` gives up.
        fake_b = _FakePlug(alias="B")
        state.plug_objects[b] = fake_b

        op = Operation(
            id="op-bounded",
            kind="all_on",
            started_at=datetime(2026, 5, 30, 12, 0, 0, tzinfo=UTC),
            started_by="w",
            targets=[a, b],
        )
        state.current_operation = op
        await run_operation(state, store, op, on=True, sleep=0.0)

        # `a` gives up after exactly BULK_OP_MAX_ATTEMPTS, b succeeds, op finishes.
        assert state.plug_objects[a].calls == BULK_OP_MAX_ATTEMPTS
        assert [pid for pid, _ in op.failed] == [a]
        assert f"after {BULK_OP_MAX_ATTEMPTS} attempts" in op.failed[0][1]
        assert op.completed == [b]
        assert op.state == "complete"
        assert fake_b.calls == 1

    @pytest.mark.asyncio
    async def test_cancel_mid_retry_records_attempts_and_stops(
        self, store: Store, monkeypatch
    ) -> None:
        async def _noop(_):
            return None

        monkeypatch.setattr("juice.collector.asyncio.sleep", _noop)

        state = RecorderState()
        a = _seed_machine(store, state, ("hs", "c01", "A"), "M1", "A", 1980, watts=0)
        b = _seed_machine(store, state, ("hs", "c02", "B"), "M2", "B", 1990, watts=0)

        # `a` fails persistently with a retryable error.
        fake_a = _FakePlug(alias="A")
        fake_a.turn_on = AsyncMock(
            side_effect=RuntimeError("Passthrough failed: Device is offline")
        )
        # `b` would succeed if we ever got there — but cancel arrives first.
        fake_b = _FakePlug(alias="B")
        state.plug_objects[a] = fake_a
        state.plug_objects[b] = fake_b

        op = Operation(
            id="op-cancel",
            kind="all_on",
            started_at=datetime(2026, 5, 26, 12, 0, 0, tzinfo=UTC),
            started_by="w",
            targets=[a, b],
        )
        state.current_operation = op

        # Side-effect-on-call: after the 3rd attempt request cancel.
        original = fake_a.turn_on.side_effect

        async def _record_and_maybe_cancel(*args, **kwargs):
            if fake_a.turn_on.call_count >= 3:
                op.cancel_requested = True
            raise original

        fake_a.turn_on.side_effect = _record_and_maybe_cancel

        await run_operation(state, store, op, on=True, sleep=0.0)

        assert op.state == "cancelled"
        assert op.completed == []
        # b was never attempted.
        fake_b.turn_on.assert_not_awaited()

        # Audit row for `a` records the error with attempt count.
        rows = store.recent_power_events(limit=10)
        assert len(rows) == 1
        assert rows[0]["plug_id"] == a
        assert rows[0]["result"] == "error"
        assert "after" in rows[0]["error"] and "attempts" in rows[0]["error"]


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


class TestUsageAPI:
    @pytest.mark.asyncio
    async def test_response_shape_and_totals(self, store: Store) -> None:
        # Two assigned HS300 plugs + one unassigned + one EP10.
        a = store.ensure_plug("hs", "c01", "A - M0001", has_emeter=True)
        b = store.ensure_plug("hs", "c02", "B - M0002", has_emeter=True)
        spare = store.ensure_plug("hs", "c03", "Spare", has_emeter=True)
        ep10 = store.ensure_plug("ep", "", "Snack", has_emeter=False)

        ma = store.ensure_machine("M0001", "Alpha")
        mb = store.ensure_machine("M0002", "Beta")
        t0 = datetime(2026, 5, 1, 0, 0, 0, tzinfo=UTC)
        store.update_assignment(a, ma, t0)
        store.update_assignment(b, mb, t0)

        # Two hours of data: 12:00 and 13:00.
        for h in [12, 13]:
            base = datetime(2026, 5, 25, h, 0, 0, tzinfo=UTC)
            for sec in (0, 30):
                ts = base.replace(second=sec)
                store.insert_readings([(ts, a, 200.0, 120.0, 1.7, 0.0)])
                store.insert_readings([(ts, b, 100.0, 120.0, 0.8, 0.0)])
                store.insert_readings([(ts, spare, 50.0, 120.0, 0.4, 0.0)])
                store.insert_readings([(ts, ep10, None, None, None, None)])

        store.refresh_hourly_usage()

        # Request a window that bounds our test data.
        start = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        end = datetime(2026, 5, 25, 14, 0, 0, tzinfo=UTC)

        state = RecorderState()
        req = _make_request(None, state, store)
        req.query = {"start": start.isoformat(), "end": end.isoformat()}
        resp = await handle_usage(req)
        body = await _json(resp)

        # Shape contract
        assert body["start"].startswith("2026-05-25T12:00")
        assert body["end"].startswith("2026-05-25T14:00")
        assert len(body["hours"]) == 2  # two hourly buckets

        # No EP10 in the response.
        names = {m["name"] for m in body["machines"]}
        assert "Snack" not in names
        # The two assigned machines and the unassigned bucket are all present.
        assert {"Alpha", "Beta", "Unassigned"} <= names

        # Each machine has an hourly_kwh array aligned to hours.
        for m in body["machines"]:
            assert len(m["hourly_kwh"]) == len(body["hours"])
            # total_kwh equals the sum of hourly entries (within float
            # rounding).
            assert m["total_kwh"] == pytest.approx(sum(m["hourly_kwh"]), abs=1e-6)
            assert m["color"]

        # Grand total equals sum across machines.
        per_machine = sum(m["total_kwh"] for m in body["machines"])
        assert body["total_kwh"] == pytest.approx(per_machine, abs=1e-6)

    @pytest.mark.asyncio
    async def test_two_machines_with_same_name_stay_distinct(self, store: Store) -> None:
        """Two machines sharing the same display name must surface as separate
        entries — the chart's d3 stack keys off machine_id, not name."""
        a_plug = store.ensure_plug("hs", "c01", "A", has_emeter=True)
        b_plug = store.ensure_plug("hs", "c02", "B", has_emeter=True)
        # Two physical machines, same display name.
        ma = store.ensure_machine("M0001", "Hyperball")
        mb = store.ensure_machine("M0002", "Hyperball")
        t0 = datetime(2026, 5, 1, 0, 0, 0, tzinfo=UTC)
        store.update_assignment(a_plug, ma, t0)
        store.update_assignment(b_plug, mb, t0)

        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        for sec in (0, 30):
            store.insert_readings([(h.replace(second=sec), a_plug, 100.0, 120.0, 0.83, 0.0)])
            store.insert_readings([(h.replace(second=sec), b_plug, 200.0, 120.0, 1.67, 0.0)])
        store.refresh_hourly_usage()

        state = RecorderState()
        req = _make_request(None, state, store)
        req.query = {"start": h.isoformat(), "end": h.replace(hour=13).isoformat()}
        resp = await handle_usage(req)
        body = await _json(resp)

        # Both Hyperball entries appear with distinct machine_ids and the
        # correct individual totals (not collapsed together).
        hyperballs = [m for m in body["machines"] if m["name"] == "Hyperball"]
        assert len(hyperballs) == 2
        ids = {m["machine_id"] for m in hyperballs}
        assert ids == {ma, mb}

    @pytest.mark.asyncio
    async def test_unassigned_color_is_grey(self, store: Store) -> None:
        pid = store.ensure_plug("hs", "c01", "Spare", has_emeter=True)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        store.insert_readings([(h, pid, 100.0, 120.0, 0.83, 0.0)])
        store.insert_readings([(h.replace(second=30), pid, 100.0, 120.0, 0.83, 0.0)])
        store.refresh_hourly_usage()

        state = RecorderState()
        start = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        end = datetime(2026, 5, 25, 13, 0, 0, tzinfo=UTC)
        req = _make_request(None, state, store)
        req.query = {"start": start.isoformat(), "end": end.isoformat()}
        resp = await handle_usage(req)
        body = await _json(resp)
        unassigned = next(m for m in body["machines"] if m["name"] == "Unassigned")
        assert unassigned["color"].lower() == "#aeaeb2"
        assert unassigned["machine_id"] is None

    @pytest.mark.asyncio
    async def test_machine_color_stable_across_calls(self, store: Store) -> None:
        pid = store.ensure_plug("hs", "c01", "A - M0001", has_emeter=True)
        mid = store.ensure_machine("M0001", "Alpha")
        t0 = datetime(2026, 5, 1, 0, 0, 0, tzinfo=UTC)
        store.update_assignment(pid, mid, t0)
        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        store.insert_readings([(h, pid, 100.0, 120.0, 0.83, 0.0)])
        store.insert_readings([(h.replace(second=30), pid, 100.0, 120.0, 0.83, 0.0)])
        store.refresh_hourly_usage()

        state = RecorderState()
        start = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        end = datetime(2026, 5, 25, 13, 0, 0, tzinfo=UTC)

        async def _fetch_color():
            req = _make_request(None, state, store)
            req.query = {"start": start.isoformat(), "end": end.isoformat()}
            resp = await handle_usage(req)
            body = await _json(resp)
            return next(m["color"] for m in body["machines"] if m["name"] == "Alpha")

        c1 = await _fetch_color()
        c2 = await _fetch_color()
        assert c1 == c2

    @pytest.mark.asyncio
    async def test_empty_window_returns_empty_machines(self, store: Store) -> None:
        state = RecorderState()
        start = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        end = datetime(2026, 5, 25, 13, 0, 0, tzinfo=UTC)
        req = _make_request(None, state, store)
        req.query = {"start": start.isoformat(), "end": end.isoformat()}
        resp = await handle_usage(req)
        body = await _json(resp)
        assert body["machines"] == []
        assert body["total_kwh"] == 0
        assert len(body["hours"]) == 1


class TestPageTemplating:
    @pytest.mark.asyncio
    async def test_dashboard_public_substitutes_login_button(self, store: Store) -> None:
        from juice.server import handle_dashboard

        state = RecorderState()
        # OAuth configured, no user → the public-readable path.
        req = _make_request(None, state, store, oauth_configured=True)
        resp = await handle_dashboard(req)
        body = resp.body.decode()
        assert 'body class="public"' in body
        assert "PUBLIC_MODE = true;" in body
        assert 'class="auth-corner login-btn"' in body
        # No user-pill ELEMENT (class name appears in CSS, but no instance).
        assert 'class="auth-corner user-pill"' not in body
        assert '<a href="/logout">' not in body

    @pytest.mark.asyncio
    async def test_dashboard_authed_substitutes_user_pill(self, store: Store) -> None:
        from juice.server import handle_dashboard

        state = RecorderState()
        req = _make_request(
            None, state, store, oauth_configured=True, user={"email": "w@theflip.museum"}
        )
        resp = await handle_dashboard(req)
        body = resp.body.decode()
        assert 'body class="authed"' in body
        assert "PUBLIC_MODE = false;" in body
        assert "user-pill" in body
        assert "log out" in body
        # Login button not shown when already authed.
        assert 'class="auth-corner login-btn"' not in body

    @pytest.mark.asyncio
    async def test_dev_mode_renders_authed_no_auth_corner(self, store: Store) -> None:
        """OAuth not configured (dev mode) → operator view, no auth chrome."""
        from juice.server import handle_dashboard

        state = RecorderState()
        req = _make_request(None, state, store, oauth_configured=False)
        resp = await handle_dashboard(req)
        body = resp.body.decode()
        assert 'body class="authed"' in body
        assert "PUBLIC_MODE = false;" in body
        # Empty auth corner — neither Login nor user-pill chrome.
        assert 'class="auth-corner login-btn"' not in body
        assert 'class="auth-corner user-pill"' not in body

    @pytest.mark.asyncio
    async def test_every_page_includes_the_flip_link(self, store: Store) -> None:
        from juice.server import (
            handle_dashboard,
            handle_events_page,
            handle_machine_detail,
            handle_strip_page,
            handle_usage_page,
        )

        state = RecorderState()
        for handler in (
            handle_dashboard,
            handle_machine_detail,
            handle_usage_page,
            handle_events_page,
            handle_strip_page,
        ):
            req = _make_request(None, state, store, oauth_configured=True, user={"email": "w"})
            resp = await handler(req)
            body = resp.body.decode()
            assert "https://theflip.museum" in body, f"{handler.__name__} missing The Flip link"

    @pytest.mark.asyncio
    async def test_dashboard_public_html_marks_private_elements(self, store: Store) -> None:
        # Events nav link is marked .private-only; CSS hides it.
        from juice.server import handle_dashboard

        state = RecorderState()
        req = _make_request(None, state, store, oauth_configured=True)
        resp = await handle_dashboard(req)
        body = resp.body.decode()
        assert 'body class="public"' in body
        assert "body.public .private-only" in body  # CSS rule present
        # The Events nav link has the private-only marker.
        assert 'class="private-only" href="/events"' in body


class TestAnonymousAccessGating:
    """Belt-and-suspenders: boot the real app via TestClient and walk every
    route. Anonymous requests must be locked out of writes and operator-only
    reads; public-readable routes must succeed without auth. If someone
    adds a new POST or private GET without thinking about auth, one of
    these assertions catches it.
    """

    _OAUTH_CONFIG = {
        "client_id": "test",
        "client_secret": "test-client-secret-that-is-long-enough",
        "provider_url": "https://flipfix.example.com",
        "redirect_uri": "http://localhost/callback",
    }

    # Routes that must be inaccessible to anonymous users. (method, path).
    # Path placeholders just need to resolve in the router — the auth
    # gate fires before any plug_id lookup runs.
    _PRIVATE_ROUTES = (
        # Write endpoints (the "actions" the user worried about):
        ("POST", "/api/operations/all-on"),
        ("POST", "/api/operations/all-off"),
        ("POST", "/api/operations/some-op-id/cancel"),
        ("POST", "/api/machines/1/calibrate"),
        ("POST", "/api/machines/1/power"),
        ("POST", "/api/plugs/1/power"),
        ("POST", "/api/machines/1/lock"),
        ("POST", "/api/strips/abc/name"),
        # Operator-only reads:
        ("GET", "/api/outlets"),
        ("GET", "/api/power-events"),
        ("GET", "/api/operations/current"),
        ("GET", "/api/strips/abc"),
        ("GET", "/api/strips/abc/usage"),
        ("GET", "/api/strip-peaks"),
        # Private pages:
        ("GET", "/events"),
        ("GET", "/api/events"),
        ("GET", "/strip/abc"),
    )

    # Public-readable counterparts — anon GETs must succeed without auth.
    _PUBLIC_ROUTES = (
        ("GET", "/"),
        ("GET", "/machine/1"),
        ("GET", "/usage"),
        ("GET", "/api/machines"),
        ("GET", "/api/usage"),
        ("GET", "/api/play-hours"),
        ("GET", "/api/machines/1/readings"),
        ("GET", "/api/machines/1/peak"),
        ("GET", "/api/me"),
    )

    @pytest.mark.asyncio
    async def test_anonymous_locked_out_of_all_actions(self, store: Store) -> None:
        from aiohttp.test_utils import TestClient, TestServer

        state = RecorderState()
        app = create_app(state, store, oauth_config=self._OAUTH_CONFIG)
        async with TestClient(TestServer(app)) as client:
            for method, path in self._PRIVATE_ROUTES:
                resp = await client.request(method, path, allow_redirects=False)
                if path.startswith("/api/"):
                    assert resp.status == 401, (
                        f"{method} {path}: expected 401 for anon, got {resp.status}"
                    )
                    body = await resp.json()
                    assert body.get("error") == "Not authenticated", (
                        f"{method} {path}: unexpected error body {body}"
                    )
                else:
                    assert resp.status == 302, (
                        f"{method} {path}: expected 302 for anon, got {resp.status}"
                    )
                    assert resp.headers["Location"] == "/login"

    @pytest.mark.asyncio
    async def test_anonymous_can_read_public_routes(self, store: Store) -> None:
        from aiohttp.test_utils import TestClient, TestServer

        state = RecorderState()
        app = create_app(state, store, oauth_config=self._OAUTH_CONFIG)
        async with TestClient(TestServer(app)) as client:
            for method, path in self._PUBLIC_ROUTES:
                resp = await client.request(method, path, allow_redirects=False)
                assert resp.status == 200, (
                    f"{method} {path}: expected 200 for anon, got {resp.status}"
                )


class TestStripPageHTML:
    def test_route_registered(self, store: Store) -> None:
        state = RecorderState()
        app = create_app(state, store)
        routes = {(r.method, r.resource.canonical) for r in app.router.routes()}
        assert ("GET", "/strip/{device_id}") in routes

    def test_template_has_markers_and_api_fetch(self) -> None:
        from juice.server import STRIP_HTML

        assert "{{PUBLIC_MODE}}" in STRIP_HTML
        assert "{{BODY_CLASS}}" in STRIP_HTML
        assert "{{AUTH_CORNER}}" in STRIP_HTML
        assert "/api/strips/" in STRIP_HTML

    def test_template_has_usage_card(self) -> None:
        from juice.server import STRIP_HTML

        assert "cdn.jsdelivr.net/npm/d3@7" in STRIP_HTML
        assert "/usage?days=" in STRIP_HTML
        assert "total-watts" in STRIP_HTML

    def test_template_has_peak_line(self) -> None:
        from juice.server import STRIP_HTML

        assert "usage-peak" in STRIP_HTML
        assert "max possible" in STRIP_HTML


class TestDetailPageHTML:
    def test_template_fetches_and_renders_peak(self) -> None:
        from juice.server import DETAIL_HTML

        assert "/peak?days=30" in DETAIL_HTML
        assert "Peak" in DETAIL_HTML


class TestUsagePageHTML:
    def test_route_registered(self, store: Store) -> None:
        state = RecorderState()
        app = create_app(state, store)
        routes = {(r.method, r.resource.canonical) for r in app.router.routes()}
        assert ("GET", "/usage") in routes
        assert ("GET", "/api/usage") in routes
        assert ("GET", "/api/play-hours") in routes
        assert ("GET", "/api/strip-peaks") in routes
        assert ("GET", "/api/machines/{plug_id}/peak") in routes

    def test_template_has_private_strip_peaks_section(self) -> None:
        from juice.server import USAGE_HTML

        assert "strip-peaks" in USAGE_HTML
        assert "/api/strip-peaks" in USAGE_HTML
        assert "{{PUBLIC_MODE}}" in USAGE_HTML
        # The section itself must carry the private-only marker.
        assert 'class="section-title private-only"' in USAGE_HTML


class TestPlayHoursAPI:
    """Most of the play-hours math lives in juice.store; the API tests here
    cover shape + reshape correctness only. They seed daily_play_seconds
    directly rather than re-exercising the rollup pipeline."""

    @pytest.mark.asyncio
    async def test_response_shape_and_totals(self, store: Store) -> None:
        # Two calibrated machines, three days of play seeded directly into the
        # rollup table — the rollup-from-readings path is exercised in
        # test_store.py; this test only checks the endpoint's reshape.
        ma = store.ensure_machine("M0001", "Alpha")
        mb = store.ensure_machine("M0002", "Beta")
        # Calibrate both (so the response includes them).
        store.set_calibration(ma, Calibration(idle_max_rsd=None, play_min_rsd=10.0))
        store.set_calibration(mb, Calibration(idle_max_rsd=None, play_min_rsd=10.0))
        # Direct rollup seeding bypasses classify() — the test focuses on
        # the API reshape, not the rollup math.
        from datetime import date as _date

        for day, mid, seconds in [
            (_date(2026, 5, 23), ma, 1800.0),  # 0.5h
            (_date(2026, 5, 23), mb, 3600.0),  # 1.0h
            (_date(2026, 5, 24), ma, 7200.0),  # 2.0h
            (_date(2026, 5, 24), mb, 1800.0),  # 0.5h
            (_date(2026, 5, 25), ma, 3600.0),  # 1.0h
        ]:
            store._conn.execute(
                "INSERT INTO daily_play_seconds (machine_id, day_local, seconds) VALUES (?, ?, ?)",
                [mid, day, seconds],
            )

        state = RecorderState()
        req = _make_request(None, state, store)
        req.query = {"start": "2026-05-23", "end": "2026-05-26"}  # 3 days
        resp = await handle_play_hours(req)
        body = await _json(resp)

        # Window contract
        assert body["start"] == "2026-05-23"
        assert body["end"] == "2026-05-26"
        assert body["days"] == ["2026-05-23", "2026-05-24", "2026-05-25"]

        # Both machines present, hourly arrays aligned, totals consistent.
        names = {m["name"] for m in body["machines"]}
        assert names == {"Alpha", "Beta"}
        for m in body["machines"]:
            assert len(m["daily_hours"]) == 3
            assert m["total_hours"] == pytest.approx(sum(m["daily_hours"]), abs=1e-6)
            assert m["color"]

        # Per-machine totals
        alpha = next(m for m in body["machines"] if m["name"] == "Alpha")
        beta = next(m for m in body["machines"] if m["name"] == "Beta")
        assert alpha["total_hours"] == pytest.approx(0.5 + 2.0 + 1.0)
        assert beta["total_hours"] == pytest.approx(1.0 + 0.5)

        # Grand total
        assert body["total_hours"] == pytest.approx(5.0, abs=1e-6)

    @pytest.mark.asyncio
    async def test_sorted_biggest_first(self, store: Store) -> None:
        ma = store.ensure_machine("M0001", "Small")
        mb = store.ensure_machine("M0002", "Big")
        store.set_calibration(ma, Calibration(idle_max_rsd=None, play_min_rsd=10.0))
        store.set_calibration(mb, Calibration(idle_max_rsd=None, play_min_rsd=10.0))
        from datetime import date as _date

        store._conn.execute(
            "INSERT INTO daily_play_seconds (machine_id, day_local, seconds) VALUES (?, ?, ?)",
            [ma, _date(2026, 5, 25), 600.0],
        )
        store._conn.execute(
            "INSERT INTO daily_play_seconds (machine_id, day_local, seconds) VALUES (?, ?, ?)",
            [mb, _date(2026, 5, 25), 7200.0],
        )

        state = RecorderState()
        req = _make_request(None, state, store)
        req.query = {"start": "2026-05-25", "end": "2026-05-26"}
        resp = await handle_play_hours(req)
        body = await _json(resp)
        assert [m["name"] for m in body["machines"]] == ["Big", "Small"]

    @pytest.mark.asyncio
    async def test_empty_window(self, store: Store) -> None:
        state = RecorderState()
        req = _make_request(None, state, store)
        req.query = {"start": "2026-05-25", "end": "2026-05-26"}
        resp = await handle_play_hours(req)
        body = await _json(resp)
        assert body["machines"] == []
        assert body["total_hours"] == 0
        assert body["days"] == ["2026-05-25"]

    @pytest.mark.asyncio
    async def test_days_param_default(self, store: Store) -> None:
        """Without explicit start/end, default to ?days=30 ending tomorrow_local."""
        state = RecorderState()
        req = _make_request(None, state, store)
        req.query = {}
        resp = await handle_play_hours(req)
        body = await _json(resp)
        # 30-day window
        assert len(body["days"]) == 30
        # Both bounds are local YYYY-MM-DD strings.
        assert len(body["start"]) == 10
        assert len(body["end"]) == 10


async def _json(resp):
    """Extract JSON body from an aiohttp web.Response."""
    import json

    return json.loads(resp.body.decode())
