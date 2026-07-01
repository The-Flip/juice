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
    REBOOT_HOLD_SECONDS,
    SPARK_POINTS,
    Operation,
    RecorderState,
    _build_targets,
    _downsample_spark,
    _nth_highest_day,
    _operation_to_dict,
    _partition_instant,
    _power_status,
    _publish,
    _readings_snapshot,
    _relay_on,
    _sse_stream,
    _strip_outlet_ids,
    _strip_plug_ids,
    create_app,
    handle_all_off,
    handle_all_on,
    handle_busy_grid,
    handle_cancel_operation,
    handle_circuit_create,
    handle_circuit_delete,
    handle_circuit_peaks,
    handle_circuit_update,
    handle_circuit_usage,
    handle_circuits,
    handle_cost,
    handle_current_operation,
    handle_lock,
    handle_machine_cost,
    handle_machine_peak,
    handle_machines,
    handle_outlets,
    handle_play_hours,
    handle_power,
    handle_power_events,
    handle_reboot,
    handle_strip_all_off,
    handle_strip_all_on,
    handle_strip_circuit_assign,
    handle_strip_detail,
    handle_strip_name,
    handle_strip_order,
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
    query: dict | None = None,
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
    req.query = query or {}
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
        # No live reading → relay unknown; a no-emeter plug has no draw to fall
        # back on either, so is_on is None (the tile renders off via power_status).
        assert outlets[0]["is_on"] is None

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
    async def test_emeter_outlet_uses_relay_for_is_on(self, store: Store) -> None:
        # An emeter outlet with the relay energized reads ON even at ~0W draw,
        # so the tile agrees with _build_targets / an all-off sweep (both key on
        # the relay, not measured watts).
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
        assert body["outlets"][0]["is_on"] is True


class TestRouter:
    def test_outlets_and_plug_power_routes_registered(self, store: Store) -> None:
        state = RecorderState()
        app = create_app(state, store)
        routes = {(r.method, r.resource.canonical) for r in app.router.routes()}
        assert ("GET", "/api/outlets") in routes
        assert ("POST", "/api/plugs/{plug_id}/power") in routes
        assert ("POST", "/api/machines/{plug_id}/lock") in routes
        assert ("POST", "/api/strips/{device_id}/all-on") in routes
        assert ("POST", "/api/strips/{device_id}/all-off") in routes


class TestFavicon:
    _OAUTH_CONFIG = {
        "client_id": "test",
        "client_secret": "test-client-secret-that-is-long-enough",
        "provider_url": "https://flipfix.example.com",
        "redirect_uri": "http://localhost/callback",
    }

    @pytest.mark.asyncio
    async def test_serves_svg_lightning_bolt(self, store: Store) -> None:
        from aiohttp.test_utils import TestClient, TestServer

        app = create_app(RecorderState(), store)
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/favicon.svg")
            assert resp.status == 200
            assert resp.content_type == "image/svg+xml"
            assert resp.headers["Cache-Control"] == "public, max-age=86400"
            body = await resp.text()
            assert "<svg" in body and "</svg>" in body

    @pytest.mark.asyncio
    async def test_ico_route_serves_same_svg(self, store: Store) -> None:
        """The bare /favicon.ico probe falls back to the same SVG bytes."""
        from aiohttp.test_utils import TestClient, TestServer

        app = create_app(RecorderState(), store)
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/favicon.ico")
            assert resp.status == 200
            assert resp.content_type == "image/svg+xml"
            body = await resp.text()
            assert "<svg" in body and "</svg>" in body

    @pytest.mark.asyncio
    async def test_reachable_without_auth(self, store: Store) -> None:
        """The login page references /favicon.svg, so it must bypass the OAuth gate."""
        from aiohttp.test_utils import TestClient, TestServer

        app = create_app(RecorderState(), store, oauth_config=self._OAUTH_CONFIG)
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/favicon.svg", allow_redirects=False)
            assert resp.status == 200


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
    async def test_turn_on_opens_watch_window_off_does_not(self, store: Store) -> None:
        state = RecorderState()
        plug_id = store.ensure_plug("hs300", "c01", "Blackout")
        state.plug_objects[plug_id] = _FakePlug(alias="Blackout")

        def _req(on: bool):
            return _make_request(
                None,
                state,
                store,
                match_info={"plug_id": str(plug_id)},
                body={"on": on},
                user={"email": "w@theflip.museum"},
            )

        # Turn ON → a future watch deadline is set.
        assert (await handle_power(_req(True))).status == 200
        deadline = state.watch_until.get(plug_id)
        assert deadline is not None and deadline > datetime.now(UTC)

        # Turn OFF → not re-watched (the OFF branch re-syncs relay state every cycle).
        state.watch_until.clear()
        assert (await handle_power(_req(False))).status == 200
        assert plug_id not in state.watch_until

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
        _seed_machine(store, state, ("hs", "c02", "Off - M2"), "M2", "Off", 1985, watts=200)
        _seed_machine(store, state, ("hs", "c03", "Free - M3"), "M3", "Free", 1990, watts=200)
        state.lock_modes["M1"] = "on"
        state.lock_modes["M2"] = "off"

        req = _make_request(None, state, store)
        resp = await handle_machines(req)
        body = await _json(resp)
        by_asset = {m["asset_id"]: (m["locked"], m["lock_mode"]) for m in body["machines"]}
        assert by_asset == {
            "M1": (True, "on"),
            "M2": (True, "off"),
            "M3": (False, None),
        }

    @pytest.mark.asyncio
    async def test_locked_visible_to_public(self, store: Store) -> None:
        state = RecorderState()
        _seed_machine(store, state, ("hs", "c01", "Lck - M1"), "M1", "Lck", 1980, watts=200)
        state.lock_modes["M1"] = "on"

        # OAuth configured, no user — the public-readable path.
        req = _make_request(None, state, store, oauth_configured=True)
        resp = await handle_machines(req)
        body = await _json(resp)
        assert body["machines"][0]["locked"] is True
        assert body["machines"][0]["lock_mode"] == "on"


class TestHandlePowerLock:
    @pytest.mark.asyncio
    async def test_turn_off_locked_machine_409(self, store: Store) -> None:
        state = RecorderState()
        plug_id = _seed_machine(
            store, state, ("hs", "c01", "Blackout - M0013"), "M0013", "Blackout", 1980
        )
        fake = _FakePlug(alias="Blackout - M0013")
        state.plug_objects[plug_id] = fake
        state.lock_modes["M0013"] = "on"
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
    async def test_turn_on_locked_on_machine_allowed(self, store: Store) -> None:
        state = RecorderState()
        plug_id = _seed_machine(
            store, state, ("hs", "c01", "Blackout - M0013"), "M0013", "Blackout", 1980
        )
        fake = _FakePlug(alias="Blackout - M0013")
        state.plug_objects[plug_id] = fake
        state.lock_modes["M0013"] = "on"

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
    async def test_turn_on_locked_off_machine_409(self, store: Store) -> None:
        state = RecorderState()
        plug_id = _seed_machine(
            store, state, ("hs", "c01", "Blackout - M0013"), "M0013", "Blackout", 1980
        )
        fake = _FakePlug(alias="Blackout - M0013")
        state.plug_objects[plug_id] = fake
        state.lock_modes["M0013"] = "off"
        q = asyncio.Queue(maxsize=8)
        state.event_subscribers.add(q)

        req = _make_request(
            None,
            state,
            store,
            match_info={"plug_id": str(plug_id)},
            body={"on": True},
            user={"email": "w@theflip.museum"},
        )
        resp = await handle_power(req)
        assert resp.status == 409
        body = await _json(resp)
        assert "locked" in body["error"]
        fake.turn_on.assert_not_called()
        # Refusal audited; no power_change published.
        rows = store.recent_power_events(limit=10)
        assert len(rows) == 1
        assert rows[0]["result"] == "refused"
        assert rows[0]["action"] == "turn_on"
        assert q.qsize() == 0

    @pytest.mark.asyncio
    async def test_turn_off_locked_off_machine_allowed(self, store: Store) -> None:
        state = RecorderState()
        plug_id = _seed_machine(
            store, state, ("hs", "c01", "Blackout - M0013"), "M0013", "Blackout", 1980
        )
        fake = _FakePlug(alias="Blackout - M0013")
        state.plug_objects[plug_id] = fake
        state.lock_modes["M0013"] = "off"

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

    @pytest.mark.asyncio
    async def test_unassigned_outlet_off_unaffected(self, store: Store) -> None:
        state = RecorderState()
        plug_id = store.ensure_plug("hs", "c01", "Outlet")
        fake = _FakePlug(alias="Outlet")
        state.plug_objects[plug_id] = fake
        state.lock_modes["M0013"] = "on"  # some other machine locked

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


class TestHandleReboot:
    @pytest.fixture(autouse=True)
    def _no_hold(self, monkeypatch):
        # Don't actually sleep the reboot hold during tests.
        import juice.server as srv

        monkeypatch.setattr(srv, "REBOOT_HOLD_SECONDS", 0.0)

    async def _settle(self, plug):
        # Let the fire-and-forget power-on task run to completion.
        for _ in range(50):
            await asyncio.sleep(0.01)
            if plug.turn_on.await_count:
                return

    @pytest.mark.asyncio
    async def test_cycles_off_then_on(self, store: Store) -> None:
        assert REBOOT_HOLD_SECONDS == 3.0  # default constant; overridden per-test
        state = RecorderState()
        plug_id = _seed_machine(
            store, state, ("hs", "c01", "Blackout - M0013"), "M0013", "Blackout", 1980
        )
        plug = _FakePlug(alias="Blackout - M0013")
        state.plug_objects[plug_id] = plug
        q = asyncio.Queue(maxsize=8)
        state.event_subscribers.add(q)

        req = _make_request(
            None,
            state,
            store,
            match_info={"plug_id": str(plug_id)},
            user={"email": "w@theflip.museum"},
        )
        resp = await handle_reboot(req)
        assert resp.status == 200
        assert await _json(resp) == {"ok": True, "rebooting": True}
        # Off-step is synchronous; on-step runs in the background.
        plug.turn_off.assert_awaited_once()
        await self._settle(plug)
        plug.turn_on.assert_awaited_once()
        assert plug_id in state.watch_until  # reboot power-on opens a watch window

        results = {
            (r["action"], r["source"], r["result"]) for r in store.recent_power_events(limit=10)
        }
        assert ("turn_off", "reboot", "ok") in results
        assert ("turn_on", "reboot", "ok") in results

        events = []
        while not q.empty():
            events.append(q.get_nowait())
        changes = {(e["on"], e["source"]) for e in events if e["type"] == "power_change"}
        assert (False, "reboot") in changes
        assert (True, "reboot") in changes

        # The detail page relies on the `on` phase to confirm the power-on landed
        # (it relaxes the relay-off→on settle gate when cloud sysinfo never
        # sampled the brief OFF). Lock the full lifecycle the client depends on.
        phases = {e["phase"] for e in events if e["type"] == "reboot"}
        assert {"start", "off", "on"} <= phases

    @pytest.mark.asyncio
    async def test_locked_during_hold_skips_power_on(self, store: Store, monkeypatch) -> None:
        # A lock applied while the reboot is holding off must veto the power-on.
        import juice.server as srv

        monkeypatch.setattr(srv, "REBOOT_HOLD_SECONDS", 0.1)
        state = RecorderState()
        plug_id = _seed_machine(
            store, state, ("hs", "c01", "Blackout - M0013"), "M0013", "Blackout", 1980
        )
        plug = _FakePlug(alias="Blackout - M0013")
        state.plug_objects[plug_id] = plug

        req = _make_request(
            None,
            state,
            store,
            match_info={"plug_id": str(plug_id)},
            user={"email": "w@theflip.museum"},
        )
        resp = await handle_reboot(req)
        assert resp.status == 200
        # Lock it off during the hold, before the background task wakes.
        state.lock_modes["M0013"] = "off"
        await asyncio.sleep(0.25)

        plug.turn_off.assert_awaited_once()
        plug.turn_on.assert_not_called()  # power-on vetoed by the lock
        rows = store.recent_power_events(limit=10)
        assert any(r["action"] == "turn_on" and r["result"] == "refused" for r in rows)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("mode", ["on", "off"])
    async def test_locked_refused_409(self, store: Store, mode: str) -> None:
        state = RecorderState()
        plug_id = _seed_machine(
            store, state, ("hs", "c01", "Blackout - M0013"), "M0013", "Blackout", 1980
        )
        plug = _FakePlug(alias="Blackout - M0013")
        state.plug_objects[plug_id] = plug
        state.lock_modes["M0013"] = mode

        req = _make_request(
            None,
            state,
            store,
            match_info={"plug_id": str(plug_id)},
            user={"email": "w@theflip.museum"},
        )
        resp = await handle_reboot(req)
        assert resp.status == 409
        assert "locked" in (await _json(resp))["error"]
        # Neither relay action fires when locked.
        plug.turn_off.assert_not_called()
        plug.turn_on.assert_not_called()
        rows = store.recent_power_events(limit=10)
        assert len(rows) == 1
        assert rows[0]["result"] == "refused"

    @pytest.mark.asyncio
    async def test_off_step_failure_500_no_turn_on(self, store: Store) -> None:
        state = RecorderState()
        plug_id = _seed_machine(
            store, state, ("hs", "c01", "Blackout - M0013"), "M0013", "Blackout", 1980
        )
        plug = _FakePlug(fail=True)
        state.plug_objects[plug_id] = plug

        req = _make_request(
            None,
            state,
            store,
            match_info={"plug_id": str(plug_id)},
            user={"email": "w@theflip.museum"},
        )
        resp = await handle_reboot(req)
        assert resp.status == 500
        plug.turn_on.assert_not_called()
        rows = store.recent_power_events(limit=10)
        assert len(rows) == 1
        assert rows[0]["result"] == "error"
        assert rows[0]["action"] == "turn_off"

    @pytest.mark.asyncio
    async def test_missing_plug_400(self, store: Store) -> None:
        state = RecorderState()
        req = _make_request(
            None,
            state,
            store,
            match_info={"plug_id": "999"},
            user={"email": "w@theflip.museum"},
        )
        resp = await handle_reboot(req)
        assert resp.status == 400

    @pytest.mark.asyncio
    async def test_requires_capability_403(self, store: Store) -> None:
        state = RecorderState()
        plug_id = _seed_machine(
            store, state, ("hs", "c01", "Blackout - M0013"), "M0013", "Blackout", 1980
        )
        state.plug_objects[plug_id] = _FakePlug(alias="Blackout - M0013")

        # Authenticated but without the control_power capability.
        req = _make_authed_request(
            None,
            state,
            store,
            match_info={"plug_id": str(plug_id)},
            oauth_configured=True,
        )
        resp = await handle_reboot(req)
        assert resp.status == 403

    def test_route_registered(self, store: Store) -> None:
        app = create_app(RecorderState(), store)
        routes = {(r.method, r.resource.canonical) for r in app.router.routes()}
        assert ("POST", "/api/machines/{plug_id}/reboot") in routes
        assert ("POST", "/api/plugs/{plug_id}/reboot") in routes


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

_ROBUST_H = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)


def _seed_robust_strip_hour(store, p1: int, p2: int) -> None:
    """Seed one dense hour where p99 peaks are exact and actual < theoretical.

    First half: p1=250, p2=100 (sum 350). Second half: p1=100, p2=400
    (sum 500). Dense enough (>=101 on-samples per group) that quantile_cont
    p99 equals the sustained level: p1 robust peak 250, p2 400, theoretical
    650; per-instant sums {350, 500} → actual robust peak 500.
    """
    rows = []
    for i in range(120):
        ts = _ROBUST_H + timedelta(seconds=i * 5)
        rows.append((ts, p1, 250.0, 120.0, 2.1, 0.0))
        rows.append((ts, p2, 100.0, 120.0, 0.8, 0.0))
    for i in range(120):
        ts = _ROBUST_H + timedelta(seconds=600 + i * 5)
        rows.append((ts, p1, 100.0, 120.0, 0.8, 0.0))
        rows.append((ts, p2, 400.0, 120.0, 3.3, 0.0))
    store.insert_readings(rows)
    store.refresh_hourly_usage()
    store.refresh_hourly_strip_peak()


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
        # Two on-readings {100, 312.34}; p99 interpolates to the top value.
        assert body["peak_watts"] == 312.3

    @pytest.mark.asyncio
    async def test_peak_is_p99_excluding_inrush(self, store: Store) -> None:
        state = RecorderState()
        pid = _seed_strip_plug(store, state, DEV, DEV[:38] + "00", "A")
        base = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        rows = [(base + timedelta(seconds=i * 7), pid, 120.0, 120.0, 1.0, 0.0) for i in range(200)]
        rows.append((base + timedelta(seconds=3), pid, 569.0, 120.0, 4.7, 0.0))
        store.insert_readings(rows)
        store.refresh_hourly_usage()

        req = _make_request(None, state, store, match_info={"plug_id": str(pid)})
        req.query = {"start": base.isoformat(), "end": (base + timedelta(hours=1)).isoformat()}
        body = await _json(await handle_machine_peak(req))
        # The 569W inrush spike is excluded; the robust peak is the ~120W draw.
        assert body["peak_watts"] == pytest.approx(120.0, abs=2.0)

    @pytest.mark.asyncio
    async def test_null_when_no_data(self, store: Store) -> None:
        state = RecorderState()
        req = _make_request(None, state, store, match_info={"plug_id": "999"})
        req.query = {}
        body = await _json(await handle_machine_peak(req))
        assert body["peak_watts"] is None

    @pytest.mark.asyncio
    async def test_non_integer_plug_id_400(self, store: Store) -> None:
        # Public path param — malformed input must be a 400, not a 500.
        state = RecorderState()
        req = _make_request(None, state, store, match_info={"plug_id": "abc"})
        req.query = {}
        resp = await handle_machine_peak(req)
        assert resp.status == 400

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

        _seed_robust_strip_hour(store, p1, p2)

        req = _make_authed_request(None, state, store)
        req.query = {
            "start": _ROBUST_H.isoformat(),
            "end": (_ROBUST_H + timedelta(hours=1)).isoformat(),
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
        # Robust (p99) peaks. The two plugs sustain their highs in different
        # halves of the hour, so actual simultaneous draw (500) is below the
        # theoretical sum-of-peaks (250 + 400 = 650).
        assert s["peak_watts_actual"] == pytest.approx(500.0, abs=2.0)
        assert s["peak_watts_theoretical"] == pytest.approx(650.0, abs=2.0)

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


class TestCircuitsListAPI:
    @pytest.mark.asyncio
    async def test_lists_circuits_with_devices(self, store: Store) -> None:
        state = RecorderState()
        cid = store.create_circuit("P1", "B20", "coin-op", 20.0)
        state.circuits[cid] = store.get_circuit(cid)
        state.strip_aliases["d1"] = "Kasa A"
        state.strip_names["d1"] = "Back Wall"
        state.circuit_devices["d1"] = cid

        req = _make_authed_request(None, state, store)
        body = await _json(await handle_circuits(req))
        assert len(body["circuits"]) == 1
        c = body["circuits"][0]
        assert c["circuit_id"] == cid
        assert c["panel"] == "P1" and c["breaker"] == "B20"
        assert c["device_ids"] == ["d1"]
        assert c["display_names"] == ["Back Wall"]


class TestCircuitWriteAPI:
    @pytest.mark.asyncio
    async def test_create(self, store: Store) -> None:
        state = RecorderState()
        req = _make_request(
            None,
            state,
            store,
            body={"panel": "P1", "breaker": "B20", "description": "coin-op", "amps": 20.0},
        )
        resp = await handle_circuit_create(req)
        assert resp.status == 200
        body = await _json(resp)
        cid = body["circuit_id"]
        assert store.get_circuit(cid)["panel"] == "P1"
        assert state.circuits[cid]["breaker"] == "B20"

    @pytest.mark.asyncio
    async def test_create_requires_capability(self, store: Store) -> None:
        state = RecorderState()
        req = _make_authed_request(
            None, state, store, body={"panel": "P1", "breaker": "B20"}, oauth_configured=True
        )
        resp = await handle_circuit_create(req)
        assert resp.status == 403

    @pytest.mark.asyncio
    async def test_create_validation(self, store: Store) -> None:
        state = RecorderState()
        for bad in (
            {"panel": "", "breaker": "B20"},  # empty panel
            {"panel": "P1", "breaker": ""},  # empty breaker
            {"panel": "P1", "breaker": "B20", "amps": -5},  # non-positive amps
            {"panel": "P1", "breaker": "B20", "amps": "twenty"},  # non-numeric amps
            {"panel": "P1", "breaker": "B20", "amps": True},  # bool amps
            ["not", "a", "dict"],  # non-object body
        ):
            req = _make_request(None, state, store, body=bad)
            resp = await handle_circuit_create(req)
            assert resp.status == 400, f"expected 400 for {bad!r}"

    @pytest.mark.asyncio
    async def test_update(self, store: Store) -> None:
        state = RecorderState()
        cid = store.create_circuit("P1", "B20", "old", 20.0)
        state.circuits[cid] = store.get_circuit(cid)
        req = _make_request(
            None,
            state,
            store,
            match_info={"id": str(cid)},
            body={"panel": "P3", "breaker": "B5", "description": "new", "amps": 15.0},
        )
        resp = await handle_circuit_update(req)
        assert resp.status == 200
        assert store.get_circuit(cid)["panel"] == "P3"
        assert state.circuits[cid]["amps"] == pytest.approx(15.0)

    @pytest.mark.asyncio
    async def test_update_unknown_404(self, store: Store) -> None:
        state = RecorderState()
        req = _make_request(
            None, state, store, match_info={"id": "999"}, body={"panel": "P1", "breaker": "B1"}
        )
        resp = await handle_circuit_update(req)
        assert resp.status == 404

    @pytest.mark.asyncio
    async def test_malformed_id_400(self, store: Store) -> None:
        state = RecorderState()
        for handler in (handle_circuit_update, handle_circuit_delete):
            req = _make_request(
                None, state, store, match_info={"id": "abc"}, body={"panel": "P1", "breaker": "B1"}
            )
            resp = await handler(req)
            assert resp.status == 400

    @pytest.mark.asyncio
    async def test_duplicate_panel_breaker_409(self, store: Store) -> None:
        state = RecorderState()
        store.create_circuit("P1", "B20", "first", 20.0)
        req = _make_request(
            None, state, store, body={"panel": "P1", "breaker": "B20", "amps": 15.0}
        )
        resp = await handle_circuit_create(req)
        assert resp.status == 409

    @pytest.mark.asyncio
    async def test_delete_clears_assignments_and_state(self, store: Store) -> None:
        state = RecorderState()
        cid = store.create_circuit("P1", "B20", "", 20.0)
        state.circuits[cid] = store.get_circuit(cid)
        store.set_device_circuit("d1", cid)
        state.circuit_devices["d1"] = cid

        req = _make_request(None, state, store, match_info={"id": str(cid)})
        resp = await handle_circuit_delete(req)
        assert resp.status == 200
        assert store.get_circuit(cid) is None
        assert cid not in state.circuits
        assert "d1" not in state.circuit_devices

    @pytest.mark.asyncio
    async def test_delete_unknown_404(self, store: Store) -> None:
        state = RecorderState()
        req = _make_request(None, state, store, match_info={"id": "999"})
        resp = await handle_circuit_delete(req)
        assert resp.status == 404


class TestStripCircuitAssignAPI:
    @pytest.mark.asyncio
    async def test_assign_existing(self, store: Store) -> None:
        state = RecorderState()
        cid = store.create_circuit("P1", "B20", "", 20.0)
        state.circuits[cid] = store.get_circuit(cid)
        _seed_strip_plug(store, state, DEV, DEV[:38] + "00", "A")

        req = _make_request(
            None, state, store, match_info={"device_id": DEV}, body={"circuit_id": cid}
        )
        resp = await handle_strip_circuit_assign(req)
        assert resp.status == 200
        assert store.get_circuit_devices() == {DEV: cid}
        assert state.circuit_devices == {DEV: cid}

    @pytest.mark.asyncio
    async def test_clear_with_null(self, store: Store) -> None:
        state = RecorderState()
        cid = store.create_circuit("P1", "B20", "", 20.0)
        _seed_strip_plug(store, state, DEV, DEV[:38] + "00", "A")
        store.set_device_circuit(DEV, cid)
        state.circuit_devices[DEV] = cid

        req = _make_request(
            None, state, store, match_info={"device_id": DEV}, body={"circuit_id": None}
        )
        resp = await handle_strip_circuit_assign(req)
        assert resp.status == 200
        assert store.get_circuit_devices() == {}
        assert state.circuit_devices == {}

    @pytest.mark.asyncio
    async def test_unknown_device_404(self, store: Store) -> None:
        state = RecorderState()
        cid = store.create_circuit("P1", "B20", "", 20.0)
        req = _make_request(
            None, state, store, match_info={"device_id": "nope"}, body={"circuit_id": cid}
        )
        resp = await handle_strip_circuit_assign(req)
        assert resp.status == 404

    @pytest.mark.asyncio
    async def test_unknown_circuit_404(self, store: Store) -> None:
        state = RecorderState()
        _seed_strip_plug(store, state, DEV, DEV[:38] + "00", "A")
        req = _make_request(
            None, state, store, match_info={"device_id": DEV}, body={"circuit_id": 999}
        )
        resp = await handle_strip_circuit_assign(req)
        assert resp.status == 404

    @pytest.mark.asyncio
    async def test_state_updated_before_rebuild(self, store: Store, monkeypatch) -> None:
        # If the rollup rebuild raises, in-memory state must already reflect
        # the committed DB membership (no stale /api/circuits until next sync).
        state = RecorderState()
        cid = store.create_circuit("P1", "B20", "", 20.0)
        state.circuits[cid] = store.get_circuit(cid)
        _seed_strip_plug(store, state, DEV, DEV[:38] + "00", "A")

        def boom() -> int:
            raise RuntimeError("rebuild failed")

        monkeypatch.setattr(store, "rebuild_hourly_circuit_peak", boom)
        req = _make_request(
            None, state, store, match_info={"device_id": DEV}, body={"circuit_id": cid}
        )
        with pytest.raises(RuntimeError):
            await handle_strip_circuit_assign(req)
        # DB committed and state synced despite the rebuild failure.
        assert store.get_circuit_devices() == {DEV: cid}
        assert state.circuit_devices == {DEV: cid}

    @pytest.mark.asyncio
    async def test_requires_capability(self, store: Store) -> None:
        state = RecorderState()
        cid = store.create_circuit("P1", "B20", "", 20.0)
        _seed_strip_plug(store, state, DEV, DEV[:38] + "00", "A")
        req = _make_authed_request(
            None,
            state,
            store,
            match_info={"device_id": DEV},
            body={"circuit_id": cid},
            oauth_configured=True,
        )
        resp = await handle_strip_circuit_assign(req)
        assert resp.status == 403


class TestCircuitPeaksAPI:
    @pytest.mark.asyncio
    async def test_shape_and_values(self, store: Store) -> None:
        state = RecorderState()
        cid = store.create_circuit("P1", "B20", "coin-op", 20.0)
        state.circuits[cid] = store.get_circuit(cid)
        # Two strips on the circuit, each one plug.
        p1 = _seed_strip_plug(store, state, "dev-a", "dac00", "A", watts=120.0)
        p2 = _seed_strip_plug(store, state, "dev-b", "dbc00", "B", watts=80.0)
        store.set_device_circuit("dev-a", cid)
        store.set_device_circuit("dev-b", cid)
        state.circuit_devices["dev-a"] = cid
        state.circuit_devices["dev-b"] = cid
        _seed_robust_strip_hour(store, p1, p2)  # both plugs, dense hour
        store.rebuild_hourly_circuit_peak()

        req = _make_authed_request(None, state, store)
        req.query = {
            "start": _ROBUST_H.isoformat(),
            "end": (_ROBUST_H + timedelta(hours=1)).isoformat(),
        }
        body = await _json(await handle_circuit_peaks(req))
        assert len(body["circuits"]) == 1
        c = body["circuits"][0]
        assert c["circuit_id"] == cid
        assert c["current_watts"] == pytest.approx(200.0)
        # Both plugs share one circuit; their per-ts sums give actual 500.
        assert c["peak_watts_actual"] == pytest.approx(500.0, abs=2.0)
        assert c["peak_watts_theoretical"] == pytest.approx(650.0, abs=2.0)
        # 20A × 120V = 2400W capacity; 500/2400 ≈ 20.8%.
        assert c["capacity_watts"] == pytest.approx(2400.0)
        assert c["pct_of_capacity"] == pytest.approx(20.8, abs=0.3)

    @pytest.mark.asyncio
    async def test_null_amps_null_capacity(self, store: Store) -> None:
        state = RecorderState()
        cid = store.create_circuit("P1", "B20", "", None)
        state.circuits[cid] = store.get_circuit(cid)

        req = _make_authed_request(None, state, store)
        req.query = {}
        body = await _json(await handle_circuit_peaks(req))
        c = body["circuits"][0]
        assert c["capacity_watts"] is None
        assert c["pct_of_capacity"] is None

    @pytest.mark.asyncio
    async def test_sorted_by_panel_breaker(self, store: Store) -> None:
        state = RecorderState()
        for panel, breaker in (("P2", "B1"), ("P1", "B20"), ("P1", "B2")):
            cid = store.create_circuit(panel, breaker, "", 20.0)
            state.circuits[cid] = store.get_circuit(cid)

        req = _make_authed_request(None, state, store)
        req.query = {}
        body = await _json(await handle_circuit_peaks(req))
        got = [(c["panel"], c["breaker"]) for c in body["circuits"]]
        assert got == [("P1", "B2"), ("P1", "B20"), ("P2", "B1")]


class TestCircuitUsageAPI:
    @pytest.mark.asyncio
    async def test_shape_and_totals(self, store: Store) -> None:
        state = RecorderState()
        cid = store.create_circuit("P1", "B20", "", 20.0)
        state.circuits[cid] = store.get_circuit(cid)
        p1 = _seed_strip_plug(store, state, "dev-a", "dac00", "A")
        store.set_device_circuit("dev-a", cid)
        state.circuit_devices["dev-a"] = cid

        h = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        for sec in (0, 30):
            store.insert_readings([(h.replace(second=sec), p1, 200.0, 120.0, 1.7, 0.0)])
        store.refresh_hourly_usage()

        req = _make_authed_request(None, state, store, match_info={"id": str(cid)})
        req.query = {"start": h.isoformat(), "end": (h + timedelta(hours=1)).isoformat()}
        resp = await handle_circuit_usage(req)
        assert resp.status == 200
        body = await _json(resp)
        assert body["circuit_id"] == cid
        assert len(body["hours"]) == 1
        assert body["total_kwh"] == pytest.approx(sum(body["hourly_kwh"]))

    @pytest.mark.asyncio
    async def test_unknown_circuit_404(self, store: Store) -> None:
        state = RecorderState()
        req = _make_authed_request(None, state, store, match_info={"id": "999"})
        req.query = {}
        resp = await handle_circuit_usage(req)
        assert resp.status == 404


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

        _seed_robust_strip_hour(store, p1, p2)

        req = _make_authed_request(None, state, store, match_info={"device_id": DEV})
        req.query = {
            "start": _ROBUST_H.isoformat(),
            "end": (_ROBUST_H + timedelta(hours=1)).isoformat(),
        }
        body = await _json(await handle_strip_usage(req))
        # Robust (p99): actual simultaneous draw 500, theoretical 250+400=650.
        assert body["peak_watts_actual"] == pytest.approx(500.0, abs=2.0)
        assert body["peak_watts_theoretical"] == pytest.approx(650.0, abs=2.0)

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
    async def test_outlet_carries_lock_mode(self, store: Store) -> None:
        # The per-outlet power button needs the assigned machine's lock_mode to
        # render a disabled "Locked"; unassigned/unlocked outlets carry None.
        state = RecorderState()
        state.strip_aliases[DEV] = "Kasa Strip"
        locked = _seed_machine(
            store, state, (DEV, DEV + "00", "Blackout - M0013"), "M0013", "Blackout", 1980
        )
        bare = _seed_strip_plug(store, state, DEV, DEV + "01", "Unused 1")
        state.lock_modes["M0013"] = "on"

        req = _make_authed_request(None, state, store, match_info={"device_id": DEV})
        body = await _json(await handle_strip_detail(req))
        by_id = {o["plug_id"]: o for o in body["outlets"]}
        assert by_id[locked]["lock_mode"] == "on"
        assert by_id[bare]["lock_mode"] is None

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
    async def test_is_on_reflects_relay_not_draw(self, store: Store) -> None:
        # An energized outlet drawing ~0W reports is_on=True (relay), so the strip
        # page agrees with the dashboard and an all-off sweep — not watts-based.
        state = RecorderState()
        state.strip_aliases[DEV] = "Kasa Strip"
        pid = _seed_strip_plug(store, state, DEV, DEV + "00", "Sign")
        state.plug_readings[pid] = _reading(is_on=True, watts=0.0)

        req = _make_authed_request(None, state, store, match_info={"device_id": DEV})
        body = await _json(await handle_strip_detail(req))
        outlet = next(o for o in body["outlets"] if o["plug_id"] == pid)
        assert outlet["is_on"] is True
        assert outlet["power_status"] == "no_draw"

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


class TestHandleMachinesStripOrder:
    @pytest.mark.asyncio
    async def test_positioned_strips_lead_in_order(self, store: Store) -> None:
        state = RecorderState()
        # Three strips; device_id alphabetical order is A, B, C.
        _seed_machine(store, state, ("devA", "devA00", "A1"), "MA", "A1", 1980)
        _seed_machine(store, state, ("devB", "devB00", "B1"), "MB", "B1", 1980)
        _seed_machine(store, state, ("devC", "devC00", "C1"), "MC", "C1", 1980)
        # Operator order: C, A (B left unpositioned → sorts last).
        state.strip_orders = {"devC": 0, "devA": 1}

        req = _make_request(None, state, store)
        body = await _json(await handle_machines(req))
        assert [m["asset_id"] for m in body["machines"]] == ["MC", "MA", "MB"]

    @pytest.mark.asyncio
    async def test_unpositioned_strips_sort_by_display_name(self, store: Store) -> None:
        state = RecorderState()
        # No positions set; order falls back to display name (not device_id).
        _seed_machine(store, state, ("devX", "devX00", "M1"), "M1", "M1", 1980)
        _seed_machine(store, state, ("devY", "devY00", "M2"), "M2", "M2", 1980)
        state.strip_names["devX"] = "Zebra"
        state.strip_names["devY"] = "Alpha"

        req = _make_request(None, state, store)
        body = await _json(await handle_machines(req))
        # Alpha (devY) before Zebra (devX), despite devX < devY lexically.
        assert [m["asset_id"] for m in body["machines"]] == ["M2", "M1"]


class TestUncalibratedRendersAttract:
    """An uncalibrated machine that's drawing must read ATTRACT (blue/on), not a
    null/gray state — it just can't distinguish attract from playing."""

    @pytest.mark.asyncio
    async def test_handle_machines_uncalibrated_drawing_is_attract(self, store: Store) -> None:
        state = RecorderState()
        pid = _seed_machine(
            store,
            state,
            (DEV, DEV + "00", "Lightning - M0019"),
            "M0019",
            "Lightning",
            1980,
            watts=3.5,
            relay_on=True,
        )
        state.watt_buffers[pid] = deque([3.5] * 40, maxlen=64)  # drawing, no calibration
        body = await _json(await handle_machines(_make_request(None, state, store)))
        m = body["machines"][0]
        assert m["state"] == "ATTRACT"  # not None/gray
        assert m["calibrated"] is False  # still uncalibrated
        assert set(m["sparkline_states"]) == {"ATTRACT"}

    @pytest.mark.asyncio
    async def test_readings_snapshot_uncalibrated_drawing_is_attract(self, store: Store) -> None:
        state = RecorderState()
        pid = _seed_machine(
            store,
            state,
            (DEV, DEV + "00", "Lightning - M0019"),
            "M0019",
            "Lightning",
            1980,
            watts=3.5,
            relay_on=True,
        )
        state.watt_buffers[pid] = deque([3.5] * 40, maxlen=64)
        assert _readings_snapshot(state)[0]["state"] == "ATTRACT"

    @pytest.mark.asyncio
    async def test_within_strip_outlet_order_preserved(self, store: Store) -> None:
        state = RecorderState()
        _seed_machine(store, state, (DEV, DEV + "04", "B - M2"), "M2", "B", 1985)
        _seed_machine(store, state, (DEV, DEV + "01", "A - M1"), "M1", "A", 1980)
        state.strip_orders = {DEV: 0}

        req = _make_request(None, state, store)
        body = await _json(await handle_machines(req))
        assert [m["asset_id"] for m in body["machines"]] == ["M1", "M2"]


class TestStripOrderAPI:
    @staticmethod
    def _known(state: RecorderState, *device_ids: str) -> None:
        for d in device_ids:
            state.strip_aliases[d] = d

    @pytest.mark.asyncio
    async def test_sets_store_and_state(self, store: Store) -> None:
        state = RecorderState()
        self._known(state, "devA", "devB", "devC")
        req = _make_request(None, state, store, body={"device_ids": ["devC", "devA", "devB"]})
        resp = await handle_strip_order(req)
        assert resp.status == 200
        body = await _json(resp)
        assert body == {"ok": True, "count": 3}
        assert store.get_strip_orders() == {"devC": 0, "devA": 1, "devB": 2}
        assert state.strip_orders == {"devC": 0, "devA": 1, "devB": 2}

    @pytest.mark.asyncio
    async def test_rejects_unknown_device(self, store: Store) -> None:
        state = RecorderState()
        self._known(state, "devA")
        req = _make_request(None, state, store, body={"device_ids": ["devA", "bogus"]})
        resp = await handle_strip_order(req)
        assert resp.status == 400
        assert store.get_strip_orders() == {}

    @pytest.mark.asyncio
    async def test_deduplicates(self, store: Store) -> None:
        state = RecorderState()
        self._known(state, "devA", "devB")
        req = _make_request(None, state, store, body={"device_ids": ["devA", "devB", "devA"]})
        resp = await handle_strip_order(req)
        assert resp.status == 200
        body = await _json(resp)
        assert body["count"] == 2
        assert state.strip_orders == {"devA": 0, "devB": 1}

    @pytest.mark.asyncio
    async def test_validation(self, store: Store) -> None:
        state = RecorderState()
        for bad in (
            ["not", "a", "dict"],  # non-object body
            {"device_ids": "nope"},  # not a list
            {"device_ids": [1, 2]},  # not strings
            {},  # missing key
        ):
            req = _make_request(None, state, store, body=bad)
            resp = await handle_strip_order(req)
            assert resp.status == 400, f"expected 400 for {bad!r}"

    @pytest.mark.asyncio
    async def test_requires_capability(self, store: Store) -> None:
        state = RecorderState()
        req = _make_authed_request(
            None, state, store, body={"device_ids": ["devA"]}, oauth_configured=True
        )
        resp = await handle_strip_order(req)
        assert resp.status == 403

    @pytest.mark.asyncio
    async def test_publishes_event(self, store: Store) -> None:
        state = RecorderState()
        self._known(state, "devA")
        q = asyncio.Queue(maxsize=8)
        state.event_subscribers.add(q)
        req = _make_request(
            None, state, store, body={"device_ids": ["devA"]}, user={"email": "w@theflip.museum"}
        )
        await handle_strip_order(req)
        ev = q.get_nowait()
        assert ev["type"] == "strip_order_change"
        assert ev["actor"] == "w@theflip.museum"


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
    async def test_lock_running_machine_pins_on(self, store: Store) -> None:
        state = RecorderState()
        plug_id = _seed_machine(
            store, state, ("hs", "c01", "Blackout - M0013"), "M0013", "Blackout", 1980, watts=200
        )

        req = _make_request(
            None, state, store, match_info={"plug_id": str(plug_id)}, body={"locked": True}
        )
        resp = await handle_lock(req)
        assert resp.status == 200
        body = await _json(resp)
        assert body == {"ok": True, "locked": True, "mode": "on"}
        assert state.lock_modes == {"M0013": "on"}
        assert store.get_lock_modes() == {"M0013": "on"}

    @pytest.mark.asyncio
    async def test_lock_powered_off_machine_pins_off(self, store: Store) -> None:
        state = RecorderState()
        # No live reading / zero watts → the machine reads OFF, so it locks off.
        plug_id = _seed_machine(
            store, state, ("hs", "c01", "Blackout - M0013"), "M0013", "Blackout", 1980, watts=0
        )

        req = _make_request(
            None, state, store, match_info={"plug_id": str(plug_id)}, body={"locked": True}
        )
        resp = await handle_lock(req)
        assert resp.status == 200
        body = await _json(resp)
        assert body == {"ok": True, "locked": True, "mode": "off"}
        assert state.lock_modes == {"M0013": "off"}
        assert store.get_lock_modes() == {"M0013": "off"}

    @pytest.mark.asyncio
    async def test_unlock_roundtrip(self, store: Store) -> None:
        state = RecorderState()
        plug_id = _seed_machine(
            store, state, ("hs", "c01", "Blackout - M0013"), "M0013", "Blackout", 1980, watts=200
        )

        for locked in (True, False):
            req = _make_request(
                None, state, store, match_info={"plug_id": str(plug_id)}, body={"locked": locked}
            )
            resp = await handle_lock(req)
            assert resp.status == 200
        assert state.lock_modes == {}
        assert store.get_lock_modes() == {}

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
        assert state.lock_modes == {}

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
        assert state.lock_modes == {}
        assert store.get_lock_modes() == {}

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
            "mode": "off",
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
    relay_on: bool | None = None,
) -> int:
    """Insert a plug + machine + assignment and register them in RecorderState.

    `relay_on` overrides the outlet relay flag independently of `watts` — pass
    `relay_on=True, watts=0` to simulate an energized outlet whose machine draws
    nothing. Defaults to `watts > 0` when not given.
    """
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
            is_on=(watts > 0) if relay_on is None else relay_on,
            watts=watts if has_emeter else None,
            voltage=120.0 if has_emeter else None,
            amps=watts / 120.0 if has_emeter else None,
            total_kwh=0.0 if has_emeter else None,
        )
    return plug_id


def _reading(*, is_on: bool, watts: float | None) -> PlugReading:
    return PlugReading(
        child_id="c01", alias="x", is_on=is_on, watts=watts, voltage=120.0, amps=0.0, total_kwh=0.0
    )


class TestPowerStatus:
    def test_emeter_relay_on_no_draw(self) -> None:
        # The headline case: outlet energized, machine drawing ~nothing.
        assert _power_status(_reading(is_on=True, watts=0.0), True, False) == "no_draw"
        assert _power_status(_reading(is_on=True, watts=1.9), True, False) == "no_draw"

    def test_emeter_relay_on_drawing(self) -> None:
        assert _power_status(_reading(is_on=True, watts=2.1), True, False) == "on"
        assert _power_status(_reading(is_on=True, watts=200.0), True, False) == "on"

    def test_lightning_low_power_draw_is_on(self) -> None:
        # Lightning draws a steady ~3.5W in attract; it must display as on, not
        # no_draw. Pinned to the real value so a threshold bump can't silently re-break it.
        assert _power_status(_reading(is_on=True, watts=3.5), True, False) == "on"

    def test_relay_off_is_off(self) -> None:
        assert _power_status(_reading(is_on=False, watts=0.0), True, False) == "off"

    def test_offline_overrides(self) -> None:
        assert _power_status(_reading(is_on=True, watts=200.0), True, True) == "offline"

    def test_no_reading_is_off(self) -> None:
        assert _power_status(None, True, False) == "off"

    def test_no_emeter_uses_relay(self) -> None:
        # No meter -> can't be 'no_draw'; relay alone decides on/off.
        assert _power_status(_reading(is_on=True, watts=None), False, False) == "on"
        assert _power_status(_reading(is_on=False, watts=None), False, False) == "off"

    def test_emeter_missing_watts_is_not_no_draw(self) -> None:
        # An emeter plug whose reading lacks a watt value -> unknown draw, not a
        # false 'no_draw' (which would claim the machine is off/unplugged).
        assert _power_status(_reading(is_on=True, watts=None), True, False) == "on"

    def test_relay_on_helper(self, store: Store) -> None:
        # _relay_on reflects the relay even when watts is 0 (the no-draw case) —
        # control keys on the relay, not measured draw.
        state = RecorderState()
        pid = _seed_machine(
            store, state, ("hs", "c01", "X - M1"), "M1", "X", 1980, watts=0.0, relay_on=True
        )
        assert _relay_on(state, pid) is True

    @pytest.mark.asyncio
    async def test_machines_api_reports_no_draw(self, store: Store) -> None:
        state = RecorderState()
        _seed_machine(
            store, state, ("hs", "c01", "TW - M1"), "M1", "TW", 1980, watts=0.0, relay_on=True
        )
        req = _make_request(None, state, store)
        resp = await handle_machines(req)
        body = await _json(resp)
        assert body["machines"][0]["power_status"] == "no_draw"

    @pytest.mark.asyncio
    async def test_lock_no_draw_pins_on(self, store: Store) -> None:
        # Locking an energized-but-idle outlet pins 'on' (the relay is on).
        state = RecorderState()
        plug_id = _seed_machine(
            store, state, ("hs", "c01", "TW - M0003"), "M0003", "TW", 1980, watts=0.0, relay_on=True
        )
        req = _make_request(
            None, state, store, match_info={"plug_id": str(plug_id)}, body={"locked": True}
        )
        resp = await handle_lock(req)
        body = await _json(resp)
        assert body["mode"] == "on"
        assert state.lock_modes["M0003"] == "on"


class TestHandleBusyGrid:
    def _cell(self, store: Store, mid: int, ts: datetime, play: float, on: float) -> None:
        store._conn.execute(
            "INSERT INTO hourly_play_seconds VALUES (?, ?, ?, ?)", [mid, ts, play, on]
        )

    @pytest.mark.asyncio
    async def test_grid_shape_and_ratio(self, store: Store) -> None:
        mid = store.ensure_machine("M1", "A")
        h = 3600.0  # all cells clear the 10-machine-hour gate
        self._cell(store, mid, datetime(2026, 6, 15, 14, 0, 0), 5 * h, 10 * h)  # 0.5
        self._cell(store, mid, datetime(2026, 6, 15, 15, 0, 0), 3 * h, 12 * h)  # 0.25
        self._cell(store, mid, datetime(2026, 6, 16, 14, 0, 0), 6 * h, 12 * h)  # 0.5
        self._cell(
            store, mid, datetime(2026, 6, 16, 20, 0, 0), 5 * h, 5 * h
        )  # below gate -> dropped

        req = _make_request(
            None, RecorderState(), store, query={"start": "2026-06-15", "end": "2026-06-17"}
        )
        body = await _json(await handle_busy_grid(req))

        assert body["dates"] == ["2026-06-15", "2026-06-16"]
        assert body["hours"] == [14, 15]  # 20h cell filtered out
        assert body["max_ratio"] == 0.5
        by = {(c["date"], c["hour"]): c for c in body["cells"]}
        assert by[("2026-06-15", 14)]["ratio"] == 0.5
        assert by[("2026-06-15", 15)]["ratio"] == 0.25
        assert by[("2026-06-15", 14)]["play_hours"] == 5.0
        assert by[("2026-06-15", 14)]["on_hours"] == 10.0

    @pytest.mark.asyncio
    async def test_empty_window(self, store: Store) -> None:
        req = _make_request(
            None, RecorderState(), store, query={"start": "2026-01-01", "end": "2026-01-02"}
        )
        body = await _json(await handle_busy_grid(req))
        assert body["dates"] == [] and body["hours"] == [] and body["cells"] == []
        assert body["max_ratio"] == 0.0


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

    def test_restrict_to_limits_the_machine_sweep(self, store: Store) -> None:
        # Strip scope: only machines whose plug is in restrict_to are swept.
        state = RecorderState()
        a = _seed_machine(store, state, ("devA", "c01", "A"), "M1", "A", 1980, watts=0)
        b = _seed_machine(store, state, ("devB", "c01", "B"), "M2", "B", 1985, watts=0)
        assert _build_targets(state, "all_on", restrict_to={a}) == [a]
        assert b not in _build_targets(state, "all_on", restrict_to={a})
        # None restriction is unchanged — both machines included.
        assert set(_build_targets(state, "all_on")) == {a, b}

    def test_skips_already_off_when_turning_off(self, store: Store) -> None:
        state = RecorderState()
        on_pid = _seed_machine(store, state, ("hs", "c01", "On"), "M1", "On", 1980, watts=200)
        _ = _seed_machine(store, state, ("hs", "c02", "Off"), "M2", "Off", 1985, watts=0)
        targets = _build_targets(state, "all_off")
        assert targets == [on_pid]

    def test_relay_on_no_draw_included_in_all_off(self, store: Store) -> None:
        # The bug: an energized outlet drawing ~nothing (relay on, 0 W — machine
        # off/unplugged) must still be turned off by all-off. Keys on the relay,
        # not measured watts.
        state = RecorderState()
        no_draw = _seed_machine(
            store, state, ("hs", "c01", "ND"), "M1", "ND", 1980, watts=0, relay_on=True
        )
        assert no_draw in _build_targets(state, "all_off")

    def test_relay_on_no_draw_skipped_in_all_on(self, store: Store) -> None:
        # Symmetric side: the relay is already on, so all-on skips it rather than
        # sending a redundant no-op turn_on.
        state = RecorderState()
        no_draw = _seed_machine(
            store, state, ("hs", "c01", "ND"), "M1", "ND", 1980, watts=0, relay_on=True
        )
        assert no_draw not in _build_targets(state, "all_on")

    def test_skips_locked_on_when_turning_off(self, store: Store) -> None:
        state = RecorderState()
        locked = _seed_machine(store, state, ("hs", "c01", "Lck"), "M1", "Lck", 1980, watts=200)
        free = _seed_machine(store, state, ("hs", "c02", "Free"), "M2", "Free", 1985, watts=200)
        state.lock_modes["M1"] = "on"
        targets = _build_targets(state, "all_off")
        assert targets == [free]
        assert locked not in targets

    def test_locked_on_included_in_all_on(self, store: Store) -> None:
        state = RecorderState()
        locked = _seed_machine(store, state, ("hs", "c01", "Lck"), "M1", "Lck", 1980, watts=0)
        state.lock_modes["M1"] = "on"
        targets = _build_targets(state, "all_on")
        assert targets == [locked]

    def test_skips_locked_off_when_turning_on(self, store: Store) -> None:
        state = RecorderState()
        locked = _seed_machine(store, state, ("hs", "c01", "Lck"), "M1", "Lck", 1980, watts=0)
        free = _seed_machine(store, state, ("hs", "c02", "Free"), "M2", "Free", 1985, watts=0)
        state.lock_modes["M1"] = "off"
        targets = _build_targets(state, "all_on")
        assert targets == [free]
        assert locked not in targets

    def test_locked_off_included_in_all_off(self, store: Store) -> None:
        state = RecorderState()
        locked = _seed_machine(store, state, ("hs", "c01", "Lck"), "M1", "Lck", 1980, watts=200)
        state.lock_modes["M1"] = "off"
        targets = _build_targets(state, "all_off")
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

    def test_no_emeter_plug_uses_relay(self, store: Store) -> None:
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
        # No-emeter plugs have watts=None, so on/off keys on the relay flag
        # (reading.is_on). Set the readings directly to make intent clear.
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

    def test_outlet_relay_on_zero_draw_included_in_all_off(self, store: Store) -> None:
        # An unassigned outlet that's energized but reads 0 W must still be swept
        # by all-off. (_register_outlet ties watts to is_on, so set the 0 W relay-on
        # reading directly to exercise the exact no-draw bug.)
        state = RecorderState()
        outlet = _register_outlet(state=state, store=store, seed=("hs", "c06", "Sign"))
        state.plug_readings[outlet] = _reading(is_on=True, watts=0.0)
        assert _build_targets(state, "all_off", [outlet]) == [outlet]

    def test_outlet_no_playing_check(self, store: Store) -> None:
        # An on outlet has no calibration/buffer, so it's swept on all_off
        # (no PLAYING gate the way machines have).
        state = RecorderState()
        outlet = _register_outlet(state=state, store=store, seed=("hs", "c06", "Sign"), is_on=True)
        assert _build_targets(state, "all_off", [outlet]) == [outlet]


class TestPartitionInstant:
    def test_machine_is_staggered(self, store: Store) -> None:
        state = RecorderState()
        m = _seed_machine(store, state, ("hs", "c01", "A"), "M1", "A", 1980, watts=0)
        instant, staggered = _partition_instant(state, [m])
        assert instant == []
        assert staggered == [m]

    def test_drawing_machine_is_staggered(self, store: Store) -> None:
        state = RecorderState()
        m = _seed_machine(store, state, ("hs", "c01", "A"), "M1", "A", 1980, watts=200)
        instant, staggered = _partition_instant(state, [m])
        assert staggered == [m]

    def test_empty_outlet_is_instant(self, store: Store) -> None:
        state = RecorderState()
        outlet = _register_outlet(state=state, store=store, seed=("hs", "c06", "Sign"), is_on=False)
        instant, staggered = _partition_instant(state, [outlet])
        assert instant == [outlet]
        assert staggered == []

    def test_relay_on_no_draw_outlet_is_instant(self, store: Store) -> None:
        # Energized but pulling nothing (relay on, 0 W): still no load, so instant.
        state = RecorderState()
        outlet = _register_outlet(state=state, store=store, seed=("hs", "c06", "Sign"))
        state.plug_readings[outlet] = _reading(is_on=True, watts=0.0)
        instant, _ = _partition_instant(state, [outlet])
        assert instant == [outlet]

    def test_no_reading_outlet_is_instant(self, store: Store) -> None:
        state = RecorderState()
        outlet = _register_outlet(state=state, store=store, seed=("hs", "c06", "Sign"))
        instant, _ = _partition_instant(state, [outlet])
        assert instant == [outlet]

    def test_drawing_outlet_is_staggered(self, store: Store) -> None:
        # An unassigned outlet pulling a real load (>= OFF_WATTS) keeps the stagger.
        state = RecorderState()
        outlet = _register_outlet(state=state, store=store, seed=("hs", "c06", "Fridge"))
        state.plug_readings[outlet] = _reading(is_on=True, watts=120.0)
        instant, staggered = _partition_instant(state, [outlet])
        assert instant == []
        assert staggered == [outlet]

    def test_preserves_order_within_groups(self, store: Store) -> None:
        state = RecorderState()
        m = _seed_machine(store, state, ("hs", "c01", "A"), "M1", "A", 1980, watts=0)
        o1 = _register_outlet(state=state, store=store, seed=("hs", "c06", "S1"), is_on=False)
        o2 = _register_outlet(state=state, store=store, seed=("hs", "c07", "S2"), is_on=False)
        # _build_targets order is machines-then-outlets; partition pulls outlets out.
        instant, staggered = _partition_instant(state, [m, o1, o2])
        assert instant == [o1, o2]
        assert staggered == [m]


class TestOperationToDict:
    def test_carries_label_for_strip_scoped_and_none_for_global(self) -> None:
        ts = datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC)
        scoped = Operation(
            id="o1",
            kind="all_off",
            started_at=ts,
            started_by="w",
            targets=[1, 2],
            label="Backline strip",
        )
        assert _operation_to_dict(scoped)["label"] == "Backline strip"
        glob = Operation(id="o2", kind="all_on", started_at=ts, started_by="w", targets=[3])
        assert _operation_to_dict(glob)["label"] is None


class TestStripOutletIds:
    def test_returns_only_unassigned_strip_plugs(self, store: Store) -> None:
        state = RecorderState()
        machine = _seed_machine(store, state, (DEV, DEV + "00", "Blk - M1"), "M1", "Blk", 1980)
        sign = _register_outlet(state=state, store=store, seed=(DEV, DEV + "01", "Sign"))
        light = _register_outlet(state=state, store=store, seed=(DEV, DEV + "02", "Light"))
        plug_ids = _strip_plug_ids(state, DEV)
        assert set(_strip_outlet_ids(state, plug_ids)) == {sign, light}
        assert machine not in _strip_outlet_ids(state, plug_ids)


class TestStripOperations:
    def _strip_with_machine_and_outlet(self, store: Store, state: RecorderState):
        state.strip_aliases[DEV] = "Backline"
        state.strip_names[DEV] = "Backline"
        machine = _seed_machine(
            store, state, (DEV, DEV + "00", "Blk - M1"), "M1", "Blk", 1980, watts=0
        )
        outlet = _register_outlet(
            state=state, store=store, seed=(DEV, DEV + "01", "Sign"), is_on=False
        )
        # A machine on a *different* strip must be excluded from the strip-scoped op.
        other = _seed_machine(store, state, ("devX", "c01", "Other"), "M9", "Other", 1990, watts=0)
        return machine, outlet, other

    @pytest.mark.asyncio
    async def test_unknown_device_404(self, store: Store) -> None:
        state = RecorderState()
        req = _make_request(
            None,
            state,
            store,
            match_info={"device_id": "nope"},
            user={"email": "w@theflip.museum"},
        )
        resp = await handle_strip_all_off(req)
        assert resp.status == 404

    @pytest.mark.asyncio
    async def test_requires_capability_403(self, store: Store) -> None:
        state = RecorderState()
        state.strip_aliases[DEV] = "Backline"
        # Authenticated but without the control_power capability → 403.
        req = _make_authed_request(
            None, state, store, match_info={"device_id": DEV}, oauth_configured=True
        )
        resp = await handle_strip_all_on(req)
        assert resp.status == 403

    @pytest.mark.asyncio
    async def test_busy_returns_409(self, store: Store) -> None:
        state = RecorderState()
        state.strip_aliases[DEV] = "Backline"
        state.current_operation = Operation(
            id="other",
            kind="all_off",
            started_at=datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC),
            started_by="x",
            targets=[],
            state="running",
        )
        req = _make_request(
            None,
            state,
            store,
            match_info={"device_id": DEV},
            user={"email": "w@theflip.museum"},
        )
        resp = await handle_strip_all_on(req)
        assert resp.status == 409

    @pytest.mark.asyncio
    async def test_scopes_targets_to_strip_and_sets_label(self, store: Store, monkeypatch) -> None:
        # Monkeypatch run_operation to a no-op so the created op survives for
        # inspection (the real one clears state.current_operation on completion).
        import juice.server as srv

        async def _noop(*a, **k):
            return None

        monkeypatch.setattr(srv, "run_operation", _noop)
        state = RecorderState()
        machine, outlet, other = self._strip_with_machine_and_outlet(store, state)
        req = _make_request(
            None,
            state,
            store,
            match_info={"device_id": DEV},
            user={"email": "w@theflip.museum"},
        )
        resp = await handle_strip_all_on(req)
        assert resp.status == 200
        op = state.current_operation
        assert op is not None
        assert set(op.targets) <= {machine, outlet}  # strip-scoped
        assert other not in op.targets
        assert op.label == "Backline strip"


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

    @pytest.mark.asyncio
    async def test_instant_batch_switches_without_stagger(self, store: Store, monkeypatch) -> None:
        # Load-free outlets in the instant slice flip with NO inter-step sleep,
        # while still emitting step + power_change events and forcing a re-poll.
        sleeps: list[float] = []
        real_sleep = asyncio.sleep

        async def _track(delay: float) -> None:
            sleeps.append(delay)
            await real_sleep(0)

        monkeypatch.setattr("juice.server.asyncio.sleep", _track)

        state = RecorderState()
        o1 = _register_outlet(state=state, store=store, seed=("hs", "c06", "S1"), is_on=False)
        o2 = _register_outlet(state=state, store=store, seed=("hs", "c07", "S2"), is_on=False)
        fake1 = _FakePlug(alias="S1")
        fake2 = _FakePlug(alias="S2")
        state.plug_objects[o1] = fake1
        state.plug_objects[o2] = fake2

        q: asyncio.Queue = asyncio.Queue(maxsize=64)
        state.event_subscribers.add(q)

        op = Operation(
            id="op-instant",
            kind="all_on",
            started_at=datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC),
            started_by="w",
            targets=[o1, o2],
        )
        state.current_operation = op
        await run_operation(state, store, op, on=True, sleep=2.0, instant_count=2)

        assert sleeps == []  # no stagger between load-free outlets
        fake1.turn_on.assert_awaited_once()
        fake2.turn_on.assert_awaited_once()
        assert op.state == "complete"
        assert set(op.completed) == {o1, o2}
        assert {o1, o2} <= state.watch_until.keys()  # each turned-on plug is watched

        events = []
        while not q.empty():
            events.append(q.get_nowait())
        types = [e["type"] for e in events]
        assert types.count("operation_step") == 2
        assert types.count("power_change") == 2

    @pytest.mark.asyncio
    async def test_instant_then_staggered_only_staggers_loads(
        self, store: Store, monkeypatch
    ) -> None:
        # One load-free outlet (instant) + two machines (staggered): the stagger
        # sleep happens only between the staggered loads, never for the outlet.
        sleeps: list[float] = []
        real_sleep = asyncio.sleep

        async def _track(delay: float) -> None:
            sleeps.append(delay)
            await real_sleep(0)

        monkeypatch.setattr("juice.server.asyncio.sleep", _track)

        state = RecorderState()
        outlet = _register_outlet(state=state, store=store, seed=("hs", "c06", "Sign"), is_on=False)
        m1 = _seed_machine(store, state, ("hs", "c01", "A"), "M1", "A", 1980, watts=0)
        m2 = _seed_machine(store, state, ("hs", "c02", "B"), "M2", "B", 1990, watts=0)
        for pid in (outlet, m1, m2):
            state.plug_objects[pid] = _FakePlug()

        op = Operation(
            id="op-mixed",
            kind="all_on",
            started_at=datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC),
            started_by="w",
            targets=[outlet, m1, m2],
        )
        state.current_operation = op
        await run_operation(state, store, op, on=True, sleep=2.0, instant_count=1)

        # total=3: stagger fires once (between the two machines), never for the outlet.
        assert sleeps == [2.0]
        assert op.state == "complete"
        assert set(op.completed) == {outlet, m1, m2}

    @pytest.mark.asyncio
    async def test_all_off_instant_batch_no_stagger(self, store: Store, monkeypatch) -> None:
        # all_off path (sleep=1.0): a relay-on no-draw outlet is load-free, so it
        # flips in the instant batch with no stagger sleep.
        sleeps: list[float] = []
        real_sleep = asyncio.sleep

        async def _track(delay: float) -> None:
            sleeps.append(delay)
            await real_sleep(0)

        monkeypatch.setattr("juice.server.asyncio.sleep", _track)

        state = RecorderState()
        outlet = _register_outlet(state=state, store=store, seed=("hs", "c06", "Sign"))
        state.plug_readings[outlet] = _reading(is_on=True, watts=0.0)  # energized, no draw
        state.plug_objects[outlet] = _FakePlug(alias="Sign")

        op = Operation(
            id="op-off-instant",
            kind="all_off",
            started_at=datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC),
            started_by="w",
            targets=[outlet],
        )
        state.current_operation = op
        await run_operation(state, store, op, on=False, sleep=1.0, instant_count=1)

        assert sleeps == []
        assert op.completed == [outlet]
        assert outlet not in state.watch_until  # watch window only on turn-on

    @pytest.mark.asyncio
    async def test_instant_outlet_missing_plug_object_completes(self, store: Store) -> None:
        # An offline instant outlet (no plug object) is recorded as failed but the
        # op still completes and clears current_operation.
        state = RecorderState()
        outlet = _register_outlet(state=state, store=store, seed=("hs", "c06", "Sign"), is_on=False)
        # No state.plug_objects[outlet] registered.

        op = Operation(
            id="op-instant-missing",
            kind="all_on",
            started_at=datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC),
            started_by="w",
            targets=[outlet],
        )
        state.current_operation = op
        await run_operation(state, store, op, on=True, sleep=0.0, instant_count=1)

        assert op.state == "complete"
        assert [pid for pid, _ in op.failed] == [outlet]
        assert state.current_operation is None
        rows = store.recent_power_events(limit=10)
        assert rows[0]["result"] == "error"

    @pytest.mark.asyncio
    async def test_unexpected_error_in_instant_batch_clears_operation(
        self, store: Store, monkeypatch
    ) -> None:
        # If a step raises an *unexpected* error inside the concurrent burst, the
        # op must still close out — otherwise current_operation 409-locks forever.
        state = RecorderState()
        outlet = _register_outlet(state=state, store=store, seed=("hs", "c06", "Sign"), is_on=False)
        state.plug_objects[outlet] = _FakePlug(alias="Sign")

        def _boom(*a, **k):
            raise RuntimeError("db exploded")

        monkeypatch.setattr(store, "record_power_event", _boom)

        op = Operation(
            id="op-boom",
            kind="all_on",
            started_at=datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC),
            started_by="w",
            targets=[outlet],
        )
        state.current_operation = op
        await run_operation(state, store, op, on=True, sleep=0.0, instant_count=1)

        # No exception propagated; op is closed and the slot is freed.
        assert op.state == "complete"
        assert state.current_operation is None

    @pytest.mark.asyncio
    async def test_pre_cancelled_instant_only_op_marked_cancelled(self, store: Store) -> None:
        # Cancel set before any step runs: an instant-only op must end "cancelled",
        # not fall through the empty staggered loop to a bogus "complete".
        state = RecorderState()
        outlet = _register_outlet(state=state, store=store, seed=("hs", "c06", "Sign"), is_on=False)
        fake = _FakePlug(alias="Sign")
        state.plug_objects[outlet] = fake

        op = Operation(
            id="op-precancel",
            kind="all_on",
            started_at=datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC),
            started_by="w",
            targets=[outlet],
        )
        op.cancel_requested = True
        state.current_operation = op
        await run_operation(state, store, op, on=True, sleep=0.0, instant_count=1)

        assert op.state == "cancelled"
        fake.turn_on.assert_not_awaited()  # nothing ran
        assert state.current_operation is None

    @pytest.mark.asyncio
    async def test_staggered_step_error_does_not_strand_remaining(
        self, store: Store, monkeypatch
    ) -> None:
        # An unexpected error in one staggered step must not abort the loop: the
        # later targets still get attempted and the op still closes out.
        state = RecorderState()
        a = _seed_machine(store, state, ("hs", "c01", "A"), "M1", "A", 1980, watts=0)
        b = _seed_machine(store, state, ("hs", "c02", "B"), "M2", "B", 1990, watts=0)
        fake_a = _FakePlug(alias="A")
        fake_b = _FakePlug(alias="B")
        state.plug_objects[a] = fake_a
        state.plug_objects[b] = fake_b

        # record_power_event blows up — an infra error escaping _execute_step.
        def _boom(*args, **kwargs):
            raise RuntimeError("db exploded")

        monkeypatch.setattr(store, "record_power_event", _boom)

        op = Operation(
            id="op-stagger-boom",
            kind="all_on",
            started_at=datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC),
            started_by="w",
            targets=[a, b],
        )
        state.current_operation = op
        await run_operation(state, store, op, on=True, sleep=0.0, instant_count=0)

        # The first step's failure didn't strand the second — both were attempted.
        fake_a.turn_on.assert_awaited_once()
        fake_b.turn_on.assert_awaited_once()
        assert op.state == "complete"
        assert state.current_operation is None


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
    async def test_global_all_on_includes_silent_outlet_first(
        self, store: Store, monkeypatch
    ) -> None:
        # A never-drawn unassigned outlet (no reading) is now swept by a global
        # all-on — and load-free, so it leads the target list (flips instantly).
        import juice.server as srv

        async def _noop(*a, **k):
            return None

        monkeypatch.setattr(srv, "run_operation", _noop)
        state = RecorderState()
        machine = _seed_machine(store, state, ("hs", "c01", "A"), "M1", "A", 1980, watts=0)
        silent = _register_outlet(
            state=state, store=store, seed=("hs", "c06", "Sign")
        )  # no reading

        req = _make_request(None, state, store, body={}, user={"email": "w"})
        resp = await handle_all_on(req)
        assert resp.status == 200
        op = state.current_operation
        assert op is not None
        assert silent in op.targets  # previously excluded from the global sweep
        assert op.targets[0] == silent  # instant (load-free) outlet leads
        assert machine in op.targets

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
    async def test_no_auth_wired_renders_authed_no_auth_corner(self, store: Store) -> None:
        """No auth wired into the app at all (bare handler, as in these tests) →
        operator view, no auth chrome. (The real `juice serve` dev path installs
        the login shim instead — see TestDevAuthShim.)"""
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
            handle_circuit_page,
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
            handle_circuit_page,
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

    def test_dashboard_has_reorder_mode(self) -> None:
        from juice.server import DASHBOARD_HTML

        assert "/api/strip-order" in DASHBOARD_HTML
        assert "Reorder strips" in DASHBOARD_HTML
        assert "startReorder" in DASHBOARD_HTML
        # Trigger link is desktop-only and operator-only.
        assert 'class="reorder-link private-only desktop-only"' in DASHBOARD_HTML


class TestDevAuthShim:
    """With `dev_auth=True` (the CLI's --dev-auth/JUICE_DEV_AUTH opt-in, only
    honoured when OAuth is absent) a one-click login shim makes local dev mirror
    the production logged-out → login → logout flow. It is never on by default."""

    def test_shim_is_opt_in_only(self, store: Store) -> None:
        """Default create_app (no OAuth, no dev_auth) wires no /login route, so a
        deployment with missing OAuth env can't fall into one-click operator."""
        bare = {r.resource.canonical for r in create_app(RecorderState(), store).router.routes()}
        assert "/login" not in bare
        shimmed = {
            r.resource.canonical
            for r in create_app(RecorderState(), store, dev_auth=True).router.routes()
        }
        assert "/login" in shimmed

    @pytest.mark.asyncio
    async def test_login_logout_flow(self, store: Store) -> None:
        from aiohttp.test_utils import TestClient, TestServer

        app = create_app(RecorderState(), store, dev_auth=True)  # no OAuth → dev shim
        async with TestClient(TestServer(app)) as client:
            # Logged out: public view with a Login button, no logout link.
            body = await (await client.get("/")).text()
            assert 'body class="public"' in body
            assert 'class="auth-corner login-btn"' in body
            assert "log out" not in body

            # One-click dev login redirects home and sets a session.
            resp = await client.get("/login", allow_redirects=False)
            assert resp.status == 302
            assert resp.headers["Location"] == "/"

            # Logged in: operator view with a log-out link, no Login button.
            body = await (await client.get("/")).text()
            assert 'body class="authed"' in body
            assert "log out" in body
            assert 'class="auth-corner login-btn"' not in body

            # /api/me reflects the dev operator + control_power capability.
            me = await (await client.get("/api/me")).json()
            assert me["authenticated"] is True
            assert "control_power" in me["capabilities"]

            # Logout clears the session and returns to the public view.
            resp = await client.get("/logout", allow_redirects=False)
            assert resp.status == 302
            body = await (await client.get("/")).text()
            assert 'body class="public"' in body

    @pytest.mark.asyncio
    async def test_writes_gated_until_login(self, store: Store) -> None:
        """A write must 401 when logged out, even though dev keeps reads open."""
        from aiohttp.test_utils import TestClient, TestServer

        app = create_app(RecorderState(), store, dev_auth=True)  # no OAuth → dev shim
        async with TestClient(TestServer(app)) as client:
            resp = await client.post(
                "/api/machines/1/power", json={"on": True}, allow_redirects=False
            )
            assert resp.status == 401

            await client.get("/login")  # mint the dev operator session
            # Now the capability gate passes; failure (if any) is downstream of
            # auth, not a 401.
            resp = await client.post(
                "/api/machines/1/power", json={"on": True}, allow_redirects=False
            )
            assert resp.status != 401


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
        ("POST", "/api/strips/abc/circuit"),
        ("POST", "/api/strip-order"),
        ("GET", "/api/circuits"),
        ("POST", "/api/circuits"),
        ("POST", "/api/circuits/1"),
        ("DELETE", "/api/circuits/1"),
        ("GET", "/api/circuit-peaks"),
        ("GET", "/api/circuits/1/usage"),
        ("GET", "/api/cost"),
        ("GET", "/api/machines/1/cost"),
        # Private pages:
        ("GET", "/events"),
        ("GET", "/strip/abc"),
        ("GET", "/circuit/1"),
    )

    # Public-readable counterparts — anon GETs must succeed without auth.
    # (/api/events — the SSE push stream — is public too, but it's a never-ending
    # stream awkward to drive in this generic loop, so it's asserted separately.)
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

    @pytest.mark.asyncio
    async def test_anonymous_can_open_sse_stream(self, store: Store) -> None:
        # The live push stream is public so kiosk/lobby displays get updates
        # without logging in. Only the headers are checked (the body never ends).
        from aiohttp.test_utils import TestClient, TestServer

        state = RecorderState()
        app = create_app(state, store, oauth_config=self._OAUTH_CONFIG)
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/api/events", allow_redirects=False)
            assert resp.status == 200
            assert resp.headers["Content-Type"].startswith("text/event-stream")
            resp.close()


class TestBackupEndpoint:
    """The backup download uses its own bearer-token auth, independent of
    OAuth, and is only registered when a token is configured."""

    _OAUTH_CONFIG = TestAnonymousAccessGating._OAUTH_CONFIG
    _TOKEN = "s3cret-backup-token-long-enough"  # noqa: S105

    def _seed(self, store: Store) -> None:
        pid = store.ensure_plug("d1", "c01", "Blackout - M0013")
        store.insert_readings(
            [(datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC), pid, 120.0, 120.0, 1.0, 0.0)]
        )

    @pytest.mark.asyncio
    async def test_404_when_token_unset(self, store: Store) -> None:
        from aiohttp.test_utils import TestClient, TestServer

        # Both OAuth-on and OAuth-off: no token → route not registered → 404.
        for oauth in (None, self._OAUTH_CONFIG):
            app = create_app(RecorderState(), store, oauth_config=oauth)
            async with TestClient(TestServer(app)) as client:
                resp = await client.get("/api/backup", allow_redirects=False)
                assert resp.status == 404

    @pytest.mark.asyncio
    async def test_200_with_valid_token_streams_openable_db(self, store: Store, tmp_path) -> None:
        from aiohttp.test_utils import TestClient, TestServer

        self._seed(store)
        app = create_app(
            RecorderState(), store, oauth_config=self._OAUTH_CONFIG, backup_token=self._TOKEN
        )
        async with TestClient(TestServer(app)) as client:
            resp = await client.get(
                "/api/backup", headers={"Authorization": f"Bearer {self._TOKEN}"}
            )
            assert resp.status == 200
            assert resp.headers["Content-Type"] == "application/octet-stream"
            assert "attachment" in resp.headers["Content-Disposition"]
            data = await resp.read()
        out = tmp_path / "pulled.duckdb"
        out.write_bytes(data)
        with Store(str(out)) as snap:
            n = snap._conn.execute("SELECT count(*) FROM readings").fetchone()[0]
            assert n == 1

    @pytest.mark.asyncio
    async def test_snapshot_staged_on_db_filesystem(self, tmp_path, monkeypatch) -> None:
        # The scratch copy must land beside the DB (its volume), not in /tmp —
        # on prod /tmp may be a small tmpfs the full-size copy would overflow.
        from aiohttp.test_utils import TestClient, TestServer

        import juice.server as server

        seen_dirs: list[str | None] = []
        real_mkstemp = server.tempfile.mkstemp

        def spy_mkstemp(*args, **kwargs):
            seen_dirs.append(kwargs.get("dir"))
            return real_mkstemp(*args, **kwargs)

        monkeypatch.setattr(server.tempfile, "mkstemp", spy_mkstemp)

        with Store(str(tmp_path / "prod.duckdb")) as fstore:
            self._seed(fstore)
            app = create_app(RecorderState(), fstore, backup_token=self._TOKEN)
            async with TestClient(TestServer(app)) as client:
                resp = await client.get(
                    "/api/backup", headers={"Authorization": f"Bearer {self._TOKEN}"}
                )
                assert resp.status == 200
                await resp.read()
        assert seen_dirs == [str(tmp_path)]

    @pytest.mark.asyncio
    async def test_401_missing_and_bad_token_not_redirect(self, store: Store) -> None:
        from aiohttp.test_utils import TestClient, TestServer

        app = create_app(
            RecorderState(), store, oauth_config=self._OAUTH_CONFIG, backup_token=self._TOKEN
        )
        async with TestClient(TestServer(app)) as client:
            # No header.
            resp = await client.get("/api/backup", allow_redirects=False)
            assert resp.status == 401
            assert "Location" not in resp.headers  # not an OAuth /login redirect
            # Wrong token.
            resp = await client.get(
                "/api/backup",
                headers={"Authorization": "Bearer wrong"},
                allow_redirects=False,
            )
            assert resp.status == 401

    @pytest.mark.asyncio
    async def test_dev_mode_no_oauth_still_enforces_token(self, store: Store) -> None:
        from aiohttp.test_utils import TestClient, TestServer

        app = create_app(RecorderState(), store, backup_token=self._TOKEN)  # no OAuth
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/api/backup", allow_redirects=False)
            assert resp.status == 401
            resp = await client.get(
                "/api/backup", headers={"Authorization": f"Bearer {self._TOKEN}"}
            )
            assert resp.status == 200


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

    def test_template_has_circuit_control(self) -> None:
        # The "Circuit: <link> <select>" line now lives in juice/web/strip.js
        # (buildCircuitLine), inlined into the strip page via the JS_STRIP marker;
        # the assignment fetch stays inline in the template.
        from juice.server import _WEB_JS, STRIP_HTML

        assert "{{JS_STRIP}}" in STRIP_HTML
        assert "/circuit/" in _WEB_JS["JS_STRIP"]  # links to the circuit page
        assert "/api/strips/" in STRIP_HTML and "/circuit" in STRIP_HTML


class TestCircuitPageHTML:
    def test_route_registered(self, store: Store) -> None:
        state = RecorderState()
        app = create_app(state, store)
        routes = {(r.method, r.resource.canonical) for r in app.router.routes()}
        assert ("GET", "/circuit/{id}") in routes

    def test_template_markers(self) -> None:
        from juice.server import CIRCUIT_HTML

        assert "{{PUBLIC_MODE}}" in CIRCUIT_HTML
        assert "{{BODY_CLASS}}" in CIRCUIT_HTML
        assert "{{AUTH_CORNER}}" in CIRCUIT_HTML
        assert "/api/circuits" in CIRCUIT_HTML

    def test_template_has_capacity_and_chart(self) -> None:
        from juice.server import CIRCUIT_HTML

        assert "% of capacity" in CIRCUIT_HTML
        assert "/usage?days=" in CIRCUIT_HTML
        assert "cdn.jsdelivr.net/npm/d3@7" in CIRCUIT_HTML


class TestDetailPageHTML:
    def test_template_fetches_and_renders_peak(self) -> None:
        from juice.server import DETAIL_HTML

        assert "/peak?days=30" in DETAIL_HTML
        assert "Peak" in DETAIL_HTML

    def test_template_has_detail_stats(self) -> None:
        from juice.server import _WEB_JS, DETAIL_HTML

        # The Details table lives below the outlet map and pulls avg daily cost.
        assert 'id="detail-stats"' in DETAIL_HTML
        assert "/cost?days=30" in DETAIL_HTML
        assert "buildDetailStats" in _WEB_JS["JS_DETAIL"]

    def test_cost_route_registered(self, store: Store) -> None:
        state = RecorderState()
        app = create_app(state, store)
        routes = {(r.method, r.resource.canonical) for r in app.router.routes()}
        assert ("GET", "/api/machines/{plug_id}/cost") in routes


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
        assert ("GET", "/api/circuit-peaks") in routes

    def test_template_has_private_strip_peaks_section(self) -> None:
        from juice.server import USAGE_HTML

        assert "strip-peaks" in USAGE_HTML
        assert "/api/strip-peaks" in USAGE_HTML
        assert "{{PUBLIC_MODE}}" in USAGE_HTML
        # The section itself must carry the private-only marker.
        assert 'class="section-title private-only"' in USAGE_HTML

    def test_strip_peaks_rendered_as_table(self) -> None:
        # The peak-table markup now lives in juice/web/peaks.js, inlined into the
        # usage page via the JS_PEAKS marker.
        from juice.server import _WEB_JS, USAGE_HTML

        assert "{{JS_PEAKS}}" in USAGE_HTML
        peaks = _WEB_JS["JS_PEAKS"]
        assert "peak-table" in peaks
        assert "<th" in peaks
        assert "Max possible" in peaks

    def test_template_has_circuit_peaks_section(self) -> None:
        from juice.server import _WEB_JS, USAGE_HTML

        assert "circuit-peaks" in USAGE_HTML
        assert "/api/circuit-peaks" in USAGE_HTML
        assert "% of capacity" in _WEB_JS["JS_PEAKS"]  # now in the peaks module
        # Two private-only section titles now (strip peaks + circuit peaks).
        assert USAGE_HTML.count('class="section-title private-only"') >= 2

    def test_template_has_cost_sections(self) -> None:
        from juice.server import _WEB_JS, USAGE_HTML

        assert 'id="energy-cost"' in USAGE_HTML  # cost-per-day chart section
        assert 'id="machine-costs"' in USAGE_HTML  # per-machine cost table section
        assert "/api/cost" in USAGE_HTML
        assert "{{JS_COST}}" in USAGE_HTML  # the cost-table module is inlined
        assert "buildCostTable" in _WEB_JS["JS_COST"]
        # strip + circuit + 2 cost sections are all operator-only.
        assert USAGE_HTML.count('class="section-title private-only"') >= 4


class TestPlayHoursAPI:
    """Most of the play-hours math lives in juice.store; the API tests here
    cover shape + reshape correctness only. They seed hourly_play_seconds
    directly rather than re-exercising the rollup pipeline."""

    @staticmethod
    def _seed_day(store, mid, day, seconds):
        # One hourly row at local noon stands in for a day's play.
        store._conn.execute(
            "INSERT INTO hourly_play_seconds VALUES (?, ?, ?, ?)",
            [mid, datetime(day.year, day.month, day.day, 12, 0, 0), seconds, seconds],
        )

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
        from datetime import date as _date

        for day, mid, seconds in [
            (_date(2026, 5, 23), ma, 1800.0),  # 0.5h
            (_date(2026, 5, 23), mb, 3600.0),  # 1.0h
            (_date(2026, 5, 24), ma, 7200.0),  # 2.0h
            (_date(2026, 5, 24), mb, 1800.0),  # 0.5h
            (_date(2026, 5, 25), ma, 3600.0),  # 1.0h
        ]:
            self._seed_day(store, mid, day, seconds)

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

        self._seed_day(store, ma, _date(2026, 5, 25), 600.0)
        self._seed_day(store, mb, _date(2026, 5, 25), 7200.0)

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


class TestNthHighestDay:
    def test_picks_nth_highest_and_handles_edges(self) -> None:
        from datetime import date as _date

        days = [
            (_date(2026, 6, 1), 3.0),
            (_date(2026, 6, 2), 1.0),
            (_date(2026, 6, 3), 2.0),
            (_date(2026, 6, 4), 5.0),
        ]
        # ranked desc: 5(6-4), 3(6-1), 2(6-3), 1(6-2) → 3rd = 6-3, 1st = 6-4.
        assert _nth_highest_day(days, 3) == _date(2026, 6, 3)
        assert _nth_highest_day(days, 1) == _date(2026, 6, 4)
        # Fewer than n with cost → falls back to the lowest-ranked that has cost.
        assert _nth_highest_day([(_date(2026, 6, 1), 2.0)], 3) == _date(2026, 6, 1)
        # No day has cost → None.
        assert _nth_highest_day([(_date(2026, 6, 1), 0.0)], 3) is None


class TestHandleCost:
    def _seed_hour(self, store: Store, pid: int, day, kwh: float) -> None:
        # 18:00 UTC = 13:00 Chicago, so the hour's local-Central day == `day`.
        store._conn.execute(
            "INSERT INTO hourly_usage (plug_id, hour_ts, kwh, samples) VALUES (?, ?, ?, ?)",
            [pid, datetime(day.year, day.month, day.day, 18, 0, 0), kwh, 60],
        )

    @pytest.mark.asyncio
    async def test_shape_cost_math_and_normal_day(self, store: Store) -> None:
        from datetime import date as _date

        pid = store.ensure_plug("d1", "c1", "Blackout - M0013", has_emeter=True)
        mid = store.ensure_machine("M0013", "Blackout")
        store.update_assignment(pid, mid, datetime(2026, 5, 1, 0, 0, 0, tzinfo=UTC))
        # 5 days; ranked-by-cost desc: 10, 8, 6, 5, 3 → 3rd-highest is 06-05 (6 kWh).
        for d, k in {
            _date(2026, 6, 1): 10.0,
            _date(2026, 6, 2): 5.0,
            _date(2026, 6, 3): 8.0,
            _date(2026, 6, 4): 3.0,
            _date(2026, 6, 5): 6.0,
        }.items():
            self._seed_hour(store, pid, d, k)

        req = _make_request(None, RecorderState(), store)
        req.query = {"start": "2026-06-01", "end": "2026-06-06"}
        body = await _json(await handle_cost(req))

        assert body["rate"] == 0.31
        assert body["days"] == [f"2026-06-0{i}" for i in range(1, 6)]
        assert body["daily_cost"] == [round(k * 0.31, 2) for k in (10, 5, 8, 3, 6)]
        assert body["normal_day"] == "2026-06-05"
        assert body["normal_day_total_cost"] == pytest.approx(round(6 * 0.31, 2))

        m = next(x for x in body["machines"] if x["name"] == "Blackout")
        assert m["month_kwh"] == pytest.approx(32.0)  # 10+5+8+3+6
        assert m["month_cost"] == pytest.approx(round(32 * 0.31, 2))
        assert m["normal_day_cost"] == pytest.approx(round(6 * 0.31, 2))  # its kWh on 06-05
        assert body["month_total_cost"] == pytest.approx(round(32 * 0.31, 2))

    @pytest.mark.asyncio
    async def test_normal_day_fallback_and_unassigned(self, store: Store) -> None:
        from datetime import date as _date

        pid = store.ensure_plug("d1", "c1", "Spare", has_emeter=True)  # never assigned
        self._seed_hour(store, pid, _date(2026, 6, 1), 10.0)
        self._seed_hour(store, pid, _date(2026, 6, 2), 5.0)

        req = _make_request(None, RecorderState(), store)
        req.query = {"start": "2026-06-01", "end": "2026-06-03"}  # only 2 days
        body = await _json(await handle_cost(req))

        # 3rd-highest with 2 days → lowest-ranked available (06-02).
        assert body["normal_day"] == "2026-06-02"
        assert [m["name"] for m in body["machines"]] == ["Unassigned"]

    @pytest.mark.asyncio
    async def test_empty_window(self, store: Store) -> None:
        req = _make_request(None, RecorderState(), store)
        req.query = {"start": "2026-06-01", "end": "2026-06-03"}
        body = await _json(await handle_cost(req))
        assert body["machines"] == []
        assert body["normal_day"] is None
        assert body["daily_cost"] == [0.0, 0.0]
        assert body["month_total_cost"] == 0.0


class TestHandleMachineCost:
    """Per-machine average daily cost over its on-days (detail-page Details table)."""

    def _seed_hour(self, store: Store, pid: int, day, kwh: float) -> None:
        # 18:00 UTC = 13:00 Chicago, so the hour's local-Central day == `day`.
        store._conn.execute(
            "INSERT INTO hourly_usage (plug_id, hour_ts, kwh, samples) VALUES (?, ?, ?, ?)",
            [pid, datetime(day.year, day.month, day.day, 18, 0, 0), kwh, 60],
        )

    @pytest.mark.asyncio
    async def test_avg_over_on_days_only(self, store: Store) -> None:
        from datetime import date as _date

        state = RecorderState()
        pid = _seed_machine(
            store, state, ("d1", "c1", "Blackout - M0013"), "M0013", "Blackout", 1980
        )
        # 3 on-days (kWh > 0) inside a 5-day window; the 2 silent days contribute
        # nothing and must NOT dilute the average ("only the days it was on").
        for d, k in {
            _date(2026, 6, 1): 4.0,
            _date(2026, 6, 3): 8.0,
            _date(2026, 6, 5): 6.0,
        }.items():
            self._seed_hour(store, pid, d, k)

        req = _make_request(None, state, store, match_info={"plug_id": str(pid)})
        req.query = {"start": "2026-06-01", "end": "2026-06-06"}
        body = await _json(await handle_machine_cost(req))

        assert body["plug_id"] == pid
        assert body["rate"] == 0.31
        assert body["on_days"] == 3
        # avg on-day kWh = (4 + 8 + 6) / 3 = 6.0 → $6.0 × 0.31.
        assert body["avg_daily_cost"] == pytest.approx(round(6.0 * 0.31, 2))

    @pytest.mark.asyncio
    async def test_null_when_no_on_days(self, store: Store) -> None:
        state = RecorderState()
        pid = _seed_machine(store, state, ("d1", "c1", "Idle - M1"), "M1", "Idle", None)
        req = _make_request(None, state, store, match_info={"plug_id": str(pid)})
        req.query = {"start": "2026-06-01", "end": "2026-06-06"}
        body = await _json(await handle_machine_cost(req))
        assert body["on_days"] == 0
        assert body["avg_daily_cost"] is None

    @pytest.mark.asyncio
    async def test_unassigned_plug_null(self, store: Store) -> None:
        # A plug with no machine has no cost history → null, not a 500.
        req = _make_request(None, RecorderState(), store, match_info={"plug_id": "999"})
        req.query = {}
        body = await _json(await handle_machine_cost(req))
        assert body["avg_daily_cost"] is None
        assert body["on_days"] == 0

    @pytest.mark.asyncio
    async def test_non_integer_plug_id_400(self, store: Store) -> None:
        req = _make_request(None, RecorderState(), store, match_info={"plug_id": "abc"})
        req.query = {}
        resp = await handle_machine_cost(req)
        assert resp.status == 400


class TestMachineCalibrationField:
    """handle_machines exposes each plug's calibration thresholds to operators only."""

    @pytest.mark.asyncio
    async def test_present_for_authed_absent_for_public(self, store: Store) -> None:
        state = RecorderState()
        pid = _seed_machine(store, state, ("hs", "c01", "Trip - M9"), "M9", "Trip", 1990, watts=100)
        state.calibrations[pid] = Calibration(idle_max_rsd=2.5, play_min_rsd=10.0)

        authed = await _json(
            await handle_machines(_make_authed_request(None, state, store, oauth_configured=True))
        )
        assert authed["machines"][0]["calibration"] == {
            "idle_max_rsd": 2.5,
            "play_min_rsd": 10.0,
        }

        public = await _json(
            await handle_machines(_make_request(None, state, store, oauth_configured=True))
        )
        # Operational detail — redacted for anon, like plug/strip names.
        assert "calibration" not in public["machines"][0]

    @pytest.mark.asyncio
    async def test_null_when_uncalibrated(self, store: Store) -> None:
        state = RecorderState()
        _seed_machine(store, state, ("hs", "c01", "Trip - M9"), "M9", "Trip", 1990, watts=100)
        authed = await _json(
            await handle_machines(_make_authed_request(None, state, store, oauth_configured=True))
        )
        assert authed["machines"][0]["calibration"] is None


async def _json(resp):
    """Extract JSON body from an aiohttp web.Response."""
    import json

    return json.loads(resp.body.decode())


class TestHandleAir:
    def _seed(self, store: Store) -> None:
        from juice.recorder import air_poll_once  # noqa: F401 (kept for symmetry)

        t0 = datetime(2026, 6, 20, 12, 0, 0, tzinfo=UTC)
        t1 = datetime(2026, 6, 20, 12, 15, 0, tzinfo=UTC)
        store.ensure_air_sensor("MAC1", "Main Floor", online=True, seen_ts=t1)
        store.ensure_air_sensor("MAC2", "Back Room", online=False, seen_ts=t1)
        # (ts, mac, temperature, humidity, co2, pm25, pm10, tvoc, noise, battery)
        store.insert_air_readings(
            [
                (t0, "MAC1", 22.0, 44.0, 600.0, 7.0, 10.0, 120.0, None, 90.0),
                (t1, "MAC1", 22.5, 45.0, 700.0, 8.0, 12.0, 130.0, None, 88.0),
            ]
        )

    @pytest.mark.asyncio
    async def test_lists_sensors_with_latest_reading(self, store: Store) -> None:
        from juice.server import handle_air

        self._seed(store)
        req = _make_request(None, RecorderState(), store)
        body = await _json(await handle_air(req))
        sensors = {s["mac"]: s for s in body["sensors"]}
        assert sensors["MAC1"]["name"] == "Main Floor"
        assert sensors["MAC1"]["online"] is True
        assert sensors["MAC1"]["co2"] == 700.0  # most-recent reading
        assert sensors["MAC1"]["ts"].endswith("Z")
        # A sensor with no readings still lists, with null metrics.
        assert sensors["MAC2"]["online"] is False
        assert sensors["MAC2"]["co2"] is None
        assert sensors["MAC2"]["ts"] is None

    @pytest.mark.asyncio
    async def test_history_returns_series(self, store: Store) -> None:
        from juice.server import handle_air_history

        self._seed(store)
        # Pass an explicit window covering both seeded rows. The default window is
        # `now - 7 days`, which would age the fixed-date seed out over time and make
        # this test flaky (it asserts the full series); the window keeps it
        # deterministic, like test_history_respects_window.
        req = _make_request(
            None,
            RecorderState(),
            store,
            match_info={"mac": "MAC1"},
            query={"from": "2026-06-20T00:00:00Z", "to": "2026-06-20T23:59:00Z"},
        )
        body = await _json(await handle_air_history(req))
        assert body["mac"] == "MAC1"
        assert [r["co2"] for r in body["readings"]] == [600.0, 700.0]
        assert all(r["ts"].endswith("Z") for r in body["readings"])

    @pytest.mark.asyncio
    async def test_history_respects_window(self, store: Store) -> None:
        from juice.server import handle_air_history

        self._seed(store)
        req = _make_request(
            None,
            RecorderState(),
            store,
            match_info={"mac": "MAC1"},
            query={"from": "2026-06-20T12:10:00Z", "to": "2026-06-20T12:20:00Z"},
        )
        body = await _json(await handle_air_history(req))
        assert [r["co2"] for r in body["readings"]] == [700.0]


class TestAirPublicReadable:
    def test_air_paths_are_public(self) -> None:

        from juice.auth import PUBLIC_READABLE_PATTERNS

        def matches(path: str) -> bool:
            return any(p.match(path) for p in PUBLIC_READABLE_PATTERNS)

        assert matches("/air")
        assert matches("/api/air")
        assert matches("/api/air/582D34AABBCC/history")


class TestAirRoutesRegistered:
    def test_routes_exist(self, store: Store) -> None:
        app = create_app(RecorderState(), store)
        paths = {r.resource.canonical for r in app.router.routes()}
        assert "/air" in paths
        assert "/api/air" in paths
        assert "/api/air/{mac}/history" in paths


class TestDownsampleSpark:
    def test_short_input_passes_through_unchanged(self) -> None:
        watts = [1.0, 2.0, 3.0]
        states = ["OFF", "PLAYING", "PLAYING"]
        out_w, out_s = _downsample_spark(watts, states, target=200)
        assert out_w is watts
        assert out_s is states

    def test_long_input_capped_to_target(self) -> None:
        watts = [float(i) for i in range(1000)]
        states = ["PLAYING"] * 999 + ["IDLE"]
        out_w, out_s = _downsample_spark(watts, states, target=200)
        assert len(out_w) == 200
        assert len(out_s) == len(out_w)
        # Last bucket's last state is preserved (the trailing band).
        assert out_s[-1] == "IDLE"
        # Bucket means are sane (first bucket averages 0..4 -> 2.0).
        assert out_w[0] == 2.0

    def test_empty_states_returned_empty(self) -> None:
        watts = [float(i) for i in range(1000)]
        out_w, out_s = _downsample_spark(watts, [], target=200)
        assert len(out_w) == 200
        assert out_s == []

    @pytest.mark.asyncio
    async def test_handle_machines_caps_sparkline_points(self, store: Store) -> None:
        state = RecorderState()
        plug_id = _seed_machine(
            store,
            state,
            ("hs300", "c01", "Blackout - M0013"),
            "M0013",
            "Blackout",
            1980,
            watts=300.0,
        )
        state.calibrations[plug_id] = Calibration(idle_max_rsd=2.0, play_min_rsd=8.0)
        state.watt_buffers[plug_id] = deque([300.0] * 3600, maxlen=3600)

        body = await _json(await handle_machines(_make_request(None, state, store)))
        m = body["machines"][0]
        assert 0 < len(m["sparkline"]) <= SPARK_POINTS
        # State band stays aligned 1:1 with the downsampled line.
        assert len(m["sparkline_states"]) == len(m["sparkline"])


class TestReadingsSnapshot:
    def test_lightweight_live_fields_no_identifiers(self, store: Store) -> None:
        state = RecorderState()
        plug_id = _seed_machine(
            store,
            state,
            ("hs300", "c01", "Blackout - M0013"),
            "M0013",
            "Blackout",
            1980,
            watts=325.4,
        )
        state.calibrations[plug_id] = Calibration(idle_max_rsd=2.0, play_min_rsd=8.0)
        state.watt_buffers[plug_id] = deque([325.4] * 10, maxlen=3600)

        snap = _readings_snapshot(state)
        assert len(snap) == 1
        r = snap[0]
        assert r["plug_id"] == plug_id
        assert r["power"]["watts"] == 325.4
        assert r["watt"] == 325.4
        assert r["is_on"] is True
        assert r["power_status"] == "on"
        assert r["offline"] is False
        # No device/strip/alias identifiers leak into the public push.
        assert "device_id" not in r
        assert "alias" not in r
        assert "child_id" not in r

    def test_offline_machine_state(self, store: Store) -> None:
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
        r = _readings_snapshot(state)[0]
        assert r["offline"] is True
        assert r["state"] == "OFFLINE"
        assert r["power"] is None


class TestPublicSseGating:
    """A public (unauthenticated) SSE subscriber must receive only 'readings'
    events and a hello with no operation detail — operator-only events leak
    fields the public /api/machines view redacts (e.g. strip aliases)."""

    async def _drain(self, state, *, public, events):
        writes: list[dict] = []

        async def write(ev):
            writes.append(ev)

        task = asyncio.create_task(_sse_stream(state, write, public=public))
        # Let the stream register + emit hello, then publish, then let it drain.
        for _ in range(3):
            await asyncio.sleep(0)
        for ev in events:
            _publish(state, ev)
        for _ in range(10):
            await asyncio.sleep(0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return writes

    @pytest.mark.asyncio
    async def test_public_subscriber_gets_readings_only(self) -> None:
        state = RecorderState()
        state.current_operation = Operation(
            id="op1",
            kind="all_on",
            started_at=datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC),
            started_by="william@theflip.museum",
            targets=[],
        )
        writes = await self._drain(
            state,
            public=True,
            events=[
                {"type": "readings", "machines": []},
                {"type": "strip_name_change", "device_id": "hs300", "name": "Secret Strip"},
                {"type": "operation_started", "operation": {"id": "op1"}},
            ],
        )
        types = [w["type"] for w in writes]
        # hello (no op detail) + the readings event; nothing operator-only.
        assert types[0] == "hello"
        assert writes[0]["current_operation"] is None
        assert "readings" in types
        assert "strip_name_change" not in types
        assert "operation_started" not in types

    @pytest.mark.asyncio
    async def test_authed_subscriber_gets_everything(self) -> None:
        state = RecorderState()
        state.current_operation = Operation(
            id="op1",
            kind="all_on",
            started_at=datetime(2026, 5, 25, 12, 0, 0, tzinfo=UTC),
            started_by="william@theflip.museum",
            targets=[],
        )
        writes = await self._drain(
            state,
            public=False,
            events=[
                {"type": "readings", "machines": []},
                {"type": "strip_name_change", "device_id": "hs300", "name": "Strip 1"},
            ],
        )
        types = [w["type"] for w in writes]
        assert writes[0]["type"] == "hello"
        assert writes[0]["current_operation"] is not None  # operator sees op detail
        assert "readings" in types
        assert "strip_name_change" in types


class TestCompressionMiddleware:
    @pytest.mark.asyncio
    async def test_machines_response_is_gzipped(self, store: Store) -> None:
        from aiohttp.test_utils import TestClient, TestServer

        app = create_app(RecorderState(), store)
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/api/machines", headers={"Accept-Encoding": "gzip"})
            assert resp.status == 200
            assert resp.headers.get("Content-Encoding") == "gzip"

    @pytest.mark.asyncio
    async def test_events_stream_not_compressed(self, store: Store) -> None:
        # SSE must stream uncompressed so chunked flushing keeps working.
        from aiohttp.test_utils import TestClient, TestServer

        app = create_app(RecorderState(), store)
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/api/events", headers={"Accept-Encoding": "gzip"})
            assert resp.status == 200
            assert "Content-Encoding" not in resp.headers
            resp.close()
