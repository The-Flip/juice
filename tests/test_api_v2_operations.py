"""Tests for /api/v2 bulk operations.

Operations stay a single global slot — the museum is one power domain and the
per-step stagger exists to limit inrush, so interleaving two bulk cycles would
defeat it. What changes is that a refusal explains itself: v1's bare "operation
already in progress" tells an operator nothing they can act on.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from aiohttp.test_utils import TestClient, TestServer

from juice.collector import PlugReading
from juice.server import Operation, RecorderState, create_app
from juice.store import Store

DEV = "DEVICE_A"


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


def _running(state: RecorderState) -> Operation:
    op = Operation(
        id="op-abc",
        kind="all_on",
        started_at=datetime.now(UTC),
        started_by="dana@theflip.museum",
        targets=[1, 2, 3],
    )
    op.index = 1
    state.current_operation = op
    return op


async def _client(state: RecorderState, store: Store) -> TestClient:
    return TestClient(TestServer(create_app(state, store, dev_auth=True)))


class TestCurrent:
    @pytest.mark.asyncio
    async def test_idle_returns_an_object_not_a_bare_null(self, store: Store) -> None:
        """v1 answers top-level `null` when idle — the only endpoint in the API
        that does, forcing every client to special-case it."""
        async with await _client(_state(), store) as client:
            await client.get("/login")
            resp = await client.get("/api/v2/operations/current")
            assert resp.status == 200
            assert await resp.json() == {"operation": None}

    @pytest.mark.asyncio
    async def test_running_operation_is_reported(self, store: Store) -> None:
        state = _state()
        _running(state)
        async with await _client(state, store) as client:
            await client.get("/login")
            body = await (await client.get("/api/v2/operations/current")).json()

        assert body["operation"]["kind"] == "all_on"
        assert body["operation"]["started_by"] == "dana@theflip.museum"

    @pytest.mark.asyncio
    async def test_anonymous_cannot_see_operations(self, store: Store) -> None:
        """started_by is an email address."""
        async with await _client(_state(), store) as client:
            resp = await client.get("/api/v2/operations/current")
            assert resp.status == 401


class TestConflict:
    @pytest.mark.asyncio
    async def test_a_second_operation_is_refused_with_the_whole_first_one(
        self, store: Store
    ) -> None:
        """The point of the phase. 'Busy' alone gives an operator no way to
        decide whether to wait or intervene."""
        state = _state()
        _running(state)

        async with await _client(state, store) as client:
            await client.get("/login")
            resp = await client.post("/api/v2/operations", json={"kind": "all_off"})
            assert resp.status == 409
            body = await resp.json()

        assert body["error"]["code"] == "operation_in_progress"
        operation = body["error"]["detail"]["operation"]
        assert operation["started_by"] == "dana@theflip.museum"
        assert operation["kind"] == "all_on"
        assert operation["index"] == 1
        assert operation["total"] == 3
        # Enough to render "Dana started All On - 1 of 3. [Watch] [Cancel]".
        assert operation["id"] == "op-abc"


class TestValidation:
    @pytest.mark.asyncio
    async def test_an_unknown_kind_is_rejected(self, store: Store) -> None:
        async with await _client(_state(), store) as client:
            await client.get("/login")
            resp = await client.post("/api/v2/operations", json={"kind": "all_sideways"})
            assert resp.status == 400
            body = await resp.json()
            assert body["error"]["code"] == "bad_request"
            assert body["error"]["detail"]["allowed"] == ["all_off", "all_on"]

    @pytest.mark.asyncio
    async def test_a_bad_scope_is_rejected(self, store: Store) -> None:
        async with await _client(_state(), store) as client:
            await client.get("/login")
            resp = await client.post(
                "/api/v2/operations", json={"kind": "all_on", "scope": {"device_id": 7}}
            )
            assert resp.status == 400

    @pytest.mark.asyncio
    async def test_anonymous_cannot_start_one(self, store: Store) -> None:
        async with await _client(_state(), store) as client:
            resp = await client.post("/api/v2/operations", json={"kind": "all_on"})
            assert resp.status == 401


class TestCancel:
    @pytest.mark.asyncio
    async def test_cancel_marks_the_running_operation(self, store: Store) -> None:
        state = _state()
        op = _running(state)

        async with await _client(state, store) as client:
            await client.get("/login")
            resp = await client.post("/api/v2/operations/op-abc/cancel")
            assert resp.status == 200

        assert op.cancel_requested is True

    @pytest.mark.asyncio
    async def test_cancelling_an_unknown_operation_is_a_coded_404(self, store: Store) -> None:
        state = _state()
        _running(state)
        async with await _client(state, store) as client:
            await client.get("/login")
            resp = await client.post("/api/v2/operations/not-this-one/cancel")
            assert resp.status == 404
            assert (await resp.json())["error"]["code"] == "unknown_operation"
