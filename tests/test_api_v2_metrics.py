"""Tests for /api/v2/metrics/*.

One window convention across every aggregate, echoed back so a client never has
to reconstruct what it got. The aggregation itself is v1's; the contract isn't.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest
from aiohttp.test_utils import TestClient, TestServer

from juice.server import RecorderState, create_app
from juice.state import Calibration
from juice.store import Store

DEV = "DEVICE_A"


@pytest.fixture
def store():
    with Store(":memory:") as s:
        yield s


def _seed(store: Store, state: RecorderState, *, days: int = 3) -> None:
    """A machine drawing steadily for a few recent local days."""
    plug_id = store.ensure_plug(DEV, DEV + "00", "Godzilla - M0001", has_emeter=True)
    machine_id = store.ensure_machine("M0001", "Godzilla")
    start = datetime.now(UTC) - timedelta(days=days)
    store.update_assignment(plug_id, machine_id, start)
    store.set_calibration(machine_id, Calibration(idle_max_rsd=None, play_min_rsd=10.0))

    # Vary the draw so the classifier actually returns PLAYING. A constant
    # wattage has zero RSD and classifies as ATTRACT, which produces NO play
    # rows at all — an earlier version of this fixture did exactly that, so the
    # play-hours row-shaping path was never executed and a KeyError shipped that
    # only the real fixture caught.
    import math

    rows = []
    for day in range(days):
        for hour in range(24):
            for minute in range(0, 60, 5):
                ts = start + timedelta(days=day, hours=hour, minutes=minute)
                watts = 200.0 + 90.0 * math.sin(minute)
                rows.append((ts, plug_id, watts, 120.0, watts / 120, 0.0))
    store.insert_readings(rows)
    store.refresh_hourly_usage(lookback_hours=days * 24 + 48)
    store.refresh_hourly_play_seconds(lookback_hours=days * 24 + 48)

    state.plugs[plug_id] = (DEV, DEV + "00", "Godzilla - M0001")
    state.plug_has_emeter[plug_id] = True
    state.assignments[plug_id] = ("Godzilla", "M0001", 2021)
    state.calibrations[plug_id] = Calibration(idle_max_rsd=None, play_min_rsd=10.0)


async def _get(state: RecorderState, store: Store, path: str, *, login: bool = True):
    async with TestClient(TestServer(create_app(state, store, dev_auth=True))) as client:
        if login:
            await client.get("/login")
        resp = await client.get(path)
        return resp.status, await resp.json()


class TestWindowIsUniform:
    @pytest.mark.parametrize(
        "path",
        [
            "/api/v2/metrics/energy",
            "/api/v2/metrics/play-hours",
            "/api/v2/metrics/utilization",
            "/api/v2/metrics/cost",
            "/api/v2/metrics/peaks",
        ],
    )
    @pytest.mark.asyncio
    async def test_every_metric_echoes_the_same_window_shape(self, store: Store, path: str) -> None:
        """v1 spreads three conventions across these same numbers, so a client
        has to learn each endpoint separately."""
        state = RecorderState()
        _seed(store, state)

        status, body = await _get(state, store, f"{path}?window=7d")
        assert status == 200, body

        window = body["window"]
        assert window["spec"] == "7d"
        assert window["days"] == 7
        assert window["tz"] == "America/Chicago"
        assert date.fromisoformat(window["from"]) < date.fromisoformat(window["to"])

    @pytest.mark.parametrize(
        "path",
        [
            "/api/v2/metrics/energy",
            "/api/v2/metrics/play-hours",
            "/api/v2/metrics/cost",
            "/api/v2/metrics/peaks",
        ],
    )
    @pytest.mark.asyncio
    async def test_every_metric_refuses_an_oversized_window(self, store: Store, path: str) -> None:
        """Refused everywhere, not clamped anywhere — a chart must not lie about
        its own axis on one endpoint and not another."""
        state = RecorderState()
        _seed(store, state)

        status, body = await _get(state, store, f"{path}?window=500d")
        assert status == 400, body
        assert body["error"]["detail"]["max_days"] == 365


class TestEnergy:
    @pytest.mark.asyncio
    async def test_reports_kwh_per_machine_and_a_total(self, store: Store) -> None:
        state = RecorderState()
        _seed(store, state)

        status, body = await _get(state, store, "/api/v2/metrics/energy?window=7d")
        assert status == 200
        assert body["machines"], body
        machine = body["machines"][0]
        assert machine["name"] == "Godzilla"
        assert machine["kwh"] > 0
        assert body["total_kwh"] >= machine["kwh"]

    @pytest.mark.asyncio
    async def test_readable_by_anonymous_viewers(self, store: Store) -> None:
        """Usage is part of the public 'what does the museum do' view; cost is not."""
        state = RecorderState()
        _seed(store, state)
        status, _ = await _get(state, store, "/api/v2/metrics/energy", login=False)
        assert status == 200


class TestPlayHours:
    @pytest.mark.asyncio
    async def test_says_how_many_machines_are_unmeasurable(self, store: Store) -> None:
        """An uncalibrated machine has no *measurable* play, which is different
        from zero play. Without this an operator reads a short list as the whole
        floor."""
        state = RecorderState()
        _seed(store, state)
        state.assignments[999] = ("Lightning", "M0999", 1981)  # no calibration

        status, body = await _get(state, store, "/api/v2/metrics/play-hours?window=7d")
        assert status == 200
        assert body["measurable_machines"] == 1
        assert body["unmeasurable_machines"] == 1
        # The rows must actually be shaped, not merely counted — this is the
        # path that raised KeyError against real data.
        assert body["machines"], "fixture produced no play rows; the path is untested"
        assert body["machines"][0]["name"] == "Godzilla"
        assert body["machines"][0]["hours"] > 0
        assert body["machines"][0]["daily"]

    @pytest.mark.asyncio
    async def test_counts_machines_not_plug_entries(self, store: Store) -> None:
        """A machine that moved outlets has two open assignments, so counting
        plug entries double-counts it — the exact case resolve_asset exists to
        handle, which I built and then didn't use here."""
        state = RecorderState()
        _seed(store, state)
        moved = next(iter(state.assignments))
        # Same asset on a second (stale) plug, as after an outlet move.
        state.plugs[500] = ("DEVICE_B", "DEVICE_B00", "Godzilla - M0001")
        state.assignments[500] = ("Godzilla", "M0001", 2021)
        state.calibrations[500] = state.calibrations[moved]

        status, body = await _get(state, store, "/api/v2/metrics/play-hours?window=7d")
        assert status == 200
        assert body["measurable_machines"] == 1, "the same machine was counted twice"
        assert body["unmeasurable_machines"] == 0


class TestUtilization:
    @pytest.mark.asyncio
    async def test_the_grid_is_dense_not_sparse(self, store: Store) -> None:
        """v1 returns only cells with data, so a missing cell is ambiguous
        between 'no play' and 'we weren't open'."""
        state = RecorderState()
        _seed(store, state)

        status, body = await _get(state, store, "/api/v2/metrics/utilization?window=2d")
        assert status == 200
        assert body["hours"] == list(range(24))
        assert len(body["cells"]) == len(body["dates"]) * 24
        assert all("measured" in c for c in body["cells"])

    @pytest.mark.asyncio
    async def test_the_grid_covers_the_whole_window_even_with_no_data(self, store: Store) -> None:
        """Densifying only the hours of days that happened to return rows still
        leaves the *days* sparse — a client would have to reconstruct the gaps,
        which is the work the dense contract exists to remove."""
        state = RecorderState()  # no readings at all

        status, body = await _get(state, store, "/api/v2/metrics/utilization?window=3d")
        assert status == 200
        assert len(body["dates"]) == 3, body["dates"]
        assert len(body["cells"]) == 3 * 24
        assert all(c["measured"] is False for c in body["cells"])
        assert body["max_ratio"] == 0.0


class TestCost:
    @pytest.mark.asyncio
    async def test_is_operator_only(self, store: Store) -> None:
        state = RecorderState()
        _seed(store, state)
        status, _ = await _get(state, store, "/api/v2/metrics/cost", login=False)
        assert status == 401

    @pytest.mark.asyncio
    async def test_total_is_rounded_once_from_the_true_total(self, store: Store) -> None:
        """v1 has two totals derived differently that can disagree by a cent."""
        state = RecorderState()
        _seed(store, state)

        status, body = await _get(state, store, "/api/v2/metrics/cost?window=7d")
        assert status == 200
        assert body["rate_per_kwh"] == 0.31
        assert body["total_cost"] == round(body["total_kwh"] * 0.31, 2)


class TestPeaks:
    @pytest.mark.asyncio
    async def test_defaults_to_circuits(self, store: Store) -> None:
        state = RecorderState()
        _seed(store, state)
        status, body = await _get(state, store, "/api/v2/metrics/peaks?window=7d")
        assert status == 200
        assert body["by"] == "circuit"

    @pytest.mark.asyncio
    async def test_by_strip(self, store: Store) -> None:
        state = RecorderState()
        _seed(store, state)
        status, body = await _get(state, store, "/api/v2/metrics/peaks?by=strip&window=7d")
        assert status == 200
        assert body["by"] == "strip"

    @pytest.mark.asyncio
    async def test_an_unknown_grouping_is_rejected(self, store: Store) -> None:
        state = RecorderState()
        _seed(store, state)
        status, body = await _get(state, store, "/api/v2/metrics/peaks?by=machine")
        assert status == 400
        assert body["error"]["detail"]["allowed"] == ["circuit", "strip"]

    @pytest.mark.asyncio
    async def test_a_circuit_without_amps_reports_no_headroom(self, store: Store) -> None:
        """None rather than a guess: a breaker with no recorded amperage has no
        headroom we can honestly report."""
        state = RecorderState()
        _seed(store, state)
        circuit_id = store.create_circuit("Panel A", "12", "No rating", None)
        state.circuit_devices[DEV] = circuit_id

        status, body = await _get(state, store, "/api/v2/metrics/peaks?window=7d")
        assert status == 200
        for item in body["items"]:
            if item["capacity_watts"] is None:
                assert item["pct_of_capacity"] is None
