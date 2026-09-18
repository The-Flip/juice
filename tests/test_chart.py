"""Tests for juice.chart -- the machine page's bucketed readings series."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest
from aiohttp.test_utils import TestClient, TestServer

from juice.chart import (
    CHART_POINTS,
    MAX_HOURS,
    MAX_QUEUED,
    ChartWorker,
    bucket_seconds,
    bucket_series,
)
from juice.floor_state import FloorState
from juice.server import create_app
from juice.state import Activity, Calibration
from juice.store import Store


@pytest.fixture
def store():
    with Store(":memory:") as s:
        yield s


def _bucket_start(hours_ago: float, width: int) -> datetime:
    """A whole-second instant `hours_ago` back, on a `width`-second boundary,
    so a synthetic series fills its buckets exactly."""
    t = datetime.now(UTC).replace(microsecond=0) - timedelta(hours=hours_ago)
    return t - timedelta(seconds=int(t.timestamp()) % width)


def _insert_1hz(store: Store, plug_id: int, start: datetime, n: int, watts_sql: str) -> None:
    """`n` seconds of 1 Hz readings from `start`, watts given as SQL over `i`.

    One INSERT ... SELECT rather than `insert_readings`: executemany takes
    ~7 s for 7,200 rows.
    """
    store._conn.execute(
        "INSERT INTO readings (ts, plug_id, watts, voltage, amps, total_kwh) "
        f"SELECT ? + INTERVAL (i) SECOND, ?, {watts_sql}, 120.0, 0.5, 1.0 FROM range(?) t(i)",
        [start, plug_id, n],
    )


class TestBucketSeconds:
    def test_24h_is_one_minute(self) -> None:
        assert bucket_seconds(24) == 60

    def test_never_below_one_second(self) -> None:
        assert bucket_seconds(0) == 1
        assert bucket_seconds(0.1) == 1

    def test_yields_about_chart_points(self) -> None:
        for hours in (1, 6, 12, 24, 168):
            points = hours * 3600 / bucket_seconds(hours)
            assert points <= CHART_POINTS
            assert points > CHART_POINTS / 2


class TestBucketSeries:
    def test_means_watts_and_aligns_to_the_epoch(self) -> None:
        # Samples begin 40 s into a minute: the first bucket is the minute
        # they fall in (960), not the first sample's time (1000).
        epochs = [1000.0 + i for i in range(80)]
        watts = [10.0] * 20 + [20.0] * 60
        activities = [Activity.ATTRACT] * 80
        out = bucket_series(epochs, watts, activities, 60)
        assert out.epochs == [960.0, 1020.0]
        assert out.watts == [10.0, 20.0]
        assert out.activities == [Activity.ATTRACT, Activity.ATTRACT]

    def test_majority_activity_wins_and_none_is_a_vote(self) -> None:
        epochs = [float(i) for i in range(60)]
        watts = [50.0] * 60
        activities = [Activity.PLAYING] * 40 + [Activity.ATTRACT] * 20
        assert bucket_series(epochs, watts, activities, 60).activities == [Activity.PLAYING]
        activities = [None] * 31 + [Activity.PLAYING] * 29
        assert bucket_series(epochs, watts, activities, 60).activities == [None]

    def test_unmeasured_watts_stay_unmeasured(self) -> None:
        epochs = [0.0, 1.0, 60.0, 61.0]
        watts = [None, None, None, 8.0]
        activities = [None, None, None, None]
        out = bucket_series(epochs, watts, activities, 60)
        assert out.watts == [None, 8.0]

    def test_empty(self) -> None:
        out = bucket_series([], [], [], 60)
        assert out.epochs == [] and out.watts == [] and out.activities == []

    def test_one_second_buckets_are_the_raw_series(self) -> None:
        epochs = [5.0, 6.0, 7.0]
        watts = [1.0, 2.0, 3.0]
        activities = [None, Activity.ATTRACT, Activity.PLAYING]
        out = bucket_series(epochs, watts, activities, 1)
        assert (out.epochs, out.watts, out.activities) == (epochs, watts, activities)


class TestReadingsEndpoint:
    @pytest.mark.asyncio
    async def test_a_day_at_1hz_is_a_minute_series(self, store: Store) -> None:
        plug_id = store.ensure_plug("d1", "c1", "Plug 1 - M0001")
        start = _bucket_start(2, 60)
        # Two hours of 1 Hz: an hour not drawing, then an hour at a steady 80 W.
        _insert_1hz(store, plug_id, start, 7200, "CASE WHEN i < 3600 THEN 0.0 ELSE 80.0 END")
        app = create_app(FloorState(), store)
        async with TestClient(TestServer(app)) as client:
            resp = await client.get(f"/api/machines/{plug_id}/readings?hours=24")
            assert resp.status == 200
            body = await resp.read()
        data = json.loads(body)
        assert data["bucket_seconds"] == 60
        assert len(data["timestamps"]) == len(data["watts"]) == len(data["states"]) == 120
        assert data["timestamps"][0] == start.replace(tzinfo=None).isoformat() + "Z"
        # The hour not drawing is OFF at 0 W; the drawing hour is a steady
        # 80 W and, uncalibrated, ATTRACT.
        assert data["watts"][:60] == [0.0] * 60 and set(data["states"][:60]) == {"OFF"}
        assert data["watts"][60:] == [80.0] * 60 and set(data["states"][60:]) == {"ATTRACT"}
        # 7,200 samples came back as 120 points: the body is a few KB, not MB.
        assert len(body) < 20_000

    @pytest.mark.asyncio
    async def test_empty_plug(self, store: Store) -> None:
        plug_id = store.ensure_plug("d1", "c1", "Plug 1")
        app = create_app(FloorState(), store)
        async with TestClient(TestServer(app)) as client:
            resp = await client.get(f"/api/machines/{plug_id}/readings")
            data = await resp.json()
        assert data == {"timestamps": [], "watts": [], "states": [], "bucket_seconds": 60}

    @pytest.mark.asyncio
    async def test_activities_come_from_the_raw_cadence(self, store: Store) -> None:
        """Classification runs on the 1 Hz samples, then the buckets vote.

        Classifying a bucket mean would smooth the flicker that says PLAYING
        out of existence: these 3-sample means have an RSD of ~17%, under
        the 20% play threshold, while the samples run at ~29%.
        """
        plug_id = store.ensure_plug("d1", "c1", "Plug 1 - M0001")
        width = bucket_seconds(1)
        assert width == 3
        start = _bucket_start(0.2, width)
        # Scattered between 50 W and 150 W second to second (a strict
        # alternation would be despiked flat).
        _insert_1hz(store, plug_id, start, 600, "50.0 + (i * 37) % 100")
        state = FloorState()
        state.calibrations[plug_id] = Calibration(idle_max_rsd=5.0, play_min_rsd=20.0)
        app = create_app(state, store)
        async with TestClient(TestServer(app)) as client:
            resp = await client.get(f"/api/machines/{plug_id}/readings?hours=1")
            data = await resp.json()
        assert data["bucket_seconds"] == width
        assert len(data["watts"]) == 200
        # Bucket means, not samples: every one sits strictly between the levels.
        assert all(50.0 < w < 150.0 for w in data["watts"])
        # The first few buckets are a partial classifier window; after that,
        # every minute of the ten was played.
        assert set(data["states"][5:]) == {"PLAYING"}

    @pytest.mark.asyncio
    async def test_hours_must_be_an_integer(self, store: Store) -> None:
        plug_id = store.ensure_plug("d1", "c1", "Plug 1")
        app = create_app(FloorState(), store)
        async with TestClient(TestServer(app)) as client:
            resp = await client.get(f"/api/machines/{plug_id}/readings?hours=abc")
            assert resp.status == 400
            assert (await resp.json())["error"] == "hours must be an integer"

    @pytest.mark.asyncio
    async def test_a_full_queue_answers_503_and_drains(self, store: Store) -> None:
        """One worker thread, a public route: admission is bounded.

        The slot is held until the thread finishes, not until the awaiting
        handler returns, so a client that disconnects mid-query cannot free a
        slot the worker is still using.
        """
        import threading

        plug_id = store.ensure_plug("d1", "c1", "Plug 1")
        app = create_app(FloorState(), store)
        worker: ChartWorker = app["chart_worker"]
        gate = threading.Event()
        real = worker._series

        def slow(*args):
            gate.wait(5)
            return real(*args)

        worker._series = slow  # type: ignore[method-assign]
        async with TestClient(TestServer(app)) as client:
            url = f"/api/machines/{plug_id}/readings"
            inflight = [asyncio.create_task(client.get(url)) for _ in range(MAX_QUEUED)]
            await asyncio.sleep(0.1)  # let every one of them reach the worker
            resp = await client.get(url)
            assert resp.status == 503
            assert resp.headers["Retry-After"] == "1"
            gate.set()
            done = await asyncio.gather(*inflight)
            assert {r.status for r in done} == {200}
            # The queue drained: the next request is admitted.
            assert (await client.get(url)).status == 200

    @pytest.mark.asyncio
    async def test_hours_is_clamped(self, store: Store) -> None:
        """The route is public-readable and the worker is one thread: a huge
        window must not hand it every retained row."""
        plug_id = store.ensure_plug("d1", "c1", "Plug 1")
        app = create_app(FloorState(), store)
        async with TestClient(TestServer(app)) as client:
            resp = await client.get(f"/api/machines/{plug_id}/readings?hours=100000")
            data = await resp.json()
            assert data["bucket_seconds"] == bucket_seconds(MAX_HOURS)
            resp = await client.get(f"/api/machines/{plug_id}/readings?hours=0")
            data = await resp.json()
            assert data["bucket_seconds"] == bucket_seconds(1)
