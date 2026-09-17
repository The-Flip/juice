"""The machine page's power chart: a day of readings, sized for a screen.

`GET /api/machines/{plug_id}/readings` used to return every raw reading in
the window. Under the tap collector that is 86,400 rows for 24 h -- a 4.4 MB
JSON body, classified, serialised and gzipped on the event loop (~400 ms,
measured), during which every command result from the floor waited behind
it. The chart it feeds is at most 1200 px wide.

So the window is bucketed to about `CHART_POINTS` points: the watts of a
bucket are the mean of its samples, and its activity is the majority vote of
their classifications -- the classifier still runs on the raw cadence its
calibration was made for, because a minute mean has no flicker left to call
PLAYING. All of it runs on `ChartWorker`'s thread with its own connection,
and the handler only serialises the ~1,440 rows that come back.
"""

from __future__ import annotations

import asyncio
import math
import threading
from collections import Counter
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from itertools import groupby

from juice.state import Activity, Calibration, classify
from juice.store import Store

# Points a chart window is bucketed down to. A 24 h window becomes 60 s
# buckets; the chart draws at most 1200 px, so ~76 s per pixel.
CHART_POINTS = 1440

# The widest window the chart will serve. The route is public-readable and the
# worker is one thread: `?hours=100000` would hand it every retained row for
# the plug and queue every other chart behind it.
MAX_HOURS = 24 * 7


def bucket_seconds(hours: float) -> int:
    """Bucket width that fits `hours` into at most `CHART_POINTS` points."""
    return max(1, math.ceil(hours * 3600 / CHART_POINTS))


@dataclass
class Series:
    epochs: list[float] = field(default_factory=list)
    watts: list[float | None] = field(default_factory=list)
    activities: list[Activity | None] = field(default_factory=list)


def bucket_series(
    epochs: Sequence[float],
    watts: Sequence[float | None],
    activities: Sequence[Activity | None],
    width: int,
) -> Series:
    """Fold a per-sample series into `width`-second buckets aligned to the epoch.

    Watts is the mean of the measured samples (None when none were). The
    activity is the most common one, `None` (not drawing) counting like any
    other so a bucket that was mostly not drawing reads that way. Majority
    rather than the tile sparkline's last-in-bucket (`_downsample_spark`): a
    sparkline wants recency, a band across a minute wants what the minute
    mostly was.
    """
    out = Series()
    rows = zip(epochs, watts, activities, strict=True)
    for start, group in groupby(rows, key=lambda r: math.floor(r[0] / width) * width):
        bucket = list(group)
        measured = [w for _, w, _ in bucket if w is not None]
        votes = Counter(a for _, _, a in bucket)
        out.epochs.append(float(start))
        out.watts.append(sum(measured) / len(measured) if measured else None)
        out.activities.append(votes.most_common(1)[0][0])
    return out


def chart_series(
    store: Store,
    conn,
    plug_id: int,
    since: datetime,
    width: int,
    calibration: Calibration,
) -> Series:
    """The bucketed series for one plug, on the caller's connection."""
    rows = store.get_readings_since(plug_id, since, conn=conn)
    epochs = [r[0] for r in rows]
    watts = [r[1] for r in rows]
    return bucket_series(epochs, watts, classify(watts, calibration), width)


class ChartWorker:
    """One thread and one connection for the chart queries.

    The pattern is `IngestWriter`'s: `Store._conn` belongs to the event loop
    and DuckDB connections are not thread-safe, so the worker gets its own via
    `Store.new_connection`, made lazily on its thread, and settles it after
    every query so an idle chart worker pins no transaction (`Store.settle`).
    One thread is deliberate: `classify` on a day of samples holds the GIL
    for ~200 ms (the loop's tick latency meanwhile is the 5 ms switch
    interval, measured), and one chart at a time bounds the memory.
    """

    def __init__(self, store: Store) -> None:
        self._store = store
        self._local = threading.local()
        self._pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="juice-chart")

    def _conn(self):
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._local.conn = self._store.new_connection()
        return conn

    def _series(
        self, plug_id: int, since: datetime, width: int, calibration: Calibration
    ) -> Series:
        conn = self._conn()
        try:
            return chart_series(self._store, conn, plug_id, since, width, calibration)
        finally:
            self._store.settle(conn)

    async def series(
        self, plug_id: int, since: datetime, width: int, calibration: Calibration
    ) -> Series:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._pool, self._series, plug_id, since, width, calibration
        )

    def close(self) -> None:
        self._pool.shutdown(wait=True)


def iso_z(epoch: float) -> str:
    """The wire timestamp v1 has always used: ISO-8601, naive, `Z`-suffixed."""
    return datetime.fromtimestamp(epoch, UTC).replace(tzinfo=None).isoformat() + "Z"
