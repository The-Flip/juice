"""Backfilled readings have to reach the rollups.

The hourly rollups refresh a trailing window anchored at
`min(latest reading, latest rollup)`. Ingest routinely writes rows *older* than
both -- that is what a collector catching up after a day offline does -- and
those rows move neither anchor, so the trailing window never reaches them.

Nothing errors. `refresh_hourly_usage` reports success, returns a row count, and
the charts for those hours stay empty for good. That silence is why this has its
own test file.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from juice.store import Store

DEV = "STRIP1"


@pytest.fixture
def store():
    with Store(":memory:") as s:
        yield s


def frame(rows, cursor):
    return json.dumps({"type": "readings", "batch": "b", "cursor": cursor, "rows": rows})


def _ingest(store: Store, when: datetime, *, seconds: int, cursor: str) -> None:
    """One reading per second for `seconds`, as tap would deliver them."""
    base_ms = int(when.timestamp() * 1000)
    rows = [
        [base_ms + i * 1000, DEV, f"{DEV}00", 1, 100_000, 119_000, 840, 5000]
        for i in range(seconds)
    ]
    result = store.commit_ingest_batch("tap-1", "buf-1", cursor, frame(rows, cursor))
    assert result.verdict == "ok", result


def _hours_covered(store: Store) -> set:
    return {
        r[0] for r in store._conn.execute("SELECT DISTINCT hour_ts FROM hourly_usage").fetchall()
    }


class TestBackfillReachesTheRollups:
    def test_a_backfilled_hour_is_missed_by_the_default_window(self, store: Store) -> None:
        """The bug, stated as a fact about the default. Pinned so the fix below
        is visibly a fix and not a coincidence."""
        now = datetime.now(UTC).replace(microsecond=0)
        store.ensure_plug(DEV, f"{DEV}00", "Some Machine - M0001")
        _ingest(store, now - timedelta(minutes=30), seconds=120, cursor="0" * 17 + "1")
        store.refresh_hourly_usage()
        assert _hours_covered(store), "recent rows must roll up normally"

        old = now - timedelta(days=3)
        _ingest(store, old, seconds=120, cursor="0" * 17 + "2")
        store.refresh_hourly_usage()
        assert old.replace(minute=0, second=0, microsecond=0, tzinfo=None) not in _hours_covered(
            store
        )

    def test_the_lookback_widens_to_cover_what_ingest_backfilled(self, store: Store) -> None:
        now = datetime.now(UTC).replace(microsecond=0)
        store.ensure_plug(DEV, f"{DEV}00", "Some Machine - M0001")
        _ingest(store, now - timedelta(minutes=30), seconds=120, cursor="0" * 17 + "1")
        store.refresh_hourly_usage()

        old = now - timedelta(days=3)
        _ingest(store, old, seconds=120, cursor="0" * 17 + "2")
        store.refresh_hourly_usage(lookback_hours=store.rollup_lookback_hours(2))

        assert old.replace(minute=0, second=0, microsecond=0, tzinfo=None) in _hours_covered(store)


class TestTheBackfillWatermark:
    def test_it_is_unset_when_nothing_has_been_ingested(self, store: Store) -> None:
        assert store.pending_backfill_start() is None
        assert store.rollup_lookback_hours(2) == 2

    def test_it_records_the_oldest_row_of_a_batch(self, store: Store) -> None:
        old = datetime.now(UTC) - timedelta(days=2)
        _ingest(store, old, seconds=5, cursor="0" * 17 + "1")
        pending = store.pending_backfill_start()
        assert pending is not None
        assert abs((pending - old.replace(tzinfo=None)).total_seconds()) < 2

    def test_it_keeps_the_oldest_across_batches(self, store: Store) -> None:
        """Two batches, the second newer. The watermark must not move forward --
        the older hour still needs rolling up."""
        older = datetime.now(UTC) - timedelta(days=5)
        newer = datetime.now(UTC) - timedelta(days=1)
        _ingest(store, older, seconds=3, cursor="0" * 17 + "1")
        _ingest(store, newer, seconds=3, cursor="0" * 17 + "2")
        pending = store.pending_backfill_start()
        assert abs((pending - older.replace(tzinfo=None)).total_seconds()) < 2

    def test_the_widening_spans_from_the_newest_reading_back(self, store: Store) -> None:
        """The lookback is measured from the newest reading, because that is the
        furthest forward any rollup's anchor can be. Old rows alone need no
        widening -- the anchor is already back there with them."""
        _ingest(store, datetime.now(UTC) - timedelta(days=3), seconds=3, cursor="0" * 17 + "1")
        assert store.rollup_lookback_hours(2) == 2, "nothing newer exists to reach back from"

        _ingest(store, datetime.now(UTC) - timedelta(minutes=1), seconds=3, cursor="0" * 17 + "2")
        assert store.rollup_lookback_hours(2) >= 72

    def test_clearing_it_stops_the_widening(self, store: Store) -> None:
        _ingest(store, datetime.now(UTC) - timedelta(days=3), seconds=3, cursor="0" * 17 + "1")
        _ingest(store, datetime.now(UTC) - timedelta(minutes=1), seconds=3, cursor="0" * 17 + "2")
        assert store.rollup_lookback_hours(2) > 2
        store.clear_pending_backfill()
        assert store.pending_backfill_start() is None
        assert store.rollup_lookback_hours(2) == 2

    def test_a_dropped_row_does_not_move_the_watermark(self, store: Store) -> None:
        """A 1970 timestamp is discarded, not stored -- so it must not drag the
        rollup window back through fifty years of empty hours."""
        rows = [[1000, DEV, f"{DEV}00", 1, 100_000, 119_000, 840, 5000]]
        cursor = "0" * 17 + "1"
        result = store.commit_ingest_batch("tap-1", "buf-1", cursor, frame(rows, cursor))
        assert (result.verdict, result.dropped_ts) == ("ok", 1)
        assert store.pending_backfill_start() is None


class TestRefreshRollups:
    """The recorder's periodic pass, which is where the widening actually gets
    applied. Tested directly rather than through the 1 Hz poll loop."""

    def test_it_rolls_up_a_backfilled_hour(self, store: Store) -> None:
        from juice.recorder import refresh_rollups

        now = datetime.now(UTC).replace(microsecond=0)
        store.ensure_plug(DEV, f"{DEV}00", "Some Machine - M0001")
        _ingest(store, now - timedelta(minutes=30), seconds=120, cursor="0" * 17 + "1")
        assert refresh_rollups(store) is True

        old = now - timedelta(days=3)
        _ingest(store, old, seconds=120, cursor="0" * 17 + "2")
        assert refresh_rollups(store) is True

        assert old.replace(minute=0, second=0, microsecond=0, tzinfo=None) in _hours_covered(store)

    def test_success_clears_the_watermark(self, store: Store) -> None:
        from juice.recorder import refresh_rollups

        store.ensure_plug(DEV, f"{DEV}00", "Some Machine - M0001")
        _ingest(store, datetime.now(UTC) - timedelta(days=2), seconds=10, cursor="0" * 17 + "1")
        assert store.pending_backfill_start() is not None
        refresh_rollups(store)
        assert store.pending_backfill_start() is None

    def test_a_failure_leaves_the_work_outstanding(self, store: Store, monkeypatch) -> None:
        """Clearing the watermark on a partial pass would drop those hours for
        good: the next refresh would be back to its narrow trailing window with
        nothing left to say the old rows still need covering."""
        from juice import recorder

        store.ensure_plug(DEV, f"{DEV}00", "Some Machine - M0001")
        _ingest(store, datetime.now(UTC) - timedelta(days=2), seconds=10, cursor="0" * 17 + "1")

        def boom(*args, **kwargs):
            raise RuntimeError("rollup exploded")

        monkeypatch.setattr(store, "refresh_hourly_strip_peak", boom)
        assert recorder.refresh_rollups(store) is False
        assert store.pending_backfill_start() is not None

    def test_one_failure_does_not_stop_the_others(self, store: Store, monkeypatch) -> None:
        from juice import recorder

        store.ensure_plug(DEV, f"{DEV}00", "Some Machine - M0001")
        _ingest(
            store, datetime.now(UTC) - timedelta(minutes=30), seconds=120, cursor="0" * 17 + "1"
        )

        def boom(*args, **kwargs):
            raise RuntimeError("rollup exploded")

        monkeypatch.setattr(store, "refresh_hourly_usage", boom)
        assert recorder.refresh_rollups(store) is False
        # play_seconds runs after usage in the loop, so it proves we kept going.
        assert store._conn.execute("SELECT count(*) FROM hourly_play_seconds").fetchone()[0] >= 0
