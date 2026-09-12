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
        store.clear_pending_backfill(store.backfill_mark())
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

        called: list[str] = []
        real_play = store.refresh_hourly_play_seconds

        def record_play(*args, **kwargs):
            called.append("play_seconds")
            return real_play(*args, **kwargs)

        monkeypatch.setattr(store, "refresh_hourly_usage", boom)
        monkeypatch.setattr(store, "refresh_hourly_play_seconds", record_play)
        assert recorder.refresh_rollups(store) is False
        # play_seconds runs after usage in the loop, so reaching it is the proof
        # that a failure did not abandon the rest. Counting rows would not be:
        # `count(*) >= 0` holds just as well for a loop that returned early.
        assert called == ["play_seconds"]


class TestClearingTheWatermarkIsScopedToWhatWasCovered:
    """The ingest writer commits on its own connection while the rollups run, so
    a batch can land mid-refresh -- writing rows the refresh never read. Retiring
    the mark anyway skips those hours for good with nothing reporting a problem.

    Two failure directions, and they pull against each other, which is why both
    are pinned here: retire too eagerly and those hours are lost; refuse too
    readily and the mark is pinned forever, keeping every pass on the widened
    scan. `ingest_backfill` holds one row per commit and a pass deletes only up
    to the id it captured, which is what satisfies both.
    """

    def test_a_mark_lowered_mid_refresh_survives(self, store: Store) -> None:
        store.ensure_plug(DEV, f"{DEV}00", "Some Machine - M0001")
        _ingest(store, datetime.now(UTC) - timedelta(days=2), seconds=10, cursor="0" * 17 + "1")
        covered = store.backfill_mark()
        assert covered is not None

        # A second tap batch, older still, arrives while the refresh is running.
        older = datetime.now(UTC) - timedelta(days=5)
        _ingest(store, older, seconds=10, cursor="0" * 17 + "2")
        lowered = store.backfill_mark()
        assert lowered is not None and lowered.oldest_ts < covered.oldest_ts

        store.clear_pending_backfill(covered)
        assert store.backfill_mark() == lowered, (
            "a mark reaching further back than the refresh did must outlive it"
        )

    def test_the_mark_the_refresh_covered_is_cleared(self, store: Store) -> None:
        store.ensure_plug(DEV, f"{DEV}00", "Some Machine - M0001")
        _ingest(store, datetime.now(UTC) - timedelta(days=2), seconds=10, cursor="0" * 17 + "1")
        covered = store.backfill_mark()
        store.clear_pending_backfill(covered)
        assert store.pending_backfill_start() is None

    def test_a_mark_replaced_mid_refresh_at_the_same_timestamp_survives(self, store: Store) -> None:
        """The near-miss the timestamp comparison cannot see.

        `_BACKFILL_SQL` merges with `LEAST`, so a batch landing mid-refresh whose
        oldest row is *newer* than the captured mark leaves `oldest_ts`
        byte-identical. Comparing the timestamp against itself then says
        "covered" about hours no refresh read, and the delete fires. The mark
        needs a second witness that moves on every commit, not one that moves
        only when the batch happens to reach further back.
        """
        store.ensure_plug(DEV, f"{DEV}00", "Some Machine - M0001")
        # A current batch, so there is a "newest reading" for the widening to be
        # measured back from, then the five-day-old catch-up the pass covers.
        _ingest(store, datetime.now(UTC) - timedelta(minutes=1), seconds=5, cursor="0" * 17 + "1")
        _ingest(store, datetime.now(UTC) - timedelta(days=5), seconds=10, cursor="0" * 17 + "2")
        mark = store.backfill_mark()
        assert mark is not None

        # Newer than the mark, so the oldest pending timestamp does not move --
        # but still old enough that its own hours need rolling up.
        _ingest(store, datetime.now(UTC) - timedelta(days=3), seconds=10, cursor="0" * 17 + "3")
        after = store.backfill_mark()
        assert after is not None and after.oldest_ts == mark.oldest_ts, (
            "this is the case a timestamp comparison cannot detect"
        )

        store.clear_pending_backfill(mark)
        assert store.backfill_mark() is not None, (
            "a batch that landed mid-refresh must keep the mark alive"
        )
        assert store.rollup_lookback_hours(2) > 2, (
            "and the next pass must still be widened to reach those hours"
        )

    def test_the_mark_still_retires_while_a_tap_keeps_streaming(self, store: Store) -> None:
        """The other half of the requirement, and the one a naive
        compare-and-swap fails.

        A healthy tap commits about a batch a second and a rollup pass takes
        seconds, so *every* pass has a batch land while it runs. A witness that
        only says "something committed" therefore refuses every clear, and the
        mark is pinned forever -- which keeps `rollup_lookback_hours` widened to
        the full backfill width and makes every 60s pass redo the expensive
        scan. Retiring what a pass covered has to survive a continuously
        streaming collector.
        """
        store.ensure_plug(DEV, f"{DEV}00", "Some Machine - M0001")
        # A current batch first, so there is a "newest reading" to measure the
        # widening back from, then the five-day-old catch-up.
        _ingest(store, datetime.now(UTC) - timedelta(minutes=1), seconds=5, cursor="0" * 17 + "1")
        _ingest(store, datetime.now(UTC) - timedelta(days=5), seconds=10, cursor="0" * 17 + "2")
        assert store.rollup_lookback_hours(2) > 2, "the backfill widens the window"

        # Three passes, each with a batch landing mid-pass, as steady state does.
        for i in range(3):
            mark = store.backfill_mark()
            _ingest(
                store,
                datetime.now(UTC) - timedelta(minutes=1),
                seconds=5,
                cursor="0" * 17 + str(i + 3),
            )
            store.clear_pending_backfill(mark)

        assert store.rollup_lookback_hours(2) == 2, (
            "the five-day-old hours were covered and must have been retired; "
            "only the batch that landed during the last pass is still pending"
        )

    def test_nothing_pending_clears_nothing(self, store: Store) -> None:
        """A refresh that began with no mark is not entitled to clear one that
        appeared while it ran."""
        store.ensure_plug(DEV, f"{DEV}00", "Some Machine - M0001")
        store.clear_pending_backfill(None)
        _ingest(store, datetime.now(UTC) - timedelta(days=2), seconds=10, cursor="0" * 17 + "1")
        store.clear_pending_backfill(None)
        assert store.pending_backfill_start() is not None


class TestAFailingClearCannotStopRecording:
    """`refresh_rollups` is called from the recorder's poll loop, and in
    `serve_cmd` an exception out of it propagates through `asyncio.gather` and
    takes the server down with the recorder. Every refresh is already wrapped
    for that reason; the clear was not."""

    def test_a_raising_clear_is_reported_not_raised(self, store: Store, monkeypatch) -> None:
        from juice import recorder

        store.ensure_plug(DEV, f"{DEV}00", "Some Machine - M0001")
        _ingest(store, datetime.now(UTC) - timedelta(days=2), seconds=10, cursor="0" * 17 + "1")

        def boom(*args, **kwargs):
            raise RuntimeError("the writer holds the table")

        monkeypatch.setattr(store, "clear_pending_backfill", boom)
        assert recorder.refresh_rollups(store) is False
        assert store.pending_backfill_start() is not None, "the work stays outstanding"
