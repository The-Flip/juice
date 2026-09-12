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

from juice.state import Calibration
from juice.store import MAX_ROLLUP_LOOKBACK_HOURS, Store

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


class TestTheLookbackIsBounded:
    """The widening feeds `refresh_hourly_play_seconds`, which reads every
    reading of every calibrated plug into Python and runs `classify()` over it --
    4.4s measured for a 73h/4-plug window, and it also drives a *destructive*
    delete of its own window. `_INGEST_TS_FLOOR_MS` accepts any timestamp after
    2025-01-01, so without a ceiling one batch from a tap with a stale clock asks
    for a window thousands of hours wide.
    """

    def test_a_stale_clock_cannot_widen_the_window_without_limit(self, store: Store) -> None:
        stale = datetime(2025, 1, 2, tzinfo=UTC)
        uncapped = (datetime.now(UTC) - stale).total_seconds() / 3600
        assert uncapped > MAX_ROLLUP_LOOKBACK_HOURS * 2, (
            "the fixture only proves something while it is far past the cap"
        )
        _ingest(store, datetime.now(UTC) - timedelta(minutes=1), seconds=5, cursor="0" * 17 + "1")
        _ingest(store, stale, seconds=5, cursor="0" * 17 + "2")
        assert store.rollup_lookback_hours(2) == MAX_ROLLUP_LOOKBACK_HOURS

    def test_a_legitimate_replay_of_taps_whole_buffer_still_fits(self, store: Store) -> None:
        """The cap sits above tap's own buffer retention on purpose: 30 days is
        the most it can ever hold, so no honest catch-up is truncated by this.
        That is what makes retiring a mark beyond the cap the right answer rather
        than data loss -- past there the timestamp is a fault, not history."""
        _ingest(store, datetime.now(UTC) - timedelta(minutes=1), seconds=5, cursor="0" * 17 + "1")
        _ingest(store, datetime.now(UTC) - timedelta(days=30), seconds=5, cursor="0" * 17 + "2")
        lookback = store.rollup_lookback_hours(2)
        assert lookback < MAX_ROLLUP_LOOKBACK_HOURS, "a full-buffer replay is not capped"
        assert lookback >= 30 * 24, "and it does reach all the way back"

    def test_the_default_is_never_narrowed(self, store: Store) -> None:
        """The cap is a ceiling on the *widening*, not on the window a caller
        asked for. Spelling it `min(max(default, hours), CAP)` -- the obvious way
        to write a clamp -- would silently narrow an over-cap default whenever a
        backfill happened to be pending, so the pending case is the one that has
        to be asserted; with nothing pending the function returns before it ever
        reaches the cap.
        """
        _ingest(store, datetime.now(UTC) - timedelta(minutes=1), seconds=5, cursor="0" * 17 + "1")
        _ingest(store, datetime.now(UTC) - timedelta(days=3), seconds=5, cursor="0" * 17 + "2")
        assert store.pending_backfill_start() is not None, "the pending path is the risky one"

        wide = MAX_ROLLUP_LOOKBACK_HOURS * 3
        assert store.rollup_lookback_hours(wide) == wide


class TestABacklogWiderThanTheWindow:
    """A trailing window is anchored at the newest reading, so past the cap no
    lookback value reaches the oldest hours -- clamping alone would roll up the
    recent end and strand the rest for good.

    And it would strand them *invisibly twice over*: `prunable_before` floors the
    prune cutoff at the pending mark, so a mark that can never be retired freezes
    retention at a fixed point while raw keeps growing. At tap's ~4.2M rows a day
    that is a disk problem in weeks. So this width switches strategy rather than
    narrowing the window, and these tests are about it converging.

    The reachable trigger is not only a broken clock: `[tap].retention_days` has
    no ceiling, so a 60-day buffer plus a long outage gets here legitimately.
    """

    def _seed_over_cap(self, store: Store) -> datetime:
        """Readings now and far past the cap, with the rollups already non-fresh
        (a fresh table takes the `MIN(ts)` full-backfill branch and masks this)."""
        pid = store.ensure_plug(DEV, f"{DEV}00", "Some Machine - M0001", has_emeter=True)
        mid = store.ensure_machine("M0001", "Some Machine")
        store.update_assignment(pid, mid, datetime.now(UTC) - timedelta(days=400))
        store.set_calibration(mid, Calibration(idle_max_rsd=None, play_min_rsd=10.0))
        _ingest(store, datetime.now(UTC) - timedelta(minutes=1), seconds=60, cursor="0" * 17 + "1")
        store.refresh_hourly_usage()
        old = datetime.now(UTC) - timedelta(hours=MAX_ROLLUP_LOOKBACK_HOURS + 24 * 13)
        _ingest(store, old, seconds=60, cursor="0" * 17 + "2")
        return old

    def test_the_backlog_is_covered_and_the_mark_retired(self, store: Store) -> None:
        from juice.rollups import refresh_rollups

        old = self._seed_over_cap(store)
        assert store.backfill_span_hours() > MAX_ROLLUP_LOOKBACK_HOURS

        assert refresh_rollups(store) is True

        hour = old.replace(minute=0, second=0, microsecond=0, tzinfo=None)
        rolled = store._conn.execute(
            "SELECT count(*) FROM hourly_usage WHERE hour_ts = ?", [hour]
        ).fetchone()[0]
        assert rolled > 0, "the hours past the cap must actually be rolled up"
        assert store.pending_backfill_start() is None, (
            "and the mark retired, since the pass really did cover it"
        )

    def test_retention_is_not_frozen_afterwards(self, store: Store) -> None:
        """The consequence that makes stranding unacceptable rather than untidy:
        a mark nothing can retire pins the prune cutoff forever."""
        from juice.rollups import refresh_rollups

        self._seed_over_cap(store)
        refresh_rollups(store)
        store.mark_migration("retro_play_hours_v1")

        assert store.pending_backfill_start() is None
        cutoff = store.prunable_before(31)
        # With no mark left, the ordinary bounds apply again rather than a fixed
        # point that never advances.
        assert cutoff is None or cutoff > datetime.now(UTC).replace(tzinfo=None) - timedelta(
            days=MAX_ROLLUP_LOOKBACK_HOURS // 24 + 20
        )

    def test_it_converges_rather_than_repeating_forever(self, store: Store) -> None:
        """The property chunking would also have bought: a second pass has nothing
        left to do, so this is not a full rebuild every 60 seconds."""
        from juice import rollups

        self._seed_over_cap(store)
        assert rollups.refresh_rollups(store) is True

        calls: list[int] = []
        real = store.rebuild_play_hours
        store.rebuild_play_hours = lambda mid, conn=None: (  # type: ignore[method-assign]
            calls.append(mid),
            real(mid, conn),
        )[1]
        assert rollups.refresh_rollups(store) is True
        assert calls == [], "the second pass must not rebuild again"

    def test_an_ordinary_backfill_uses_the_trailing_window(self, store: Store) -> None:
        """The cheap path must stay cheap: a normal catch-up never rebuilds."""
        from juice import rollups

        pid = store.ensure_plug(DEV, f"{DEV}00", "Some Machine - M0001", has_emeter=True)
        mid = store.ensure_machine("M0001", "Some Machine")
        store.update_assignment(pid, mid, datetime.now(UTC) - timedelta(days=10))
        store.set_calibration(mid, Calibration(idle_max_rsd=None, play_min_rsd=10.0))
        _ingest(store, datetime.now(UTC) - timedelta(minutes=1), seconds=60, cursor="0" * 17 + "1")
        store.refresh_hourly_usage()
        _ingest(store, datetime.now(UTC) - timedelta(days=3), seconds=60, cursor="0" * 17 + "2")
        assert store.backfill_span_hours() <= MAX_ROLLUP_LOOKBACK_HOURS

        calls: list[int] = []
        store.rebuild_play_hours = lambda mid, conn=None: calls.append(mid)  # type: ignore[method-assign]
        assert rollups.refresh_rollups(store) is True
        assert calls == [], "an in-window backfill must not trigger a rebuild"
        assert store.pending_backfill_start() is None


class TestRefreshRollups:
    """The recorder's periodic pass, which is where the widening actually gets
    applied. Tested directly rather than through the 1 Hz poll loop."""

    def test_it_rolls_up_a_backfilled_hour(self, store: Store) -> None:
        from juice.rollups import refresh_rollups

        now = datetime.now(UTC).replace(microsecond=0)
        store.ensure_plug(DEV, f"{DEV}00", "Some Machine - M0001")
        _ingest(store, now - timedelta(minutes=30), seconds=120, cursor="0" * 17 + "1")
        assert refresh_rollups(store) is True

        old = now - timedelta(days=3)
        _ingest(store, old, seconds=120, cursor="0" * 17 + "2")
        assert refresh_rollups(store) is True

        assert old.replace(minute=0, second=0, microsecond=0, tzinfo=None) in _hours_covered(store)

    def test_success_clears_the_watermark(self, store: Store) -> None:
        from juice.rollups import refresh_rollups

        store.ensure_plug(DEV, f"{DEV}00", "Some Machine - M0001")
        _ingest(store, datetime.now(UTC) - timedelta(days=2), seconds=10, cursor="0" * 17 + "1")
        assert store.pending_backfill_start() is not None
        refresh_rollups(store)
        assert store.pending_backfill_start() is None

    def test_a_failure_leaves_the_work_outstanding(self, store: Store, monkeypatch) -> None:
        """Clearing the watermark on a partial pass would drop those hours for
        good: the next refresh would be back to its narrow trailing window with
        nothing left to say the old rows still need covering."""
        from juice import rollups

        store.ensure_plug(DEV, f"{DEV}00", "Some Machine - M0001")
        _ingest(store, datetime.now(UTC) - timedelta(days=2), seconds=10, cursor="0" * 17 + "1")

        def boom(*args, **kwargs):
            raise RuntimeError("rollup exploded")

        monkeypatch.setattr(store, "refresh_hourly_strip_peak", boom)
        assert rollups.refresh_rollups(store) is False
        assert store.pending_backfill_start() is not None

    def test_one_failure_does_not_stop_the_others(self, store: Store, monkeypatch) -> None:
        from juice import rollups

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
        assert rollups.refresh_rollups(store) is False
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

        Before this branch `_BACKFILL_SQL` merged into one row with `LEAST`, so a
        batch landing mid-refresh whose oldest row was *newer* than the captured
        mark left `oldest_ts` byte-identical. Comparing the timestamp against itself then says
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
        from juice import rollups

        store.ensure_plug(DEV, f"{DEV}00", "Some Machine - M0001")
        _ingest(store, datetime.now(UTC) - timedelta(days=2), seconds=10, cursor="0" * 17 + "1")

        def boom(*args, **kwargs):
            raise RuntimeError("the writer holds the table")

        monkeypatch.setattr(store, "clear_pending_backfill", boom)
        assert rollups.refresh_rollups(store) is False
        assert store.pending_backfill_start() is not None, "the work stays outstanding"
