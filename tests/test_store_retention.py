"""Pruning raw readings, and the guards that decide when it must not happen.

tap at 1 Hz produces ~4.23M rows/day against today's ~200k, into a table that
has never had a row deleted. Pruning is how that stays survivable.

Almost everything here is about refusing to prune. Raw readings are the only
copy of the data: the rollups are derived from them and several code paths
rebuild from them. A delete that runs an hour too early is not a performance
regression, it is history that no longer exists.
"""

from __future__ import annotations

import contextlib
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from juice.recorder import RETRO_PLAY_HOURS_MIGRATION
from juice.state import Calibration
from juice.store import MIN_RETENTION_DAYS, Store, _local_hour

DEV = "STRIP1"


@pytest.fixture
def store():
    with Store(":memory:") as s:
        yield s


def _seed(store: Store, *, days: int = 45) -> int:
    """Readings every 30 minutes across `days`, with all four rollups built.

    45 days rather than months: enough that a 31-day retention has two weeks
    to prune, small enough that `refresh_hourly_play_seconds` -- which walks
    each plug's rows in Python -- does not dominate the test run.
    """
    plug_id = store.ensure_plug(DEV, f"{DEV}00", "Some Machine - M0001")
    machine_id = store.ensure_machine("M0001", "Some Machine")
    # All four rollups must be non-empty or the guards (correctly) refuse to
    # prune at all, so the fixture needs a circuit and a calibration too.
    circuit_id = store.create_circuit("A", "1", "test circuit", amps=20.0)
    store.set_device_circuit(DEV, circuit_id)
    store.set_calibration(machine_id, Calibration(idle_max_rsd=0.05, play_min_rsd=0.15))
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    start = now - timedelta(days=days)
    store.update_assignment(plug_id, machine_id, start)
    # Seeded in SQL rather than through insert_readings: DuckDB's parameter
    # binding runs at ~425 rows/s, which turns a realistic multi-month fixture
    # into a minute of waiting per test.
    store._conn.execute(
        """
        INSERT INTO readings (ts, plug_id, watts, voltage, amps, total_kwh)
        SELECT ts, ?, 100.0, 119.0, 0.84, 5.0
        FROM generate_series(?::TIMESTAMP, ?::TIMESTAMP, INTERVAL 30 MINUTE) AS g(ts)
        """,
        [plug_id, start.replace(tzinfo=None), now.replace(tzinfo=None)],
    )
    store.refresh_hourly_usage(lookback_hours=days * 24)
    store.refresh_hourly_strip_peak(lookback_hours=days * 24)
    store.refresh_hourly_circuit_peak(lookback_hours=days * 24)
    store.refresh_hourly_play_seconds(lookback_hours=days * 24)
    store.mark_migration(RETRO_PLAY_HOURS_MIGRATION)
    return plug_id


class TestTheGuards:
    def test_zero_disables_pruning(self, store: Store) -> None:
        _seed(store)
        assert store.prunable_before(0) is None

    def test_a_retention_shorter_than_the_baseline_window_is_refused(self, store: Store) -> None:
        """`refresh_power_baselines` reads 30 days of raw. A shorter retention
        would quietly disarm overload protection rather than obviously break."""
        _seed(store)
        assert store.prunable_before(MIN_RETENTION_DAYS - 1) is None

    def test_nothing_is_pruned_before_the_rollups_have_consumed_it(self, store: Store) -> None:
        """The rollups are the reason short raw retention is acceptable at all.
        Deleting raw they have not read yet loses it from both places."""
        _seed(store)
        # Pretend hourly_usage has only been computed up to 40 days ago: the
        # 31-day cutoff must then be held back to that, not applied as asked.
        behind = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=40)
        store._conn.execute("DELETE FROM hourly_usage WHERE hour_ts > ?", [behind])
        cutoff = store.prunable_before(31)
        assert cutoff is not None
        assert cutoff <= behind

    def test_an_empty_rollup_table_blocks_pruning_entirely(self, store: Store) -> None:
        """An empty rollup means its next refresh does a full backfill from raw.
        Pruning first would make that backfill silently produce a truncated
        history and then look finished."""
        _seed(store)
        store._conn.execute("DELETE FROM hourly_play_seconds")
        assert store.prunable_before(31) is None

    def test_pruning_waits_for_the_retro_play_hours_migration(self, store: Store) -> None:
        """That migration backfills play-hours across all of history, once. Run
        it after a prune and the pruned span is simply absent from it."""
        _seed(store)
        store._conn.execute(
            "DELETE FROM applied_migrations WHERE name = ?", [RETRO_PLAY_HOURS_MIGRATION]
        )
        assert store.prunable_before(31) is None

    def test_an_empty_database_prunes_nothing(self, store: Store) -> None:
        assert store.prunable_before(31) is None


class TestPruning:
    def test_old_rows_go_and_recent_rows_stay(self, store: Store) -> None:
        _seed(store)
        cutoff = store.prunable_before(31)
        assert cutoff is not None
        before = store._conn.execute("SELECT count(*) FROM readings").fetchone()[0]
        deleted = store.prune_readings(cutoff)
        after = store._conn.execute("SELECT count(*) FROM readings").fetchone()[0]
        assert deleted > 0
        assert before - after == deleted
        assert (
            store._conn.execute("SELECT count(*) FROM readings WHERE ts < ?", [cutoff]).fetchone()[
                0
            ]
            == 0
        )

    def test_the_rollups_keep_the_history_the_raw_rows_no_longer_hold(self, store: Store) -> None:
        """The whole point: the detail goes, the shape stays."""
        _seed(store)
        hours_before = store._conn.execute("SELECT count(*) FROM hourly_usage").fetchone()[0]
        store.prune_readings(store.prunable_before(31))
        assert (
            store._conn.execute("SELECT count(*) FROM hourly_usage").fetchone()[0] == hours_before
        )

    def test_pruning_twice_is_harmless(self, store: Store) -> None:
        _seed(store)
        cutoff = store.prunable_before(31)
        store.prune_readings(cutoff)
        assert store.prune_readings(cutoff) == 0


class TestTheMarkAndTheDeleteCommitTogether:
    """`raw_prune_mark` is what later tells a rebuild that the hours before the
    cut are unrecomputable rather than merely absent. A delete that lands
    without its mark is therefore the worst of both: raw is gone, and nothing
    records that it went, so the next rebuild deletes rollup history it cannot
    regenerate and reports success.
    """

    class _FailsOnTheMark:
        """A connection that refuses the mark and passes everything else on."""

        def __init__(self, real) -> None:
            self._real = real
            self.rolled_back = False

        def execute(self, sql: str, *args):
            if "raw_prune_mark" in sql and sql.lstrip().upper().startswith("INSERT"):
                raise RuntimeError("no space left on device")
            if sql.strip().upper() == "ROLLBACK":
                self.rolled_back = True
            return self._real.execute(sql, *args)

    def test_a_mark_that_cannot_be_written_takes_the_delete_with_it(self, store: Store) -> None:
        _seed(store)
        cutoff = store.prunable_before(31)
        assert cutoff is not None
        doomed = "SELECT count(*) FROM readings WHERE ts < ?"
        before = store._conn.execute(doomed, [cutoff]).fetchone()[0]
        assert before > 0, "the fixture must give the prune something to delete"

        conn = self._FailsOnTheMark(store._conn)
        with pytest.raises(RuntimeError):
            store.prune_readings(cutoff, conn=conn)

        assert conn.rolled_back, "the transaction must be rolled back, not left open"
        assert store._conn.execute(doomed, [cutoff]).fetchone()[0] == before, (
            "raw was deleted with nothing recording that the cut happened"
        )
        assert store._unrecomputable_before() is None

    def test_a_successful_prune_leaves_the_mark_at_the_cut(self, store: Store) -> None:
        _seed(store)
        cutoff = store.prunable_before(31)
        store.prune_readings(cutoff)
        assert store._unrecomputable_before() == cutoff.replace(tzinfo=None)


class TestRebuildsSurvivePruning:
    """Two paths delete a rollup and recompute it from raw. After a prune the
    raw for older hours is gone, so a naive rebuild would erase history that
    cannot be regenerated -- and `rebuild_play_hours` runs on recalibration,
    which is a routine operator action, not an unusual one."""

    def test_rebuilding_circuit_peaks_keeps_unrecomputable_hours(self, store: Store) -> None:
        _seed(store)
        store.prune_readings(store.prunable_before(31))
        oldest_raw = store._conn.execute("SELECT MIN(ts) FROM readings").fetchone()[0]
        before = store._conn.execute(
            "SELECT count(*) FROM hourly_circuit_peak WHERE hour_ts < ?", [oldest_raw]
        ).fetchone()[0]
        store.rebuild_hourly_circuit_peak()
        after = store._conn.execute(
            "SELECT count(*) FROM hourly_circuit_peak WHERE hour_ts < ?", [oldest_raw]
        ).fetchone()[0]
        assert after == before

    def test_rebuilding_play_hours_keeps_unrecomputable_hours(self, store: Store) -> None:
        _seed(store)
        machine_id = store.ensure_machine("M0001", "Some Machine")
        store.prune_readings(store.prunable_before(31))
        oldest_raw = store._conn.execute("SELECT MIN(ts) FROM readings").fetchone()[0]
        before = store._conn.execute(
            "SELECT count(*) FROM hourly_play_seconds WHERE hour_local < ?", [oldest_raw]
        ).fetchone()[0]
        assert before > 0, "the fixture must leave some unrecomputable history"
        store.rebuild_play_hours(machine_id)
        after = store._conn.execute(
            "SELECT count(*) FROM hourly_play_seconds WHERE hour_local < ?", [oldest_raw]
        ).fetchone()[0]
        assert after == before


def _seed_varying(store: Store, *, days: int = 45) -> tuple[int, int]:
    """Like `_seed`, but with watts that fall through each hour, so an
    hour's peak lives in the part a mid-hour prune removes."""
    plug_id = store.ensure_plug(DEV, f"{DEV}00", "Some Machine - M0001")
    machine_id = store.ensure_machine("M0001", "Some Machine")
    circuit_id = store.create_circuit("A", "1", "test circuit", amps=20.0)
    store.set_device_circuit(DEV, circuit_id)
    store.set_calibration(machine_id, Calibration(idle_max_rsd=0.05, play_min_rsd=0.15))
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    start = now - timedelta(days=days)
    store.update_assignment(plug_id, machine_id, start)
    store._conn.execute(
        """
        INSERT INTO readings (ts, plug_id, watts, voltage, amps, total_kwh)
        SELECT ts, ?, 200.0 - extract(minute FROM ts), 119.0, 0.84, 5.0
        FROM generate_series(?::TIMESTAMP, ?::TIMESTAMP, INTERVAL 10 MINUTE) AS g(ts)
        """,
        [plug_id, start.replace(tzinfo=None), now.replace(tzinfo=None)],
    )
    store.refresh_hourly_usage(lookback_hours=days * 24)
    store.refresh_hourly_strip_peak(lookback_hours=days * 24)
    store.refresh_hourly_circuit_peak(lookback_hours=days * 24)
    store.refresh_hourly_play_seconds(lookback_hours=days * 24)
    store.mark_migration(RETRO_PLAY_HOURS_MIGRATION)
    return plug_id, machine_id


def _prune_mid_hour(store: Store) -> datetime:
    """Prune to half past an hour, and return that hour."""
    cutoff = store.prunable_before(31).replace(minute=30, second=0, microsecond=0)
    store.prune_readings(cutoff)
    return cutoff.replace(minute=0)


class TestTheBoundaryBucketIsNotRecomputedFromItsTail:
    """A prune cuts raw at an instant, not at an hour. The rollup bucket
    *containing* that instant keeps a value computed from the whole hour, while
    raw now holds only the hour's tail. Recomputing it from what survives
    silently replaces a correct number with a smaller one -- and unlike a
    deleted bucket, nothing about the result looks wrong.
    """

    def test_rebuilding_circuit_peaks_leaves_the_boundary_hour_alone(self, store: Store) -> None:
        _seed_varying(store)
        hour = _prune_mid_hour(store)
        peak = "SELECT peak_watts FROM hourly_circuit_peak WHERE hour_ts = ?"
        before = store._conn.execute(peak, [hour]).fetchone()
        assert before is not None, "the fixture must leave a boundary bucket to protect"

        store.rebuild_hourly_circuit_peak()

        after = store._conn.execute(peak, [hour]).fetchone()
        assert after is not None, "the boundary bucket must survive the rebuild"
        assert after[0] == before[0], (
            "the boundary hour was recomputed from the readings that survived the "
            f"prune: {before[0]} became {after[0]}"
        )

    def test_rebuilding_play_hours_leaves_the_boundary_hour_alone(self, store: Store) -> None:
        _plug_id, machine_id = _seed_varying(store)
        hour_utc = _prune_mid_hour(store)
        hour_local = _local_hour(hour_utc.replace(tzinfo=UTC), ZoneInfo("America/Chicago"))
        stored = (
            "SELECT on_seconds FROM hourly_play_seconds WHERE machine_id = ? AND hour_local = ?"
        )
        before = store._conn.execute(stored, [machine_id, hour_local]).fetchone()
        assert before is not None, "the fixture must leave a boundary bucket to protect"

        store.rebuild_play_hours(machine_id)

        after = store._conn.execute(stored, [machine_id, hour_local]).fetchone()
        assert after is not None, "the boundary bucket must survive the rebuild"
        assert after[0] == before[0], (
            "the boundary hour was recomputed from the readings that survived the "
            f"prune: {before[0]} became {after[0]}"
        )


class TestARefreshDoesNotReachPastThePruneCut:
    """The refreshes are windowed, and after a tap backfill that window is not
    small: `rollup_lookback_hours` widens it to span everything ingest just
    delivered, which can reach back past the prune cut. Each refresh then does
    damage of its own kind -- the peak tables overwrite the boundary bucket
    from the tail of raw that survived it, and play-hours, which deletes its
    window before reinserting, removes every pre-cut hour and has nothing left
    to put them back from.
    """

    LOOKBACK = 45 * 24

    def test_circuit_peaks_keep_the_boundary_hour(self, store: Store) -> None:
        _seed_varying(store)
        hour = _prune_mid_hour(store)
        peak = "SELECT peak_watts FROM hourly_circuit_peak WHERE hour_ts = ?"
        before = store._conn.execute(peak, [hour]).fetchone()
        assert before is not None, "the fixture must leave a boundary bucket to protect"

        store.refresh_hourly_circuit_peak(lookback_hours=self.LOOKBACK)

        assert store._conn.execute(peak, [hour]).fetchone() == before

    def test_strip_peaks_keep_the_boundary_hour(self, store: Store) -> None:
        _seed_varying(store)
        hour = _prune_mid_hour(store)
        peak = "SELECT peak_watts FROM hourly_strip_peak WHERE hour_ts = ?"
        before = store._conn.execute(peak, [hour]).fetchone()
        assert before is not None, "the fixture must leave a boundary bucket to protect"

        store.refresh_hourly_strip_peak(lookback_hours=self.LOOKBACK)

        assert store._conn.execute(peak, [hour]).fetchone() == before

    def test_usage_keeps_the_boundary_hour(self, store: Store) -> None:
        _seed_varying(store)
        hour = _prune_mid_hour(store)
        usage = "SELECT kwh, samples FROM hourly_usage WHERE hour_ts = ?"
        before = store._conn.execute(usage, [hour]).fetchone()
        assert before is not None, "the fixture must leave a boundary bucket to protect"

        store.refresh_hourly_usage(lookback_hours=self.LOOKBACK)

        assert store._conn.execute(usage, [hour]).fetchone() == before

    def test_play_hours_keep_everything_the_cut_put_out_of_reach(self, store: Store) -> None:
        """The destructive one. `refresh_hourly_play_seconds` wipes its window
        before reinserting, so a window reaching past the cut deletes hours it
        cannot recompute -- not a wrong number, an absent one."""
        _seed_varying(store)
        hour_utc = _prune_mid_hour(store)
        hour_local = _local_hour(hour_utc.replace(tzinfo=UTC), ZoneInfo("America/Chicago"))
        stored = "SELECT hour_local, on_seconds FROM hourly_play_seconds WHERE hour_local <= ?"
        before = store._conn.execute(stored, [hour_local]).fetchall()
        assert len(before) > 1, "the fixture must leave pre-cut hours to protect"

        store.refresh_hourly_play_seconds(lookback_hours=self.LOOKBACK)

        assert store._conn.execute(stored, [hour_local]).fetchall() == before


class TestTheRetentionTask:
    def test_prune_once_respects_the_guards(self, store: Store) -> None:
        from juice.retention import prune_once

        _seed(store)
        store._conn.execute("DELETE FROM hourly_play_seconds")
        assert prune_once(store, 31) == 0

    def test_prune_once_deletes_when_it_is_safe(self, store: Store) -> None:
        from juice.retention import prune_once

        _seed(store)
        assert prune_once(store, 31) > 0

    def test_zero_days_prunes_nothing(self, store: Store) -> None:
        from juice.retention import prune_once

        _seed(store)
        assert prune_once(store, 0) == 0

    async def test_the_loop_returns_immediately_when_disabled(self, store: Store) -> None:
        """Disabled must mean the task exits, not that it spins forever doing
        nothing -- `serve` gathers it alongside the recorder."""
        import asyncio

        from juice.retention import retention_loop

        await asyncio.wait_for(retention_loop(store, 0), timeout=2)

    async def test_a_failing_pass_does_not_kill_the_loop(self, store: Store, monkeypatch) -> None:
        import asyncio

        from juice import retention

        calls = []

        def boom(*args, **kwargs):
            calls.append(1)
            raise RuntimeError("prune exploded")

        monkeypatch.setattr(retention, "prune_once", boom)
        task = asyncio.create_task(retention_loop_fast(store))
        await asyncio.sleep(0.25)
        task.cancel()
        assert len(calls) >= 2, "the loop must keep going after a failure"


class TestThePruneRunsOffTheEventLoop:
    """`retention_loop` is gathered alongside the recorder and the aiohttp
    server. A prune is guard queries, a DELETE over tens of millions of rows and
    a CHECKPOINT; run inline it stalls recorder polls, SSE delivery and every
    HTTP request for its whole duration. It cannot simply be handed to
    `asyncio.to_thread` either -- `Store._conn` is the event loop's own
    connection, shared with the recorder and the backup snapshot -- so the
    worker needs a connection of its own.
    """

    async def test_the_pass_runs_in_a_worker_with_its_own_connection(
        self, store: Store, monkeypatch
    ) -> None:
        import asyncio
        import threading

        from juice import retention

        seen: list[tuple[int, object]] = []

        def spy(store_, retention_days, *, conn=None):
            seen.append((threading.get_ident(), conn))
            return 0

        monkeypatch.setattr(retention, "prune_once", spy)
        task = asyncio.create_task(retention_loop_fast(store))
        await asyncio.sleep(0.2)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        assert seen, "the loop must have run a pass"
        thread_id, conn = seen[0]
        assert thread_id != threading.get_ident(), "the prune ran on the event loop thread"
        assert conn is not None, "the worker must get its own connection"
        assert conn is not store._conn, "the worker must not borrow the event loop's connection"

    async def test_the_worker_connection_is_closed_with_the_loop(self, store: Store) -> None:
        """It outlives no task: `serve` cancels the loop at shutdown, and a
        connection left open holds the database file."""
        import asyncio

        opened: list[object] = []
        real_new = store.new_connection

        def spy_new():
            conn = real_new()
            opened.append(conn)
            return conn

        store.new_connection = spy_new  # type: ignore[method-assign]
        task = asyncio.create_task(retention_loop_fast(store))
        await asyncio.sleep(0.15)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        assert opened, "the loop must have opened a worker connection"
        with pytest.raises(Exception):  # noqa: B017 - duckdb raises its own type
            opened[0].execute("SELECT 1")

    def test_prune_once_accepts_a_connection(self, store: Store) -> None:
        from juice.retention import prune_once

        _seed(store)
        conn = store.new_connection()
        try:
            assert prune_once(store, 31, conn=conn) > 0
        finally:
            conn.close()
        assert store._conn.execute("SELECT count(*) FROM readings").fetchone()[0] > 0


async def retention_loop_fast(store: Store):
    from juice.retention import retention_loop

    await retention_loop(store, 31, interval=0.05)
