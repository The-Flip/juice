"""Pruning raw readings, and the guards that decide when it must not happen.

tap at 1 Hz produces ~4.23M rows/day against today's ~200k, into a table that
has never had a row deleted. Pruning is how that stays survivable.

Almost everything here is about refusing to prune. Raw readings are the only
copy of the data: the rollups are derived from them and several code paths
rebuild from them. A delete that runs an hour too early is not a performance
regression, it is history that no longer exists.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from juice.recorder import RETRO_PLAY_HOURS_MIGRATION
from juice.state import Calibration
from juice.store import MIN_RETENTION_DAYS, Store

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


async def retention_loop_fast(store: Store):
    from juice.retention import retention_loop

    await retention_loop(store, 31, interval=0.05)
