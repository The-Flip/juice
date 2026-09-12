"""The rollup driver, and the one-off retro play-hours migration.

Split out of `tests/test_recorder.py` when the code moved out of
`juice/recorder.py`: none of this is about collecting readings.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from juice.rollups import RETRO_PLAY_HOURS_MIGRATION, apply_retro_migration
from juice.store import Store


@pytest.fixture
def store():
    with Store(":memory:") as s:
        yield s


class TestRetroPlayHoursMigration:
    """The one-off startup migration that reapplies current calibrations to the
    frozen historical play-hours rollup (fixes Indiana Jones' stale Jul 6-12)."""

    @staticmethod
    def _seed_stale_rollup(store: Store) -> tuple[int, int]:
        """A calibrated+assigned machine with a historical hourly_play_seconds row
        that its readings (all ATTRACT under the current calibration) don't
        justify — i.e. a leftover from an older, laxer calibration."""
        from juice.state import Calibration

        pid = store.ensure_plug("d1", "c01", "Blackout - M0013")
        mid = store.ensure_machine("M0013", "Blackout")
        store.update_assignment(pid, mid, datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC))
        store.set_calibration(mid, Calibration(idle_max_rsd=None, play_min_rsd=50.0))  # strict
        t0 = datetime(2026, 7, 6, 20, 0, 0, tzinfo=UTC)
        store.insert_readings(
            [
                (t0 + timedelta(seconds=i), pid, 300.0 * (1 + 0.003), 120.0, 2.5, 0.0)
                for i in range(120)
            ]
        )
        # Stale inflated rollup row (as if rolled up under a lenient calibration).
        store._conn.execute(
            "INSERT INTO hourly_play_seconds VALUES (?, ?, ?, ?)",
            [mid, datetime(2026, 7, 6, 15, 0, 0), 3000.0, 3000.0],
        )
        return pid, mid

    def test_rebuilds_history_and_marks_once(self, store: Store) -> None:
        _pid, mid = self._seed_stale_rollup(store)
        assert store.has_migration(RETRO_PLAY_HOURS_MIGRATION) is False

        apply_retro_migration(store)

        # The stale play_seconds is gone — recomputed under the strict calibration.
        play = store._conn.execute(
            "SELECT COALESCE(SUM(play_seconds), 0) FROM hourly_play_seconds WHERE machine_id = ?",
            [mid],
        ).fetchone()[0]
        assert play == pytest.approx(0.0, abs=1.0)
        assert store.has_migration(RETRO_PLAY_HOURS_MIGRATION) is True

    def test_failure_leaves_migration_unmarked_for_retry(self, store: Store, monkeypatch) -> None:
        self._seed_stale_rollup(store)

        def boom(_mid: int, _conn: object = None) -> int:
            raise RuntimeError("rebuild blew up")

        monkeypatch.setattr(store, "rebuild_play_hours", boom)
        apply_retro_migration(store)  # swallows, doesn't mark

        assert store.has_migration(RETRO_PLAY_HOURS_MIGRATION) is False

    def test_is_noop_when_already_applied(self, store: Store) -> None:
        _pid, mid = self._seed_stale_rollup(store)
        store.mark_migration(RETRO_PLAY_HOURS_MIGRATION)

        apply_retro_migration(store)

        # Marker was already set, so the stale row is left untouched.
        play = store._conn.execute(
            "SELECT SUM(play_seconds) FROM hourly_play_seconds WHERE machine_id = ?", [mid]
        ).fetchone()[0]
        assert play == pytest.approx(3000.0)


class TestTheRollupsRunOffTheEventLoop:
    """`refresh_hourly_play_seconds` reads every reading of every calibrated plug
    into Python and runs `classify()` over it -- 4.4s measured for a 73h/4-plug
    window, ~44s for the one-day ingest backfill this pipeline exists to accept.
    Run inline that stalls the poll loop, the SSE stream and every HTTP request
    for the duration, and it cannot go to `asyncio.to_thread` either, because
    `Store._conn` is the event loop's own connection.
    """

    async def test_a_pass_runs_in_a_worker_with_its_own_connection(
        self, store: Store, monkeypatch
    ) -> None:
        import threading

        from juice import rollups

        seen: list[tuple[int, object]] = []

        def spy(store_, conn=None):
            seen.append((threading.get_ident(), conn))
            return True

        monkeypatch.setattr(rollups, "refresh_rollups", spy)
        worker = rollups.RollupWorker(store)
        try:
            assert await worker.refresh() is True
        finally:
            worker.close()

        assert seen, "the worker must have run a pass"
        thread_id, conn = seen[0]
        assert thread_id != threading.get_ident(), "the pass ran on the event loop thread"
        assert conn is not None, "the worker must get its own connection"
        assert conn is not store._conn, "the worker must not borrow the event loop's connection"

    async def test_the_retro_migration_runs_on_the_same_worker(
        self, store: Store, monkeypatch
    ) -> None:
        """One thread, so the retro rebuild and a periodic pass can never
        interleave their transactions on `hourly_play_seconds` -- which they would
        otherwise do, since the rebuild deletes a machine's whole history."""
        import threading

        from juice import rollups

        threads: list[int] = []
        monkeypatch.setattr(
            rollups,
            "refresh_rollups",
            lambda *a, **k: threads.append(threading.get_ident()) or True,
        )
        monkeypatch.setattr(
            rollups, "apply_retro_migration", lambda *a, **k: threads.append(threading.get_ident())
        )
        worker = rollups.RollupWorker(store)
        try:
            await worker.apply_retro_migration()
            await worker.refresh()
        finally:
            worker.close()

        assert len(threads) == 2
        assert threads[0] == threads[1], "both must run on the one worker thread"
        assert threads[0] != threading.get_ident()

    async def test_the_worker_connection_is_closed_on_its_own_thread(self, store: Store) -> None:
        """Closed from the event loop it would race a pass still inside DuckDB on
        that connection, which does not raise -- it wedges the worker, and the
        executor's threads are joined at interpreter exit, so shutdown hangs."""
        from juice import rollups

        opened: list[object] = []
        real_new = store.new_connection

        def spy_new():
            conn = real_new()
            opened.append(conn)
            return conn

        store.new_connection = spy_new  # type: ignore[method-assign]
        worker = rollups.RollupWorker(store)
        await worker.refresh()
        worker.close()
        # `close` queues the close behind the in-flight pass on the same thread,
        # so give that thread a moment to drain rather than racing it here.
        for _ in range(100):
            try:
                opened[0].execute("SELECT 1")
            except Exception:
                break
            await asyncio.sleep(0.01)
        else:
            pytest.fail("the worker connection was never closed")

    async def test_a_real_pass_rolls_up_through_the_worker(self, store: Store) -> None:
        """Not a spy: the worker's own connection has to be able to do the work.
        A cursor comes up in the creating thread's timezone, and getting that
        wrong made every fresh reading look hours in the future once before."""
        from juice import rollups

        pid = store.ensure_plug("d1", "c01", "Blackout - M0013")
        t0 = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=30)
        store.insert_readings(
            [(t0 + timedelta(seconds=i), pid, 300.0, 120.0, 2.5, 0.0) for i in range(120)]
        )
        worker = rollups.RollupWorker(store)
        try:
            assert await worker.refresh() is True
        finally:
            worker.close()

        rolled = store._conn.execute("SELECT count(*) FROM hourly_usage").fetchone()[0]
        assert rolled > 0, "the worker's connection must actually see and roll up the readings"
