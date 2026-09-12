"""The rollup driver, and the one-off retro play-hours migration.

Split out of `tests/test_recorder.py` when the code moved out of
`juice/recorder.py`: none of this is about collecting readings.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

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
        executor's threads are joined at interpreter exit, so shutdown hangs.

        So this asserts *which thread* runs the close, not merely that the
        connection ends up closed. Checking only the latter passes just as well
        when `close()` is called inline from the event loop, which is the bug.
        """
        import threading

        from juice import rollups

        class _RecordingConn:
            """Forwards everything, remembering which thread closed it."""

            def __init__(self, real) -> None:
                self._real = real
                self.closed_on: int | None = None

            def __getattr__(self, name):
                return getattr(self._real, name)

            def close(self) -> None:
                self.closed_on = threading.get_ident()
                self._real.close()

        opened: list[_RecordingConn] = []
        real_new = store.new_connection

        def spy_new():
            conn = _RecordingConn(real_new())
            opened.append(conn)
            return conn

        store.new_connection = spy_new  # type: ignore[method-assign]
        worker = rollups.RollupWorker(store)
        ran_on: list[int] = []
        monkeypatch = pytest.MonkeyPatch()
        monkeypatch.setattr(
            rollups,
            "refresh_rollups",
            lambda *a, **k: ran_on.append(threading.get_ident()) or True,
        )
        try:
            await worker.refresh()
        finally:
            monkeypatch.undo()
        worker.close()

        assert opened, "the worker must have opened a connection of its own"
        assert ran_on, "the pass must have run"
        conn = opened[0]
        # `close` queues behind the in-flight pass on the same thread, so wait for
        # that thread to drain rather than racing it.
        for _ in range(200):
            if conn.closed_on is not None:
                break
            await asyncio.sleep(0.01)
        assert conn.closed_on is not None, "the worker connection was never closed"
        assert conn.closed_on == ran_on[0], (
            "the connection must be closed on the thread that used it, not from "
            "the event loop -- closing it under a running pass wedges the worker"
        )
        assert conn.closed_on != threading.get_ident()

    async def test_close_waits_for_an_in_flight_pass(self, store: Store) -> None:
        """`close` must not return while a pass is still using the connection.

        The worker's connection is a cursor of `Store._conn`, and the caller's
        next move after `close()` is normally to leave the `with Store(...)`
        block -- so returning early tears the database out from under a running
        pass and leaves a non-daemon pool thread inside DuckDB for the
        interpreter to join at exit. That was a ~50% shutdown hang before this
        waited, reproduced through `record()`.
        """
        import threading

        from juice import rollups

        started = threading.Event()
        finished = threading.Event()

        def slow(store_, conn=None):
            started.set()
            time.sleep(0.4)
            finished.set()
            return True

        monkeypatch = pytest.MonkeyPatch()
        monkeypatch.setattr(rollups, "refresh_rollups", slow)
        try:
            worker = rollups.RollupWorker(store)
            task = asyncio.create_task(worker.refresh())
            await asyncio.to_thread(started.wait, 5)
            await asyncio.to_thread(worker.close)
            assert finished.is_set(), "close returned while the pass was still running"
            with contextlib.suppress(Exception):
                await task
        finally:
            monkeypatch.undo()

    async def test_close_is_idempotent(self, store: Store) -> None:
        """`serve` closes it in a `finally` that can run after `record` already
        did on its own error path."""
        from juice import rollups

        worker = rollups.RollupWorker(store)
        await worker.refresh()
        worker.close()
        worker.close()

    async def test_a_real_pass_rolls_up_through_the_worker(self, store: Store) -> None:
        """Not a spy: the worker's own connection has to be able to do the work.

        Deliberately a *calibrated, assigned* machine, so `hourly_play_seconds`
        gets past its `plug_cals` guard and the expensive `classify()` path --
        the entire reason the worker exists -- actually runs on the worker's
        cursor. A bare plug leaves that refresh returning 0 at its first line, so
        the test would pass while never touching the code it is named for. It also
        exercises `_local_hour`/`ZoneInfo` there, which is what the timezone
        anecdote in `Store._configure` is about.
        """
        from juice import rollups
        from juice.state import Calibration

        pid = store.ensure_plug("d1", "c01", "Blackout - M0013", has_emeter=True)
        mid = store.ensure_machine("M0013", "Blackout")
        t0 = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=30)
        store.update_assignment(pid, mid, t0 - timedelta(days=1))
        store.set_calibration(mid, Calibration(idle_max_rsd=None, play_min_rsd=10.0))
        store.insert_readings(
            [
                (t0 + timedelta(seconds=i), pid, 300.0 + 80.0 * math.sin(i * 0.7), 120.0, 2.5, 0.0)
                for i in range(300)
            ]
        )
        worker = rollups.RollupWorker(store)
        try:
            assert await worker.refresh() is True
        finally:
            worker.close()

        usage = store._conn.execute("SELECT count(*) FROM hourly_usage").fetchone()[0]
        assert usage > 0, "the worker's connection must see and roll up the readings"
        play = store._conn.execute(
            "SELECT count(*), COALESCE(SUM(on_seconds), 0) FROM hourly_play_seconds"
        ).fetchone()
        assert play[0] > 0 and play[1] > 0, (
            "the play-seconds refresh -- the expensive path the worker exists for -- "
            "must actually have run on the worker's connection"
        )

    async def test_the_baseline_refresh_reaches_the_live_state(self, store: Store) -> None:
        """`refresh_baselines_into` is the one place a write on the worker's
        connection has to be visible to a read on the event loop's, and what it
        feeds is overload auto-shutdown arming. A silent staleness regression here
        disarms protection, so it gets a test rather than a comment.
        """
        from juice import rollups

        pid = store.ensure_plug("d1", "c01", "Blackout - M0013", has_emeter=True)
        mid = store.ensure_machine("M0013", "Blackout")
        t0 = datetime.now(UTC).replace(microsecond=0) - timedelta(days=2)
        store.update_assignment(pid, mid, t0 - timedelta(days=1))
        # The baseline is a quantile over *per-minute* averages and needs
        # `min_minutes` of them, so one reading a minute is enough -- and 600 rows
        # instead of 600 * 60.
        store.insert_readings(
            [(t0 + timedelta(minutes=i), pid, 300.0, 120.0, 2.5, 0.0) for i in range(600)]
        )

        state = SimpleNamespace(power_baselines={})
        worker = rollups.RollupWorker(store)
        try:
            await rollups.refresh_baselines_into(store, worker, state)
        finally:
            worker.close()

        assert state.power_baselines, (
            "a baseline computed on the worker's connection must be readable on "
            "the event loop's and reach RecorderState"
        )
        assert state.power_baselines == store.get_power_baselines()


class TestARollupPassDoesNotStallThePollLoop:
    """The point of all of this, and the thing the first version of it missed.

    Putting the work on a worker thread frees the *event loop* -- SSE and HTTP
    keep serving -- but `await`ing the pass from inside the collector's poll loop
    still suspends that loop for the pass's whole duration, whatever thread the
    work is on. A one-day tap backfill is ~44s of `classify()`, and the cap
    allows a window 32x wider than that, so the hole is not small.
    """

    async def test_polling_continues_while_a_slow_pass_runs(self, store: Store) -> None:
        """A deliberately slow pass, with a 1 Hz ticker alongside it. The ticker
        must keep ticking -- which it only does if the pass is not in its loop."""
        from juice import rollups

        def slow(store_, conn=None):
            time.sleep(0.6)
            return True

        monkeypatch = pytest.MonkeyPatch()
        monkeypatch.setattr(rollups, "refresh_rollups", slow)
        try:
            worker = rollups.RollupWorker(store)
            ticks = 0

            async def ticker() -> None:
                nonlocal ticks
                while True:
                    ticks += 1
                    await asyncio.sleep(0.05)

            tick_task = asyncio.create_task(ticker())
            try:
                await worker.refresh()
            finally:
                tick_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await tick_task
                worker.close()
        finally:
            monkeypatch.undo()

        # ~12 ticks are due over a 0.6s pass. Anything near 1 means the loop was
        # blocked; this is the assertion that fails if the work moves back onto
        # the event loop.
        assert ticks >= 6, f"the event loop was blocked during the pass ({ticks} ticks)"

    async def test_the_poll_loop_does_not_await_a_rollup_pass(self) -> None:
        """The structural half, and the one that actually caught the bug: a
        passing liveness test above is not enough, because the stall was in the
        *poll loop* rather than the event loop. `_record_loop` must not reach the
        worker at all -- the periodic passes belong to `rollup_loop`.
        """
        import ast
        import inspect
        import textwrap

        from juice import recorder

        assert "rollups" not in inspect.signature(recorder._record_loop).parameters, (
            "the poll loop takes the rollup worker again"
        )
        # The executable body only -- the docstring legitimately mentions
        # `rollup_loop` to explain why none of this is here.
        tree = ast.parse(textwrap.dedent(inspect.getsource(recorder._record_loop)))
        fn = tree.body[0]
        statements = fn.body[1:] if ast.get_docstring(fn) else fn.body
        code = "\n".join(ast.unparse(node) for node in statements)
        for forbidden in ("rollups", "refresh_baselines", ".refresh("):
            assert forbidden not in code, (
                f"the poll loop reaches {forbidden!r} again; awaiting a rollup pass "
                "there suspends polling for its whole duration even on a worker "
                "thread (see rollups.rollup_loop)"
            )
