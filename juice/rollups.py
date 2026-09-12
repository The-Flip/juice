"""The hourly rollup refreshes, and the thread they run on.

Split out of `juice/recorder.py` because none of it is about collecting: the
cloud recorder merely happened to be the thing with a periodic loop. At tap
cutover that loop goes away and the rollups must not go with it -- the same
reasoning `juice/retention.py` records for the prune.

**Why a worker thread, and why its own task.** `refresh_hourly_play_seconds`
reads every reading of every calibrated plug into Python and runs `classify()`
over it: 4.4 s measured for a 73 h / 4-plug window, and ~44 s at ~40 outlets for
the one-day backfill a tap catching up after an outage delivers.

Two separate problems, and the thread only solves the first:

1. On the event loop, that duration blocks the SSE stream and every HTTP request.
   The thread fixes that, and it needs a connection of its own because
   `Store._conn` is the event loop's and DuckDB connections are not thread-safe.
   This follows `retention.py` exactly -- one thread, one long-lived connection,
   closed on that same thread.
2. `await`ing the pass from inside the collector's poll loop still suspends *that
   loop* for the whole pass, whatever thread the work is on. Measured: a 3 s pass
   produced a clean 3 s hole in a 1 Hz poll. So the pass does not belong in the
   poll loop at all, and `rollup_loop` below is its own task, gathered beside
   `retention_loop`. This is the stall `api_v2_findings.md` §3 measured in
   production as the 18 s outlier.

`refresh_rollups` and `apply_retro_migration` are plain synchronous functions
taking a `conn`, so they can also be run inline (by a test, or by a CLI) with no
thread at all. `RollupWorker` is only the scheduling.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

import duckdb

from juice.store import MAX_ROLLUP_LOOKBACK_HOURS, Store

if TYPE_CHECKING:  # pragma: no cover - import cycle; RecorderState lives in server
    from juice.server import RecorderState

log = logging.getLogger(__name__)

# Defaults for the trailing rollup windows; `refresh_rollups` widens them when
# ingest has backfilled older rows, bounded by `MAX_ROLLUP_LOOKBACK_HOURS`.
# Play-hours reaches further back because its buckets are local-hour aligned and
# a recalibration has to be able to reach yesterday.
_DEFAULT_ROLLUP_LOOKBACK_HOURS = 2
_DEFAULT_PLAY_LOOKBACK_HOURS = 49

# Marker for the one-shot rebuild of historical play-hours under current
# calibrations. Retention refuses to prune until it has run, because a pruned
# span is simply missing from it.
RETRO_PLAY_HOURS_MIGRATION = "retro_play_hours_v1"

# How often the rollups are refreshed. Matches the cadence they had as a step in
# the recorder's poll loop, so `/usage` is no less fresh than it was.
ROLLUP_INTERVAL_SECONDS = 60.0
# Baselines are a 30-day scan and drift slowly, so far less often.
BASELINE_INTERVAL_SECONDS = 3600.0
# How long `close` waits for an interrupted pass to unwind. Generous, because the
# alternative to waiting is closing the database under it; short enough that a
# deploy's grace period is not spent here.
CLOSE_TIMEOUT_SECONDS = 10.0


def refresh_rollups(store: Store, conn: duckdb.DuckDBPyConnection | None = None) -> bool:
    """Refresh the four hourly rollups. Returns True if all four succeeded.

    Each is wrapped separately so one failing cannot stop the others, matching
    every other periodic job.

    The lookback is widened to cover anything tap ingest backfilled since the
    last pass. Without that, rows older than both "latest reading" and "latest
    rollup" -- which is exactly what a collector catching up after an outage
    delivers -- fall outside the trailing window and are never rolled up. The
    refresh reports success either way, so the only symptom is charts that stay
    blank. The mark is retired only when all four have actually run, so a
    failure leaves the work outstanding rather than silently dropping it.
    """
    # Captured before the refreshes, not after: the mark is what we are about to
    # cover, and ingest keeps committing while we work. The mark carries the
    # highest pending row id along with the timestamp, and that id is what lets
    # the clear below retire what this pass covered without also retiring
    # whatever landed during it.
    # Wrapped like the refreshes below, and for the same reason: `serve` gathers
    # this loop beside the recorder without `return_exceptions`, so a database
    # error in these three reads would take the whole server down rather than
    # costing one pass.
    try:
        mark = store.backfill_mark(conn)
        lookback = store.rollup_lookback_hours(_DEFAULT_ROLLUP_LOOKBACK_HOURS, conn)
        play_lookback = store.rollup_lookback_hours(_DEFAULT_PLAY_LOOKBACK_HOURS, conn)
    except Exception:
        log.warning("reading the rollup window failed; skipping this pass", exc_info=True)
        return False

    ok = True
    for name, refresh in (
        ("hourly_usage", lambda: store.refresh_hourly_usage(lookback_hours=lookback, conn=conn)),
        (
            "hourly_strip_peak",
            lambda: store.refresh_hourly_strip_peak(lookback_hours=lookback, conn=conn),
        ),
        (
            "hourly_circuit_peak",
            lambda: store.refresh_hourly_circuit_peak(lookback_hours=lookback, conn=conn),
        ),
        (
            "hourly_play_seconds",
            lambda: store.refresh_hourly_play_seconds(lookback_hours=play_lookback, conn=conn),
        ),
    ):
        try:
            refresh()
        except Exception:
            ok = False
            log.warning("%s refresh failed", name, exc_info=True)

    if ok:
        try:
            if store.backfill_exceeds_lookback_cap(conn):
                # The window was capped, so this pass covered only part of what
                # the mark names. Keeping it is what leaves the rest reachable and
                # -- the half that actually loses data -- what keeps
                # `prunable_before` flooring the prune cutoff at those hours.
                # `rollup_lookback_hours` has already logged why, loudly.
                log.error(
                    "keeping the ingest backfill mark: the rollup window was capped at %d "
                    "hours and the hours before it are still outstanding",
                    MAX_ROLLUP_LOOKBACK_HOURS,
                )
            else:
                # Wrapped like every refresh above, and for the same reason: a
                # periodic job must never take the server down with it. A failure
                # here leaves the mark for the next pass.
                store.clear_pending_backfill(mark, conn)
        except Exception:
            ok = False
            log.warning("clearing the backfill watermark failed", exc_info=True)
    return ok


def apply_retro_migration(store: Store, conn: duckdb.DuckDBPyConnection | None = None) -> None:
    """Reapply all current calibrations to the historical play-hours rollup, once.

    Historical `hourly_play_seconds` rows were frozen under whatever calibration
    was live when each hour was first rolled up, so a later recalibration never
    reached them. Rebuild each calibrated machine's full history under its
    current calibration. Guarded by a persisted marker, so it runs a single time
    per DB and is a no-op on later startups.

    Synchronous, and no longer yields between machines: it runs on the rollup
    worker's thread, so there is no event loop here to be polite to.
    """
    if store.has_migration(RETRO_PLAY_HOURS_MIGRATION, conn):
        return
    log.info("Applying retroactive play-hours migration...")
    failed = False
    for mid in store.calibrated_assigned_machine_ids(conn):
        try:
            store.rebuild_play_hours(mid, conn)
        except Exception:
            failed = True
            log.warning("Retroactive rebuild failed for machine %s", mid, exc_info=True)
    if failed:
        # Leave the marker unset so the machines that failed get retried on the
        # next startup (each rebuild is idempotent, so re-running the ones that
        # already succeeded is harmless).
        log.warning("Retroactive play-hours migration incomplete; will retry next startup")
        return
    store.mark_migration(RETRO_PLAY_HOURS_MIGRATION, conn)
    log.info("Retroactive play-hours migration complete")


class RollupWorker:
    """Runs the rollup passes on one thread with a connection of its own.

    `max_workers=1` is load-bearing rather than conservative, the same way it is
    in `IngestWriter`: one thread means one connection and one writer, so nothing
    can interleave its transactions on the rollup tables.

    That guarantee only holds while **every** writer of those tables comes through
    here. It is why `rebuild_play_hours` and `rebuild_circuit_peak` are methods on
    the worker rather than direct `Store` calls from their HTTP handlers: the
    moment one of them writes from the event loop's connection instead, two
    connections are deleting and rewriting the same rows and DuckDB fails one of
    them mid-rewrite.
    """

    def __init__(self, store: Store) -> None:
        self._store = store
        self._pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="juice-rollups")
        self._conn: duckdb.DuckDBPyConnection | None = None

    def _connection(self) -> duckdb.DuckDBPyConnection:
        # Created lazily, on the worker thread. Not because it has to be --
        # `Store._configure`'s settings are per-connection, not per-thread, which
        # is why `retention.py` can make its connection on the event loop and use
        # it on the worker -- but because creating it here means the only thread
        # that ever touches `Store._conn` to mint it is also the only thread that
        # uses the result, so there is one fewer cross-thread access to reason
        # about.
        if self._conn is None:
            self._conn = self._store.new_connection()
        return self._conn

    async def refresh(self) -> bool:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._pool, self._refresh)

    def _refresh(self) -> bool:
        return refresh_rollups(self._store, self._connection())

    async def apply_retro_migration(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._pool, self._retro)

    def _retro(self) -> None:
        apply_retro_migration(self._store, self._connection())

    async def rebuild_play_hours(self, machine_id: int) -> int:
        """Recompute one machine's whole play history, on the worker thread.

        Here rather than on the event loop because this is the **other** writer of
        `hourly_play_seconds`, and the periodic refresh is on this thread. Two
        connections writing that table interleave their untransacted
        delete-then-rewrite, and DuckDB answers the loser with a
        `TransactionException` -- measured: a calibration during a pass failed
        every time, and once left the machine with zero buckets, because the
        whole-history `DELETE` had already committed. One thread means one writer
        and no conflict to lose.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._pool, self._rebuild_play_hours, machine_id)

    def _rebuild_play_hours(self, machine_id: int) -> int:
        return self._store.rebuild_play_hours(machine_id, self._connection())

    async def rebuild_circuit_peak(self) -> int:
        """Recompute the circuit-peak rollup, on the worker thread.

        Same reasoning as `rebuild_play_hours`, and worse in one respect: this one
        truncates the entire table before backfilling it.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._pool, self._rebuild_circuit_peak)

    def _rebuild_circuit_peak(self) -> int:
        return self._store.rebuild_hourly_circuit_peak(self._connection())

    async def refresh_baselines(self) -> None:
        """Recompute the per-machine overload baselines on the worker thread.

        `refresh_power_baselines` reads 30 days of raw readings, so it belongs
        here for the same reason the rollups do. Only the recompute goes to the
        thread: reading the result back into `RecorderState` is the caller's job,
        on the event loop, because that state is the loop's.
        """
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._pool, self._baselines)

    def _baselines(self) -> None:
        self._store.refresh_power_baselines(conn=self._connection())

    def close(self, *, timeout: float = CLOSE_TIMEOUT_SECONDS) -> None:
        """Shut down: stop the in-flight pass, close the connection, and wait.

        **Waiting is the point.** The worker's connection is a cursor of
        `Store._conn`, so it dies with the store -- and the caller's next move
        after `close()` is usually to leave the `with Store(...)` block. Returning
        early therefore pulls the database out from under a running pass
        ("Connection Error: Connection already closed!"), and leaves a non-daemon
        pool thread inside DuckDB for `concurrent.futures` to join at interpreter
        exit, which is a hang rather than a shutdown.

        A pass may legitimately be tens of seconds long, and a SIGTERM grace
        period is not, so we `interrupt()` first: DuckDB raises out of the running
        query, the per-refresh handlers in `refresh_rollups` log it, and the pass
        returns in milliseconds. Every rollup write is transactional, so an
        interrupted one rolls back rather than leaving a half-rewritten window.
        `timeout` is the backstop for anything that ignores the interrupt; blowing
        through it is worth a loud line, because what follows is the hang.
        """
        conn, self._conn = self._conn, None
        if conn is not None:
            # Safe from this thread: `interrupt` only signals the running query.
            with contextlib.suppress(Exception):
                conn.interrupt()
            self._pool.submit(conn.close)
        self._pool.shutdown(wait=False)
        for thread in list(self._pool._threads):  # noqa: SLF001 - no public join
            thread.join(timeout)
            if thread.is_alive():
                log.error(
                    "rollup worker did not stop within %.0fs; shutdown may hang and the "
                    "database is being closed under a running pass",
                    timeout,
                )


async def refresh_baselines_into(
    store: Store, worker: RollupWorker, recorder_state: RecorderState | None
) -> None:
    """Recompute the overload baselines, then hand them to the live state.

    Split this way because the two halves belong on different threads: the
    recompute reads 30 days of raw readings and goes to the worker, while
    `RecorderState` is the event loop's and must only ever be written here.
    """
    try:
        await worker.refresh_baselines()
        if recorder_state is not None:
            recorder_state.power_baselines = store.get_power_baselines()
    except Exception:  # noqa: BLE001 - a failed refresh must not kill the caller
        log.warning("Power baseline refresh failed", exc_info=True)


async def rollup_loop(
    store: Store,
    worker: RollupWorker,
    recorder_state: RecorderState | None = None,
    *,
    interval: float = ROLLUP_INTERVAL_SECONDS,
    baseline_interval: float = BASELINE_INTERVAL_SECONDS,
) -> None:
    """Refresh the rollups periodically, forever.

    **Its own task, not a step in a collector's poll loop.** Running it there
    suspends that loop for the length of the pass even with the work on a worker
    thread -- a 3 s pass leaves a 3 s hole in a 1 Hz poll -- and the pass is
    allowed to take ~44 s on a one-day backfill. Out here it overlaps polling
    instead of interrupting it, and it also outlives the collector: at tap
    cutover the poll loop goes away and the rollups must not go with it, which is
    the same argument `retention.py` records for the prune.

    Baselines ride along on their own slower timer rather than in a second task,
    because they share the one worker thread anyway and a second task would only
    add a second thing to cancel.
    """
    elapsed_since_baseline = baseline_interval  # arm it for the first pass
    while True:
        try:
            await worker.refresh()
        except Exception:  # noqa: BLE001 - a failed pass must not kill the server
            log.warning("rollup pass failed", exc_info=True)
        if elapsed_since_baseline >= baseline_interval:
            await refresh_baselines_into(store, worker, recorder_state)
            elapsed_since_baseline = 0.0
        await asyncio.sleep(interval)
        elapsed_since_baseline += interval
