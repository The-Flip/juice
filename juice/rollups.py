"""The hourly rollup refreshes, and the thread they run on.

Split out of `juice/recorder.py` because none of it is about collecting: the
cloud recorder merely happened to be the thing with a periodic loop. At tap
cutover that loop goes away and the rollups must not go with it -- the same
reasoning `juice/retention.py` records for the prune.

**Why a worker thread.** `refresh_hourly_play_seconds` reads every reading of
every calibrated plug into Python and runs `classify()` over it: 4.4 s measured
for a 73 h / 4-plug window, and ~44 s at ~40 outlets for the one-day backfill a
tap catching up after an outage delivers. On the event loop that stalls the poll
loop, the SSE stream and every HTTP request for the duration. So this follows
`retention.py` exactly -- one thread, one long-lived connection of its own,
closed on that same thread -- because `Store._conn` is the event loop's and
DuckDB connections are not thread-safe.

`refresh_rollups` and `apply_retro_migration` are plain synchronous functions
taking a `conn`, so they can also be run inline (by a test, or by a CLI) with no
thread at all. `RollupWorker` is only the scheduling.
"""

from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

import duckdb

from juice.store import Store

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
    mark = store.backfill_mark(conn)
    lookback = store.rollup_lookback_hours(_DEFAULT_ROLLUP_LOOKBACK_HOURS, conn)
    play_lookback = store.rollup_lookback_hours(_DEFAULT_PLAY_LOOKBACK_HOURS, conn)

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
        # Wrapped like every refresh above, and for the same reason: a periodic
        # job must never take the server down with it. A failure here leaves the
        # mark for the next pass.
        try:
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
    in `IngestWriter`: one thread means one connection and one writer, so two
    passes can never interleave their transactions on the same rollup tables.
    """

    def __init__(self, store: Store) -> None:
        self._store = store
        self._pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="juice-rollups")
        self._conn: duckdb.DuckDBPyConnection | None = None

    def _connection(self) -> duckdb.DuckDBPyConnection:
        # Created on the worker thread, not in __init__: a DuckDB cursor comes up
        # in the *creating* thread's context, and `Store._configure` has to run
        # on it to pin the session timezone (see `Store.new_connection`).
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

    def close(self) -> None:
        """Shut down, closing the connection on the thread that owns it.

        Queued behind whatever pass is in flight, so a refresh mid-run finishes
        against a live connection rather than having it pulled out from under it.
        """
        self._pool.submit(self._close_conn)
        self._pool.shutdown(wait=False)

    def _close_conn(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None


async def rollup_loop(store: Store, worker: RollupWorker, *, interval: float) -> None:
    """Refresh the rollups periodically, forever.

    Its own task rather than a step in a collector's poll loop, for the reason in
    the module docstring: the collector is replaceable and this is not.
    """
    while True:
        try:
            await worker.refresh()
        except Exception:  # noqa: BLE001 - a failed pass must not kill the server
            log.warning("rollup pass failed", exc_info=True)
        await asyncio.sleep(interval)
