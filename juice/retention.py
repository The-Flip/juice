"""Raw-reading retention.

`readings` has never had a row deleted. That was tenable while the cloud
recorder produced ~200k rows/day; tap at 1 Hz produces ~4.23M, into a table
with no index that several code paths scan in full.

The deletion itself is trivial. The interesting part is all in
`Store.prunable_before`, which decides whether it is safe to delete anything at
all -- raw readings are the only copy, and a prune that runs an hour too early
is not a slow query, it is history that no longer exists.

Off by default in the sense that matters: an unset or zero
`JUICE_RAW_RETENTION_DAYS` prunes nothing, and every guard failure means "don't".
"""

from __future__ import annotations

import asyncio
import logging

import duckdb

from juice.store import Store

log = logging.getLogger(__name__)

# Six hours. The window moves by a day at a time, so there is nothing to gain
# from checking often, and each pass ends in a CHECKPOINT.
PRUNE_INTERVAL_SECONDS = 6 * 3600

DEFAULT_RETENTION_DAYS = 90


def prune_once(
    store: Store, retention_days: int, *, conn: duckdb.DuckDBPyConnection | None = None
) -> int:
    """One prune pass. Returns rows deleted (0 when the guards say no).

    Fully synchronous, and meant to be: `retention_loop` runs it in a worker
    thread on `conn`, and `juice prune` runs it inline with none.
    """
    cutoff = store.prunable_before(retention_days, conn=conn)
    if cutoff is None:
        return 0
    return store.prune_readings(cutoff, conn=conn)


async def retention_loop(
    store: Store, retention_days: int, *, interval: float = PRUNE_INTERVAL_SECONDS
) -> None:
    """Prune raw readings periodically.

    Its own task rather than a step in the recorder's poll loop: at cutover the
    cloud recorder goes away, and a prune living inside it would silently stop
    with it -- exactly when the data volume that makes pruning necessary
    arrives.
    """
    if retention_days <= 0:
        log.info("raw retention disabled; readings will not be pruned")
        return

    log.info(
        "raw retention: keeping %d days, checking every %.0fh", retention_days, interval / 3600
    )
    # The pass is guard queries, a DELETE over tens of millions of rows and a
    # CHECKPOINT. Run inline it would stall recorder polls, SSE delivery and
    # every HTTP request for its whole duration, so it goes to a worker thread
    # -- and therefore needs a connection of its own: `Store._conn` belongs to
    # the event loop, shared with the recorder and the backup snapshot.
    conn = store.new_connection()
    try:
        while True:
            try:
                deleted = await asyncio.to_thread(prune_once, store, retention_days, conn=conn)
                if deleted:
                    log.info("retention: pruned %d raw readings", deleted)
            except Exception:  # noqa: BLE001 - a failed prune must not kill the server
                log.warning("retention pass failed", exc_info=True)
            await asyncio.sleep(interval)
    finally:
        # `serve` cancels this task at shutdown; an abandoned connection would
        # keep a handle on the database file.
        conn.close()
