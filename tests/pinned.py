"""An oracle for "is some connection holding a DuckDB transaction open?".

`Store.settle` explains the mechanism. What matters here: `duckdb_memory()`
reports the retained undo buffers under the `TRANSACTION` tag, one block
(256 KB) per pinned write, which is what makes a pin measurable from a test.

Both helpers use connections of their own, never `store._conn`. Running any
statement on the pinning connection releases its pin, so a probe that wrote or
measured through `_conn` would be blind to a pin held by `_conn` itself --
which is exactly the one `Store.ingest_cursor` used to leave at every hello.
"""

from __future__ import annotations

import itertools

from juice.store import Store

# `hammer(store)` runs this many write transactions. Pinned, they retain a
# block each (~52 MB measured); released, the tag reads 0.
WRITES = 200
PINNED_AT_LEAST = 32 * 1024 * 1024
RELEASED_AT_MOST = 8 * 1024 * 1024


def pinned_bytes(store: Store) -> int:
    """Bytes DuckDB is holding for transactions it could not yet clean up."""
    conn = store.new_connection()
    try:
        row = conn.execute(
            "SELECT coalesce(max(memory_usage_bytes), 0) FROM duckdb_memory() "
            "WHERE tag = 'TRANSACTION'"
        ).fetchall()
        return int(row[0][0])
    finally:
        conn.close()


_probe = itertools.count()


def hammer(store: Store) -> None:
    """`WRITES` small write transactions, on a connection of their own.

    A fresh cursor row each call: the upsert never retreats, so re-running
    the same cursors would be `WRITES` no-ops rather than `WRITES` writes.
    """
    tap_id = f"pin-probe-{next(_probe)}"
    conn = store.new_connection()
    try:
        for n in range(WRITES):
            store.set_ingest_cursor(tap_id, "buf", f"{n:018d}", conn=conn)
    finally:
        conn.close()
