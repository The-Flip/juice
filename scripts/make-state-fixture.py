#!/usr/bin/env python
"""Regenerate the `tests/test_state.py` fixture from a production snapshot.

`tests/test_state.py` characterises `juice.state` against real power data. It
used to read `data/juice.duckdb` — a hand-placed copy of production — which
meant the tests silently skipped whenever that file was absent, and in
September 2026 they had been skipping unnoticed for some time. The readings
they described are also gone: production now starts at 2026-03-23, two days
after the last window those tests used.

So the data they need is checked in instead, as parquet: ~200k readings for six
machines across seven evenings, about 800 KB. Only the columns `juice.state`
needs are kept (`voltage`, `amps` and `total_kwh` are entirely NULL in this
range anyway).

    uv run python scripts/make-state-fixture.py [path/to/juice.duckdb]

Pick new evenings by their behaviour, not their date: the suite needs powered-
down hours, pure attract, active play, Godzilla idle, near-miss windows that sit
just inside the PLAYING and ABANDONED thresholds, and an evening where
`auto_calibrate` finds Godzilla's idle cluster (only about half of them do).
"""

from __future__ import annotations

import pathlib
import sys

import duckdb

# Seven evenings, chosen because between them they contain every behaviour the
# suite characterises. See the module docstring before changing them.
DAYS = [
    "2026-07-16",  # the only EBD playing -> idle -> playing transition in range;
    # EBD is idle for just 1.8% of readings, so these are scarce
    "2026-08-25",  # attract, RFM quiet attract, EBD play, Blackout dip
    "2026-08-26",  # the 01:07 power-down and the quiet hours the OFF tests use
    "2026-08-28",  # TAF near-miss on the idle threshold
    "2026-08-29",  # Godzilla play and idle; the auto_calibrate evening for four
    "2026-09-01",  # RFM near-miss on the play threshold, TAF near-miss
    "2026-09-02",  # Hyperball play, Godzilla idle, and the one evening where
    # auto_calibrate finds Godzilla's idle cluster
]
MACHINES = [
    "Blackout",
    "Eight Ball Deluxe Limited Edition",
    "Godzilla (Premium)",
    "Hyperball",
    "Revenge From Mars",
    "The Addams Family (Coin Op)",
]
# 21:00 UTC (4pm CT) through 05:00, so each evening carries both the open hours
# and the powered-down ones after close.
SPAN_HOURS = 8
OUT = pathlib.Path(__file__).resolve().parent.parent / "tests" / "data"


def main(src_path: str) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    names = ",".join(f"'{n}'" for n in MACHINES)
    spans = " OR ".join(
        f"(r.ts >= TIMESTAMP '{d} 21:00:00' "
        f"AND r.ts < TIMESTAMP '{d} 21:00:00' + INTERVAL {SPAN_HOURS} HOUR)"
        for d in DAYS
    )
    con = duckdb.connect(":memory:")
    con.execute(f"ATTACH '{src_path}' AS src (READ_ONLY)")
    con.execute(f"CREATE TABLE machines AS SELECT * FROM src.machines WHERE name IN ({names})")
    con.execute(
        "CREATE TABLE assignments AS SELECT a.* FROM src.assignments a "
        "WHERE a.machine_id IN (SELECT machine_id FROM machines)"
    )
    con.execute(
        "CREATE TABLE plugs AS SELECT p.* FROM src.plugs p "
        "WHERE p.plug_id IN (SELECT plug_id FROM assignments)"
    )
    con.execute(
        f"CREATE TABLE readings AS SELECT r.ts, r.plug_id, r.watts FROM src.readings r "
        f"WHERE r.plug_id IN (SELECT plug_id FROM assignments) AND ({spans})"
    )
    for table in ("machines", "assignments", "plugs", "readings"):
        path = OUT / f"{table}.parquet"
        con.execute(f"COPY {table} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        n = con.sql(f"SELECT count(*) FROM {table}").fetchone()[0]
        print(f"  {table:12} {n:>8,} rows  ->  {path.name} ({path.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "juice.duckdb")
