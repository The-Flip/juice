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

Regeneration is not byte-stable — parquet row-group boundaries shift slightly —
so re-running this always shows a binary diff even when the content is
identical. Compare row counts, not checksums.

Pick new evenings by their behaviour, not their date: the suite needs powered-
down hours, pure attract, active play, Godzilla idle, near-miss windows that sit
just inside the PLAYING and ABANDONED thresholds, and an evening where
`auto_calibrate` finds Godzilla's idle cluster (only about half of them do).
"""

from __future__ import annotations

import pathlib
import sys

import duckdb

# Eight days, chosen because between them they contain every behaviour the
# suite characterises. See the module docstring before changing them.
# (day, first hour UTC, hours) — each span carries exactly what its day is here
# for and no more, which keeps the checked-in file small. A day's own comment
# says which behaviour it supplies; widen its span before adding a window
# outside it, or `_fetch_watts`'s 25-reading floor will fail loudly.
DAYS = [
    # TAF's only idle stretch in five weeks. Afternoon, which is why a
    # 21:00 start made it look as though TAF never went idle at all.
    ("2026-08-11", 17, 2),
    # EBD playing -> idle -> playing, all three segments real. Also afternoon.
    ("2026-08-21", 18, 2),
    # Attract, RFM quiet attract, EBD play, a real Blackout dip at 16:18, and
    # the 01:07 power-down on the 26th that the OFF tests use.
    ("2026-08-25", 16, 10),
    # RFM near-miss on the play threshold, Godzilla idle, EBD idle.
    ("2026-08-26", 21, 5),
    # TAF near-miss on the idle threshold, Godzilla play, Godzilla idle.
    ("2026-08-28", 22, 4),
    # The Godzilla transition across midnight, a third RFM near-miss, and the
    # auto_calibrate evening for Hyperball, RFM, EBD and TAF.
    ("2026-08-29", 21, 6),
    # RFM and TAF near-misses, Godzilla idle.
    ("2026-09-01", 21, 5),
    # Hyperball play, Godzilla idle, and the one evening in the set where
    # auto_calibrate finds Godzilla's idle cluster.
    ("2026-09-02", 21, 5),
]
MACHINES = [
    "Blackout",
    "Eight Ball Deluxe Limited Edition",
    "Godzilla (Premium)",
    "Hyperball",
    "Revenge From Mars",
    "The Addams Family (Coin Op)",
]
OUT = pathlib.Path(__file__).resolve().parent.parent / "tests" / "data"


def main(src_path: str) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    names = ",".join(f"'{n}'" for n in MACHINES)
    spans = " OR ".join(
        f"(r.ts >= TIMESTAMP '{day} {hour:02d}:00:00' "
        f"AND r.ts < TIMESTAMP '{day} {hour:02d}:00:00' + INTERVAL {hours} HOUR)"
        for day, hour, hours in DAYS
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
    # Joined through `assignments` on time, not just on plug_id. These six
    # machines moved outlets on 2026-05-24, so their old plug ids belong to six
    # *other* machines in this date range — filtering on plug_id alone pulled in
    # 88,563 rows of Twilight Zone, Lightning, Cyclone, Comet, Stock Car and
    # Dr. Dude, 48% of the file, none of it reachable through the tests' own
    # join and all of it liable to be attributed to the wrong machine by anyone
    # who queried it more loosely.
    con.execute(
        f"CREATE TABLE readings AS SELECT r.ts, r.plug_id, r.watts FROM src.readings r "
        f"JOIN assignments a ON r.plug_id = a.plug_id AND r.ts >= a.assigned_from "
        f"AND (a.assigned_until IS NULL OR r.ts < a.assigned_until) "
        f"WHERE ({spans})"
    )
    for table in ("machines", "assignments", "plugs", "readings"):
        path = OUT / f"{table}.parquet"
        con.execute(f"COPY {table} TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        n = con.sql(f"SELECT count(*) FROM {table}").fetchone()[0]
        print(f"  {table:12} {n:>8,} rows  ->  {path.name} ({path.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "juice.duckdb")
