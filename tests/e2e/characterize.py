"""Print a statistical profile of a juice DuckDB, read-only.

Used to keep the e2e fixture (``seed.py``) realistic against production: pull a
fresh prod snapshot (``make backup`` → ``data/backups/``), run this against it,
and tune the seed constants to match. Also run it against the seeded fixture to
confirm the two profiles line up.

    uv run python -m tests.e2e.characterize data/backups/juice-<ts>.duckdb
"""

from __future__ import annotations

import sys

import duckdb


def _profile(path: str) -> None:
    con = duckdb.connect(path, read_only=True)

    def q(sql: str) -> list[tuple]:
        return con.execute(sql).fetchall()

    print(f"== profile: {path} ==\n")

    print("counts:")
    for table in (
        "machines",
        "plugs",
        "assignments",
        "readings",
        "circuits",
        "strips",
        "air_sensors",
        "air_readings",
        "power_events",
    ):
        try:
            n = q(f"SELECT count(*) FROM {table}")[0][0]
        except duckdb.Error:
            n = "—"
        print(f"  {table:14} {n}")
    print(f"  devices        {q('SELECT count(DISTINCT device_id) FROM plugs')[0][0]}")
    print(
        "  open assigns   "
        f"{q('SELECT count(*) FROM assignments WHERE assigned_until IS NULL')[0][0]}"
    )

    print("\noutlets per device:")
    for device_id, n in q("SELECT device_id, count(*) FROM plugs GROUP BY 1 ORDER BY 2 DESC"):
        print(f"  {device_id[:12]}…  {n}")

    print("\nwatts (non-null readings):")
    p = q(
        "SELECT quantile_cont(watts,[0.5,0.9,0.99,1.0]), avg(watts) "
        "FROM readings WHERE watts IS NOT NULL"
    )[0]
    pcts = ", ".join(f"{v:.0f}" for v in p[0])
    print(f"  p50/p90/p99/max = {pcts} W   avg = {p[1]:.1f} W")
    bands = q(
        "SELECT 100.0*count(*) FILTER (WHERE watts<5)/count(*), "
        "100.0*count(*) FILTER (WHERE watts>=5 AND watts<60)/count(*), "
        "100.0*count(*) FILTER (WHERE watts>=60)/count(*) "
        "FROM readings WHERE watts IS NOT NULL"
    )[0]
    print(
        f"  band shares: off<5={bands[0]:.1f}%  low5-60={bands[1]:.1f}%  drawing>=60={bands[2]:.1f}%"
    )
    span = q("SELECT min(ts), max(ts) FROM readings")[0]
    print(f"  span: {span[0]} → {span[1]}")

    print("\nper-machine drawing watts (>=60 W), top 12 by sample count:")
    for name, med, peak, n in q(
        "SELECT m.name, quantile_cont(r.watts,0.5)::DOUBLE, max(r.watts)::DOUBLE, count(*) "
        "FROM readings r "
        "JOIN assignments a ON a.plug_id=r.plug_id AND a.assigned_until IS NULL "
        "JOIN machines m ON m.machine_id=a.machine_id "
        "WHERE r.watts>=60 GROUP BY m.name ORDER BY 4 DESC LIMIT 12"
    ):
        print(f"  {name[:34]:34} median {med:4.0f} W   peak {peak:4.0f} W   n={n}")

    if q("SELECT count(*) FROM air_readings")[0][0]:
        a = q(
            "SELECT min(temperature),max(temperature),min(humidity),max(humidity),"
            "min(co2),max(co2),min(pm25),max(pm25) FROM air_readings"
        )[0]
        print("\nair ranges:")
        print(
            f"  temp {a[0]:.1f}–{a[1]:.1f} °C  humidity {a[2]:.0f}–{a[3]:.0f} %  "
            f"co2 {a[4]:.0f}–{a[5]:.0f} ppm  pm25 {a[6]:.0f}–{a[7]:.0f}"
        )

    print("\npower events by action/source:")
    for action, source, n in q(
        "SELECT action, source, count(*) FROM power_events GROUP BY 1,2 ORDER BY 3 DESC LIMIT 8"
    ):
        print(f"  {action:9} {source:12} {n}")

    con.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: python -m tests.e2e.characterize <path-to.duckdb>")
    _profile(sys.argv[1])
