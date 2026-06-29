"""Build a deterministic, production-shaped fixture DuckDB for the e2e harness.

The data is **synthetic** but tuned to the real prod profile (see
``tests/e2e/characterize.py`` and the plan): ~31 machines across 8 HS300 strips
(6 outlets each) + 3 single-outlet plugs, watt distributions and a daily on/off/
play rhythm that mirror production. Synthetic (not a copy of prod) keeps the
fixture deterministic and small and never ships private data.

Volume strategy: the multi-day history is 1 reading/min (plenty for the hourly
kWh/peak/play rollups that back the charts), plus the last hour at 1 Hz for the
assigned emeter plugs so live sparklines look real. Everything is anchored to
build-time "now" so the data lands in each page's default time window.

    uv run python -m tests.e2e.seed /tmp/e2e.duckdb
"""

from __future__ import annotations

import random
import sys
from datetime import UTC, datetime, timedelta

import numpy as np

from juice.state import Calibration
from juice.store import Store


def _bulk_insert_readings(store: Store, rows: list[tuple]) -> None:
    """Vectorized bulk insert of reading rows, ~5000x faster than executemany.

    Each row is (ts, plug_id, watts, voltage, amps, total_kwh); a ``None`` metric
    (no-emeter plug) becomes SQL NULL via the NaN→NULL mapping below. Fixture-only
    fast path — registers a numpy relation on the Store's connection rather than
    looping ``insert_readings``, which is far too slow at ~500k rows.
    """
    if not rows:
        return
    cols = list(zip(*rows, strict=True))
    nan = lambda xs: np.array([np.nan if v is None else v for v in xs], dtype="float64")  # noqa: E731
    # Drop tzinfo (all UTC) before datetime64 — the connection runs at TimeZone=UTC,
    # so the naive instant lands as the same UTC point and avoids a numpy tz warning.
    ts_naive = [t.replace(tzinfo=None) for t in cols[0]]
    data = {
        "ts": np.array(ts_naive, dtype="datetime64[us]"),
        "plug_id": np.array(cols[1], dtype="int64"),
        "watts": nan(cols[2]),
        "voltage": nan(cols[3]),
        "amps": nan(cols[4]),
        "total_kwh": nan(cols[5]),
    }
    con = store._conn
    con.register("_seed_rows", data)
    try:
        con.execute(
            "INSERT INTO readings (ts, plug_id, watts, voltage, amps, total_kwh) "
            "SELECT ts, plug_id, "
            "  CASE WHEN isnan(watts) THEN NULL ELSE watts END, "
            "  CASE WHEN isnan(voltage) THEN NULL ELSE voltage END, "
            "  CASE WHEN isnan(amps) THEN NULL ELSE amps END, "
            "  CASE WHEN isnan(total_kwh) THEN NULL ELSE total_kwh END "
            "FROM _seed_rows"
        )
    finally:
        con.unregister("_seed_rows")


# --- fixed shape (matches the measured prod profile) -------------------------
SEED = 1729  # deterministic RNG
STRIPS = 8  # HS300 strips
OUTLETS_PER_STRIP = 6
HISTORY_DAYS = 12  # multi-day history at 1/min
LIVE_SECONDS = 3600  # trailing hour at 1 Hz for sparklines
PLAY_MIN_RSD = 8.0  # calibration: PLAYING when rolling RSD% exceeds this

# 31 machine names (public pinball / arcade titles) + the strips they live on.
MACHINE_NAMES = [
    "Blackout",
    "Cyclone",
    "Comet",
    "Monopoly",
    "Star Trek",
    "Revenge From Mars",
    "The Getaway: High Speed II",
    "The Addams Family",
    "Medieval Madness",
    "Twilight Zone",
    "Attack From Mars",
    "Cactus Canyon",
    "Theatre of Magic",
    "Tales of the Arabian Nights",
    "Indiana Jones",
    "Godzilla",
    "Stranger Things",
    "Jurassic Park",
    "Black Knight",
    "Funhouse",
    "Whirlwind",
    "Tempest (Arcade)",
    "Centipede (Arcade)",
    "Ms. Pac-Man (Arcade)",
    "Galaga (Arcade)",
    "Defender (Arcade)",
    "Asteroids (Arcade)",
    "Pokemon (Premium)",
    "Scared Stiff",
    "Junk Yard",
    "No Good Gofers",
]
# 3 of them sit on the single no-emeter plugs (on/off only, like prod EP10s).
NO_EMETER_NAMES = set(MACHINE_NAMES[-3:])

AIR_SENSORS = [
    ("E2E:AIR:FLOOR", "Arcade Floor"),
    ("E2E:AIR:BACK", "Back Room"),
    ("E2E:AIR:OFFICE", "Office"),
]


def _device_id(i: int) -> str:
    return f"E2ESTRIP{i:02d}" + "0" * 24  # 32-char, stable, prod-like length


def _single_device_id(i: int) -> str:
    return f"E2ESINGLE{i:02d}" + "0" * 23


def seed_fixture_db(path: str) -> None:
    """Create/overwrite a fixture DuckDB at ``path`` with prod-shaped data."""
    rng = random.Random(SEED)
    now = datetime.now(UTC).replace(microsecond=0)
    history_start = now - timedelta(days=HISTORY_DAYS)

    with Store(path) as store:
        strips, singles = _seed_topology(store, rng, now)
        _seed_history(store, rng, strips, singles, history_start, now)
        _seed_live_hour(store, rng, strips, now)
        _seed_air(store, rng, now)
        _seed_power_events(store, rng, strips, now)
        # Rollups back every chart — must run after the readings are inserted.
        store.refresh_hourly_usage()
        store.refresh_hourly_strip_peak()
        store.refresh_hourly_circuit_peak()
        store.refresh_hourly_play_seconds()
        store.refresh_power_baselines()


def _seed_topology(store: Store, rng: random.Random, now: datetime) -> tuple[list, list]:
    """Create strips/outlets/machines/circuits. Returns (emeter_plugs, single_plugs).

    Each emeter plug entry: dict(plug_id, machine_id|None, name|None, profile|None).
    The profile carries per-machine attract/play watt levels for reading synthesis.
    """
    assigned_from = now - timedelta(days=60)
    # Strip outlets draw from the emeter titles only; the no-emeter names are
    # reserved for the single-outlet plugs (so no title appears twice).
    name_iter = iter(n for n in MACHINE_NAMES if n not in NO_EMETER_NAMES)
    emeter: list[dict] = []

    # 2 circuits, 4 strips each (prod has none; seeded for circuit-page coverage).
    circuits = [
        store.create_circuit("A", "12", "Front room row", 20.0),
        store.create_circuit("A", "14", "Back room row", 20.0),
    ]

    for s in range(STRIPS):
        dev = _device_id(s)
        store.set_strip_name(dev, f"Row {s + 1}")
        store.set_device_circuit(dev, circuits[0] if s < STRIPS // 2 else circuits[1])
        for o in range(OUTLETS_PER_STRIP):
            child = f"{dev}{o:02d}"
            # The first ~28 of 48 strip outlets get a machine; the rest are
            # unassigned outlets (some draw, like signs/power bars; some idle).
            name = next(name_iter, None)
            alias = f"{name} - M{len(emeter) + 1:04d}" if name else f"Outlet {s + 1}-{o + 1}"
            pid = store.ensure_plug(dev, child, alias, has_emeter=True)
            entry: dict = {"plug_id": pid, "machine_id": None, "name": name, "profile": None}
            if name:
                asset = f"M{len(emeter) + 1:04d}"
                mid = store.ensure_machine(asset, name)
                store.update_assignment(pid, mid, assigned_from)
                store.set_calibration(
                    mid, Calibration(idle_max_rsd=None, play_min_rsd=PLAY_MIN_RSD)
                )
                attract = rng.uniform(65, 150)
                entry.update(
                    machine_id=mid,
                    profile={"attract": attract, "play": attract * rng.uniform(1.25, 1.7)},
                )
            else:
                # Unassigned outlets are a mix (chosen so the overall off/low/
                # drawing band shares land near prod): ~37% a powered fixture
                # (sign/display, drawing), ~26% a low steady draw, ~37% idle.
                r = rng.random()
                entry["draw"] = (
                    rng.uniform(80, 150) if r < 0.37 else rng.uniform(18, 48) if r < 0.63 else 0.0
                )
            emeter.append(entry)

    # 3 single-outlet no-emeter plugs, each a machine (on/off only).
    singles: list[dict] = []
    for i, name in enumerate(sorted(NO_EMETER_NAMES)):
        dev = _single_device_id(i)
        pid = store.ensure_plug(dev, "", name, has_emeter=False)
        asset = f"S{i + 1:04d}"
        mid = store.ensure_machine(asset, name)
        store.update_assignment(pid, mid, assigned_from)
        singles.append({"plug_id": pid, "machine_id": mid, "name": name})
    return emeter, singles


def _minute_state(rng: random.Random, ts: datetime, off_block: tuple[int, int]) -> str:
    """OFF / ATTRACT / PLAY for one minute, by hour-of-day. Machines sit in attract
    most of the day (left energized, like prod) with a short overnight off block and
    play bursts during local-ish open hours (UTC 16–04)."""
    h = ts.hour
    lo, hi = off_block
    if lo <= h < hi:
        return "off"
    open_hours = h >= 16 or h <= 4
    if open_hours and rng.random() < 0.35:  # ~play share within open hours
        return "play"
    return "attract"


def _watts(rng: random.Random, state: str, profile: dict) -> float:
    if state == "off":
        return 0.0
    if state == "attract":
        # ~1 in 12 attract minutes is a low-power standby dip (the 5–60 W band);
        # the rest is steady attract draw with low RSD.
        if rng.random() < 0.08:
            return rng.uniform(20, 48)
        return max(5.0, profile["attract"] * (1 + rng.gauss(0, 0.03)))  # stable, low RSD
    # PLAY: high variance (solenoid spikes) → high rolling RSD → classified PLAYING.
    # Modest jitter for the bulk, with a rare big spike (flippers/pops) for the
    # long tail toward the machine's peak — keeps p99 near prod's while max stays high.
    spike = 1.6 if rng.random() < 0.04 else 1.0
    return max(5.0, profile["play"] * spike * (1 + rng.uniform(-0.3, 0.5)))


def _off_block(rng: random.Random) -> tuple[int, int]:
    """A ~6h overnight off window [start, start+6) in UTC hours (~prod's off share)."""
    lo = rng.randint(5, 8)  # ~00:00–03:00 Central
    return (lo, lo + 6)


def _seed_history(store, rng, emeter, singles, start, now) -> None:
    """HISTORY_DAYS of 1/min readings for every plug."""
    for e in emeter:
        off_block = _off_block(rng)
        rows = []
        t = start
        while t < now:
            if e["profile"]:
                st = _minute_state(rng, t, off_block)
                w = _watts(rng, st, e["profile"])
            else:  # unassigned outlet: steady draw or idle
                w = e.get("draw", 0.0)
            rows.append((t, e["plug_id"], w, 120.0, w / 120.0, 0.0))
            t += timedelta(minutes=1)
        _bulk_insert_readings(store, rows)

    # No-emeter singles: NULL-watt when on, 0 when off (mirrors the recorder).
    for sgl in singles:
        off_block = _off_block(rng)
        rows = []
        t = start
        while t < now:
            on = not (off_block[0] <= t.hour < off_block[1])
            rows.append(
                (t, sgl["plug_id"], None, None, None, None)
                if on
                else (t, sgl["plug_id"], 0.0, 0.0, 0.0, 0.0)
            )
            t += timedelta(minutes=1)
        _bulk_insert_readings(store, rows)


def _seed_live_hour(store, rng, emeter, now) -> None:
    """Trailing hour at 1 Hz for assigned emeter plugs → fine-grained sparklines."""
    start = now - timedelta(seconds=LIVE_SECONDS)
    for e in emeter:
        if not e["profile"]:
            continue
        playing = rng.random() < 0.3
        rows = []
        for s in range(LIVE_SECONDS):
            st = "play" if playing else "attract"
            w = _watts(rng, st, e["profile"])
            rows.append((start + timedelta(seconds=s), e["plug_id"], w, 120.0, w / 120.0, 0.0))
        _bulk_insert_readings(store, rows)


def _seed_air(store, rng, now) -> None:
    start = now - timedelta(days=7)
    for mac, name in AIR_SENSORS:
        store.ensure_air_sensor(mac, name, online=True, seen_ts=now)
        rows = []
        t = start
        while t < now:
            co2 = rng.uniform(450, 1400) if rng.random() > 0.05 else rng.uniform(1800, 2600)
            rows.append(
                (
                    t,
                    mac,
                    round(rng.uniform(20, 26), 1),  # temperature
                    round(rng.uniform(45, 60), 0),  # humidity
                    round(co2, 0),  # co2
                    round(rng.uniform(2, 30), 0),  # pm25
                    round(rng.uniform(3, 40), 0),  # pm10
                    round(rng.uniform(20, 250), 0),  # tvoc
                    round(rng.uniform(35, 55), 0),  # noise
                    round(rng.uniform(80, 100), 0),  # battery
                )
            )
            t += timedelta(minutes=15)
        store.insert_air_readings(rows)


def _seed_power_events(store, rng, emeter, now) -> None:
    """A handful of audit rows over recent days for the /events page (prod mix:
    mostly all-on/all-off, then individual, a few reboots)."""
    assigned = [e for e in emeter if e["machine_id"]]
    actor = "dev@localhost"
    for d in range(6):
        day = now - timedelta(days=d, hours=2)
        for pid in (e["plug_id"] for e in rng.sample(assigned, k=min(8, len(assigned)))):
            store.record_power_event(day, pid, "turn_on", "all_on", actor, "ok", operation_id="op")
        for pid in (e["plug_id"] for e in rng.sample(assigned, k=min(6, len(assigned)))):
            store.record_power_event(
                day + timedelta(hours=10),
                pid,
                "turn_off",
                "all_off",
                actor,
                "ok",
                operation_id="op",
            )
    one = rng.choice(assigned)["plug_id"]
    store.record_power_event(now - timedelta(hours=3), one, "turn_off", "individual", actor, "ok")
    store.record_power_event(now - timedelta(hours=1), one, "turn_off", "reboot", actor, "ok")


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "tests/e2e/fixture.duckdb"
    seed_fixture_db(out)
    print(f"seeded fixture → {out}")
