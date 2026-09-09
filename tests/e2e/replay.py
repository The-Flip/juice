"""Replay a day of production readings through tap's real uplink into juice.

`tap` cannot be deployed to the museum for a few days, so the only honest
source of realistic ingest traffic is production's own history. This reads a
chosen day out of a `make backup` snapshot, reshapes it into what tap would
have collected, and pushes it through the **real** `tap.buffer.Buffer` and
`tap.uplink.Uplink`. Nothing below the device seam is faked, so the receiver is
exercised against the actual client's cursor, ack, window and reconnect
behaviour rather than against a convenient stub.

Two reshapings are needed, and both are deliberate rather than incidental:

**Relay state has to be inferred**, because juice's `readings` table has never
had a column for it. The cloud recorder's own write conventions are the key:
relay off writes all zeros, on-but-unmetered writes all NULLs, and on-and-metered
writes real values. So an all-zero row means off and everything else means on --
`watts > 0` would be wrong, because on 2026-09-02 5% of rows are a live outlet
drawing nothing, and calling those "off" would invent thousands of state changes.

**The cadence has to be raised to 1 Hz.** Production recorded at a p50 of 6.7 s
because the cloud recorder polls devices sequentially; tap polls at 1 Hz. Held
values fill the gaps, which is what makes this a ~4.2M row day rather than a
215k row one -- and that volume is the entire point of the exercise.

    uv run python -m tests.e2e.replay --source data/backups/juice-....duckdb \
        --day 2026-09-02 --url http://127.0.0.1:8099/api/v2/ingest --token devtoken

Work on a copy of the target database, never on `juice.duckdb` itself.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import sys
import time
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from datetime import time as clock
from zoneinfo import ZoneInfo

import duckdb

from tap.buffer import Buffer
from tap.config import Config, UplinkConfig
from tap.device import OutletReading, Sweep
from tap.health import Health
from tap.uplink import Uplink

log = logging.getLogger("replay")

# Past this, an outlet is treated as having stopped reporting rather than as
# holding steady. Production's p99 gap is 61 s, so this changes almost nothing
# in normal operation -- but without it a plug that went offline for six hours
# would be replayed as six hours of invented steady draw, and the "an outlet
# disappears from its strip's sweep" path would never be exercised at all.
DEFAULT_MAX_HOLD_S = 300

# tap's own retention would delete the replayed day file by filename before it
# was ever sent.
BUFFER_RETENTION_DAYS = 3650

# Windows are given in museum time, not UTC. "9am to 9pm on a Saturday" is a
# statement about when the doors were open; expressed in UTC it is 14:00 to
# 02:00 and lands on two different dates, which is a good way to replay the
# wrong twelve hours. Same zone juice buckets its local-day metrics in.
LOCAL_TZ = ZoneInfo("America/Chicago")

# Backfill-only backpressure. tap's submit queue holds 600 sweeps and silently
# drops the oldest when it overflows -- correct for a 1 Hz collector that must
# never block its poll loop, fatal for a replayer that can produce a day of
# sweeps in under a second. Live mode does not use this: there the writer task
# keeps up unaided, and forcing a commit on a timer would batch the stream into
# lumps that look nothing like what tap really sends.
FLUSH_EVERY_SECONDS = 30

# A long live run is otherwise silent between its first and last line, which
# leaves nothing to read afterwards when the question is "how did it go".
PROGRESS_EVERY_SECONDS = 300


@dataclass(frozen=True, slots=True)
class Sample:
    """One outlet reading, in tap's units."""

    second: int
    device_id: str
    child_id: str
    relay_on: bool
    power_mw: int | None
    voltage_mv: int | None
    current_ma: int | None
    energy_wh: int | None


def derive_relay_on(watts, voltage, amps, total_kwh) -> bool:
    """Was the relay closed, per the cloud recorder's write conventions?

    `juice/recorder.py` writes all zeros for a relay that is off, all NULLs for
    an outlet that is on but has no meter, and real values otherwise. So:

    - every metered field zero  -> off
    - everything NULL           -> on, unmetered
    - anything else             -> on

    Note this is *not* `watts > 0`. On 2026-09-02, 5% of rows read 0 W with
    live voltage -- a machine switched on at the outlet and off at its own
    switch. Those are on.
    """
    if watts is None and voltage is None and amps is None and total_kwh is None:
        return True
    return not (not watts and not voltage and not amps)


def to_milli(value: float | None) -> int | None:
    """Units to milli-units. None stays None -- it means unmeasured, not zero."""
    return None if value is None else int(round(value * 1000))


def resolve_window(day: str, start: str | None, end: str | None) -> tuple[datetime, datetime]:
    """UTC bounds for a local-clock window on `day`.

    With no `--start`/`--end`, the whole UTC day, which is what the earlier
    runs used. With them, local times on that local date -- and an `end` at or
    before `start` means it runs past midnight into the next day.
    """
    date = datetime.strptime(day, "%Y-%m-%d").date()
    if start is None and end is None:
        begin = datetime.combine(date, clock(0, 0), tzinfo=UTC)
        return begin, begin + timedelta(days=1)

    def local(value: str, default: clock) -> datetime:
        parsed = clock.fromisoformat(value) if value else default
        return datetime.combine(date, parsed, tzinfo=LOCAL_TZ)

    begin = local(start, clock(0, 0))
    finish = local(end, clock(0, 0))
    if finish <= begin:
        finish += timedelta(days=1)
    return begin.astimezone(UTC), finish.astimezone(UTC)


def load_window(source: str, begin: datetime, end: datetime) -> list[Sample]:
    """Every recorded reading in a UTC half-open range, in tap's shape."""
    con = duckdb.connect(source, read_only=True)
    try:
        con.execute("SET TimeZone='UTC'")
        rows = con.execute(
            """
            SELECT r.ts, p.device_id, p.child_id, r.watts, r.voltage, r.amps, r.total_kwh
            FROM readings r
            JOIN plugs p USING (plug_id)
            WHERE r.ts >= ? AND r.ts < ?
            ORDER BY r.ts
            """,
            [begin.replace(tzinfo=None), end.replace(tzinfo=None)],
        ).fetchall()
    finally:
        con.close()

    return [
        Sample(
            second=int(ts.replace(tzinfo=UTC).timestamp()),
            device_id=device_id,
            child_id=child_id,
            relay_on=derive_relay_on(watts, voltage, amps, total_kwh),
            power_mw=to_milli(watts),
            voltage_mv=to_milli(voltage),
            current_ma=to_milli(amps),
            # Wh, not kWh: tap ships the device's raw integer counter.
            energy_wh=None if total_kwh is None else int(round(total_kwh * 1000)),
        )
        for ts, device_id, child_id, watts, voltage, amps, total_kwh in rows
    ]


def upsample(
    samples: list[Sample], *, max_hold_s: int = DEFAULT_MAX_HOLD_S
) -> Iterator[tuple[int, list[Sample]]]:
    """Yield `(second, samples)` at 1 Hz, holding each outlet's last value.

    An outlet appears only once it has reported, and drops out again after
    `max_hold_s` without a fresh reading -- reproducing what a sweep of a strip
    with one unreachable outlet actually looks like.
    """
    by_second: dict[int, list[Sample]] = defaultdict(list)
    for sample in samples:
        by_second[sample.second].append(sample)
    if not by_second:
        return

    last: dict[tuple[str, str], tuple[int, Sample]] = {}
    for second in range(min(by_second), max(by_second) + 1):
        for sample in by_second.get(second, ()):
            last[(sample.device_id, sample.child_id)] = (second, sample)
        live = [s for seen, s in last.values() if second - seen <= max_hold_s]
        if live:
            yield second, live


def to_sweeps(second: int, samples: list[Sample]) -> list[Sweep]:
    """Group one second's outlets into one Sweep per strip.

    A `Sweep` is one device observed at one instant (`tap/device.py`), which is
    also what juice's `hourly_strip_peak` relies on to reconstruct simultaneous
    draw: it groups on an exact timestamp.
    """
    by_device: dict[str, list[Sample]] = defaultdict(list)
    for sample in samples:
        by_device[sample.device_id].append(sample)

    ts = datetime.fromtimestamp(second, UTC)
    return [
        Sweep(
            device_id=device_id,
            ts=ts,
            outlets=[
                OutletReading(
                    child_id=s.child_id,
                    alias="",  # tap learns aliases from the device, not from us
                    relay_on=s.relay_on,
                    power_mw=s.power_mw,
                    voltage_mv=s.voltage_mv,
                    current_ma=s.current_ma,
                    energy_wh=s.energy_wh,
                )
                for s in outlets
            ],
        )
        for device_id, outlets in by_device.items()
    ]


async def _drain(buffer: Buffer, health: Health, timeout: float) -> None:
    """Wait until everything buffered has been acked.

    The uplink reports its durable cursor through `Health`; when that reaches
    the buffer's high water mark, juice has committed every row we submitted.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        high = await buffer.high_water()
        if health.uplink.acked_cursor == high:
            return
        await asyncio.sleep(0.5)
    log.warning("timed out waiting for the uplink to drain")


async def replay(args: argparse.Namespace) -> int:
    begin, end = resolve_window(args.day, args.start, args.end)
    log.info(
        "loading %s .. %s (local %s .. %s) from %s",
        begin,
        end,
        begin.astimezone(LOCAL_TZ).strftime("%a %H:%M"),
        end.astimezone(LOCAL_TZ).strftime("%a %H:%M"),
        args.source,
    )
    samples = load_window(args.source, begin, end)
    if not samples:
        log.error("no readings in that window in %s", args.source)
        return 1
    outlets = len({(s.device_id, s.child_id) for s in samples})
    log.info("%d recorded readings across %d outlets", len(samples), outlets)

    # Where the replayed day lands in wall-clock time.
    #
    # `start` is what a live run wants: the first replayed second becomes now,
    # so tap reports ~zero lag and behaves like a collector streaming the
    # present. With the recorded timestamps it reports days of lag, suppresses
    # its live frames and runs its whole backfill path -- correct behaviour, but
    # not what a steady-state observation is trying to look at.
    #
    # `end` puts the *last* second at now, which is the shape of a tap that has
    # been offline and is catching up.
    shift = 0
    if args.anchor == "start":
        shift = int(datetime.now(UTC).timestamp()) - min(s.second for s in samples)
    elif args.anchor == "end":
        shift = int(datetime.now(UTC).timestamp()) - max(s.second for s in samples)
    if shift:
        log.info("anchor=%s: shifting timestamps by %+d seconds", args.anchor, shift)

    buffer = Buffer(args.buffer_dir, retention_days=BUFFER_RETENTION_DAYS)
    await buffer.open()
    config = Config(
        tap_id=args.tap_id,
        uplink=UplinkConfig(url=args.url, token=args.token, enabled=True),
    )
    health = Health()
    uplink = Uplink(config, buffer, health)
    # The Buffer does NOT start its own writer; production does it in
    # tap/supervise.py. Without this task nothing ever drains the submit queue,
    # so rows reach disk only when something calls flush() -- which silently
    # turns a 1 Hz stream into whatever the flush interval happens to be, and
    # overflows the 600-sweep queue under any real load.
    writer = asyncio.create_task(buffer.run())
    task = asyncio.create_task(uplink.run())

    submitted = 0
    started = time.monotonic()
    first_second: int | None = None
    try:
        for second, live in upsample(samples, max_hold_s=args.max_hold):
            if first_second is None:
                first_second = second
            for sweep in to_sweeps(second + shift, live):
                buffer.submit(sweep)
            submitted += len(live)

            elapsed_replay = second - first_second
            # Backfill submits a day of sweeps in seconds, far faster than the
            # writer commits, so it needs a brake. Live mode must NOT have one:
            # letting the writer drain continuously is what makes the uplink
            # send production-shaped batches instead of one lump per interval.
            if (
                args.mode == "backfill"
                and elapsed_replay
                and elapsed_replay % FLUSH_EVERY_SECONDS == 0
            ):
                await buffer.flush()
            if elapsed_replay and elapsed_replay % PROGRESS_EVERY_SECONDS == 0:
                up = health.uplink
                log.info(
                    "progress: %6d submitted | %5.0fs replayed | %5.0fs wall | "
                    "acked=%s lag=%s rows / %ss | batches sent=%d acked=%d "
                    "nacked=%d poisoned=%d | reconnects=%d%s",
                    submitted,
                    elapsed_replay,
                    time.monotonic() - started,
                    up.acked_cursor,
                    up.lag_rows,
                    None if up.lag_seconds is None else round(up.lag_seconds),
                    up.batches_sent,
                    up.batches_acked,
                    up.batches_nacked,
                    up.batches_poisoned,
                    up.reconnects,
                    f" | last_error={up.last_error!r}" if up.last_error else "",
                )

            if args.mode == "live":
                # Wall-clock pacing: one replayed second per real second.
                target = started + (second - first_second) / args.speed
                delay = target - time.monotonic()
                if delay > 0:
                    await asyncio.sleep(delay)

            if args.limit and submitted >= args.limit:
                break

        await buffer.flush()
        log.info("submitted %d readings in %.1fs", submitted, time.monotonic() - started)
        await _drain(buffer, health, timeout=args.drain_timeout)
    finally:
        uplink.stop()
        task.cancel()
        writer.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        with contextlib.suppress(asyncio.CancelledError):
            await writer
        await buffer.close()
    return 0


def verify(args: argparse.Namespace) -> int:
    """Check what actually landed, asserting on values rather than vibes."""
    begin, end = resolve_window(args.day, args.start, args.end)
    day = begin.replace(tzinfo=None)
    con = duckdb.connect(args.db, read_only=True)
    try:
        con.execute("SET TimeZone='UTC'")
        window = [day, end.replace(tzinfo=None)]
        total, first, last = con.execute(
            "SELECT count(*), min(ts), max(ts) FROM readings WHERE ts >= ? AND ts < ?", window
        ).fetchone()
        dupes = con.execute(
            """
            SELECT count(*) FROM (
                SELECT ts, plug_id FROM readings WHERE ts >= ? AND ts < ?
                GROUP BY ts, plug_id HAVING count(*) > 1
            )
            """,
            window,
        ).fetchone()[0]
        with_relay = con.execute(
            "SELECT count(*) FROM readings WHERE ts >= ? AND ts < ? AND relay_on IS NOT NULL",
            window,
        ).fetchone()[0]
        cloud_era_relay = con.execute(
            "SELECT count(*) FROM readings WHERE relay_on IS NOT NULL AND ts < ?", [day]
        ).fetchone()[0]
        unmetered_on = con.execute(
            "SELECT count(*) FROM readings WHERE ts >= ? AND ts < ? AND relay_on AND watts IS NULL",
            window,
        ).fetchone()[0]
        cursors = con.execute("SELECT tap_id, buffer_id, cursor FROM ingest_cursors").fetchall()
    finally:
        con.close()

    # Replaying into a copy of production means the day already holds the
    # cloud-era rows it was recorded from. Those SHOULD have a null relay --
    # the recorder never knew it -- so the two populations are counted apart
    # rather than summed.
    cloud_era_in_window = total - with_relay
    print(f"rows in {args.day}:        {total}")
    print(f"  span:                   {first} .. {last}")
    print(f"  ingested by tap:        {with_relay}   (relay_on recorded)")
    print(f"  pre-existing cloud-era: {cloud_era_in_window}   (relay_on null, as expected)")
    print(f"  on but unmetered:       {unmetered_on}")
    print(f"  (ts, plug_id) dupes:    {dupes}")
    print(f"  cloud-era relay_on set: {cloud_era_relay}  (must be 0)")
    print(f"  ingest cursors:         {cursors}")

    ok = True
    if dupes:
        print("FAIL: duplicate (ts, plug_id) rows -- the cursor design is not holding")
        ok = False
    if not with_relay:
        print("FAIL: nothing was ingested for this day")
        ok = False
    if cloud_era_relay:
        print("FAIL: rows predating the replay have relay_on set")
        ok = False
    if cursors and with_relay:
        # A fresh buffer numbers rows from 1, so the cursor is exactly the
        # number of rows tap believes it delivered. If juice stored fewer, it
        # acked something it did not commit -- the one bug this whole design
        # exists to prevent.
        delivered = max(int(c) for _, _, c in cursors)
        print(f"  cursor vs stored:       {delivered} vs {with_relay}")
        if delivered != with_relay:
            print("FAIL: the acked cursor and the stored row count disagree")
            ok = False
    print("OK" if ok else "FAILED")
    return 0 if ok else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source", help="Production backup to replay from.")
    parser.add_argument("--day", required=True, help="Day to replay, YYYY-MM-DD.")
    parser.add_argument(
        "--start", default=None, help="Local (America/Chicago) start time, e.g. 09:00."
    )
    parser.add_argument(
        "--end",
        default=None,
        help="Local end time, e.g. 21:00. At or before --start means the next day.",
    )
    parser.add_argument("--url", default="http://127.0.0.1:8099/api/v2/ingest")
    parser.add_argument("--token", default=None, help="JUICE_INGEST_TOKEN of the target server.")
    parser.add_argument("--tap-id", default="replay")
    parser.add_argument("--buffer-dir", default="./data/replay-buffer")
    parser.add_argument(
        "--mode",
        choices=("live", "backfill"),
        default="live",
        help="live: one replayed second per wall-clock second. "
        "backfill: fill the buffer unpaced and let the uplink drain it, which is "
        "what a tap reconnecting after an outage actually does.",
    )
    parser.add_argument("--speed", type=float, default=1.0, help="Live-mode multiplier.")
    parser.add_argument("--max-hold", type=int, default=DEFAULT_MAX_HOLD_S)
    parser.add_argument("--limit", type=int, default=0, help="Stop after N readings.")
    parser.add_argument("--drain-timeout", type=float, default=300.0)
    parser.add_argument(
        "--anchor",
        choices=("none", "start", "end"),
        default="none",
        help="Where the replayed day lands in wall-clock time. "
        "none: keep the recorded timestamps. "
        "start: the first replayed second is now (a live collector). "
        "end: the last replayed second is now (one catching up).",
    )
    parser.add_argument("--verify", action="store_true", help="Inspect a target DB and exit.")
    parser.add_argument("--db", help="Target DuckDB, for --verify.")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    if args.verify:
        if not args.db:
            parser.error("--verify needs --db")
        return verify(args)
    if not args.source:
        parser.error("--source is required unless --verify")
    return asyncio.run(replay(args))


if __name__ == "__main__":
    sys.exit(main())
