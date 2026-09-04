"""Buffer correctness: ordering, day rolls, retention, overflow, crash safety.

The buffer is the one component where the storage engine *is* the correctness
question, so these tests use real SQLite in a tmp_path rather than a fake.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import subprocess
import sys
import textwrap
import time
from datetime import UTC, datetime, timedelta

import pytest

from tap.buffer import CLOCK_FLOOR, Buffer, make_cursor, parse_cursor
from tap.device import OutletReading, Sweep
from tap.errors import FatalError


def _sweep(ts: datetime, device_id: str = "DEV1", n: int = 2, watts: int = 1000) -> Sweep:
    return Sweep(
        device_id=device_id,
        ts=ts,
        outlets=[
            OutletReading(
                child_id=f"{device_id}{i:02d}",
                alias=f"outlet {i}",
                relay_on=True,
                power_mw=watts,
                voltage_mv=119_000,
                current_ma=8,
                energy_wh=3,
            )
            for i in range(n)
        ],
    )


BASE = datetime(2026, 9, 3, 12, 0, 0, tzinfo=UTC)


@pytest.fixture
async def buf(tmp_path):
    b = Buffer(tmp_path / "buffer", retention_days=30)
    await b.open()
    yield b
    await b.close()


class TestCursors:
    def test_zero_padded_so_string_order_is_seq_order(self):
        assert make_cursor(9) < make_cursor(10)
        assert make_cursor(999) < make_cursor(1000)

    def test_roundtrip(self):
        assert parse_cursor(make_cursor(42)) == 42

    def test_a_malformed_cursor_raises_rather_than_being_guessed_at(self):
        """The uplink validates before trusting a server-supplied cursor."""
        for bad in ("", "garbage", "20260903:12"):
            with pytest.raises(ValueError):
                parse_cursor(bad)


class TestWriting:
    async def test_submitted_rows_are_readable_in_order(self, buf):
        buf.submit(_sweep(BASE))
        buf.submit(_sweep(BASE + timedelta(seconds=1)))
        await buf.flush()

        rows = await buf.read_after(None)
        assert len(rows) == 4
        assert [r.seq for r in rows] == sorted(r.seq for r in rows)
        assert rows[0].device_id == "DEV1"
        assert rows[0].power_mw == 1000
        assert rows[0].relay_on is True

    async def test_read_after_is_strictly_after(self, buf):
        buf.submit(_sweep(BASE, n=3))
        await buf.flush()
        first = await buf.read_after(None, limit=1)
        rest = await buf.read_after(buf.cursor_of(first[0]))
        assert len(rest) == 2
        assert all(r.seq > first[0].seq for r in rest)

    async def test_none_power_survives_the_roundtrip(self, buf):
        """`None` means unmeasured and must never come back as zero."""
        buf.submit(
            Sweep(
                device_id="EP10",
                ts=BASE,
                outlets=[OutletReading(child_id="", alias="lamp", relay_on=True)],
            )
        )
        await buf.flush()
        (row,) = await buf.read_after(None)
        assert row.power_mw is None
        assert row.child_id == ""

    async def test_batches_commit_as_one_transaction(self, buf):
        for i in range(50):
            buf.submit(_sweep(BASE + timedelta(seconds=i)))
        await buf.flush()
        assert len(await buf.read_after(None)) == 100
        # 50 sweeps arriving together must not cost 50 commits.
        assert buf._health.batches_committed == 1


class TestDayPartitioning:
    async def test_rows_land_in_the_file_for_their_utc_day(self, buf, tmp_path):
        buf.submit(_sweep(datetime(2026, 9, 3, 23, 59, 59, tzinfo=UTC)))
        buf.submit(_sweep(datetime(2026, 9, 4, 0, 0, 1, tzinfo=UTC)))
        await buf.flush()

        directory = tmp_path / "buffer"
        assert (directory / "20260903.sqlite").exists()
        assert (directory / "20260904.sqlite").exists()

    async def test_a_row_written_into_an_older_day_after_the_cursor_moved_is_still_read(self, buf):
        """The bug a per-day cursor has, and a global sequence does not.

        A sweep that starts at 23:59:59.9 commits into yesterday's file, which
        can happen after a faster device's post-midnight sweep has already been
        read and acked. Ordering by insertion rather than by day means the late
        row is still after the cursor, so it is still delivered.
        """
        just_after_midnight = datetime(2026, 9, 4, 0, 0, 0, 100_000, tzinfo=UTC)
        just_before_midnight = datetime(2026, 9, 3, 23, 59, 59, 900_000, tzinfo=UTC)

        buf.submit(_sweep(just_after_midnight, device_id="FAST", n=1))
        await buf.flush()
        first = await buf.read_after(None)
        assert [r.device_id for r in first] == ["FAST"]
        cursor = buf.cursor_of(first[-1])

        # The straggler commits afterwards, into the *previous* day's file.
        buf.submit(_sweep(just_before_midnight, device_id="SLOW", n=1))
        await buf.flush()

        remaining = await buf.read_after(cursor)
        assert [r.device_id for r in remaining] == ["SLOW"]
        rows, oldest = await buf.lag_after(cursor)
        assert rows == 1 and oldest is not None

    async def test_cursor_order_crosses_a_day_boundary(self, buf):
        buf.submit(_sweep(datetime(2026, 9, 3, 23, 59, 59, tzinfo=UTC), n=1))
        buf.submit(_sweep(datetime(2026, 9, 4, 0, 0, 1, tzinfo=UTC), n=1))
        await buf.flush()

        rows = await buf.read_after(None)
        assert len(rows) == 2
        cursors = [buf.cursor_of(r) for r in rows]
        assert cursors == sorted(cursors)
        # Continuing from the last row of day one yields day two.
        rest = await buf.read_after(cursors[0])
        assert [r.ts_ms for r in rest] == [rows[1].ts_ms]

    async def test_extent_spans_all_day_files(self, buf):
        early = datetime.now(UTC) - timedelta(days=3)
        late = datetime.now(UTC) - timedelta(minutes=1)
        buf.submit(_sweep(early, n=1))
        buf.submit(_sweep(late, n=1))
        await buf.flush()
        oldest, newest = await buf.extent()
        rows = await buf.read_after(None)
        assert oldest == buf.cursor_of(rows[0])
        assert newest == buf.cursor_of(rows[-1])
        assert oldest < newest


class TestRetention:
    async def test_prune_unlinks_files_past_the_window(self, tmp_path):
        b = Buffer(tmp_path / "buffer", retention_days=7)
        await b.open()
        try:
            now = datetime.now(UTC)
            b.submit(_sweep(now - timedelta(days=30), n=1))
            b.submit(_sweep(now, n=1))
            await b.flush()

            directory = tmp_path / "buffer"
            old_day = (now - timedelta(days=30)).strftime("%Y%m%d")
            assert (directory / f"{old_day}.sqlite").exists()

            dropped = await b.prune()
            assert old_day in dropped
            assert not (directory / f"{old_day}.sqlite").exists()
            # The recent day survives.
            assert (directory / f"{now.strftime('%Y%m%d')}.sqlite").exists()
        finally:
            await b.close()

    async def test_prune_is_decided_from_the_filename_not_the_contents(self, tmp_path):
        """A day file too corrupt to open must still be expirable."""
        directory = tmp_path / "buffer"
        directory.mkdir(parents=True)
        old_day = (datetime.now(UTC) - timedelta(days=60)).strftime("%Y%m%d")
        (directory / f"{old_day}.sqlite").write_bytes(b"this is not a database")

        b = Buffer(directory, retention_days=30)
        # open() prunes, and must do so without ever opening the corrupt file.
        await b.open()
        try:
            assert not (directory / f"{old_day}.sqlite").exists()
        finally:
            await b.close()


class TestOverflow:
    async def test_full_queue_drops_oldest_and_counts_it(self, tmp_path):
        b = Buffer(tmp_path / "buffer", queue_maxsize=2)
        await b.open()
        try:
            for i in range(5):
                b.submit(_sweep(BASE + timedelta(seconds=i), n=2))
            # Two sweeps fit; the other three each displaced one.
            assert b._health.rows_dropped == 6
            await b.flush()
            rows = await b.read_after(None)
            assert len(rows) == 4  # the two that survived
        finally:
            await b.close()

    async def test_submit_never_raises_and_never_blocks(self, tmp_path):
        """A poll task must never be stalled or killed by the write path."""
        b = Buffer(tmp_path / "buffer", queue_maxsize=1)
        await b.open()
        try:
            started = time.perf_counter()
            for i in range(500):
                b.submit(_sweep(BASE + timedelta(seconds=i)))
            elapsed = time.perf_counter() - started
            # 500 submits against a queue of 1: all overflow, none block.
            assert elapsed < 0.5, f"submit blocked for {elapsed:.2f}s"
            assert b._health.rows_dropped > 0
            assert b._queue.qsize() <= 1
        finally:
            await b.close()


class TestClockGuards:
    async def test_prehistoric_timestamps_are_refused(self, buf):
        """An unsynced clock must not poison the record."""
        buf.submit(_sweep(CLOCK_FLOOR - timedelta(days=1), n=2))
        await buf.flush()
        assert await buf.read_after(None) == []
        assert buf._health.rows_dropped == 2

    async def test_future_timestamps_are_refused(self, buf):
        """A forward clock jump would create a day file that never expires."""
        buf.submit(_sweep(datetime.now(UTC) + timedelta(days=400), n=2))
        await buf.flush()
        assert await buf.read_after(None) == []
        assert buf._health.rows_dropped == 2

    async def test_ordinary_clock_skew_is_tolerated(self, buf):
        """The ceiling must not reject a reading a second into the future."""
        buf.submit(_sweep(datetime.now(UTC) + timedelta(seconds=1), n=1))
        await buf.flush()
        assert len(await buf.read_after(None)) == 1


class TestAliases:
    async def test_aliases_are_recorded_out_of_band(self, buf):
        """Aliases live in meta, not on every reading row."""
        buf.submit(_sweep(BASE, n=2))
        await buf.flush()
        aliases = await buf.aliases()
        assert {a["alias"] for a in aliases} == {"outlet 0", "outlet 1"}
        rows = await buf.read_after(None)
        assert not hasattr(rows[0], "alias")


class TestState:
    async def test_cursor_state_roundtrips(self, buf):
        assert await buf.get_state("acked") is None
        await buf.set_state("acked", "20260903:0000000000000007")
        assert await buf.get_state("acked") == "20260903:0000000000000007"
        await buf.set_state("acked", "20260903:0000000000000009")
        assert await buf.get_state("acked") == "20260903:0000000000000009"


class TestGaps:
    async def test_gap_is_opened_and_closed(self, buf, tmp_path):
        await buf.record_gap("DEV1", "offline", BASE)
        await buf.close_gap("DEV1", "offline", BASE + timedelta(minutes=5))
        conn = sqlite3.connect(tmp_path / "buffer" / "meta.sqlite")
        try:
            row = conn.execute("SELECT device_id, reason, to_ms FROM gaps").fetchone()
        finally:
            conn.close()
        assert row[0] == "DEV1"
        assert row[1] == "offline"
        assert row[2] is not None


class TestUnwritableDirectory:
    async def test_fatal_when_the_buffer_dir_cannot_be_written(self, tmp_path):
        blocked = tmp_path / "ro"
        blocked.mkdir()
        blocked.chmod(0o500)
        b = Buffer(blocked / "buffer")
        try:
            with pytest.raises(FatalError):
                await b.open()
        finally:
            blocked.chmod(0o700)
            await b.close()


class TestCrashSafety:
    def test_last_commit_survives_a_kill(self, tmp_path):
        """kill -9 mid-write must leave a readable buffer with no torn rows."""
        directory = tmp_path / "buffer"
        script = textwrap.dedent(
            f"""
            import asyncio, os, sys
            from datetime import UTC, datetime, timedelta
            sys.path.insert(0, {str(os.getcwd())!r})
            from tap.buffer import Buffer
            from tap.device import OutletReading, Sweep

            async def main():
                b = Buffer({str(directory)!r})
                await b.open()
                base = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)
                for i in range(20):
                    b.submit(Sweep(
                        device_id="DEV1", ts=base + timedelta(seconds=i),
                        outlets=[OutletReading(child_id="A", alias="a", relay_on=True,
                                               power_mw=1000)],
                    ))
                await b.flush()
                print("committed", flush=True)
                os._exit(9)  # no close, no checkpoint: the hard case

            asyncio.run(main())
            """
        )
        proc = subprocess.run(  # noqa: S603 — our own interpreter, our own script
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=60, check=False
        )
        assert "committed" in proc.stdout, proc.stderr

        async def verify():
            b = Buffer(directory)
            await b.open()
            try:
                return await b.read_after(None)
            finally:
                await b.close()

        rows = asyncio.run(verify())
        assert len(rows) == 20
        assert all(r.power_mw == 1000 for r in rows)


class TestLag:
    async def test_lag_counts_rows_without_reading_them(self, buf):
        buf.submit(_sweep(BASE, n=2))
        buf.submit(_sweep(BASE + timedelta(seconds=1), n=2))
        await buf.flush()

        rows, oldest = await buf.lag_after(None)
        assert rows == 4
        assert oldest == int(BASE.timestamp() * 1000)

        first = (await buf.read_after(None, limit=1))[0]
        rows, oldest = await buf.lag_after(buf.cursor_of(first))
        assert rows == 3
        assert oldest is not None and oldest >= int(BASE.timestamp() * 1000)

    async def test_lag_is_zero_when_caught_up(self, buf):
        buf.submit(_sweep(BASE, n=2))
        await buf.flush()
        last = (await buf.read_after(None))[-1]
        assert await buf.lag_after(buf.cursor_of(last)) == (0, None)

    async def test_lag_spans_day_files(self, buf):
        buf.submit(_sweep(datetime(2026, 9, 3, 23, 59, 59, tzinfo=UTC), n=1))
        buf.submit(_sweep(datetime(2026, 9, 4, 0, 0, 1, tzinfo=UTC), n=1))
        await buf.flush()
        rows, _oldest = await buf.lag_after(None)
        assert rows == 2

    async def test_lag_agrees_with_read_after(self, buf):
        """The cheap count must not drift from what the sender will actually send."""
        for i in range(7):
            buf.submit(_sweep(BASE + timedelta(seconds=i), n=3))
        await buf.flush()
        cursor = buf.cursor_of((await buf.read_after(None, limit=5))[-1])
        rows, _ = await buf.lag_after(cursor)
        assert rows == len(await buf.read_after(cursor, limit=10_000))


class TestRetentionBoundary:
    async def test_retention_days_is_the_number_of_days_kept(self, tmp_path):
        b = Buffer(tmp_path / "buffer", retention_days=3)
        await b.open()
        try:
            now = datetime.now(UTC)
            for age in range(6):
                b.submit(_sweep(now - timedelta(days=age), n=1))
            await b.flush()
            await b.prune()
            days = sorted(p.stem for p in (tmp_path / "buffer").glob("2*.sqlite"))
            assert len(days) == 3, days
            assert days[-1] == now.strftime("%Y%m%d")
        finally:
            await b.close()

    async def test_todays_file_survives_retention_of_one(self, tmp_path):
        b = Buffer(tmp_path / "buffer", retention_days=1)
        await b.open()
        try:
            b.submit(_sweep(datetime.now(UTC), n=1))
            await b.flush()
            await b.prune()
            assert len(await b.read_after(None)) == 1
        finally:
            await b.close()


class TestStats:
    async def test_counts_and_timestamps_are_reported(self, buf):
        first = datetime.now(UTC) - timedelta(minutes=10)
        last = datetime.now(UTC) - timedelta(minutes=1)
        buf.submit(_sweep(first, n=2))
        buf.submit(_sweep(last, n=2))
        await buf.flush()
        await buf.refresh_stats()
        health = buf._health
        assert sum(d["rows"] for d in health.days) == 4
        assert health.oldest_ts is not None and health.newest_ts is not None
        # Millisecond truncation, so compare at second resolution.
        assert abs((health.oldest_ts - first).total_seconds()) < 1
        assert abs((health.newest_ts - last).total_seconds()) < 1

    async def test_cached_counts_match_a_full_rescan(self, buf):
        """The incremental counters must not drift from the truth on disk."""
        for i in range(10):
            buf.submit(_sweep(BASE + timedelta(seconds=i), n=3))
        await buf.flush()
        cached = dict(buf._day_rows)
        await buf._run(buf._rescan)
        assert buf._day_rows == cached

    async def test_sequence_survives_a_reopen(self, tmp_path):
        """A restart must never hand out a sequence number twice."""
        directory = tmp_path / "buffer"
        b = Buffer(directory)
        await b.open()
        b.submit(_sweep(BASE, n=3))
        await b.flush()
        last = (await b.read_after(None))[-1].seq
        await b.close()

        b2 = Buffer(directory)
        await b2.open()
        try:
            b2.submit(_sweep(BASE + timedelta(seconds=1), n=3))
            await b2.flush()
            rows = await b2.read_after(None)
            assert [r.seq for r in rows] == sorted(r.seq for r in rows)
            assert len({r.seq for r in rows}) == 6
            assert min(r.seq for r in rows[3:]) > last
        finally:
            await b2.close()


class TestCorruption:
    async def test_a_corrupt_file_inside_the_window_is_fatal_not_a_traceback(self, tmp_path):
        directory = tmp_path / "buffer"
        directory.mkdir(parents=True)
        today = datetime.now(UTC).strftime("%Y%m%d")
        (directory / f"{today}.sqlite").write_bytes(b"this is not a database")
        b = Buffer(directory, retention_days=30)
        with pytest.raises(FatalError, match="unusable"):
            await b.open()
        await b.close()
