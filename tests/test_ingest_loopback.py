"""The real tap uplink against the real juice receiver.

Everything else tests one side against a fake of the other. This is the only
test where both halves are the shipped code, which makes it the only one that
can catch the two duplicated `wire` modules disagreeing -- the failure mode the
duplication deliberately accepts in exchange for the two codebases staying
independent.

It is also where the durability claim is actually proven: kill the server
mid-stream, bring it back, and assert the data has neither a gap nor a
duplicate.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime, timedelta

import pytest
from aiohttp.test_utils import TestServer

from juice.server import RecorderState, create_app
from juice.store import Store
from tap.buffer import Buffer
from tap.config import Config, UplinkConfig
from tap.device import OutletReading, Sweep
from tap.health import Health
from tap.uplink import Uplink

TOKEN = "loopback-token"  # noqa: S105
DEVICE = "STRIP1"


@pytest.fixture
async def buf(tmp_path):
    b = Buffer(tmp_path / "buffer", retention_days=3650)
    await b.open()
    yield b
    await b.close()


@pytest.fixture
def store():
    with Store(":memory:") as s:
        yield s


async def fill(
    buffer: Buffer,
    count: int,
    *,
    outlets: int = 2,
    relay_on: bool = True,
    metered: bool = True,
    start: datetime | None = None,
) -> None:
    base = start or (datetime.now(UTC) - timedelta(seconds=count + 5))
    for i in range(count):
        buffer.submit(
            Sweep(
                device_id=DEVICE,
                ts=base + timedelta(seconds=i),
                outlets=[
                    OutletReading(
                        child_id=f"{DEVICE}{n:02d}",
                        alias=f"outlet {n}",
                        relay_on=relay_on,
                        power_mw=(42_000 + i) if metered else None,
                        voltage_mv=119_000 if metered else None,
                        current_ma=350 if metered else None,
                        energy_wh=12_345 if metered else None,
                    )
                    for n in range(outlets)
                ],
            )
        )
    await buffer.flush()


@contextlib.asynccontextmanager
async def juice_server(store: Store):
    server = TestServer(create_app(RecorderState(), store, dev_auth=True, ingest_token=TOKEN))
    await server.start_server()
    try:
        yield f"http://127.0.0.1:{server.port}/api/v2/ingest"
    finally:
        await server.close()


@contextlib.asynccontextmanager
async def running_tap(url: str, buffer: Buffer, tap_id: str = "loopback-tap"):
    config = Config(tap_id=tap_id, uplink=UplinkConfig(url=url, token=TOKEN, enabled=True))
    uplink = Uplink(config, buffer, Health())
    task = asyncio.create_task(uplink.run())
    try:
        yield uplink
    finally:
        uplink.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def wait_for(predicate, timeout: float = 15.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("condition not met in time")


def stored(store: Store) -> int:
    return store._conn.execute("SELECT count(*) FROM readings").fetchone()[0]


def duplicates(store: Store) -> list:
    return store._conn.execute(
        "SELECT ts, plug_id, count(*) FROM readings GROUP BY ts, plug_id HAVING count(*) > 1"
    ).fetchall()


class TestARealTapDeliversToARealJuice:
    async def test_every_buffered_row_lands_exactly_once(self, buf, store) -> None:
        await fill(buf, 50)  # 50 sweeps x 2 outlets
        async with juice_server(store) as url, running_tap(url, buf):
            await wait_for(lambda: stored(store) == 100)
        assert duplicates(store) == []

    async def test_the_stored_cursor_matches_the_buffers_high_water_mark(self, buf, store) -> None:
        await fill(buf, 20)
        buffer_id = await buf.buffer_id()
        async with juice_server(store) as url, running_tap(url, buf):
            await wait_for(lambda: stored(store) == 40)
            await wait_for(lambda: store.ingest_cursor("loopback-tap", buffer_id) is not None)
        assert store.ingest_cursor("loopback-tap", buffer_id) == await buf.high_water()

    async def test_relay_state_and_nulls_survive_the_whole_pipeline(self, buf, store) -> None:
        """An unmetered outlet that is switched on is the case the cloud
        recorder could not express: it wrote all-NULL and left "is it on?" to be
        guessed from the absence. Null must stay null rather than becoming 0.0,
        which would read as a machine drawing nothing."""
        await fill(buf, 5, outlets=1, relay_on=True, metered=False)
        async with juice_server(store) as url, running_tap(url, buf):
            await wait_for(lambda: stored(store) == 5)
        rows = store._conn.execute(
            "SELECT DISTINCT watts, voltage, amps, total_kwh, relay_on FROM readings"
        ).fetchall()
        assert rows == [(None, None, None, None, True)]

    async def test_a_relay_that_is_off_is_recorded_as_off(self, buf, store) -> None:
        await fill(buf, 5, outlets=1, relay_on=False, metered=False)
        async with juice_server(store) as url, running_tap(url, buf):
            await wait_for(lambda: stored(store) == 5)
        assert store._conn.execute("SELECT DISTINCT relay_on FROM readings").fetchall() == [
            (False,)
        ]

    async def test_units_arrive_as_units(self, buf, store) -> None:
        await fill(buf, 1, outlets=1)
        async with juice_server(store) as url, running_tap(url, buf):
            await wait_for(lambda: stored(store) == 1)
        got = store._conn.execute("SELECT watts, voltage, amps, total_kwh FROM readings").fetchone()
        assert got == pytest.approx((42.0, 119.0, 0.35, 12.345), rel=1e-5)


class TestRestartsNeitherLoseNorDuplicate:
    async def test_a_server_restart_mid_stream_costs_nothing(self, buf, store) -> None:
        """The headline claim. tap is mid-flight when the server disappears; on
        reconnect the cursor we committed is the one it resumes from, so the
        rows in between are neither lost nor sent twice."""
        await fill(buf, 40)
        async with juice_server(store) as url, running_tap(url, buf):
            await wait_for(lambda: stored(store) > 0)
        # The server is gone; tap is now reconnecting. Add more while it is down.
        await fill(buf, 40, start=datetime.now(UTC) + timedelta(seconds=1))

        async with juice_server(store) as url2, running_tap(url2, buf):
            await wait_for(lambda: stored(store) == 160)
        assert duplicates(store) == []

    async def test_a_server_restored_from_backup_gets_its_missing_rows_back(
        self, buf, store
    ) -> None:
        """`resume_from` is the server's decision, not tap's: a juice restored
        from an older snapshot asks for rows tap already considers delivered,
        and tap re-sends them (`tap/wire.py:70-74`). Without this, restoring a
        backup would leave a permanent hole."""
        await fill(buf, 30)
        async with juice_server(store) as url, running_tap(url, buf):
            await wait_for(lambda: stored(store) == 60)

        # Roll juice back to a snapshot taken before any of it arrived. Note
        # that the cursor is set *back*, not deleted -- see the test below.
        store._conn.execute("DELETE FROM readings")
        store._conn.execute(
            "UPDATE ingest_cursors SET cursor = ? WHERE tap_id = ?", ["0" * 18, "loopback-tap"]
        )

        async with juice_server(store) as url2, running_tap(url2, buf):
            await wait_for(lambda: stored(store) == 60)
        assert duplicates(store) == []

    async def test_a_server_with_no_cursor_gets_the_history_back(self, buf, store) -> None:
        """The restore case, end to end across both halves.

        `tap/wire.py` makes juice the authority on durability, and a null
        `resume_from` an instruction: "from the start of tap's buffer". A juice
        restored from a backup taken before this tap connected holds no cursor,
        so it answers null -- and the rows it is missing are still sitting in
        tap's buffer. If tap kept its own cursor there, nothing would ever ask
        for them again and the restore would silently lose the gap.
        """
        await fill(buf, 10)
        async with juice_server(store) as url, running_tap(url, buf):
            await wait_for(lambda: stored(store) == 20)

        # A juice restored from a backup that predates this tap entirely.
        store._conn.execute("DELETE FROM readings")
        store._conn.execute("DELETE FROM ingest_cursors")

        async with juice_server(store) as url2, running_tap(url2, buf):
            await wait_for(lambda: stored(store) == 20)


class TestPlugIdentity:
    async def test_tap_rows_land_on_the_plug_the_cloud_recorder_already_knew(
        self, buf, store
    ) -> None:
        """Same outlet, one plug. If tap's `(device_id, child_id)` did not
        resolve to the recorder's existing plug, every machine's history would
        fork in two at cutover."""
        plug_id = store.ensure_plug(DEVICE, f"{DEVICE}00", "The Addams Family - M0017")
        await fill(buf, 3, outlets=1)
        async with juice_server(store) as url, running_tap(url, buf):
            await wait_for(lambda: stored(store) == 3)
        assert store._conn.execute("SELECT DISTINCT plug_id FROM readings").fetchall() == [
            (plug_id,)
        ]
        assert store._conn.execute(
            "SELECT alias FROM plugs WHERE plug_id = ?", [plug_id]
        ).fetchone() == ("The Addams Family - M0017",)
