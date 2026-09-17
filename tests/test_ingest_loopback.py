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
from tap.device import DeviceState, OutletReading, Sweep
from tap.health import Health, OutletHealth
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
async def juice_server(
    store: Store,
    state: RecorderState | None = None,
    tap_devices=None,
    tap_live=None,
    tap_control=None,
    out: dict | None = None,
):
    app = create_app(
        state or RecorderState(),
        store,
        dev_auth=True,
        ingest_token=TOKEN,
        tap_devices=tap_devices,
        tap_live=tap_live,
        tap_control=tap_control,
    )
    if out is not None:
        out["app"] = app  # for a look at what create_app installed
    server = TestServer(app)
    if out is not None:
        out["server"] = server
    await server.start_server()
    try:
        yield f"http://127.0.0.1:{server.port}/api/v2/ingest"
    finally:
        await server.close()


@contextlib.asynccontextmanager
async def running_tap(
    url: str,
    buffer: Buffer,
    tap_id: str = "loopback-tap",
    health: Health | None = None,
    pollers=None,
):
    config = Config(tap_id=tap_id, uplink=UplinkConfig(url=url, token=TOKEN, enabled=True))
    uplink = Uplink(config, buffer, health or Health(), pollers)
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


class TestTheRosterArrives:
    """The alias is the whole cutover, and this is the only test where a real tap
    sends it to a real receiver.

    Ingest creates plugs for outlets it has never seen with a deliberately *empty*
    alias -- it has no roster to write -- so without this frame a tap-only juice
    stores every reading correctly and still shows the whole floor unassigned.
    """

    async def test_a_real_roster_becomes_a_real_assignment(self, buf, store) -> None:
        from juice.collector_tap import apply_devices

        state = RecorderState()
        machines = {"M0013": {"name": "Blackout", "year": 1980}}

        def project(entries):
            apply_devices(state, store, entries, machines, datetime.now(UTC))

        base = datetime.now(UTC) - timedelta(seconds=2)
        for i in range(2):
            buf.submit(
                Sweep(
                    device_id=DEVICE,
                    ts=base + timedelta(seconds=i),
                    device_alias="Front Row Strip",
                    has_emeter=True,
                    outlets=[
                        OutletReading(
                            child_id=f"{DEVICE}00",
                            alias="Blackout - M0013",
                            relay_on=True,
                            power_mw=42_000,
                        )
                    ],
                )
            )
        await buf.flush()

        async with juice_server(store, state=state, tap_devices=project) as url:
            async with running_tap(url, buf):
                await wait_for(lambda: bool(state.assignments))

        plug_id = store.ensure_plug(DEVICE, f"{DEVICE}00", "Blackout - M0013")
        assert state.assignments[plug_id] == ("Blackout", "M0013", 1980)
        assert state.strip_aliases[DEVICE] == "Front Row Strip"
        # And the alias reached the durable side, not just memory -- this is what
        # survives a restart and what `hydrate_assignments` reads back.
        assert (
            store._conn.execute("SELECT alias FROM plugs WHERE plug_id = ?", [plug_id]).fetchone()[
                0
            ]
            == "Blackout - M0013"
        )

    async def test_a_meterless_outlet_arrives_as_meterless(self, buf, store) -> None:
        """`has_emeter` has to survive the round trip: `refresh_hourly_usage`
        filters on it, so an outlet wrongly marked metered or unmetered is an
        energy chart that is quietly wrong."""
        from juice.collector_tap import apply_devices

        state = RecorderState()

        def project(entries):
            apply_devices(state, store, entries, {}, datetime.now(UTC))

        buf.submit(
            Sweep(
                device_id=DEVICE,
                ts=datetime.now(UTC),
                device_alias="Duck Locker",
                has_emeter=False,
                outlets=[
                    OutletReading(
                        child_id="", alias="Duck Locker - M0037", relay_on=True, power_mw=None
                    )
                ],
            )
        )
        await buf.flush()

        async with juice_server(store, state=state, tap_devices=project) as url:
            async with running_tap(url, buf):
                await wait_for(lambda: bool(state.plug_has_emeter))

        plug_id = store.ensure_plug(DEVICE, "", "Duck Locker - M0037", has_emeter=False)
        assert state.plug_has_emeter[plug_id] is False
        assert (
            store._conn.execute(
                "SELECT has_emeter FROM plugs WHERE plug_id = ?", [plug_id]
            ).fetchone()[0]
            is False
        )


def reachable(health: Health, *, relay_on: bool = True, power_mw: int | None = 42_000) -> None:
    """What the poller leaves in `Health` after a good sweep -- the source
    `Uplink._live_rows` reads, and empty until something writes it."""
    device = health.device(DEVICE)
    device.state = DeviceState.ONLINE
    device.outlets[f"{DEVICE}00"] = OutletHealth(
        child_id=f"{DEVICE}00", relay_on=relay_on, power_mw=power_mw, voltage_mv=119_000
    )


class TestTheLiveFrameArrives:
    """A real tap's `live` frame through the real receiver into the real
    projection. `readings` and `live` ride the same socket; this is where a
    disagreement between the two wire copies about the *live* row would show."""

    async def test_a_real_live_frame_drives_live_state(self, buf, store) -> None:
        from juice.collector_tap import LiveProjector

        state = RecorderState()
        plug_id = store.ensure_plug(DEVICE, f"{DEVICE}00", "Blackout - M0013")
        state.plugs[plug_id] = (DEVICE, f"{DEVICE}00", "Blackout - M0013")
        state.plug_has_emeter[plug_id] = True
        health = Health()
        reachable(health, relay_on=True, power_mw=42_000)
        projector = LiveProjector(state, store)
        await fill(buf, 2, outlets=1)

        async with juice_server(store, state=state, tap_live=projector) as url:
            async with running_tap(url, buf, health=health):
                await wait_for(lambda: plug_id in state.plug_readings)
                await wait_for(lambda: stored(store) == 2)

        reading = state.plug_readings[plug_id]
        assert reading.is_on is True and reading.watts == 42.0
        assert DEVICE in projector.last_seen
        assert projector.dropped_skew == 0, "two processes on one clock must not skew"
        assert stored(store) == 2, "the durable channel is unaffected"


class Relays:
    """The `pollers` a real `Uplink` actuates through, reduced to the one
    call it makes: `find(device_id).set_relay(child_id, on)`. Records every
    throw, and can be made to hang or refuse."""

    def __init__(self, *known: str) -> None:
        self.known = set(known)
        self.thrown: list[tuple[str, str, bool]] = []
        self.gate: asyncio.Event | None = None
        self.refuse: str | None = None

    def find(self, device_id: str):
        if device_id not in self.known:
            return None
        relays = self

        class _Poller:
            async def set_relay(self, child_id: str, on: bool) -> None:
                if relays.gate is not None:
                    await relays.gate.wait()
                if relays.refuse:
                    raise ConnectionError(relays.refuse)
                relays.thrown.append((device_id, child_id, on))

        return _Poller()


def _controllable_state(store: Store) -> tuple[RecorderState, int]:
    from juice.readings import PlugReading

    state = RecorderState()
    plug_id = store.ensure_plug(DEVICE, f"{DEVICE}00", "Blackout - M0013")
    state.plugs[plug_id] = (DEVICE, f"{DEVICE}00", "Blackout - M0013")
    state.plug_has_emeter[plug_id] = True
    state.assignments[plug_id] = ("Blackout", "M0013", 1980)
    state.plug_readings[plug_id] = PlugReading(
        f"{DEVICE}00", "Blackout - M0013", False, 0.0, 0, 0, 0
    )
    return state, plug_id


class TestCommandsReachARealTap:
    """The control round trip against the real `Uplink`: its command handler,
    its redelivery cache, its expiry check. The only place the two wire
    copies of `command` and `command_result` meet."""

    async def test_turn_on_throws_the_relay_once(self, buf, store) -> None:
        from juice.collector_tap import TapControl, TapPlug

        control = TapControl()
        relays = Relays(DEVICE)
        await fill(buf, 1, outlets=1)
        async with juice_server(store, tap_control=control) as url:
            async with running_tap(url, buf, pollers=relays):
                await wait_for(lambda: control.connected == ["loopback-tap"])
                plug = TapPlug(control, DEVICE, f"{DEVICE}00", "Blackout - M0013")
                await asyncio.wait_for(plug.turn_on(), timeout=5.0)

        assert relays.thrown == [(DEVICE, f"{DEVICE}00", True)]
        assert control.commands_ok == 1

    async def test_a_redelivery_does_not_throw_the_relay_twice(self, buf, store) -> None:
        """juice's retry after silence re-sends the same command id; tap
        answers from its cache. One physical actuation, however many asks."""
        from juice.collector_tap import TapControl, TapPlug

        control = TapControl(result_timeout=0.3)
        relays = Relays(DEVICE)
        relays.gate = asyncio.Event()  # the device is slow to answer
        await fill(buf, 1, outlets=1)
        async with juice_server(store, tap_control=control) as url:
            async with running_tap(url, buf, pollers=relays):
                await wait_for(lambda: control.connected == ["loopback-tap"])
                plug = TapPlug(control, DEVICE, f"{DEVICE}00", "x")
                with pytest.raises(TimeoutError):
                    await plug.turn_on()
                relays.gate.set()
                await asyncio.wait_for(plug.turn_on(), timeout=5.0)  # the retry

        assert relays.thrown == [(DEVICE, f"{DEVICE}00", True)]

    async def test_a_device_tap_cannot_reach_is_retried_not_refused(self, buf, store) -> None:
        """tap's poller raises `ConnectionError` before its own retries when
        it has dropped the device; the cloud path retries "Device is offline"
        for the whole budget, so this must come back as the retryable kind."""
        from juice.collector_tap import TapControl, TapPlug

        control = TapControl()
        relays = Relays(DEVICE)
        relays.refuse = "192.168.2.45 is not connected"
        await fill(buf, 1, outlets=1)
        async with juice_server(store, tap_control=control) as url:
            async with running_tap(url, buf, pollers=relays):
                await wait_for(lambda: control.connected == ["loopback-tap"])
                plug = TapPlug(control, DEVICE, f"{DEVICE}00", "x")
                with pytest.raises(TimeoutError, match="not connected"):
                    await asyncio.wait_for(plug.turn_on(), timeout=5.0)
                # The strip comes back; the retry actuates, since tap caches
                # no failures.
                relays.refuse = None
                await asyncio.wait_for(plug.turn_on(), timeout=5.0)

        assert relays.thrown == [(DEVICE, f"{DEVICE}00", True)]

    async def test_a_command_that_refuses_for_good_is_not_retried(self, buf, store) -> None:
        from juice.collector_tap import TapCommandFailedError, TapControl, TapPlug

        control = TapControl()
        relays = Relays("SOME-OTHER-STRIP")  # tap has never heard of DEVICE
        await fill(buf, 1, outlets=1)
        async with juice_server(store, tap_control=control) as url:
            async with running_tap(url, buf, pollers=relays):
                await wait_for(lambda: control.connected == ["loopback-tap"])
                plug = TapPlug(control, DEVICE, f"{DEVICE}00", "x")
                with pytest.raises(TapCommandFailedError, match="unknown device"):
                    await asyncio.wait_for(plug.turn_on(), timeout=5.0)

        assert relays.thrown == []

    async def test_an_expired_command_is_refused_by_tap(self, buf, store) -> None:
        from juice.collector_tap import TapCommandFailedError, TapControl

        control = TapControl()
        relays = Relays(DEVICE)
        await fill(buf, 1, outlets=1)
        async with juice_server(store, tap_control=control) as url:
            async with running_tap(url, buf, pollers=relays):
                await wait_for(lambda: control.connected == ["loopback-tap"])
                stale = datetime.now(UTC) - timedelta(minutes=2)
                with pytest.raises(TapCommandFailedError, match="expired"):
                    await asyncio.wait_for(
                        control.command("turn_on", DEVICE, f"{DEVICE}00", expires_at=stale),
                        timeout=5.0,
                    )

        assert relays.thrown == []

    async def test_no_tap_means_not_controllable_now(self, store) -> None:
        from juice.collector_tap import TapControl, TapPlug, TapUnavailableError

        control = TapControl()
        plug = TapPlug(control, DEVICE, f"{DEVICE}00", "x")
        with pytest.raises(TapUnavailableError, match="collector is offline"):
            await plug.turn_on()


class TestPowerControlEndToEnd:
    """The whole path an operator's tap on the dashboard takes: the v2 power
    endpoint, the command lifecycle, `call_with_retry`, `TapPlug`, the real
    uplink, the fake relay -- and the confirmation, from a live frame."""

    async def test_power_on_reaches_the_relay_and_is_confirmed_by_the_next_reading(
        self, buf, store
    ) -> None:
        from aiohttp.test_utils import TestClient

        from juice.collector_tap import LiveProjector, TapControl, TapPlug

        state, plug_id = _controllable_state(store)
        control = TapControl()
        state.plug_objects[plug_id] = TapPlug(control, DEVICE, f"{DEVICE}00", "Blackout - M0013")
        projector = LiveProjector(state, store)
        relays = Relays(DEVICE)
        health = Health()
        reachable(health, relay_on=False, power_mw=0)
        await fill(buf, 1, outlets=1)
        out: dict = {}

        async with juice_server(
            store, state=state, tap_live=projector, tap_control=control, out=out
        ) as url:
            # Closing a TestClient closes its server, so it must outlive the
            # tap's connection rather than be torn down under it.
            client = TestClient(out["server"])
            await client.start_server()
            try:
                async with running_tap(url, buf, health=health, pollers=relays):
                    await wait_for(lambda: control.connected == ["loopback-tap"])
                    await client.get("/login")
                    resp = await client.post("/api/v2/machines/M0013/power", json={"on": True})
                    assert resp.status == 202, await resp.text()
                    command_id = (await resp.json())["command_id"]

                    await wait_for(lambda: relays.thrown == [(DEVICE, f"{DEVICE}00", True)])
                    command = state.commands.get(command_id)
                    assert command is not None
                    await wait_for(lambda: command.phase == "awaiting_relay")

                    # tap's next sweep sees the relay closed; its live frame
                    # is the reading that confirms the command.
                    reachable(health, relay_on=True, power_mw=42_000)
                    await wait_for(lambda: command.phase == "confirmed")
            finally:
                await client.close()

        assert command.confirmed_by == "relay"
        assert state.plug_readings[plug_id].is_on is True

    async def test_with_the_collector_offline_the_command_is_refused_up_front(self, store) -> None:
        """No tap connected: 409 before a command is minted, not six retries of
        backoff and an audit row. (`TapUnavailableError` still refuses in one
        round trip for the window between a tap leaving and the next check.)"""
        import time

        from aiohttp.test_utils import TestClient

        from juice.collector_tap import TapControl, TapPlug

        state, plug_id = _controllable_state(store)
        control = TapControl()
        state.plug_objects[plug_id] = TapPlug(control, DEVICE, f"{DEVICE}00", "Blackout - M0013")
        out: dict = {}
        async with juice_server(store, state=state, tap_control=control, out=out):
            client = TestClient(out["server"])
            await client.start_server()
            try:
                await client.get("/login")
                started = time.monotonic()
                resp = await client.post("/api/v2/machines/M0013/power", json={"on": True})
                body = await resp.json()
            finally:
                await client.close()

        assert resp.status == 409, body
        assert body["error"]["code"] == "not_controllable"
        assert "collector is offline" in body["error"]["message"]
        assert time.monotonic() - started < 2.0, "refused, not retried"
        assert state.commands.in_flight_for_plug(plug_id) is None, "nothing was minted"
