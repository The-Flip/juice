"""The live projection: tap's `live` frame becoming the floor's current state.

This is the half of the collector that decides what the dashboard shows. The
cloud recorder used to feed `cache_reading` / `update_buffer` / `check_overload`
from a device it had just polled; here the same three are fed from a frame --
which changes what "now" means, what "offline" means, and what happens when
frames stop.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

import pytest

from juice.collector_tap import (
    LIVE_MAX_SKEW_S,
    LIVE_STALE_S,
    LiveProjector,
    apply_live,
    live_loop,
    live_reading,
    mark_device_offline,
    note_device_ok,
)
from juice.floor_state import FloorState
from juice.readings import PlugReading
from juice.store import Store

DEV = "STRIP1"
OTHER = "STRIP2"
NOW = datetime(2026, 9, 13, 16, 0, 0, tzinfo=UTC)


@pytest.fixture
def store():
    with Store(":memory:") as s:
        yield s


@pytest.fixture
def state():
    return FloorState()


def _plug(state: FloorState, store: Store, child: str, *, device=DEV, metered=True) -> int:
    """A plug the way `hydrate_assignments` or `apply_devices` leaves it."""
    plug_id = store.ensure_plug(device, child, f"outlet {child}", has_emeter=metered)
    state.plugs[plug_id] = (device, child, f"outlet {child}")
    state.plug_has_emeter[plug_id] = metered
    return plug_id


def _row(child: str, *, device=DEV, relay=1, mw=42_000, mv=119_000, ts: datetime = NOW) -> list:
    """A live row as `Uplink._live_rows` builds it: current_ma and energy_wh
    are always null on the live wire."""
    return [int(ts.timestamp() * 1000), device, child, relay, mw, mv, None, None]


class TestLiveRowsBecomeReadings:
    """Byte-for-byte what `poll_once` would have cached for the same outlet."""

    def test_a_metered_outlet_that_is_on(self) -> None:
        reading = live_reading(_row("A", relay=1, mw=42_500, mv=119_000), "outlet A", True)
        assert reading == PlugReading(
            child_id="A",
            alias="outlet A",
            is_on=True,
            watts=42.5,
            voltage=119.0,
            amps=None,
            total_kwh=None,
        )

    def test_a_metered_outlet_that_is_off_reads_zero_like_the_recorder(self) -> None:
        """The cloud recorder wrote and cached all-zeros for a metered OFF outlet,
        and every stored reading before the cutover has that shape; a tap reads
        the meter anyway and may report a few mW of nothing. The cache must say
        what the stored history does."""
        reading = live_reading(_row("A", relay=0, mw=12, mv=118_000), "outlet A", True)
        assert reading == PlugReading("A", "outlet A", False, 0.0, 0.0, 0.0, 0.0)

    def test_a_meterless_outlet_has_no_watts_either_way(self) -> None:
        assert live_reading(_row("A", relay=1, mw=None, mv=None), "outlet A", False).watts is None
        assert live_reading(_row("A", relay=0, mw=None, mv=None), "outlet A", False).watts is None
        assert live_reading(_row("A", relay=1, mw=None, mv=None), "outlet A", False).is_on is True

    def test_an_unmeasured_watt_on_a_metered_outlet_stays_unknown(self) -> None:
        """A failed meter read inside a good sweep is null on the wire and must
        stay null: since #101 an unmeasured reading is unknown, not zero."""
        reading = live_reading(_row("A", relay=1, mw=None, mv=119_000), "outlet A", True)
        assert reading.is_on is True and reading.watts is None

    async def test_rows_land_in_the_cache_with_juices_clock(self, state, store) -> None:
        """The timestamp cached beside the reading is *juice's* now, not tap's
        `ts_ms`: command reconciliation ignores anything at or before
        `issued_at`, so a tap 30s slow would time out every command."""
        plug = _plug(state, store, "A")
        taps_clock = NOW - timedelta(seconds=30)

        await apply_live(state, store, [_row("A", ts=taps_clock)], now=NOW)

        assert state.plug_readings[plug].watts == 42.0
        assert state.plug_reading_ts[plug] == NOW

    async def test_a_measured_watt_feeds_the_sparkline_buffer(self, state, store) -> None:
        plug = _plug(state, store, "A")
        await apply_live(state, store, [_row("A", mw=42_000)], now=NOW)
        await apply_live(state, store, [_row("A", mw=43_000)], now=NOW + timedelta(seconds=1))
        assert list(state.watt_buffers[plug]) == [42.0, 43.0]

    async def test_an_unmeasured_watt_is_cached_but_not_buffered(self, state, store) -> None:
        """`classify` would see a None it cannot use; the cloud path skips the
        buffer for exactly this case. The status is still `powered` -- the relay
        is known -- with the draw marked as unmeasured."""
        plug = _plug(state, store, "A")
        await apply_live(state, store, [_row("A", relay=1, mw=None)], now=NOW)

        assert state.plug_readings[plug].watts is None
        assert plug not in state.watt_buffers
        assert state.status_since[plug][0] == "powered"

    async def test_a_metered_off_outlet_buffers_zero(self, state, store) -> None:
        plug = _plug(state, store, "A")
        await apply_live(state, store, [_row("A", relay=0, mw=0)], now=NOW)
        assert list(state.watt_buffers[plug]) == [0.0]
        assert state.status_since[plug][0] == "off"

    async def test_a_live_row_confirms_a_dispatched_command(self, state, store) -> None:
        """The command lifecycle already expects confirmation from a later
        reading; a live row is that reading."""
        plug = _plug(state, store, "A")
        state.commands = state.commands.__class__(now=lambda: NOW - timedelta(seconds=5))
        cmd, _ = state.commands.open_ex(kind="turn_on", plug_id=plug, actor="t", source="t")
        state.commands.record_dispatched(cmd)

        await apply_live(state, store, [_row("A", relay=1)], now=NOW)

        assert cmd.phase == "confirmed"

    async def test_overload_detection_is_fed(self, state, store) -> None:
        """Assert through the window rather than the shutdown: a window exists
        for the plug only if `check_overload` ran on it."""
        plug = _plug(state, store, "A")
        state.assignments[plug] = ("Blackout", "M0013", 1980)
        state.power_baselines["M0013"] = 100.0
        state.overload_mode = "shadow"

        await apply_live(state, store, [_row("A", mw=900_000)], now=NOW)

        assert plug in state.overload_windows

    async def test_the_window_takes_the_collectors_gap_bound(self, state, store) -> None:
        """`check_overload` builds the window with the collector's bound: a
        window fed at 1 Hz that tolerated 30 s holes could fire on six seconds
        of evidence."""
        from juice.overload import MAX_GAP_S

        plug = _plug(state, store, "A")
        state.assignments[plug] = ("Blackout", "M0013", 1980)
        state.power_baselines["M0013"] = 100.0
        state.overload_mode = "shadow"

        await apply_live(state, store, [_row("A", mw=900_000)], now=NOW)

        assert state.overload_windows[plug].max_gap_seconds == MAX_GAP_S

    async def test_one_snapshot_is_published_per_frame(self, state, store) -> None:
        """The snapshot is per *machine*, as the recorder's is: the SSE tick
        carries assigned plugs only."""
        for child, tag in (("A", "M0001"), ("B", "M0002")):
            plug = _plug(state, store, child)
            state.assignments[plug] = (f"Machine {tag}", tag, None)
        queue: asyncio.Queue = asyncio.Queue()
        state.event_subscribers.add(queue)

        await apply_live(state, store, [_row("A"), _row("B")], now=NOW)

        assert queue.qsize() == 1
        event = queue.get_nowait()
        assert event["type"] == "readings"
        assert len(event["machines"]) == 2

    async def test_nothing_is_published_when_nobody_is_listening(self, state, store) -> None:
        """`_readings_snapshot` classifies every buffer; skipping it when there
        are no subscribers is what the recorder does and what keeps a 1 Hz frame
        cheap."""
        _plug(state, store, "A")
        outcome = await apply_live(state, store, [_row("A")], now=NOW)
        assert outcome.applied == 1
        assert outcome.published is False


class TestLiveNeverCreatesAPlug:
    async def test_an_unknown_outlet_is_skipped_and_counted(self, state, store) -> None:
        """Only the roster has an alias. A plug created from a live row would
        have an empty one and would then need the roster to fix it -- so the
        live path resolves against `state.plugs` and never calls `ensure_plug`."""
        _plug(state, store, "A")
        before = store.list_plugs()

        outcome = await apply_live(state, store, [_row("A"), _row("ZZ", device="GHOST")], now=NOW)

        assert outcome.applied == 1 and outcome.unknown == 1
        assert store.list_plugs() == before
        assert len(state.plug_readings) == 1

    async def test_a_malformed_row_costs_only_itself(self, state, store) -> None:
        plug_a = _plug(state, store, "A")
        plug_b = _plug(state, store, "B")

        await apply_live(state, store, [_row("A", mw="forty-two"), _row("B")], now=NOW)

        assert plug_a not in state.plug_readings
        assert plug_b in state.plug_readings


class TestAbsenceIsOffline:
    """tap omits a device it cannot reach from live rows entirely, so there is
    no failure to count -- only silence to notice."""

    async def test_a_device_present_in_a_frame_is_back_online(self, state, store) -> None:
        plug = _plug(state, store, "A")
        state.offline_since[DEV] = NOW - timedelta(minutes=5)

        await apply_live(state, store, [_row("A")], now=NOW)

        assert DEV not in state.offline_since
        assert state.status_since[plug][0] == "powered"

    async def test_a_device_unseen_for_the_stale_window_goes_offline(self, state, store) -> None:
        plug = _plug(state, store, "A")
        clock = [NOW]
        projector = LiveProjector(state, store, now=lambda: clock[0])
        await projector([_row("A")])
        await projector.settle()

        clock[0] = NOW + timedelta(seconds=LIVE_STALE_S - 1)
        assert projector.sweep() is False
        assert DEV not in state.offline_since

        clock[0] = NOW + timedelta(seconds=LIVE_STALE_S + 1)
        assert projector.sweep() is True
        assert state.offline_since[DEV] == clock[0]
        assert state.status_since[plug] == ("unreachable", clock[0])

    async def test_a_device_never_heard_from_goes_offline_from_startup(self, state, store) -> None:
        """A plug hydrated from the store whose device tap never mentions -- a
        strip that died in May -- is unreachable, and the sweep must say so
        rather than leaving whatever status the store implied."""
        _plug(state, store, "A", device=OTHER)
        clock = [NOW]
        projector = LiveProjector(state, store, now=lambda: clock[0])

        clock[0] = NOW + timedelta(seconds=LIVE_STALE_S + 1)
        projector.sweep()

        assert OTHER in state.offline_since

    async def test_a_dropped_uplink_takes_every_device_offline(self, state, store) -> None:
        _plug(state, store, "A")
        _plug(state, store, "B", device=OTHER)
        clock = [NOW]
        projector = LiveProjector(state, store, now=lambda: clock[0])
        await projector([_row("A"), _row("B", device=OTHER)])
        await projector.settle()

        clock[0] = NOW + timedelta(seconds=LIVE_STALE_S + 1)
        projector.sweep()

        assert set(state.offline_since) == {DEV, OTHER}

    async def test_the_sweep_publishes_only_when_something_changed(self, state, store) -> None:
        _plug(state, store, "A")
        queue: asyncio.Queue = asyncio.Queue()
        state.event_subscribers.add(queue)
        clock = [NOW]
        projector = LiveProjector(state, store, now=lambda: clock[0])

        clock[0] = NOW + timedelta(seconds=LIVE_STALE_S + 1)
        projector.sweep()
        assert queue.qsize() == 1, "the transition is news"
        projector.sweep()
        assert queue.qsize() == 1, "still offline is not"

    async def test_a_reappearing_device_recovers(self, state, store) -> None:
        plug = _plug(state, store, "A")
        clock = [NOW]
        projector = LiveProjector(state, store, now=lambda: clock[0])
        clock[0] = NOW + timedelta(seconds=LIVE_STALE_S + 1)
        projector.sweep()
        assert DEV in state.offline_since

        await projector([_row("A")])
        await projector.settle()

        assert DEV not in state.offline_since
        assert state.status_since[plug][0] == "powered"
        assert projector.sweep() is False

    async def test_the_loop_sweeps_and_expires_commands(self, state, store) -> None:
        """`commands.sweep()` belongs to the 1 Hz loop, not to frame arrival:
        it must time a command out precisely when frames have stopped."""
        import contextlib

        plug = _plug(state, store, "A")
        state.commands = state.commands.__class__(now=lambda: datetime.now(UTC))
        cmd, _ = state.commands.open_ex(kind="turn_on", plug_id=plug, actor="t", source="t")
        cmd.deadline = datetime.now(UTC) - timedelta(seconds=1)
        projector = LiveProjector(state, store)

        task = asyncio.create_task(live_loop(projector, interval=0.02))
        await asyncio.sleep(0.1)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        assert cmd.phase == "timed_out"
        assert projector.sweeps >= 3


class TestClockSkewIsRefused:
    """A live row's `ts_ms` is used for exactly one thing: noticing that tap's
    clock is wrong. Deliberately stricter than the durable channel, which
    accepts an hour of drift: a tap three minutes fast keeps its history and
    loses its dashboard, because the fix is chrony on the box."""

    async def test_a_frame_within_the_bound_is_applied(self, state, store) -> None:
        plug = _plug(state, store, "A")
        projector = LiveProjector(state, store, now=lambda: NOW)
        await projector([_row("A", ts=NOW + timedelta(seconds=LIVE_MAX_SKEW_S - 1))])
        await projector.settle()
        assert plug in state.plug_readings

    @pytest.mark.parametrize("sign", [1, -1])
    async def test_a_frame_beyond_the_bound_is_dropped(self, state, store, sign) -> None:
        plug = _plug(state, store, "A")
        projector = LiveProjector(state, store, now=lambda: NOW)
        skewed = NOW + sign * timedelta(seconds=LIVE_MAX_SKEW_S + 1)

        await projector([_row("A", ts=skewed)])
        await projector.settle()

        assert plug not in state.plug_readings
        assert projector.dropped_skew == 1
        assert DEV not in projector.last_seen, "a skewed frame proves nothing about reachability"

    async def test_the_transition_is_logged_once_each_way(self, state, store, caplog) -> None:
        _plug(state, store, "A")
        projector = LiveProjector(state, store, now=lambda: NOW)
        skewed = NOW + timedelta(seconds=LIVE_MAX_SKEW_S + 60)

        with caplog.at_level(logging.INFO, logger="juice.collector_tap"):
            await projector([_row("A", ts=skewed)])
            await projector([_row("A", ts=skewed)])
            await projector([_row("A", ts=skewed)])
            await projector([_row("A")])
            await projector.settle()

        skew_lines = [r for r in caplog.records if "skew" in r.getMessage().lower()]
        assert len(skew_lines) == 2, caplog.text
        assert skew_lines[0].levelno == logging.ERROR
        assert "180" in skew_lines[0].getMessage(), "the offset is what the operator needs"


class TestFramesNeverQueue:
    """The receive loop owns the durable channel's acks; an apply that awaits
    (overload actuation retries for up to a minute) must never hold it. Frames
    that arrive meanwhile wait in a slot of one: the newest wins."""

    async def test_the_latest_frame_wins_while_an_apply_is_running(
        self, state, store, monkeypatch
    ) -> None:
        plug = _plug(state, store, "A")
        gate = asyncio.Event()

        async def slow_check_overload(*_args, **_kwargs):
            await gate.wait()

        monkeypatch.setattr("juice.collector_tap.check_overload", slow_check_overload)
        projector = LiveProjector(state, store, now=lambda: NOW)

        await asyncio.wait_for(projector([_row("A", mw=10_000)]), timeout=0.5)
        await asyncio.wait_for(projector([_row("A", mw=20_000)]), timeout=0.5)
        await asyncio.wait_for(projector([_row("A", mw=30_000)]), timeout=0.5)
        assert projector.dropped_busy == 1, "the middle frame was superseded"
        assert projector.applied_frames == 0, "nothing finished while the gate was shut"

        gate.set()
        await projector.settle()
        assert projector.applied_frames == 2
        assert state.plug_readings[plug].watts == 30.0, "the newest frame is what shows"

    async def test_a_hung_apply_is_cancelled_by_the_sweep(
        self, state, store, monkeypatch, caplog
    ) -> None:
        """The cloud actuation path has no timeout. An apply stuck in it would
        otherwise hold the slot forever, and the floor -- `last_seen` still
        refreshing -- would read as current while frozen."""
        _plug(state, store, "A")
        gate = asyncio.Event()

        async def hung_check_overload(*_args, **_kwargs):
            await gate.wait()

        monkeypatch.setattr("juice.collector_tap.check_overload", hung_check_overload)
        clock = [NOW]
        projector = LiveProjector(state, store, now=lambda: clock[0])
        await projector([_row("A")])

        clock[0] = NOW + timedelta(seconds=LIVE_STALE_S + 1)
        with caplog.at_level(logging.ERROR, logger="juice.collector_tap"):
            projector.sweep()
        await asyncio.wait_for(projector.settle(), timeout=2.0)

        assert any("cancelling" in r.getMessage() for r in caplog.records), caplog.text
        monkeypatch.undo()
        await projector([_row("A")])
        await projector.settle()
        assert projector.applied_frames == 1, "the slot is free again"

    async def test_frames_at_cadence_each_publish_a_tick(self, state, store) -> None:
        """tap's 1 Hz frames are each a tick: the interval only guards against
        a rate above it, and every reading lands either way."""
        from juice.collector_tap import LIVE_PUBLISH_INTERVAL_S

        assert LIVE_PUBLISH_INTERVAL_S < 1.0, "1 Hz frames must clear the gate with jitter to spare"
        plug = _plug(state, store, "A")
        state.assignments[plug] = ("Machine", "M0001", None)
        queue: asyncio.Queue = asyncio.Queue()
        state.event_subscribers.add(queue)
        clock = [NOW]
        projector = LiveProjector(state, store, now=lambda: clock[0])

        for i in range(4):
            clock[0] = NOW + timedelta(seconds=i)
            await projector([_row("A", mw=(i + 1) * 1000)])
            await projector.settle()

        assert state.plug_readings[plug].watts == 4.0
        assert list(state.watt_buffers[plug]) == [1.0, 2.0, 3.0, 4.0]
        assert queue.qsize() == 4

    async def test_a_frame_inside_the_interval_is_published_when_it_elapses(
        self, state, store
    ) -> None:
        """tap sends a frame out of cadence the moment a command moves a relay,
        and the operator's button settles on the tick that carries it. Inside
        the interval the tick is held, not dropped: it goes out as soon as the
        interval elapses, showing the state the held frame brought, and a
        burst inside one interval costs one tick."""
        plug = _plug(state, store, "A")
        state.assignments[plug] = ("Machine", "M0001", None)
        queue: asyncio.Queue = asyncio.Queue()
        state.event_subscribers.add(queue)
        clock = [NOW]
        projector = LiveProjector(state, store, now=lambda: clock[0], publish_interval_s=0.2)

        await projector([_row("A", relay=1, mw=42_000)])
        await projector.settle()
        assert queue.qsize() == 1

        # The command's frame, 50 ms after the tick: the relay is off now.
        clock[0] = NOW + timedelta(milliseconds=50)
        await projector([_row("A", relay=0, mw=0)])
        await projector.settle()
        clock[0] = NOW + timedelta(milliseconds=100)
        await projector([_row("A", relay=0, mw=0)])
        await projector.settle()
        assert state.plug_readings[plug].is_on is False
        assert queue.qsize() == 1, "held, not published, inside the interval"

        assert projector._held_tick is not None
        await projector._held_tick
        assert queue.qsize() == 2, "one tick for the burst, once the interval elapsed"
        queue.get_nowait()
        tick = queue.get_nowait()
        assert tick["type"] == "readings"
        assert [m["is_on"] for m in tick["machines"] if m["plug_id"] == plug] == [False]

    async def test_a_held_tick_is_dropped_when_a_frame_publishes_first(self, state, store) -> None:
        """A frame landing once the interval has elapsed publishes on its own,
        and the tick that was held for the earlier frame must not follow it as
        a second copy of the same state."""
        plug = _plug(state, store, "A")
        state.assignments[plug] = ("Machine", "M0001", None)
        queue: asyncio.Queue = asyncio.Queue()
        state.event_subscribers.add(queue)
        clock = [NOW]
        projector = LiveProjector(state, store, now=lambda: clock[0], publish_interval_s=0.2)

        await projector([_row("A")])
        await projector.settle()
        clock[0] = NOW + timedelta(milliseconds=50)
        await projector([_row("A", relay=0, mw=0)])
        await projector.settle()
        clock[0] = NOW + timedelta(milliseconds=250)
        await projector([_row("A", relay=0, mw=0)])
        await projector.settle()
        assert queue.qsize() == 2
        assert projector._held_tick is None, "the frame's own tick dropped the held one"

    async def test_an_apply_that_raises_is_logged_not_fatal(
        self, state, store, monkeypatch, caplog
    ) -> None:
        _plug(state, store, "A")

        async def boom(*_args, **_kwargs):
            raise RuntimeError("bookkeeping blew up")

        monkeypatch.setattr("juice.collector_tap.apply_live", boom)
        projector = LiveProjector(state, store, now=lambda: NOW)
        with caplog.at_level(logging.WARNING, logger="juice.collector_tap"):
            await projector([_row("A")])
            await projector.settle()
        assert any("blew up" in r.getMessage() or r.exc_info for r in caplog.records)

        # And the next frame is not blocked behind the failed one.
        monkeypatch.undo()
        await projector([_row("A")])
        await projector.settle()
        assert projector.applied_frames == 1


class TestTheLiveChannelMeasuresTheGaps:
    """The overload gap bound was picked from a LAN measurement against a fake
    server. The projector measures the same thing on the real path, so the
    number can be read from production before overload leaves shadow."""

    async def test_inter_arrival_is_summarised_against_the_bound(
        self, state, store, caplog
    ) -> None:
        from juice.overload import MAX_GAP_S

        _plug(state, store, "A")
        clock = [NOW]
        projector = LiveProjector(state, store, now=lambda: clock[0])
        for offset in (0, 1, 2, 3, 3 + MAX_GAP_S + 2):  # one stall-sized hole
            clock[0] = NOW + timedelta(seconds=offset)
            await projector([_row("A", relay=1, ts=clock[0])])
            await projector.settle()
        with caplog.at_level(logging.INFO, logger="juice.collector_tap"):
            projector.summarise()

        line = next(r.getMessage() for r in caplog.records if "tap live:" in r.getMessage())
        assert "gaps p50 1.00s" in line, line
        assert f"1 ever over the {MAX_GAP_S:.0f}s overload bound" in line, line
        assert f"max {MAX_GAP_S + 2:.2f}s" in line, line
        assert "0 outlet absences" in line, line

    async def test_an_outlet_missing_from_frames_is_an_absence_not_a_gap(
        self, state, store
    ) -> None:
        """A parked device vanishes from the frame and comes back: that is the
        staleness sweep's business, and must not count against the bound."""
        from juice.overload import MAX_GAP_S

        _plug(state, store, "A")
        _plug(state, store, "B", device=OTHER)
        clock = [NOW]
        projector = LiveProjector(state, store, now=lambda: clock[0])
        for offset, children in (
            (0, ("A", "B")),
            (1, ("A", "B")),
            (2, ("B",)),
            (3, ("B",)),
            (40, ("B",)),
            (41, ("A", "B")),
        ):
            clock[0] = NOW + timedelta(seconds=offset)
            rows = [
                _row(c, device=DEV if c == "A" else OTHER, relay=1, ts=clock[0]) for c in children
            ]
            await projector(rows)
            await projector.settle()

        line = projector.gaps.describe()
        assert "1 outlet absences" in line, line
        # B's 37 s gap between consecutive frames is real uplink latency and counts.
        assert f"1 ever over the {MAX_GAP_S:.0f}s overload bound" in line, line

    async def test_a_skewed_frame_is_not_measured(self, state, store) -> None:
        """A dropped frame never reached the floor, so it says nothing about
        the uplink's cadence either."""
        _plug(state, store, "A")
        projector = LiveProjector(state, store, now=lambda: NOW)
        await projector([_row("A", ts=NOW)])
        await projector([_row("A", ts=NOW - timedelta(seconds=LIVE_MAX_SKEW_S + 60))])
        await projector.settle()
        assert projector.gaps.frames == 1
        assert projector.gaps.describe() == "gaps: none measured yet"

    def test_before_any_second_arrival_there_is_nothing_to_say(self, state, store) -> None:
        projector = LiveProjector(state, store, now=lambda: NOW)
        assert projector.gaps.describe() == "gaps: none measured yet"


class TestDeviceHealth:
    def test_ok_clears_offline(self) -> None:
        state = FloorState()
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=UTC)
        mark_device_offline(state, "d1", ts, reason="unseen in live frames")
        assert "d1" in state.offline_since

        note_device_ok(state, "d1")
        assert "d1" not in state.offline_since

    def test_helpers_noop_without_state(self) -> None:
        note_device_ok(None, "d1")  # must not raise
