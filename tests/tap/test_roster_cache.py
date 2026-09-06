"""The outlet roster is fetched off the critical path, not inside every sweep.

Measured against a real P316M over 20 minutes (1,195 sweeps): the roster call
(`get_child_device_list`) costs p50 98 ms against p50 18 ms for an outlet's
meter read, so inside the sweep it was roughly 40% of the work for one call in
seven. Moved after the outlets and run only in the idle time before the next
tick, the sweep drops to p50 159 ms, every sweep completes, and the roster is
skipped 0.67% of the time — never more than three sweeps stale.

Relay state and alias come from the roster, so a carried-over roster means
those can lag by a sweep or two while power stays live. That is the trade:
`juice` decides on power, and a relay that changed is visible within seconds.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from tap.buffer import Buffer
from tap.config import Config, DeviceSpec
from tap.device import Family
from tap.health import Health
from tap.poller import DevicePoller
from tests.tap.fakes import FakeDevice

INTERVAL = 0.05


@pytest.fixture
async def buf(tmp_path):
    b = Buffer(tmp_path / "buffer", retention_days=30)
    await b.open()
    yield b
    await b.close()


def _poller(device, buf, health, **kw):
    spec = DeviceSpec(host=device.host, family=Family.SMART, pinned=True)
    p = DevicePoller(
        spec,
        Config(),
        buf,
        health,
        interval=kw.pop("interval", INTERVAL),
        sweep_budget=kw.pop("sweep_budget", 5.0),
        connect_budget=kw.pop("connect_budget", 1.0),
    )
    p.factory = lambda _s, _c: device
    return p


class TestTheSweepDoesNotFetchTheRoster:
    async def test_the_first_sweep_fetches_it(self, buf):
        d = FakeDevice(host="10.0.0.1")
        assert d.roster_fetches == 0
        await d.open()
        await d.sweep()
        assert d.roster_fetches == 1, "the first sweep has no roster to work from"

    async def test_later_sweeps_reuse_it(self, buf):
        d = FakeDevice(host="10.0.0.1")
        await d.open()
        for _ in range(5):
            await d.sweep()
        assert d.roster_fetches == 1, "the roster must not ride on every sweep"

    async def test_a_sweep_on_a_carried_over_roster_still_reads_live_power(self, buf):
        d = FakeDevice(host="10.0.0.1", watts=1_000)
        await d.open()
        first = await d.sweep()
        d.watts = 77_000
        second = await d.sweep()
        assert d.roster_fetches == 1
        assert [o.power_mw for o in first.outlets] == [1_000] * len(first.outlets)
        assert [o.power_mw for o in second.outlets] == [77_000] * len(second.outlets)

    async def test_refreshing_picks_up_a_relay_change(self, buf):
        d = FakeDevice(host="10.0.0.1")
        await d.open()
        await d.sweep()
        d.relay_state = {o.child_id: False for o in (await d.sweep()).outlets}
        await d.refresh_roster()
        assert d.roster_fetches == 2
        assert all(not o.relay_on for o in (await d.sweep()).outlets)

    async def test_the_sweep_reports_how_stale_its_roster_is(self, buf):
        d = FakeDevice(host="10.0.0.1")
        await d.open()
        assert (await d.sweep()).roster_age == 0
        assert (await d.sweep()).roster_age == 1
        assert (await d.sweep()).roster_age == 2
        await d.refresh_roster()
        assert (await d.sweep()).roster_age == 0


class TestThePollerRefreshesInTheIdleTime:
    async def test_it_refreshes_when_the_sweep_left_room(self, buf):
        health = Health()
        d = FakeDevice(host="10.0.0.1", sweep_ms=1.0)
        p = _poller(d, buf, health, interval=0.3)
        p.start()
        await asyncio.sleep(0.75)
        await p.stop()
        assert d.roster_fetches >= 2, "a fast sweep leaves plenty of room"

    async def test_it_skips_the_refresh_when_the_sweep_used_the_whole_interval(self, buf):
        """The sweep must be what consumes the room, not the interval itself.

        An earlier version used interval=0.05 against a 0.2 s margin, so the
        skip was guaranteed however fast the sweep ran — it passed with
        `sweep_ms=0`, proving nothing.
        """
        health = Health()
        d = FakeDevice(host="10.0.0.1", sweep_ms=280.0)
        p = _poller(d, buf, health, interval=0.3)
        p.start()
        # `run` jitters its start by up to one interval before the first sweep.
        await asyncio.sleep(1.4)
        await p.stop()
        # One fetch for the first sweep, and none after: there is never room.
        assert d.roster_fetches == 1, f"{d.roster_fetches} fetches with no idle time"
        assert health.device("FAKE0001", host="10.0.0.1").roster_skips > 0

    async def test_a_failed_refresh_does_not_fail_the_sweep(self, buf):
        """A refresh that fails leaves the previous roster in place.

        The first sweep is the exception: with no roster at all there are no
        outlets to read, so that one does fail. After that a broken refresh is
        indistinguishable from no room for one.
        """
        health = Health()
        d = FakeDevice(host="10.0.0.1", sweep_ms=1.0)
        p = _poller(d, buf, health, interval=0.5)  # room to spare for a refresh
        p.start()
        await asyncio.sleep(0.6)  # one good sweep first
        d.roster_fail = RuntimeError("roster went away")
        await asyncio.sleep(1.2)
        await p.stop()
        entry = health.device("FAKE0001", host="10.0.0.1")
        assert entry.sweeps_ok >= 2, "sweeps keep succeeding on the carried-over roster"
        assert entry.roster_failures > 0


class TestAFrozenRosterIsVisible:
    """A roster that never refreshes does not fail a sweep.

    Power keeps flowing and every reading looks current, while `relay_on`,
    `alias` and the protection flags stay frozen at whatever they were and are
    still written to the buffer and shipped upstream as if live. Before this
    change a broken `get_child_device_list` failed the sweep and drove the
    device OFFLINE; now nothing else would ever notice.
    """

    async def test_it_says_so_after_enough_stale_sweeps(self, buf, caplog):
        import tap.poller as poller_mod

        health = Health()
        # The sweep eats the interval, so there is never room for a refresh.
        d = FakeDevice(host="10.0.0.1", sweep_ms=45.0)
        p = _poller(d, buf, health, interval=0.05)
        original = poller_mod.ROSTER_STALE_THRESHOLD
        poller_mod.ROSTER_STALE_THRESHOLD = 3
        try:
            with caplog.at_level(logging.WARNING, logger="tap.poller"):
                p.start()
                await asyncio.sleep(0.4)
                await p.stop()
        finally:
            poller_mod.ROSTER_STALE_THRESHOLD = original

        said = [r.getMessage() for r in caplog.records if "roster" in r.getMessage()]
        assert said, f"a frozen roster went unreported: {[r.getMessage() for r in caplog.records]}"
        assert "10.0.0.1" in said[0]

    async def test_a_healthy_device_says_nothing(self, buf, caplog):
        health = Health()
        d = FakeDevice(host="10.0.0.1", sweep_ms=1.0)
        p = _poller(d, buf, health, interval=0.4)  # plenty of room
        with caplog.at_level(logging.WARNING, logger="tap.poller"):
            p.start()
            await asyncio.sleep(1.0)
            await p.stop()
        assert not [r for r in caplog.records if "roster" in r.getMessage()]
        assert health.device("FAKE0001", host="10.0.0.1").roster_refreshes > 0

    async def test_a_failed_refresh_is_not_also_counted_as_a_skip(self, buf):
        """They are different events: one we chose not to ask, one that would
        not answer. Counting both put one failure in two counters."""
        health = Health()
        d = FakeDevice(host="10.0.0.1", sweep_ms=1.0)
        p = _poller(d, buf, health, interval=0.5)
        p.start()
        await asyncio.sleep(0.6)
        d.roster_fail = RuntimeError("roster went away")
        await asyncio.sleep(1.2)
        await p.stop()

        entry = health.device("FAKE0001", host="10.0.0.1")
        assert entry.roster_failures > 0
        assert entry.roster_skips == 0, "a failure is not a skip"

    async def test_a_sweep_that_fetched_its_own_roster_ends_the_stale_streak(self, buf):
        """A reconnect gets a fresh roster inside the sweep. A streak from
        before it must not then warn about data that is current."""
        health = Health()
        d = FakeDevice(host="10.0.0.1", sweep_ms=1.0)
        p = _poller(d, buf, health, interval=0.5)
        p._roster_stale = 999  # a streak from before

        await p._tick()

        assert p._roster_stale == 0
        await p.stop()
