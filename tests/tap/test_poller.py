"""The requirement this file exists to defend: **issues with one device do not
slow polling on other devices.**

juice's recorder walks its devices sequentially over a session with no timeout,
so one wedged device stalls the whole fleet. These tests assert the replacement
behaves differently, and that the offline state machine stays quiet.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from tap.buffer import Buffer
from tap.config import Config, DeviceSpec, ExcludeRule
from tap.device import DeviceState, Family
from tap.errors import TransientError
from tap.health import DeviceHealth, Health
from tap.poller import DevicePoller, PollerSet
from tests.tap.fakes import FakeDevice, wait_for

INTERVAL = 0.02
BUDGET = 0.05


@pytest.fixture
async def buf(tmp_path):
    b = Buffer(tmp_path / "buffer", retention_days=30)
    await b.open()
    yield b
    await b.close()


_NO_DEVICE = DeviceHealth(device_id="", host="")


def _poller(device: FakeDevice, buf, health, **kw) -> DevicePoller:
    spec = DeviceSpec(host=device.host, family=Family.SMART, pinned=True)
    poller = DevicePoller(
        spec,
        Config(),
        buf,
        health,
        interval=kw.pop("interval", INTERVAL),
        sweep_budget=kw.pop("sweep_budget", BUDGET),
        connect_budget=kw.pop("connect_budget", BUDGET),
    )
    poller.factory = lambda _spec, _cfg: device
    return poller


class TestIsolation:
    async def test_a_hung_device_does_not_slow_its_neighbours(self, buf):
        """The headline requirement, asserted directly."""
        health = Health()
        healthy = [FakeDevice(device_id=f"OK{i}", host=f"10.0.0.{i}") for i in range(3)]
        hung = FakeDevice(device_id="HUNG", host="10.0.0.99", hang=True)
        pollers = [_poller(d, buf, health) for d in (*healthy, hung)]
        for p in pollers:
            p.start()
        try:
            await asyncio.sleep(0.5)
        finally:
            for p in pollers:
                await p.stop()

        # ~0.5s at a 20ms interval, minus up to one interval of start jitter.
        for device in healthy:
            assert device.sweeps >= 8, f"{device.device_id} only swept {device.sweeps}"
        assert hung.sweeps == 0

    async def test_a_hung_sweep_is_cancelled_at_the_budget(self, buf):
        """A sweep must never outlive its budget, or sweeps would pile up."""
        health = Health()
        hung = FakeDevice(device_id="HUNG", host="10.0.0.99", hang=True)
        poller = _poller(hung, buf, health, interval=0.01, sweep_budget=0.05)
        poller.start()
        try:
            await asyncio.sleep(0.35)
        finally:
            await poller.stop()
        # Each attempt costs one budget, so we get several — not one that hangs
        # forever, and not an unbounded pile.
        entry = next(iter(health.devices.values()))
        assert entry.sweeps_failed >= 2
        assert entry.state is DeviceState.OFFLINE


class TestOfflineStateMachine:
    async def test_parks_after_the_threshold_and_logs_once(self, buf, caplog):
        health = Health()
        broken = FakeDevice(host="10.0.0.5", fail_with=TransientError("nope"))
        poller = _poller(broken, buf, health)
        with caplog.at_level(logging.DEBUG, logger="tap.poller"):
            poller.start()
            try:
                await asyncio.sleep(0.4)
            finally:
                await poller.stop()

        entry = next(iter(health.devices.values()))
        assert entry.state is DeviceState.OFFLINE
        # Exactly one WARNING for the whole outage, however long it lasts.
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert "offline" in warnings[0].getMessage()

    async def test_recovery_logs_once_and_resumes_fast_polling(self, buf, caplog):
        health = Health()
        device = FakeDevice(host="10.0.0.6", fail_with=TransientError("nope"))
        poller = _poller(device, buf, health)
        # A short re-probe so the test does not wait for the real schedule.
        import tap.poller as poller_mod

        original = poller_mod.OFFLINE_BACKOFF
        poller_mod.OFFLINE_BACKOFF = (0.05,)
        try:
            with caplog.at_level(logging.INFO, logger="tap.poller"):
                poller.start()
                await asyncio.sleep(0.2)
                assert poller.state is DeviceState.OFFLINE
                device.fail_with = None  # the device comes back
                await asyncio.sleep(0.3)
        finally:
            poller_mod.OFFLINE_BACKOFF = original
            await poller.stop()

        assert poller.state is DeviceState.ONLINE
        recovered = [r for r in caplog.records if "back online" in r.getMessage()]
        assert len(recovered) == 1

    async def test_auth_failure_parks_separately_and_logs_error(self, buf, caplog):
        """A rejected credential needs a human, not a retry at poll cadence."""
        health = Health()
        device = FakeDevice(host="10.0.0.7", auth_error=True)
        poller = _poller(device, buf, health)
        with caplog.at_level(logging.ERROR, logger="tap.poller"):
            poller.start()
            try:
                await asyncio.sleep(0.15)
            finally:
                await poller.stop()

        assert poller.state is DeviceState.UNAUTHORIZED
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        assert "credentials" in errors[0].getMessage()
        # Parked immediately: one failed attempt, not three.
        entry = next(iter(health.devices.values()))
        assert entry.sweeps_failed == 1


class TestBuffering:
    async def test_sweeps_reach_the_buffer(self, buf):
        health = Health()
        device = FakeDevice(host="10.0.0.8", outlets=3)
        poller = _poller(device, buf, health)
        poller.start()
        try:
            await asyncio.sleep(0.2)
        finally:
            await poller.stop()
        await buf.flush()
        rows = await buf.read_after(None)
        assert rows
        assert len(rows) % 3 == 0
        assert rows[0].power_mw == 42_000

    async def test_health_carries_live_outlet_values(self, buf):
        health = Health()
        device = FakeDevice(device_id="LIVE1", host="10.0.0.9", outlets=2)
        poller = _poller(device, buf, health)
        poller.start()
        try:
            await asyncio.sleep(0.15)
        finally:
            await poller.stop()
        (entry,) = health.snapshot()["devices"]
        assert entry["device_id"] == "LIVE1"
        assert entry["state"] == "online"
        assert entry["sweep_p50_ms"] is not None
        assert [o["watts"] for o in entry["outlets"]] == [42.0, 42.0]
        assert all(o["relay_on"] for o in entry["outlets"])

    async def test_leaving_the_roster_forgets_the_device(self, buf):
        health = Health()
        device = FakeDevice(device_id="GONE", host="10.0.0.12")
        poller = _poller(device, buf, health)
        poller.start()
        await asyncio.sleep(0.1)
        await poller.stop(forget=True)
        assert health.snapshot()["devices"] == []

    async def test_gap_is_recorded_when_a_device_goes_offline(self, buf, tmp_path):
        import sqlite3

        health = Health()
        device = FakeDevice(device_id="GAP1", host="10.0.0.10", fail_with=TransientError("nope"))
        poller = _poller(device, buf, health)
        poller.start()
        try:
            await asyncio.sleep(0.3)
        finally:
            await poller.stop()
        conn = sqlite3.connect(tmp_path / "buffer" / "meta.sqlite")
        try:
            rows = conn.execute("SELECT device_id, reason FROM gaps").fetchall()
        finally:
            conn.close()
        assert rows == [("GAP1", "unreachable")]


class TestPollerSet:
    async def test_reconcile_starts_stops_and_leaves_healthy_pollers_alone(self, buf):
        health = Health()
        devices = {
            "10.0.0.1": FakeDevice(device_id="A", host="10.0.0.1"),
            "10.0.0.2": FakeDevice(device_id="B", host="10.0.0.2"),
        }
        pset = PollerSet(
            Config(),
            buf,
            health,
            interval=INTERVAL,
            sweep_budget=BUDGET,
            factory=lambda spec, _cfg: devices[spec.host],
        )
        specs = {h: DeviceSpec(host=h, family=Family.SMART) for h in devices}
        await pset.reconcile(specs)
        assert len(pset) == 2
        first = pset.pollers["10.0.0.1"]

        await asyncio.sleep(0.1)
        # Reconciling with the same roster must not restart anything.
        await pset.reconcile(specs)
        assert pset.pollers["10.0.0.1"] is first
        assert devices["10.0.0.1"].opens == 1

        # Dropping one stops exactly one.
        await pset.reconcile({"10.0.0.1": specs["10.0.0.1"]})
        assert len(pset) == 1
        assert devices["10.0.0.2"].closes >= 1
        await pset.stop()
        assert len(pset) == 0

    async def test_find_by_device_id(self, buf):
        health = Health()
        device = FakeDevice(device_id="FINDME", host="10.0.0.3")
        pset = PollerSet(
            Config(),
            buf,
            health,
            interval=INTERVAL,
            sweep_budget=BUDGET,
            factory=lambda _spec, _cfg: device,
        )
        await pset.reconcile({"10.0.0.3": DeviceSpec(host="10.0.0.3", family=Family.SMART)})
        try:
            await asyncio.sleep(0.1)
            assert pset.find("FINDME") is not None
            assert pset.find("NOPE") is None
        finally:
            await pset.stop()


class TestCommands:
    async def test_set_relay_reaches_the_device(self, buf):
        health = Health()
        device = FakeDevice(device_id="CMD1", host="10.0.0.4")
        poller = _poller(device, buf, health)
        poller.start()
        try:
            await asyncio.sleep(0.1)
            await poller.set_relay("CMD100", False)
        finally:
            await poller.stop()
        assert device.relay_calls == [("CMD100", False)]

    async def test_set_relay_is_visible_in_health_before_the_next_sweep(self, buf):
        """A live frame reads relay state from Health, which a sweep refreshes
        once a second. The switch we just made is the one fact about the
        outlet we know for certain, so it goes in now -- and an outlet whose
        relay is off draws nothing, so its power does too."""
        health = Health()
        device = FakeDevice(device_id="CMD2", host="10.0.0.5")
        poller = _poller(device, buf, health)
        poller.start()
        try:
            await wait_for(lambda: "CMD200" in health.devices.get("CMD2", _NO_DEVICE).outlets)
            outlet = health.devices["CMD2"].outlets["CMD200"]
            assert outlet.relay_on is True and outlet.power_mw == 42_000
            # From here no sweep completes, so only the command can move Health.
            device.hang = True
            sweeps_before = device.sweeps
            await poller.set_relay("CMD200", False)
            assert device.sweeps == sweeps_before
            assert outlet.relay_on is False
            assert outlet.power_mw == 0
            await poller.set_relay("CMD200", True)
            assert outlet.relay_on is True
            # Draw after power-on is not known until a sweep measures it.
            assert outlet.power_mw == 0
        finally:
            await poller.stop()

    async def test_a_sweep_in_flight_when_the_command_lands_does_not_undo_it(self, buf):
        """A sweep is several round trips, and a command's own round trip
        interleaves between two of them. A sweep that captured the outlet
        before the switch finishes after it, and must not put the old relay
        state -- and the draw measured behind it -- back into Health for a
        frame: the button would settle, flip back, and settle again."""
        health = Health()
        device = FakeDevice(device_id="CMD3", host="10.0.0.6", sweep_ms=200)
        poller = _poller(device, buf, health, interval=0.05, sweep_budget=1.0)
        poller.start()
        try:
            await wait_for(lambda: "CMD300" in health.devices.get("CMD3", _NO_DEVICE).outlets)
            outlet = health.devices["CMD3"].outlets["CMD300"]
            sweeps_seen = device.sweeps
            # Land the command while the next sweep is between its round trips.
            await wait_for(lambda: device.sweeping)
            await poller.set_relay("CMD300", False)
            assert outlet.relay_on is False and outlet.power_mw == 0
            # That sweep finishes with its pre-switch reading (relay on, 42 W)...
            await wait_for(lambda: device.sweeps > sweeps_seen)
            assert outlet.relay_on is False
            assert outlet.power_mw == 0
            # ...and the one after it reads the switch off the patched roster.
            await wait_for(lambda: device.sweeps > sweeps_seen + 1)
            assert outlet.relay_on is False
            assert outlet.power_mw == 42_000, "a sweep taken after the switch is believed"
        finally:
            await poller.stop()

    async def test_set_relay_refuses_when_disconnected(self, buf):
        health = Health()
        device = FakeDevice(host="10.0.0.11")
        poller = _poller(device, buf, health)
        with pytest.raises(ConnectionError):
            await poller.set_relay("X", True)


class TestRefusalBeforeAdoption:
    """A device the poller refuses must not end up being polled anyway.

    `_tick` skips `_connect` whenever `self._device` is set. Assigning the
    device before the identity and exclusion checks meant a refusal left it
    adopted: the check never ran again and the device was swept and buffered
    under exactly the identity that had been rejected.
    """

    async def test_a_duplicate_device_id_is_refused_and_never_polled(self, buf):
        health = Health()
        first = FakeDevice(device_id="SAME", host="10.0.0.1")
        second = FakeDevice(device_id="SAME", host="10.0.0.2")
        p1 = _poller(first, buf, health)
        p2 = _poller(second, buf, health)
        p1.start()
        await asyncio.sleep(0.1)
        p2.start()
        try:
            await asyncio.sleep(0.3)
        finally:
            await p1.stop()
            await p2.stop()

        # The first keeps its entry; the second never gets to sweep at all.
        assert first.sweeps > 0
        assert second.sweeps == 0
        # And it is not left holding an open connection.
        assert second.closes >= second.opens

    async def test_an_excluded_device_id_stops_the_poller_rather_than_retrying(self, buf):
        """An IOT device only reveals its real id on connect, so the exclusion
        has to be enforced here rather than at discovery."""
        health = Health()
        device = FakeDevice(device_id="BENCH1", host="10.0.0.3")
        spec = DeviceSpec(host="10.0.0.3", family=Family.SMART, pinned=True)
        config = Config(excludes=(ExcludeRule(device_id="BENCH1", reason="bench unit"),))
        poller = DevicePoller(
            spec, config, buf, health, interval=INTERVAL, sweep_budget=BUDGET, connect_budget=BUDGET
        )
        poller.factory = lambda _spec, _cfg: device
        poller.start()
        try:
            await asyncio.sleep(0.3)
        finally:
            await poller.stop()

        assert device.sweeps == 0
        assert poller.state is DeviceState.EXCLUDED
        # Stopped, not spinning: it did not reconnect once per interval.
        assert device.opens == 1

    async def test_a_failed_open_does_not_leak_the_connection(self, buf):
        health = Health()
        device = FakeDevice(host="10.0.0.4", open_fail=TransientError("refused"))
        poller = _poller(device, buf, health)
        poller.start()
        try:
            await asyncio.sleep(0.15)
        finally:
            await poller.stop()
        # Every attempt that opened something also closed it.
        assert device.closes >= device.opens
