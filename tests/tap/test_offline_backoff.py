"""How long tap waits before re-probing a device it has parked.

Eleven hours against a real P316M made this the most expensive policy in the
daemon. Sweep losses were perfectly bimodal — 130 single misses, 6 doubles, and
5 holes of exactly 63 sweeps — with nothing in between. The 63-sweep holes were
not outages: three timeouts in a row tripped `OFFLINE_FAILURE_THRESHOLD`, tap
parked the device for a flat 60 s, and **the first re-probe succeeded every
time, all five times**. 315 sweeps were lost to the backoff against 142 lost to
the timeouts that triggered it — 69% of all lost data was self-inflicted.

A flat 60 s is the right patience for a device that has genuinely been
unplugged. It is the wrong patience for a three-second network blip, and on
this hardware a blip is all that ever actually happened.
"""

from __future__ import annotations

import logging

import pytest

from tap.buffer import Buffer
from tap.config import Config, DeviceSpec
from tap.device import DeviceState, Family
from tap.errors import TransientError
from tap.health import Health
from tap.poller import OFFLINE_BACKOFF, DevicePoller, offline_backoff_delay
from tests.tap.fakes import FakeDevice

INTERVAL = 0.02
BUDGET = 0.05


@pytest.fixture
async def buf(tmp_path):
    b = Buffer(tmp_path / "buffer", retention_days=30)
    await b.open()
    yield b
    await b.close()


def _poller(device, buf, health, **kw):
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


class TestTheSchedule:
    def test_the_first_reprobe_is_quick(self):
        """The measured case: the device is already back. Ask it."""
        assert offline_backoff_delay(0) <= 2.0

    def test_it_escalates_and_then_holds(self):
        delays = [offline_backoff_delay(i) for i in range(len(OFFLINE_BACKOFF) + 3)]
        assert delays == sorted(delays), delays
        # A flat schedule is sorted too; this is what says it escalates.
        assert delays[0] < delays[-1], delays
        assert delays[-1] == OFFLINE_BACKOFF[-1]
        assert delays[len(OFFLINE_BACKOFF) - 1] == OFFLINE_BACKOFF[-1]

    def test_it_still_ends_up_patient(self):
        """A device that is genuinely unplugged must not be polled forever."""
        assert OFFLINE_BACKOFF[-1] >= 60.0

    def test_a_negative_or_silly_attempt_count_is_clamped(self):
        assert offline_backoff_delay(-1) == OFFLINE_BACKOFF[0]
        assert offline_backoff_delay(10_000) == OFFLINE_BACKOFF[-1]


class TestPauseUsesTheSchedule:
    async def test_an_offline_device_waits_the_escalating_delay(self, buf):
        health = Health()
        device = FakeDevice(host="10.0.0.5", fail_with=TransientError("nope"))
        poller = _poller(device, buf, health)

        for _ in range(3):
            await poller._tick()
        assert poller.state is DeviceState.OFFLINE

        # First re-probe: short. Each subsequent failure lengthens the wait.
        seen = [poller._pause()]
        for _ in range(4):
            await poller._tick()
            seen.append(poller._pause())

        assert seen[0] == OFFLINE_BACKOFF[0]
        assert seen == sorted(seen), seen
        assert seen[-1] > seen[0]

    async def test_a_later_blip_starts_from_the_short_delay_again(self, buf):
        """The next blip must start from the short delay, not inherit 60s."""
        health = Health()
        device = FakeDevice(host="10.0.0.5", fail_with=TransientError("nope"))
        poller = _poller(device, buf, health)

        for _ in range(6):
            await poller._tick()
        assert poller._pause() > OFFLINE_BACKOFF[0]

        device.fail_with = None
        await poller._tick()
        assert poller.state is DeviceState.ONLINE

        device.fail_with = TransientError("nope")
        for _ in range(3):
            await poller._tick()
        assert poller._pause() == OFFLINE_BACKOFF[0]

    async def test_answering_clears_the_probe_count(self, buf):
        """Pinned directly. The reset in `_note_ok` and the one at the OFFLINE
        transition are individually redundant, so a round-trip assertion passes
        with either one deleted and therefore proves neither."""
        health = Health()
        device = FakeDevice(host="10.0.0.5", fail_with=TransientError("nope"))
        poller = _poller(device, buf, health)

        for _ in range(5):
            await poller._tick()
        assert poller._offline_probes > 0

        device.fail_with = None
        await poller._tick()
        assert poller._offline_probes == 0

    async def test_an_online_device_still_polls_at_the_interval(self, buf):
        health = Health()
        poller = _poller(FakeDevice(host="10.0.0.5"), buf, health)
        await poller._tick()
        assert poller._pause() == INTERVAL

    async def test_unauthorized_is_untouched_by_the_schedule(self, buf):
        """A rejected credential is not a blip; nothing about retrying fixes it."""
        from tap.poller import AUTH_REPROBE_SECONDS

        health = Health()
        poller = _poller(FakeDevice(host="10.0.0.5", auth_error=True), buf, health)
        await poller._tick()
        assert poller.state is DeviceState.UNAUTHORIZED
        assert poller._pause() == AUTH_REPROBE_SECONDS


class TestTheHoleGetsSmaller:
    async def test_a_blip_costs_far_less_than_the_flat_minute_did(self, buf):
        """End to end, with the real shape: three failures then the device is fine.

        Under the old flat 60 s this cost 63 sweeps. It should now cost the
        first backoff step.
        """
        health = Health()
        device = FakeDevice(host="10.0.0.6", fail_with=TransientError("nope"))
        poller = _poller(device, buf, health)

        for _ in range(3):
            await poller._tick()
        assert poller.state is DeviceState.OFFLINE
        wait = poller._pause()
        device.fail_with = None
        await poller._tick()

        assert poller.state is DeviceState.ONLINE
        assert wait == OFFLINE_BACKOFF[0]
        assert wait < 5.0, "a three-second blip should not cost a minute of data"


class TestTheOfflineLineSaysWhy:
    async def test_a_bare_timeout_does_not_render_as_empty_parentheses(self, buf, caplog):
        """Observed in production:

            device 192.168.4.38 offline after 3 failures (); backing off to 60s

        `asyncio.timeout` raises a `TimeoutError` whose `str()` is empty, and
        this line interpolated the exception rather than describing it — in the
        one line whose own comment calls it what an operator reads at 11pm.
        """
        health = Health()
        device = FakeDevice(host="10.0.0.5", hang=True)
        poller = _poller(device, buf, health)

        with caplog.at_level(logging.WARNING, logger="tap.poller"):
            for _ in range(3):
                await poller._tick()

        (line,) = [r.getMessage() for r in caplog.records if "offline" in r.getMessage()]
        assert "()" not in line, line
        assert "TimeoutError" in line
        assert "sweep" in line

    async def test_it_reports_the_delay_it_will_actually_wait(self, buf, caplog):
        """It used to name the constant; the wait is now a schedule."""
        health = Health()
        device = FakeDevice(host="10.0.0.5", fail_with=TransientError("nope"))
        poller = _poller(device, buf, health)

        with caplog.at_level(logging.WARNING, logger="tap.poller"):
            for _ in range(3):
                await poller._tick()

        (line,) = [r.getMessage() for r in caplog.records if "offline" in r.getMessage()]
        assert f"{OFFLINE_BACKOFF[0]:g}s" in line, line
