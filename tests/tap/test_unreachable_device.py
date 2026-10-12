"""A device tap cannot contact is parked, not reported frozen.

The incident this file exists for: five museum strips stopped accepting the
legacy Kasa protocol on TCP 9999 overnight. `refresh_roster` was refused,
`sweep()` kept consuming the frozen roster cache, and every emeter read failed
and was swallowed into a null power field — so the sweep *succeeded*, the device
stayed ONLINE, and `Uplink._live_rows` kept shipping a full row per outlet with a
relay state nobody had been able to verify for hours. juice saw rows, so it never
marked the device offline, and `/api/v2/floor` reported `"infrastructure": []`
while twenty machines sat dark. The museum opened that way twice.

Two rules close it, and both are needed:

  A. A sweep of a *metered* device that measured nothing is not a success. Every
     round trip failed; only cached fields came back.
  B. A sustained run of failed roster refreshes is an outage. This is the only
     rule that can see a *meterless* device (an EP10, the claw machine), which
     issues no emeter call at all and would otherwise sweep "successfully"
     forever off a frozen roster.

A *skipped* refresh is not a failed one and must never park anything: skips are
the sanctioned 0.67% of sweeps that find no room in the interval.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from tap.buffer import Buffer
from tap.config import Config, DeviceSpec
from tap.device import DeviceState, Family, OutletReading, Sweep
from tap.errors import MeasuredNothingError, TransientError
from tap.health import Health
from tap.poller import DevicePoller, measured_nothing
from tests.tap.fakes import FakeDevice

INTERVAL = 0.05
BUDGET = 0.5


@pytest.fixture
async def buf(tmp_path):
    b = Buffer(tmp_path / "buffer", retention_days=30)
    await b.open()
    yield b
    await b.close()


def _poller(device, buf, health, **kw) -> DevicePoller:
    spec = DeviceSpec(host=device.host, family=Family.SMART, pinned=True)
    p = DevicePoller(
        spec,
        Config(),
        buf,
        health,
        interval=kw.pop("interval", INTERVAL),
        sweep_budget=kw.pop("sweep_budget", BUDGET),
        connect_budget=kw.pop("connect_budget", BUDGET),
    )
    p.factory = lambda _s, _c: device
    return p


def _sweep(*, has_emeter: bool = True, powers: list[int | None]) -> Sweep:
    from datetime import UTC, datetime

    return Sweep(
        device_id="FAKE0001",
        ts=datetime.now(UTC),
        has_emeter=has_emeter,
        outlets=[
            OutletReading(child_id=f"c{i}", alias=f"outlet {i}", relay_on=True, power_mw=p)
            for i, p in enumerate(powers)
        ],
    )


class TestMeasuredNothing:
    """The predicate, on its own. Pure, so it needs no poller and no loop."""

    def test_every_metered_outlet_null_measured_nothing(self) -> None:
        assert measured_nothing(_sweep(powers=[None, None, None])) is True

    def test_one_outlet_still_measuring_is_a_real_sweep(self) -> None:
        """One dead meter among five is the case the swallow exists for."""
        assert measured_nothing(_sweep(powers=[None, None, 42_000, None, None])) is False

    def test_a_measured_zero_is_a_measurement(self) -> None:
        """A relay-off outlet reports 0 W, not null. Confirmed in production: a
        recovered strip reads `watts=0 relay=off`. So an all-off strip must not
        look unreachable."""
        assert measured_nothing(_sweep(powers=[0, 0, 0])) is False

    def test_a_meterless_device_never_measured_anything_to_begin_with(self) -> None:
        """An EP10 issues no emeter call, so all-null is its healthy state. Rule
        B is what covers these."""
        assert measured_nothing(_sweep(has_emeter=False, powers=[None, None])) is False

    def test_a_sweep_with_no_outlets_says_nothing_either_way(self) -> None:
        assert measured_nothing(_sweep(powers=[])) is False


class TestRuleAAMeteredStripWhoseReadsAllFail:
    async def test_it_is_parked(self, buf) -> None:
        health = Health()
        d = FakeDevice(host="10.0.0.1", meters_fail=True)
        p = _poller(d, buf, health)

        for _ in range(3):
            await p._tick()

        entry = health.device("FAKE0001", host="10.0.0.1")
        assert entry.state is DeviceState.OFFLINE
        assert "MeasuredNothing" in entry.last_error, entry.last_error
        assert entry.failures_by_kind.get("MeasuredNothingError") == 3

    async def test_a_hollow_sweep_reaches_neither_the_buffer_nor_the_live_frame(self, buf) -> None:
        """The assertion that fails against the old code.

        A hollow sweep used to be submitted to the buffer and shipped in the next
        live frame, carrying a frozen relay as if it were current. Both are the
        bug, so both are asserted.
        """
        from tap.uplink import Uplink

        health = Health()
        d = FakeDevice(host="10.0.0.1", meters_fail=True)
        p = _poller(d, buf, health)
        submitted: list[Sweep] = []
        real_submit = buf.submit
        buf.submit = lambda sweep: (submitted.append(sweep), real_submit(sweep))[1]  # type: ignore[method-assign]

        await p._tick()

        assert d.sweeps == 1, "the sweep itself succeeded — that is the whole problem"
        assert submitted == [], "a sweep that measured nothing is not data"
        rows = Uplink._live_rows(_HealthOnly(health))
        assert rows == [], "a device we could not read must not appear in a live frame"

    async def test_a_single_bad_sweep_does_not_park_it(self, buf) -> None:
        """The existing offline threshold governs: one blip is not an outage."""
        health = Health()
        d = FakeDevice(host="10.0.0.1", meters_fail=True)
        p = _poller(d, buf, health)

        await p._tick()
        d.meters_fail = False
        await p._tick()

        entry = health.device("FAKE0001", host="10.0.0.1")
        assert entry.state is DeviceState.ONLINE
        assert entry.sweeps_ok == 1

    async def test_a_parked_strip_costs_exactly_two_log_lines(self, buf, caplog) -> None:
        """The README's discipline, through the new path: one line entering the
        outage, one leaving. At 1 Hz across a dozen strips a per-tick line is a
        million lines a day."""
        health = Health()
        d = FakeDevice(host="10.0.0.1", meters_fail=True)
        p = _poller(d, buf, health)

        with caplog.at_level(logging.INFO, logger="tap.poller"):
            for _ in range(5):
                await p._tick()
            d.meters_fail = False
            await p._tick()

        lines = [r.getMessage() for r in caplog.records if r.levelno >= logging.INFO]
        assert len(lines) == 2, lines
        assert "offline" in lines[0]
        assert "measured nothing" in lines[0], "the line must say which kind of outage"
        assert "back online" in lines[1]

    async def test_recovery_closes_the_gap_and_resets_the_backoff(self, buf) -> None:
        health = Health()
        d = FakeDevice(host="10.0.0.1", meters_fail=True)
        p = _poller(d, buf, health)

        for _ in range(3):
            await p._tick()
        assert p._open_gap is not None, "a parked device marks the hole in the data"

        d.meters_fail = False
        await p._tick()

        assert p._open_gap is None
        assert p._offline_probes == 0
        assert health.device("FAKE0001", host="10.0.0.1").state is DeviceState.ONLINE


class TestRuleBASustainedRosterFailure:
    async def test_a_meterless_device_is_parked_by_the_roster_streak_alone(
        self, buf, monkeypatch
    ) -> None:
        """The EP10 case. Rule A cannot see it, so rule B has to."""
        import tap.poller as poller_mod

        monkeypatch.setattr(poller_mod, "ROSTER_FAILURE_THRESHOLD", 3)
        health = Health()
        d = FakeDevice(host="10.0.0.1", meters_fail=True)
        d.has_emeter = False  # no emeter call at all, so no rule-A signal
        p = _poller(d, buf, health, interval=0.5)  # room to spare for a refresh

        await p._tick()  # one good sweep, so the roster exists to go stale
        d.roster_fail = TransientError("connection refused")
        # Exactly to the threshold. `run()` only refreshes while the device is
        # ONLINE with a live connection, so a further call is something the real
        # loop would never make.
        for _ in range(3):
            await p._refresh_roster()

        entry = health.device("FAKE0001", host="10.0.0.1")
        assert entry.state is DeviceState.OFFLINE
        assert entry.roster_failure_streak >= 3
        assert "refused" in entry.last_roster_error
        assert entry.failures_by_kind.get("MeasuredNothingError") is None, (
            "rule A must not be what parked a meterless device"
        )

    async def test_one_transient_refresh_failure_does_not_park(self, buf) -> None:
        """A strip evicts sessions once about six are open, so a refresh can fail
        on a perfectly healthy device. #98's trade stays intact."""
        health = Health()
        d = FakeDevice(host="10.0.0.1")
        p = _poller(d, buf, health, interval=0.5)

        await p._tick()
        d.roster_fail = TransientError("evicted")
        await p._refresh_roster()
        d.roster_fail = None
        await p._refresh_roster()

        entry = health.device("FAKE0001", host="10.0.0.1")
        assert entry.state is DeviceState.ONLINE
        assert entry.roster_failure_streak == 0, "a success ends the streak"
        assert entry.roster_failures == 1, "but the cumulative count remembers"

    async def test_a_skipped_refresh_never_parks_anything(self, buf) -> None:
        """Skips are the sanctioned 0.67%: a refresh we chose not to attempt
        because the sweep used the interval. Parking on those would take the
        floor down on a busy strip."""
        health = Health()
        d = FakeDevice(host="10.0.0.1", sweep_ms=45.0)  # eats the interval
        p = _poller(d, buf, health, interval=0.05)

        p.start()
        await asyncio.sleep(0.6)
        await p.stop()

        entry = health.device("FAKE0001", host="10.0.0.1")
        assert entry.roster_skips > 0, "the fixture must actually be skipping"
        assert entry.roster_failure_streak == 0, "a skip is not a failure"
        assert entry.state is DeviceState.ONLINE

    async def test_a_sweep_that_fetched_its_own_roster_ends_the_streak(self, buf) -> None:
        """A reconnect gets a fresh roster inside the sweep, so a streak from
        before it must not park a device whose data is current."""
        health = Health()
        d = FakeDevice(host="10.0.0.1")
        p = _poller(d, buf, health, interval=0.5)
        health.device("FAKE0001", host="10.0.0.1").roster_failure_streak = 999

        await p._tick()

        assert health.device("FAKE0001", host="10.0.0.1").roster_failure_streak == 0


class _HealthOnly:
    """Just enough of an `Uplink` for `_live_rows`, which reads only `_health`.

    Calling the real method rather than reimplementing its rule is the point:
    the claim under test is that *the shipped frame* omits the device.
    """

    def __init__(self, health: Health) -> None:
        self._health = health


def test_measured_nothing_is_a_transient_error() -> None:
    """It has to be retryable: the strip comes back when someone power-cycles it."""
    assert issubclass(MeasuredNothingError, TransientError)


class TestCommandFailuresAreAttributedToTheirDevice:
    """The uplink counts commands process-wide, which cannot answer the question
    an operator actually has: *which* strip will not switch.

    Deliberately not an input to the offline decision — see `DeviceHealth`.
    """

    async def test_a_refused_command_is_counted_against_the_device(self, buf) -> None:
        health = Health()
        d = FakeDevice(host="10.0.0.1")
        p = _poller(d, buf, health)
        await p._tick()  # connect, so set_relay reaches the device
        d.fail_with = TransientError("connection refused")

        with pytest.raises(BaseException):  # noqa: B017, PT011 — retry re-raises the last
            await p.set_relay("FAKE000100", True)

        entry = health.device("FAKE0001", host="10.0.0.1")
        assert entry.commands_failed == 1
        assert "refused" in entry.last_command_error
        assert entry.state is DeviceState.ONLINE, (
            "one refused command is not an outage: the sweep decides that, at 1 Hz"
        )

    async def test_a_command_to_a_parked_device_is_counted_not_swallowed(self, buf) -> None:
        health = Health()
        d = FakeDevice(host="10.0.0.1", meters_fail=True)
        p = _poller(d, buf, health)
        for _ in range(3):
            await p._tick()  # park it, which drops the connection

        with pytest.raises(ConnectionError):
            await p.set_relay("FAKE000100", True)

        assert health.device("FAKE0001", host="10.0.0.1").commands_failed == 1

    async def test_a_cancelled_command_is_not_a_failure(self, buf) -> None:
        """Shutdown cancels in-flight commands; that is not a strip refusing."""
        health = Health()
        d = FakeDevice(host="10.0.0.1")
        p = _poller(d, buf, health)
        await p._tick()
        d.fail_with = asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            await p.set_relay("FAKE000100", True)

        entry = health.device("FAKE0001", host="10.0.0.1")
        assert (entry.commands_ok, entry.commands_failed) == (0, 0)
        assert entry.last_command_error == ""

    async def test_a_successful_command_is_counted_too(self, buf) -> None:
        """Without this the failure count has no denominator."""
        health = Health()
        d = FakeDevice(host="10.0.0.1")
        p = _poller(d, buf, health)
        await p._tick()

        await p.set_relay("FAKE000100", False)

        entry = health.device("FAKE0001", host="10.0.0.1")
        assert (entry.commands_ok, entry.commands_failed) == (1, 0)
