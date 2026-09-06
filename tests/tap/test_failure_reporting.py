"""What tap can tell you about a failed sweep.

Eight hours against a real P316M produced 132 failed sweeps and this much
evidence: `sweeps_failed: 132` and `last_error: "TimeoutError: "`. 131 of them
were logged at DEBUG (below the offline threshold) and so never appeared at the
default level; the 132nd crossed the threshold and got a line. `last_error`
carries no phase, no timestamp and no history, and `asyncio.timeout` raises a
`TimeoutError` whose message is empty.

Worse, `record_sweep` fed the latency deque only on success, so every slow sweep
was censored out of the p50/p95 the status page reports — the tail was invisible
in exactly the percentile you would use to size the budget.

These tests pin down enough to answer "why did it fail" from the status page and
the default log level alone.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from tap.buffer import Buffer
from tap.config import Config, DeviceSpec
from tap.device import DeviceState, Family
from tap.errors import TransientError
from tap.health import DeviceHealth, Health
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
    from tap.poller import DevicePoller

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


class TestFailureTaxonomy:
    def test_failures_are_counted_by_kind_not_just_totalled(self):
        h = DeviceHealth(device_id="D", host="10.0.0.1")
        h.record_failure(TimeoutError(), phase="sweep:emeter[3/6]", duration_ms=800.0)
        h.record_failure(TimeoutError(), phase="sweep:emeter[1/6]", duration_ms=801.0)
        h.record_failure(TransientError("child list came back empty"), phase="sweep")

        snap = h.snapshot()
        assert snap["sweeps_failed"] == 3
        assert snap["failures_by_kind"] == {"TimeoutError": 2, "TransientError": 1}

    def test_a_messageless_timeout_still_says_what_it_was_doing(self):
        """`TimeoutError: ` was the whole report before this."""
        h = DeviceHealth(device_id="D", host="10.0.0.1")
        h.record_failure(TimeoutError(), phase="sweep:get_child_device_list", duration_ms=800.0)

        snap = h.snapshot()
        assert "get_child_device_list" in snap["last_error"]
        assert snap["last_error"].rstrip() != "TimeoutError:"
        assert snap["last_error_phase"] == "sweep:get_child_device_list"
        assert snap["last_error_at"] is not None

    def test_an_exception_with_a_message_keeps_it(self):
        h = DeviceHealth(device_id="D", host="10.0.0.1")
        h.record_failure(TransientError("child list came back empty"), phase="sweep")
        assert "child list came back empty" in h.snapshot()["last_error"]


class TestLatencyIsNotSurvivorshipBiased:
    def test_a_failed_attempt_is_reported_separately_not_dropped(self):
        """Successes and failures are both timed, and kept apart.

        Mixing them would hide a fleet that fails fast; dropping the failures —
        the old behaviour — hides the tail that causes the failures.
        """
        h = DeviceHealth(device_id="D", host="10.0.0.1")
        for ms in (100.0, 110.0, 120.0):
            h.record_sweep(ms)
        for ms in (800.0, 800.0):
            h.record_failure(TimeoutError(), phase="sweep", duration_ms=ms)

        snap = h.snapshot()
        assert snap["sweep_p50_ms"] == 110.0
        assert snap["sweep_fail_p50_ms"] == 800.0

    def test_fail_percentiles_are_null_with_no_failures(self):
        h = DeviceHealth(device_id="D", host="10.0.0.1")
        h.record_sweep(100.0)
        assert h.snapshot()["sweep_fail_p50_ms"] is None


class TestPhaseAttribution:
    async def test_a_timeout_names_the_call_that_was_in_flight(self, buf):
        """A budget cancels from outside, so the device records where it was."""
        health = Health()
        device = FakeDevice(device_id="SLOW", host="10.0.0.9", hang=True)
        device.phase = "get_child_device_list"
        poller = _poller(device, buf, health)

        await poller._tick()

        snap = health.device("SLOW", host="10.0.0.9").snapshot()
        assert snap["last_error_phase"] == "sweep:get_child_device_list"
        assert snap["failures_by_kind"] == {"TimeoutError": 1}

    async def test_a_connect_timeout_is_attributed_to_connect_not_the_sweep(self, buf):
        health = Health()
        device = FakeDevice(device_id="SLOW", host="10.0.0.9")

        async def never() -> None:
            await asyncio.Event().wait()

        device.open = never
        poller = _poller(device, buf, health, connect_budget=0.02)

        await poller._tick()

        entry = health.device("host:10.0.0.9", host="10.0.0.9")
        assert entry.snapshot()["last_error_phase"] == "connect"


class TestFailuresAreVisibleAtTheDefaultLogLevel:
    async def test_an_isolated_failure_is_reported_when_the_device_recovers(self, buf, caplog):
        """131 of 132 real failures were DEBUG-only, so nobody saw them.

        Recovery is the moment we know a failure was isolated rather than the
        start of an outage, so it is where the line belongs.
        """
        health = Health()
        device = FakeDevice(device_id="D1", host="10.0.0.5", fail_with=TransientError("boom"))
        poller = _poller(device, buf, health)

        with caplog.at_level(logging.WARNING, logger="tap.poller"):
            await poller._tick()
            device.fail_with = None
            await poller._tick()

        assert any(
            "10.0.0.5" in r.getMessage() and "boom" in r.getMessage() for r in caplog.records
        ), f"nothing logged at WARNING; got {[r.getMessage() for r in caplog.records]}"

    async def test_the_recovery_line_says_what_failed_and_how_often(self, buf, caplog):
        health = Health()
        device = FakeDevice(device_id="D1", host="10.0.0.5", fail_with=TransientError("boom"))
        poller = _poller(device, buf, health)

        with caplog.at_level(logging.WARNING, logger="tap.poller"):
            await poller._tick()
            await poller._tick()
            device.fail_with = None
            await poller._tick()

        (line,) = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert "2 failed sweep" in line
        assert "boom" in line

    async def test_a_storm_of_failures_does_not_become_a_line_per_second(self, buf, caplog):
        """The reason it was DEBUG. Rate-limit it rather than hiding it."""
        health = Health()
        device = FakeDevice(device_id="D1", host="10.0.0.5", fail_with=TransientError("boom"))
        poller = _poller(device, buf, health)

        with caplog.at_level(logging.WARNING, logger="tap.poller"):
            for _ in range(60):
                await poller._tick()
                device.fail_with = None
                await poller._tick()
                device.fail_with = TransientError("boom")

        lines = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(lines) == 1, f"{len(lines)} warnings for 60 failure/recovery cycles"
        assert health.device("D1", host="10.0.0.5").sweeps_failed == 60

    async def test_a_device_going_offline_still_costs_exactly_two_lines(self, buf, caplog):
        """The README's discipline: one line entering an outage, one leaving.

        The recovery report must not add a third by firing on the way down.
        """
        health = Health()
        device = FakeDevice(device_id="D1", host="10.0.0.5", fail_with=TransientError("boom"))
        poller = _poller(device, buf, health)

        with caplog.at_level(logging.INFO, logger="tap.poller"):
            for _ in range(5):
                await poller._tick()
            device.fail_with = None
            await poller._tick()

        lines = [r.getMessage() for r in caplog.records if r.levelno >= logging.INFO]
        assert len(lines) == 2, lines
        assert "offline" in lines[0]
        assert "back online" in lines[1]

    async def test_the_offline_transition_still_gets_its_own_line(self, buf, caplog):
        health = Health()
        device = FakeDevice(device_id="D1", host="10.0.0.5", fail_with=TransientError("boom"))
        poller = _poller(device, buf, health)

        with caplog.at_level(logging.WARNING, logger="tap.poller"):
            for _ in range(3):
                await poller._tick()

        assert any("offline" in r.message for r in caplog.records)
        assert health.device("D1", host="10.0.0.5").state is DeviceState.OFFLINE


class TestFailuresSurviveLearningTheDeviceId:
    """A poller is keyed on `host:<addr>` until it connects and learns the id.

    Re-keying used to drop the placeholder entry outright, which threw away
    every failure recorded before the connect succeeded — and those are exactly
    the connect failures, the only ones whose phase says `connect`. The
    recovery line then reported an empty `last_error`, reproducing the very
    `"TimeoutError: "` bug this work exists to fix.
    """

    async def test_the_recovery_line_is_not_empty_after_a_connect_failure(self, buf, caplog):
        health = Health()
        device = FakeDevice(device_id="D1", host="10.0.0.5")
        device.open_fail = TransientError("connection refused")
        poller = _poller(device, buf, health)

        with caplog.at_level(logging.WARNING, logger="tap.poller"):
            await poller._tick()
            device.open_fail = None
            await poller._tick()

        (line,) = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert "connection refused" in line, f"empty recovery line: {line!r}"
        assert not line.rstrip().endswith("last:")

    async def test_failures_recorded_before_the_id_was_known_are_kept(self, buf):
        health = Health()
        device = FakeDevice(device_id="D1", host="10.0.0.5")
        device.open_fail = TransientError("connection refused")
        poller = _poller(device, buf, health)

        await poller._tick()
        device.open_fail = None
        await poller._tick()

        assert "host:10.0.0.5" not in health.devices
        entry = health.devices["D1"]
        assert entry.sweeps_failed == 1
        assert entry.failures_by_kind == {"TransientError": 1}
        assert entry.device_id == "D1"

    async def test_an_established_entry_is_not_clobbered_by_a_placeholder(self):
        """Renaming onto an existing key keeps the established entry."""
        health = Health()
        real = health.device("D1", host="10.0.0.5")
        real.record_sweep(120.0)
        health.device("host:10.0.0.5", host="10.0.0.5").record_failure(TimeoutError())

        health.rename_device("host:10.0.0.5", "D1")

        assert "host:10.0.0.5" not in health.devices
        assert health.devices["D1"] is real
        assert health.devices["D1"].sweeps_ok == 1


class TestFailPercentilesAreScopedToTheSweepBudget:
    """`CONNECT_BUDGET` is 15 s and `SWEEP_BUDGET` is 0.8 s.

    Feeding both into one deque means an offline device re-probing every 60 s
    fills all 300 samples with 15 s connect timeouts inside five hours, and the
    800 ms sweep timeouts these percentiles exist to expose are buried under a
    p95 of 15000 — just as useless as the survivorship bias they replaced.
    """

    def test_a_connect_failure_stays_out_of_the_sweep_percentiles(self):
        h = DeviceHealth(device_id="D", host="10.0.0.1")
        h.record_failure(TimeoutError(), phase="connect", duration_ms=15_000.0)
        h.record_failure(TimeoutError(), phase="sweep:emeter[3/6]", duration_ms=800.0)

        snap = h.snapshot()
        assert snap["sweep_fail_p50_ms"] == 800.0
        assert snap["sweep_fail_p95_ms"] == 800.0
        # Still counted and still reported, just not in the sweep percentiles.
        assert snap["sweeps_failed"] == 2
        assert snap["failures_by_kind"] == {"TimeoutError": 2}

    def test_a_connect_failure_alone_leaves_the_percentiles_null(self):
        h = DeviceHealth(device_id="D", host="10.0.0.1")
        h.record_failure(TimeoutError(), phase="connect", duration_ms=15_000.0)
        assert h.snapshot()["sweep_fail_p50_ms"] is None

    async def test_a_sweep_failure_is_timed_from_after_the_connect(self, buf):
        """Otherwise a slow connect inflates the sweep's elapsed time."""
        health = Health()
        device = FakeDevice(device_id="D1", host="10.0.0.5", hang=True)

        slow_open = 0.05

        async def open_slowly() -> None:
            await asyncio.sleep(slow_open)

        device.open = open_slowly
        poller = _poller(device, buf, health, connect_budget=1.0, sweep_budget=0.02)

        await poller._tick()

        # open() succeeded, so the entry has already re-keyed to the device id.
        entry = health.devices["D1"]
        (sample,) = list(entry._fail_latency)
        # The sweep budget, not the sweep budget plus 50ms of connecting.
        assert sample < slow_open * 1000, f"{sample}ms includes the connect"

    async def test_an_auth_failure_carries_the_same_detail(self, buf):
        health = Health()
        device = FakeDevice(device_id="D1", host="10.0.0.5", auth_error=True)
        poller = _poller(device, buf, health)

        await poller._tick()

        snap = health.devices["D1"].snapshot()
        assert snap["failures_by_kind"] == {"DeviceAuthError": 1}
        assert snap["last_error_phase"].startswith("sweep")
        assert snap["last_error_at"] is not None


class TestRateLimitedDoesNotSwallowTheFirstLine:
    """`time.monotonic()` counts from boot, so a `_last` of 0.0 read as
    "emitted at boot" and suppressed everything for the first interval of a
    machine's uptime. tap restarts with its host, so that window is exactly
    when the first failures happen — and it was silent. This surfaced as a CI
    flake: it depends on the runner's uptime when the suite runs."""

    def _capture(self, monotonic_at):
        import time as time_mod

        import tap.logmod as logmod

        seen: list[str] = []

        class Sink(logging.Handler):
            def emit(self, record):
                seen.append(record.getMessage())

        log = logging.getLogger(f"ratelimited-probe-{monotonic_at}")
        log.setLevel(logging.WARNING)
        log.propagate = False
        log.addHandler(Sink())
        real = time_mod.monotonic
        logmod.time.monotonic = lambda: monotonic_at
        try:
            rl = logmod.RateLimited(log, interval=60.0)
            rl.warning("first failure")
        finally:
            logmod.time.monotonic = real
        return seen

    def test_the_first_line_survives_on_a_freshly_booted_host(self):
        assert self._capture(5.0) == ["first failure"], "swallowed 5s into boot"

    def test_the_first_line_survives_on_a_long_running_host(self):
        assert self._capture(200_000.0) == ["first failure"]

    def test_a_second_line_inside_the_interval_is_still_suppressed(self):
        import tap.logmod as logmod

        seen: list[str] = []

        class Sink(logging.Handler):
            def emit(self, record):
                seen.append(record.getMessage())

        log = logging.getLogger("ratelimited-probe-suppress")
        log.setLevel(logging.WARNING)
        log.propagate = False
        log.addHandler(Sink())
        rl = logmod.RateLimited(log, interval=60.0)
        rl.warning("one")
        rl.warning("two")
        assert seen == ["one"]
