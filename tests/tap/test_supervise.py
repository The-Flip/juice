"""Config reload, and the watchdog's judgement about what is fatal."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from tap.config import load_config
from tap.device import DeviceState
from tap.errors import EXIT_INTERNAL, FatalError
from tap.health import Health
from tap.supervise import STRUCTURAL_KEYS, Supervisor

BASE_TOML = """
[tap]
id = "reload-test"
log_level = "INFO"
[web]
port = 8099
[uplink]
enabled = false
[discovery]
enabled = false
[[device]]
host = "10.0.0.1"
family = "smart"
"""


def _supervisor(tmp_path: Path, toml: str, **overrides) -> tuple[Supervisor, Path]:
    path = tmp_path / "tap.toml"
    path.write_text(toml)
    config = load_config(path=path, overrides=overrides, environ={})
    return Supervisor(config, config_path=path, overrides=overrides), path


class TestReload:
    def test_cli_overrides_survive_a_reload(self, tmp_path):
        """A SIGHUP must not silently demote every --flag back to the file.

        The buffer_dir case is the sharp one: losing it would leave the running
        config disagreeing with the buffer we are actually writing to, and would
        report a spurious "restart required" every time.
        """
        buffer_dir = str(tmp_path / "chosen")
        supervisor, path = _supervisor(tmp_path, BASE_TOML, buffer_dir=buffer_dir)
        assert str(supervisor._config.buffer_dir) == buffer_dir

        path.write_text(BASE_TOML + '\n[[exclude]]\nhost = "10.0.0.9"\n')
        supervisor._apply_reload()

        assert str(supervisor._config.buffer_dir) == buffer_dir
        assert supervisor._config.excludes[0].host == "10.0.0.9"

    def test_a_genuine_structural_change_is_reported_not_silently_ignored(self, tmp_path, caplog):
        import logging

        supervisor, path = _supervisor(tmp_path, BASE_TOML)
        path.write_text(
            BASE_TOML.replace('id = "reload-test"', 'id = "reload-test"\nretention_days = 7')
        )
        with caplog.at_level(logging.WARNING, logger="tap.supervise"):
            supervisor._apply_reload()
        messages = [r.getMessage() for r in caplog.records]
        assert any("retention_days changed on reload" in m for m in messages)
        assert any("restart required" in m for m in messages)

    def test_an_unchanged_file_reports_nothing_structural(self, tmp_path, caplog):
        import logging

        supervisor, _path = _supervisor(tmp_path, BASE_TOML, buffer_dir=str(tmp_path / "b"))
        with caplog.at_level(logging.WARNING, logger="tap.supervise"):
            supervisor._apply_reload()
        assert [
            r.getMessage() for r in caplog.records if "restart required" in r.getMessage()
        ] == []

    def test_a_reload_schedules_a_reconcile(self, tmp_path):
        """Otherwise a newly excluded device keeps being polled forever."""
        supervisor, path = _supervisor(tmp_path, BASE_TOML)
        assert "10.0.0.1" in supervisor.roster.specs
        path.write_text(BASE_TOML + '\n[[exclude]]\nhost = "10.0.0.1"\n')
        supervisor._apply_reload()
        assert supervisor._reconcile_after_reload is True
        assert supervisor.roster.specs == {}

    def test_a_broken_file_keeps_the_running_config(self, tmp_path, caplog):
        import logging

        supervisor, path = _supervisor(tmp_path, BASE_TOML)
        before = supervisor._config
        path.write_text("[tap\nbroken = ")
        with caplog.at_level(logging.WARNING, logger="tap.supervise"):
            supervisor._apply_reload()
        assert supervisor._config is before
        assert any("reload failed" in r.getMessage() for r in caplog.records)

    def test_structural_keys_are_real_config_fields(self, tmp_path):
        """Guards against a typo turning the warning into a permanent no-op."""
        supervisor, _ = _supervisor(tmp_path, BASE_TOML)
        for key in STRUCTURAL_KEYS:
            assert hasattr(supervisor._config, key), key


class TestHealthSummary:
    def test_any_device_online_ignores_parked_devices(self):
        health = Health()
        health.device("A", host="10.0.0.1").state = DeviceState.OFFLINE
        health.device("B", host="10.0.0.2").state = DeviceState.UNAUTHORIZED
        assert health.any_device_online() is False
        health.device("C", host="10.0.0.3").state = DeviceState.DEGRADED
        assert health.any_device_online() is True

    def test_last_successful_sweep_is_the_most_recent_across_devices(self):
        health = Health()
        a = health.device("A", host="10.0.0.1")
        b = health.device("B", host="10.0.0.2")
        assert health.last_successful_sweep() is None
        a.record_sweep(10.0)
        b.record_sweep(10.0)
        assert health.last_successful_sweep() == max(a.last_ok, b.last_ok)


class TestPercentiles:
    @pytest.mark.parametrize(
        ("values", "q", "expected"),
        [([], 0.5, None), ([1.0], 0.5, 1.0), ([1.0, 2.0, 3.0, 4.0], 0.5, 2.0)],
    )
    def test_nearest_rank(self, values, q, expected):
        from tap.health import _pct

        assert _pct(values, q) == expected


class TestWatchdog:
    """What is fatal, what is merely a warning, and what is neither.

    This is the code that decides when the process dies, so the boundary
    matters: too eager and a network blip crash-loops the museum's collector;
    too lax and a wedged daemon sits there looking healthy.
    """

    @staticmethod
    def _fast(monkeypatch, **overrides):
        import tap.supervise as mod

        defaults = {
            "WATCHDOG_INTERVAL": 0.01,
            "STARTUP_GRACE_SECONDS": 0.0,
            "NO_SWEEP_FATAL_SECONDS": 0.2,
            "STALE_BUFFER_FATAL_SECONDS": 0.2,
        }
        for name, value in {**defaults, **overrides}.items():
            monkeypatch.setattr(mod, name, value)

    @staticmethod
    def _with_a_device(supervisor, state=DeviceState.ONLINE):
        """Give the supervisor one poller, without starting anything."""

        class _StubPoller:
            def __init__(self):
                self.state = state

        supervisor.pollers._pollers["10.0.0.1"] = _StubPoller()

    async def test_no_successful_read_at_all_is_fatal(self, tmp_path, monkeypatch):
        self._fast(monkeypatch)
        supervisor, _ = _supervisor(tmp_path, BASE_TOML)
        self._with_a_device(supervisor)
        with pytest.raises(FatalError) as excinfo:
            await asyncio.wait_for(supervisor._watchdog(), timeout=5)
        assert "no device has been read" in str(excinfo.value)
        assert excinfo.value.code == EXIT_INTERNAL

    async def test_the_startup_grace_holds_it_off(self, tmp_path, monkeypatch):
        """A collector must be allowed time to reach its devices before dying."""
        self._fast(monkeypatch, STARTUP_GRACE_SECONDS=30.0)
        supervisor, _ = _supervisor(tmp_path, BASE_TOML)
        self._with_a_device(supervisor)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(supervisor._watchdog(), timeout=0.3)

    async def test_no_devices_means_nothing_to_be_fatal_about(self, tmp_path, monkeypatch):
        """An empty roster warns loudly, but restarting would not fix it."""
        self._fast(monkeypatch)
        supervisor, _ = _supervisor(tmp_path, BASE_TOML)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(supervisor._watchdog(), timeout=0.3)
        assert any("no devices" in w for w in supervisor.health.warnings)

    async def test_a_wedged_write_path_is_fatal(self, tmp_path, monkeypatch):
        """Devices reporting but nothing reaching disk is the silent killer."""
        self._fast(monkeypatch, NO_SWEEP_FATAL_SECONDS=3600.0)
        supervisor, _ = _supervisor(tmp_path, BASE_TOML)
        self._with_a_device(supervisor)
        entry = supervisor.health.device("DEV1", host="10.0.0.1")
        entry.state = DeviceState.ONLINE
        entry.record_sweep(10.0)
        supervisor.health.buffer.newest_ts = datetime.now(UTC) - timedelta(seconds=600)
        with pytest.raises(FatalError, match="write path is wedged"):
            await asyncio.wait_for(supervisor._watchdog(), timeout=5)

    async def test_a_healthy_collector_is_left_alone(self, tmp_path, monkeypatch):
        self._fast(monkeypatch, NO_SWEEP_FATAL_SECONDS=3600.0, STALE_BUFFER_FATAL_SECONDS=3600.0)
        supervisor, _ = _supervisor(tmp_path, BASE_TOML)
        self._with_a_device(supervisor)
        entry = supervisor.health.device("DEV1", host="10.0.0.1")
        entry.state = DeviceState.ONLINE
        entry.record_sweep(10.0)
        supervisor.health.buffer.newest_ts = datetime.now(UTC)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(supervisor._watchdog(), timeout=0.3)
        assert supervisor.health.warnings == []

    async def test_a_parked_credential_failure_warns_rather_than_kills(self, tmp_path, monkeypatch):
        """Restarting does not fix a wrong password; a human has to."""
        self._fast(monkeypatch, NO_SWEEP_FATAL_SECONDS=3600.0)
        supervisor, _ = _supervisor(tmp_path, BASE_TOML)
        self._with_a_device(supervisor)
        entry = supervisor.health.device("DEV1", host="10.0.0.1")
        entry.state = DeviceState.UNAUTHORIZED
        entry.record_sweep(10.0)
        entry.state = DeviceState.UNAUTHORIZED
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(supervisor._watchdog(), timeout=0.3)
        assert any("credentials rejected" in w for w in supervisor.health.warnings)

    async def test_dropped_rows_are_surfaced(self, tmp_path, monkeypatch):
        self._fast(monkeypatch, NO_SWEEP_FATAL_SECONDS=3600.0)
        supervisor, _ = _supervisor(tmp_path, BASE_TOML)
        self._with_a_device(supervisor)
        entry = supervisor.health.device("DEV1", host="10.0.0.1")
        entry.record_sweep(10.0)
        supervisor.health.buffer.rows_dropped = 17
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(supervisor._watchdog(), timeout=0.3)
        assert any("17 rows dropped" in w for w in supervisor.health.warnings)


class TestWatchdogWatchesTheUplink:
    """The watchdog checked everything but the one link that matters after
    cutover. A connected-yet-stuck or never-connecting uplink is a warning:
    a restart does not fix a server outage, so it is never fatal."""

    @staticmethod
    def _quiet(monkeypatch, **overrides):
        defaults = {
            "NO_SWEEP_FATAL_SECONDS": 3600.0,
            "STALE_BUFFER_FATAL_SECONDS": 3600.0,
            "UPLINK_DISCONNECTED_WARN_SECONDS": 0.05,
            "UPLINK_LAG_STUCK_TICKS": 3,
        }
        TestWatchdog._fast(monkeypatch, **{**defaults, **overrides})

    @staticmethod
    def _healthy(tmp_path):
        supervisor, _ = _supervisor(tmp_path, BASE_TOML)
        TestWatchdog._with_a_device(supervisor)
        entry = supervisor.health.device("DEV1", host="10.0.0.1")
        entry.state = DeviceState.ONLINE
        entry.record_sweep(10.0)
        supervisor.health.buffer.newest_ts = datetime.now(UTC)
        return supervisor

    async def test_a_configured_uplink_that_stays_disconnected_warns(self, tmp_path, monkeypatch):
        self._quiet(monkeypatch)
        supervisor = self._healthy(tmp_path)
        supervisor.health.uplink.enabled = True
        supervisor.health.uplink.connected = False
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(supervisor._watchdog(), timeout=0.3)
        assert any("uplink not connected" in w for w in supervisor.health.warnings)

    async def test_standalone_has_no_uplink_to_warn_about(self, tmp_path, monkeypatch):
        self._quiet(monkeypatch)
        supervisor = self._healthy(tmp_path)
        supervisor.health.uplink.enabled = False
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(supervisor._watchdog(), timeout=0.3)
        assert supervisor.health.warnings == []

    async def test_lag_that_stops_falling_warns(self, tmp_path, monkeypatch):
        self._quiet(monkeypatch)
        supervisor = self._healthy(tmp_path)
        supervisor.health.uplink.enabled = True
        supervisor.health.uplink.connected = True
        supervisor.health.uplink.lag_seconds = 900.0
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(supervisor._watchdog(), timeout=0.3)
        assert any("lag not falling" in w for w in supervisor.health.warnings)

    async def test_lag_that_is_falling_is_a_backfill_not_a_warning(self, tmp_path, monkeypatch):
        self._quiet(monkeypatch)
        supervisor = self._healthy(tmp_path)
        supervisor.health.uplink.enabled = True
        supervisor.health.uplink.connected = True
        supervisor.health.uplink.lag_seconds = 900.0

        async def drain():
            while True:
                await asyncio.sleep(0.005)
                supervisor.health.uplink.lag_seconds -= 1.0

        drainer = asyncio.create_task(drain())
        try:
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(supervisor._watchdog(), timeout=0.3)
        finally:
            drainer.cancel()
        assert supervisor.health.warnings == []

    async def test_a_second_backlog_after_a_reconnect_is_not_stuck(self, tmp_path, monkeypatch):
        """Backfill drains 500 -> 455, the socket drops, it reconnects with 605
        behind and drains again: judged against the earlier low point that
        warns for as long as the second drain takes to get under it."""
        self._quiet(monkeypatch)
        supervisor = self._healthy(tmp_path)
        up = supervisor.health.uplink
        up.enabled = True
        up.connected = True
        up.lag_seconds = 500.0
        seen: list[list[str]] = []

        async def script():
            for _ in range(10):
                await asyncio.sleep(0.005)
                up.lag_seconds -= 5.0
            up.connected = False
            await asyncio.sleep(0.05)
            up.connected = True
            up.lag_seconds = 605.0
            while True:
                await asyncio.sleep(0.005)
                up.lag_seconds -= 5.0
                seen.append(list(supervisor.health.warnings))

        driver = asyncio.create_task(script())
        try:
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(supervisor._watchdog(), timeout=0.4)
        finally:
            driver.cancel()
        assert not any(w for tick in seen for w in tick if "lag" in w), seen

    async def test_lag_is_not_judged_while_disconnected(self, tmp_path, monkeypatch):
        """The number is stale while the socket is down -- it is only measured
        inside a session -- so a big lag frozen across an outage is not "stuck"."""
        self._quiet(monkeypatch, UPLINK_DISCONNECTED_WARN_SECONDS=3600.0)
        supervisor = self._healthy(tmp_path)
        supervisor.health.uplink.enabled = True
        supervisor.health.uplink.connected = False
        supervisor.health.uplink.lag_seconds = 900.0
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(supervisor._watchdog(), timeout=0.3)
        assert supervisor.health.warnings == []

    async def test_small_steady_lag_is_normal(self, tmp_path, monkeypatch):
        """A healthy tap sits at a second or two of lag forever."""
        self._quiet(monkeypatch)
        supervisor = self._healthy(tmp_path)
        supervisor.health.uplink.enabled = True
        supervisor.health.uplink.connected = True
        supervisor.health.uplink.lag_seconds = 1.6
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(supervisor._watchdog(), timeout=0.3)
        assert supervisor.health.warnings == []


class TestGuard:
    async def test_a_crashing_structural_task_becomes_a_fatal(self, tmp_path):
        supervisor, _ = _supervisor(tmp_path, BASE_TOML)

        async def boom():
            raise RuntimeError("the writer died")

        with pytest.raises(FatalError) as excinfo:
            await supervisor._guard(boom(), "buffer")
        assert "buffer task crashed" in str(excinfo.value)
        assert excinfo.value.code == EXIT_INTERNAL

    async def test_a_fatal_passes_through_with_its_own_code(self, tmp_path):
        supervisor, _ = _supervisor(tmp_path, BASE_TOML)

        async def boom():
            raise FatalError("buffer unwritable", EXIT_INTERNAL)

        with pytest.raises(FatalError, match="buffer unwritable"):
            await supervisor._guard(boom(), "buffer")

    async def test_cancellation_is_not_turned_into_a_fatal(self, tmp_path):
        """Shutdown cancels these tasks; that must not read as a crash."""
        supervisor, _ = _supervisor(tmp_path, BASE_TOML)

        async def cancelled():
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await supervisor._guard(cancelled(), "uplink")


class TestExcludedDevicesDoNotCrashLoop:
    async def test_an_all_excluded_roster_warns_instead_of_dying(self, tmp_path, monkeypatch):
        """A device refused by config is not a device we are failing to read.

        Counting it would produce "no device has been read for 120s", exit 70,
        and a restart that reaches exactly the same state — a crash loop no
        restart can fix, over a config decision.
        """
        TestWatchdog._fast(monkeypatch)
        supervisor, _ = _supervisor(tmp_path, BASE_TOML)
        TestWatchdog._with_a_device(supervisor, state=DeviceState.EXCLUDED)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(supervisor._watchdog(), timeout=0.3)
        assert any("no devices being polled" in w for w in supervisor.health.warnings)
