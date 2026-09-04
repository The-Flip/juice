"""Config reload, and the watchdog's judgement about what is fatal."""

from __future__ import annotations

from pathlib import Path

import pytest

from tap.config import load_config
from tap.device import DeviceState
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
