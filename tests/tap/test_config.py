"""Config loading, and above all the precedence order.

CLI > env > TOML > default. Everybody guesses that wrong at least once, so it
gets a test rather than a comment.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tap.config import Config, DeviceSpec, ExcludeRule, find_config_path, load_config
from tap.device import Family
from tap.errors import EXIT_CONFIG, FatalError

FULL = """
[tap]
id = "museum-1"
buffer_dir = "/var/lib/tap"
retention_days = 14
log_level = "DEBUG"

[web]
host = "0.0.0.0"
port = 9000

[uplink]
url = "wss://example.test/api/v2/ingest"
enabled = true

[discovery]
enabled = true
interval_seconds = 120
timeout_seconds = 3

[credentials]
username = "file-user"
password = "file-pass"

[[device]]
host = "192.168.4.38"
family = "smart"

[[device]]
host = "192.168.4.51"
family = "iot"
username = "per-device"
password = "per-device-pw"

[[exclude]]
host = "192.168.4.99"
reason = "the neighbour's plug"
"""


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "tap.toml"
    path.write_text(text)
    return path


class TestParsing:
    def test_reads_every_section(self, tmp_path):
        cfg = load_config(path=_write(tmp_path, FULL), environ={})
        assert cfg.tap_id == "museum-1"
        assert cfg.buffer_dir == Path("/var/lib/tap")
        assert cfg.retention_days == 14
        assert cfg.web.port == 9000
        assert cfg.uplink.url == "wss://example.test/api/v2/ingest"
        assert cfg.discovery.interval_seconds == 120
        assert cfg.credentials.username == "file-user"
        assert [d.host for d in cfg.devices] == ["192.168.4.38", "192.168.4.51"]
        assert cfg.devices[0].family is Family.SMART
        assert cfg.devices[1].family is Family.IOT
        assert all(d.pinned for d in cfg.devices)
        assert cfg.excludes[0].host == "192.168.4.99"

    def test_no_config_file_is_legitimate(self, tmp_path, monkeypatch):
        """Discovery plus credentials in the environment needs no TOML."""
        monkeypatch.chdir(tmp_path)
        cfg = load_config(environ={"KASA_USERNAME": "u", "KASA_PASSWORD": "p"})
        assert cfg.source_path is None
        assert cfg.credentials.username == "u"

    def test_per_device_credentials_override_the_global_pair(self, tmp_path):
        cfg = load_config(path=_write(tmp_path, FULL), environ={})
        assert cfg.credentials_for(cfg.devices[0]).username == "file-user"
        assert cfg.credentials_for(cfg.devices[1]).username == "per-device"


class TestPrecedence:
    def test_env_beats_file(self, tmp_path):
        cfg = load_config(
            path=_write(tmp_path, FULL),
            environ={"TAP_WEB_PORT": "9999", "TAP_ID": "from-env"},
        )
        assert cfg.web.port == 9999
        assert cfg.tap_id == "from-env"

    def test_cli_beats_env(self, tmp_path):
        cfg = load_config(
            path=_write(tmp_path, FULL),
            environ={"TAP_WEB_PORT": "9999"},
            overrides={"web_port": 7777},
        )
        assert cfg.web.port == 7777

    def test_unset_overrides_do_not_clobber(self, tmp_path):
        """A click option that was not given arrives as None and must be ignored."""
        cfg = load_config(
            path=_write(tmp_path, FULL),
            environ={},
            overrides={"web_port": None, "tap_id": None, "retention_days": None},
        )
        assert cfg.web.port == 9000
        assert cfg.tap_id == "museum-1"
        assert cfg.retention_days == 14

    def test_env_credentials_beat_the_file(self, tmp_path):
        """A secret belongs in the environment, so the environment wins."""
        cfg = load_config(
            path=_write(tmp_path, FULL),
            environ={"KASA_USERNAME": "env-user", "KASA_PASSWORD": "env-pass"},
        )
        assert cfg.credentials.username == "env-user"

    def test_no_uplink_flag_disables_a_configured_url(self, tmp_path):
        cfg = load_config(path=_write(tmp_path, FULL), environ={}, overrides={"no_uplink": True})
        assert cfg.uplink.url  # still configured...
        assert not cfg.uplink.active  # ...but switched off


class TestValidation:
    @pytest.mark.parametrize(
        "text",
        [
            "[tap]\nretention_days = 0\n",
            "[web]\nport = 70000\n",
            "[discovery]\ninterval_seconds = 0\n",
            "[tap]\nid = ''\n",
            '[uplink]\nurl = "ftp://nope"\n',
            "[[device]]\nfamily = 'smart'\n",  # no host
            "[[device]]\nhost = '1.2.3.4'\nfamily = 'nonsense'\n",
            "[[exclude]]\nreason = 'no selector'\n",
            "[credentials]\nusername = 'lonely'\n",  # password missing
            "[tap]\nretention_days = true\n",  # bool is not an int
        ],
    )
    def test_bad_config_is_a_clean_fatal_error(self, tmp_path, text):
        with pytest.raises(FatalError) as excinfo:
            load_config(path=_write(tmp_path, text), environ={})
        assert excinfo.value.code == EXIT_CONFIG

    def test_malformed_toml_is_reported_as_such(self, tmp_path):
        with pytest.raises(FatalError, match="not valid TOML"):
            load_config(path=_write(tmp_path, "[tap\nid = "), environ={})

    def test_polling_nothing_is_refused(self, tmp_path):
        """Discovery off and nothing pinned means tap would do nothing at all."""
        with pytest.raises(FatalError, match="poll nothing"):
            load_config(path=_write(tmp_path, "[discovery]\nenabled = false\n"), environ={})

    def test_missing_explicit_config_is_fatal(self, tmp_path):
        with pytest.raises(FatalError, match="not found"):
            find_config_path(tmp_path / "nope.toml", environ={})

    def test_env_pointing_at_a_missing_file_is_fatal(self, tmp_path):
        with pytest.raises(FatalError, match="missing file"):
            find_config_path(None, environ={"TAP_CONFIG": str(tmp_path / "nope.toml")})


class TestExclusions:
    def test_exclusion_matches_host_or_device_id(self):
        cfg = Config(
            excludes=(
                ExcludeRule(host="10.0.0.1"),
                ExcludeRule(device_id="ABC123"),
            )
        )
        assert cfg.is_excluded(host="10.0.0.1") is not None
        assert cfg.is_excluded(host="10.0.0.2", device_id="ABC123") is not None
        assert cfg.is_excluded(host="10.0.0.2", device_id="OTHER") is None

    def test_credentials_fall_back_to_the_global_pair(self):
        from tap.config import Credentials

        cfg = Config(credentials=Credentials("u", "p"))
        assert cfg.credentials_for(DeviceSpec(host="x")).username == "u"
