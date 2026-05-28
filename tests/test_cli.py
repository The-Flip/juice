"""Tests for juice.cli — the doctor diagnostic command."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

from click.testing import CliRunner

from juice import cli as cli_module
from juice.cli import cli
from juice.store import Store


class _FakeDevice:
    def __init__(self, device_id, alias, model, *, children=None, offline=False):
        self.device_id = device_id
        self.alias = alias
        self.model = model
        self._children = children or []
        self._offline = offline

    async def child_states(self):
        if self._offline:
            raise RuntimeError("Passthrough failed: Device is offline")
        return self._children


def _seed_db(path: str) -> None:
    with Store(path) as s:
        ts = datetime(2026, 5, 27, 1, 15, 0, tzinfo=UTC)
        # Offline device that holds an assigned machine.
        dead = s.ensure_plug("ep10-dead", "", "Blackout - M0013", has_emeter=False)
        s.ensure_machine("M0013", "Blackout")
        s.update_assignment(dead, s._machine_cache["M0013"][0], ts)
        # Assignment whose outlet is no longer discovered (stale).
        gone = s.ensure_plug("gone-dev", "", "Star Trip - M0009", has_emeter=False)
        s.ensure_machine("M0009", "Star Trip")
        s.update_assignment(gone, s._machine_cache["M0009"][0], ts)


def test_doctor_reports_offline_relabel_and_stale(tmp_path, monkeypatch) -> None:
    db = str(tmp_path / "doctor.duckdb")
    _seed_db(db)

    devices = [
        _FakeDevice(
            "hs300",
            "Main Strip",
            "HS300(US)",
            children=[
                {"id": "c01", "alias": "Tempest - M0035", "state": 1},
                {"id": "c02", "alias": "New Outlet", "state": 1},  # powered, untagged
                {"id": "c03", "alias": "Plug 4 (Unused)", "state": 0},  # idle, untagged
            ],
        ),
        _FakeDevice("ep10-dead", "Blackout EP10", "EP10(US)", offline=True),
    ]

    @asynccontextmanager
    async def _fake_connect(_user, _password):
        account = MagicMock()
        account.devices = AsyncMock(return_value=devices)
        yield account

    monkeypatch.setattr(cli_module, "connect", _fake_connect)

    result = CliRunner().invoke(cli, ["-u", "x", "-p", "y", "doctor", "--db", db])
    assert result.exit_code == 0, result.output
    out = result.output

    # Offline device is flagged with the machine it affects.
    assert "[OFFLINE]" in out
    assert "affects: Blackout (M0013)" in out

    # The powered, untagged outlet is a relabel candidate; the idle one is not.
    assert "New Outlet" in out
    assert "Relabel candidates" in out
    relabel_section = out.split("Relabel candidates", 1)[1]
    assert "New Outlet" in relabel_section
    assert "Plug 4 (Unused)" not in relabel_section

    # The assignment whose outlet vanished surfaces as stale.
    stale_section = out.split("Stale assignments", 1)[1]
    assert "Star Trip (M0009)" in stale_section
