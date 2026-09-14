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


def test_air_discover_lists_sensors(monkeypatch) -> None:
    from juice.air_collector import AirReading, AirSensor

    @asynccontextmanager
    async def _fake_air_connect(_key, _secret):
        account = MagicMock()
        account.devices = AsyncMock(
            return_value=[
                (
                    AirSensor(mac="MAC1", name="Main Floor", online=True),
                    AirReading(
                        mac="MAC1",
                        ts=datetime(2026, 6, 20, 12, 0, 0, tzinfo=UTC),
                        temperature=22.5,
                        humidity=45.0,
                        co2=620.0,
                        pm25=8.0,
                    ),
                )
            ]
        )
        yield account

    import juice.air_collector as air_module

    monkeypatch.setattr(air_module, "connect", _fake_air_connect)

    result = CliRunner().invoke(
        cli,
        ["-u", "x", "-p", "y", "air-discover"],
        env={"QINGPING_APP_KEY": "k", "QINGPING_APP_SECRET": "s"},
    )
    assert result.exit_code == 0, result.output
    assert "Main Floor" in result.output
    assert "CO2 620ppm" in result.output


def test_air_discover_requires_credentials() -> None:
    # Explicit empty values isolate the test from any real creds in the runner's env.
    result = CliRunner().invoke(
        cli,
        ["-u", "x", "-p", "y", "air-discover"],
        env={"QINGPING_APP_KEY": "", "QINGPING_APP_SECRET": ""},
    )
    assert result.exit_code != 0
    assert "QINGPING_APP_KEY" in result.output


def test_commands_that_never_touch_the_cloud_run_without_kasa_credentials() -> None:
    """The group used to declare them `required=True`, so `tui` and
    `air-discover` refused to start on a machine with no TP-Link account."""
    result = CliRunner().invoke(
        cli, ["tui", "--help"], env={"KASA_USERNAME": "", "KASA_PASSWORD": ""}
    )
    assert result.exit_code == 0
    assert "--cookie" in result.output


def test_a_cloud_command_without_credentials_says_which_ones() -> None:
    result = CliRunner().invoke(cli, ["discover"], env={"KASA_USERNAME": "", "KASA_PASSWORD": ""})
    assert result.exit_code != 0
    assert "KASA_USERNAME" in result.output


def test_half_set_credentials_are_rejected_too() -> None:
    """A username with no password is still no credentials.

    Invoking a cloud command matters: `cli -u someone` alone exits 2 for
    "Missing command" before `_kasa_creds` ever runs, so asserting only on the
    exit code would pass without the check existing at all.
    """
    result = CliRunner().invoke(cli, ["-u", "someone", "discover"], env={"KASA_PASSWORD": ""})
    assert result.exit_code != 0
    assert "KASA_PASSWORD" in result.output


def test_serve_without_credentials_creates_no_database(tmp_path) -> None:
    """`required=True` rejected at parse time, before any side effect. The
    use-site check must not regress that into a stray migrated DB file."""
    db = tmp_path / "should-not-exist.duckdb"
    result = CliRunner().invoke(
        cli,
        ["serve", "--db", str(db), "--dev-auth"],
        env={"KASA_USERNAME": "", "KASA_PASSWORD": ""},
    )
    assert result.exit_code != 0
    assert "KASA_USERNAME" in result.output
    assert not db.exists()


def test_serve_refuses_tap_shadow_without_an_ingest_token(tmp_path) -> None:
    """Shadow mode receives tap's frames over /api/v2/ingest, and that route is
    only registered with a token. Without one it would be a server that looks
    like it is rehearsing a cutover and cannot receive anything -- fail closed,
    in the same style as a no-OAuth serve without --dev-auth."""
    db = tmp_path / "should-not-exist.duckdb"
    result = CliRunner().invoke(
        cli,
        ["serve", "--db", str(db), "--dev-auth", "--tap-shadow"],
        env={
            "KASA_USERNAME": "u",
            "KASA_PASSWORD": "p",
            "JUICE_INGEST_TOKEN": "",
            "JUICE_TAP_SHADOW": "",
        },
    )
    assert result.exit_code != 0
    assert "--tap-shadow needs --ingest-token" in result.output
    assert not db.exists(), "the refusal must come before the database is touched"


def test_serve_warns_when_the_ingest_token_is_set_without_shadow(tmp_path, caplog) -> None:
    """There is no tap-only mode yet, so a token without shadow mode means both
    collectors store readings over the same hours and every rollup double-counts.
    Nothing else would say so, so the CLI must."""
    import logging

    db = tmp_path / "x.duckdb"
    with caplog.at_level(logging.WARNING):
        result = CliRunner().invoke(
            cli,
            ["serve", "--db", str(db), "--dev-auth"],
            env={
                "KASA_USERNAME": "",  # so it stops right after the warning
                "KASA_PASSWORD": "",
                "JUICE_INGEST_TOKEN": "tok",
                "JUICE_TAP_SHADOW": "",
            },
        )
    assert result.exit_code != 0  # stopped by the missing Kasa creds, as intended
    assert any("double-count" in r.getMessage() for r in caplog.records), caplog.text


class TestServeCollectorTap:
    """`--collector tap` is the cutover switch. It must refuse the two
    configurations that would look healthy and do nothing, and it must not
    ask for a Kasa account it has no use for."""

    def test_it_refuses_without_an_ingest_token(self, tmp_path) -> None:
        db = tmp_path / "should-not-exist.duckdb"
        result = CliRunner().invoke(
            cli,
            ["serve", "--db", str(db), "--dev-auth", "--collector", "tap"],
            env={"KASA_USERNAME": "", "KASA_PASSWORD": "", "JUICE_INGEST_TOKEN": ""},
        )
        assert result.exit_code != 0
        assert "--collector tap needs --ingest-token" in result.output
        assert not db.exists()

    def test_it_refuses_shadow_mode_beside_it(self, tmp_path) -> None:
        db = tmp_path / "should-not-exist.duckdb"
        result = CliRunner().invoke(
            cli,
            ["serve", "--db", str(db), "--dev-auth", "--collector", "tap", "--tap-shadow"],
            env={"KASA_USERNAME": "", "KASA_PASSWORD": "", "JUICE_INGEST_TOKEN": "tok"},
        )
        assert result.exit_code != 0
        assert "Pick one" in result.output
        assert not db.exists()

    def test_it_starts_without_kasa_credentials(self, tmp_path, monkeypatch) -> None:
        """The whole point: a tap-driven juice has no cloud session. The seam is
        `_serve_tap`; what it is handed is what matters."""
        import juice.cli as cli_mod

        seen: dict = {}

        async def fake_serve_tap(db, server_kwargs, **kw):
            seen["db"] = db
            seen["server_kwargs"] = server_kwargs
            seen.update(kw)

        monkeypatch.setattr(cli_mod, "_serve_tap", fake_serve_tap)
        db = tmp_path / "x.duckdb"
        result = CliRunner().invoke(
            cli,
            ["serve", "--db", str(db), "--dev-auth", "--collector", "tap", "--port", "8123"],
            env={
                "KASA_USERNAME": "",
                "KASA_PASSWORD": "",
                "JUICE_INGEST_TOKEN": "tok",
                "FLIPFIX_API_URL": "https://flipfix.test",
                "FLIPFIX_API_KEY": "k",
                "JUICE_RAW_RETENTION_DAYS": "45",
            },
        )
        assert result.exit_code == 0, result.output
        assert seen["db"] == str(db)
        assert seen["server_kwargs"]["ingest_token"] == "tok"
        assert seen["server_kwargs"]["port"] == 8123
        assert seen["flipfix_url"] == "https://flipfix.test"
        assert seen["retention_days"] == 45

    def test_the_env_var_selects_it(self, tmp_path, monkeypatch) -> None:
        import juice.cli as cli_mod

        called = []

        async def fake_serve_tap(*_a, **_kw):
            called.append(True)

        monkeypatch.setattr(cli_mod, "_serve_tap", fake_serve_tap)
        result = CliRunner().invoke(
            cli,
            ["serve", "--db", str(tmp_path / "x.duckdb"), "--dev-auth"],
            env={"JUICE_COLLECTOR": "tap", "JUICE_INGEST_TOKEN": "tok", "KASA_USERNAME": ""},
        )
        assert result.exit_code == 0, result.output
        assert called

    def test_cloud_mode_still_needs_kasa(self, tmp_path) -> None:
        db = tmp_path / "should-not-exist.duckdb"
        result = CliRunner().invoke(
            cli,
            ["serve", "--db", str(db), "--dev-auth", "--collector", "cloud"],
            env={"KASA_USERNAME": "", "KASA_PASSWORD": ""},
        )
        assert result.exit_code != 0
        assert "KASA_USERNAME" in result.output
        assert not db.exists()


class TestIngestSkip:
    """The rollback tool: move juice's cursor up to tap's high-water mark so a
    return to tap mode does not replay hours the cloud recorder covered."""

    def _db(self, tmp_path):
        from juice.store import Store

        path = tmp_path / "x.duckdb"
        with Store(str(path)) as store:
            store.set_ingest_cursor("bumper", "buf-1", "000000000000060000")
        return str(path)

    def test_with_no_cursor_it_lists(self, tmp_path) -> None:
        result = CliRunner().invoke(cli, ["ingest-skip", "--db", self._db(tmp_path)])
        assert result.exit_code == 0, result.output
        assert "bumper  buffer buf-1  cursor 000000000000060000" in result.output

    def test_it_moves_the_cursor_forward(self, tmp_path) -> None:
        from juice.store import Store

        db = self._db(tmp_path)
        result = CliRunner().invoke(
            cli, ["ingest-skip", "--db", db, "--tap-id", "bumper", "--cursor", "000000000000090000"]
        )
        assert result.exit_code == 0, result.output
        assert "Moved tap bumper" in result.output
        with Store(db) as store:
            assert store.ingest_cursor("bumper", "buf-1") == "000000000000090000"

    def test_dry_run_moves_nothing(self, tmp_path) -> None:
        from juice.store import Store

        db = self._db(tmp_path)
        result = CliRunner().invoke(
            cli,
            [
                "ingest-skip",
                "--db",
                db,
                "--tap-id",
                "bumper",
                "--cursor",
                "000000000000090000",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0 and "Would move" in result.output
        with Store(db) as store:
            assert store.ingest_cursor("bumper", "buf-1") == "000000000000060000"

    def test_it_never_retreats(self, tmp_path) -> None:
        from juice.store import Store

        db = self._db(tmp_path)
        result = CliRunner().invoke(
            cli, ["ingest-skip", "--db", db, "--tap-id", "bumper", "--cursor", "000000000000010000"]
        )
        assert result.exit_code == 0 and "already at or past" in result.output
        with Store(db) as store:
            assert store.ingest_cursor("bumper", "buf-1") == "000000000000060000"

    def test_an_unknown_tap_and_a_malformed_cursor_are_refused(self, tmp_path) -> None:
        db = self._db(tmp_path)
        result = CliRunner().invoke(
            cli, ["ingest-skip", "--db", db, "--tap-id", "ghost", "--cursor", "000000000000090000"]
        )
        assert result.exit_code != 0 and "never connected" in result.output
        result = CliRunner().invoke(
            cli, ["ingest-skip", "--db", db, "--tap-id", "bumper", "--cursor", "90000"]
        )
        assert result.exit_code != 0 and "fixed-width" in result.output


class TestServeTapRuns:
    """`_serve_tap` is the production entrypoint for the cutover. Run it for
    real on an ephemeral port: the three seams into `create_app`, the gather,
    the shutdown order."""

    async def test_it_serves_a_floor_and_refuses_operations_with_no_tap(self, tmp_path) -> None:
        import asyncio
        import contextlib
        import socket

        import aiohttp

        from juice.cli import _serve_tap
        from juice.store import Store

        db = tmp_path / "x.duckdb"
        with Store(str(db)) as store:
            plug = store.ensure_plug("STRIP1", "STRIP100", "Blackout - M0013")
            machine = store.ensure_machine("M0013", "Blackout")
            store.update_assignment(plug, machine, datetime.now(UTC))
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]

        task = asyncio.create_task(
            _serve_tap(
                str(db),
                {
                    "host": "127.0.0.1",
                    "port": port,
                    "oauth_config": None,
                    "backup_token": None,
                    "dev_auth": True,
                    "ingest_token": "tok",
                },
                flipfix_url=None,
                flipfix_key=None,
                public_url=None,
                qingping=(None, None),
                retention_days=90,
            )
        )
        try:
            # An unsafe jar: aiohttp drops cookies for bare IP hosts otherwise,
            # and the dev login is a cookie.
            async with aiohttp.ClientSession(
                f"http://127.0.0.1:{port}", cookie_jar=aiohttp.CookieJar(unsafe=True)
            ) as http:
                for _ in range(100):
                    await asyncio.sleep(0.05)
                    with contextlib.suppress(aiohttp.ClientError):
                        if (await http.get("/api/v2/floor")).status == 200:
                            break
                await http.get("/login")
                floor = await (await http.get("/api/v2/floor")).json()
                resp = await http.post("/api/v2/operations", json={"kind": "all_on"})
                op = await resp.json()
                resp2 = await http.post("/api/v2/machines/M0013/power", json={"on": True})
                power = await resp2.json()
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

        assert floor["counts"]["total"] == 1
        assert [e["kind"] for e in floor["infrastructure"]] == ["collector_offline"]
        assert resp.status == 409 and op["error"]["code"] == "not_controllable"
        assert resp2.status == 409 and power["error"]["code"] == "not_controllable"

    def test_the_skip_option_is_parsed_and_passed(self, tmp_path, monkeypatch) -> None:
        import juice.cli as cli_mod

        seen: dict = {}

        async def fake_serve_tap(db, server_kwargs, **kw):
            seen.update(kw)

        monkeypatch.setattr(cli_mod, "_serve_tap", fake_serve_tap)
        result = CliRunner().invoke(
            cli,
            ["serve", "--db", str(tmp_path / "x.duckdb"), "--dev-auth", "--collector", "tap"],
            env={"JUICE_INGEST_TOKEN": "tok", "JUICE_INGEST_SKIP_TO": "bumper=000000000000090000"},
        )
        assert result.exit_code == 0, result.output
        assert seen["skip_to"] == {("bumper", None): "000000000000090000"}

        result = CliRunner().invoke(
            cli,
            ["serve", "--db", str(tmp_path / "x.duckdb"), "--dev-auth", "--collector", "tap"],
            env={
                "JUICE_INGEST_TOKEN": "tok",
                "JUICE_INGEST_SKIP_TO": "bumper:buf-2=000000000000090000",
            },
        )
        assert result.exit_code == 0, result.output
        assert seen["skip_to"] == {("bumper", "buf-2"): "000000000000090000"}

        result = CliRunner().invoke(
            cli,
            ["serve", "--db", str(tmp_path / "x.duckdb"), "--dev-auth", "--collector", "tap"],
            env={"JUICE_INGEST_TOKEN": "tok", "JUICE_INGEST_SKIP_TO": "bumper"},
        )
        assert result.exit_code != 0 and "=cursor" in result.output
