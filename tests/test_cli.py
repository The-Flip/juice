"""Tests for juice.cli."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

from click.testing import CliRunner

from juice.cli import cli
from juice.store import Store


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
        ["air-discover"],
        env={"QINGPING_APP_KEY": "k", "QINGPING_APP_SECRET": "s"},
    )
    assert result.exit_code == 0, result.output
    assert "Main Floor" in result.output
    assert "CO2 620ppm" in result.output


def test_air_discover_requires_credentials() -> None:
    # Explicit empty values isolate the test from any real creds in the runner's env.
    result = CliRunner().invoke(
        cli,
        ["air-discover"],
        env={"QINGPING_APP_KEY": "", "QINGPING_APP_SECRET": ""},
    )
    assert result.exit_code != 0
    assert "QINGPING_APP_KEY" in result.output


class TestServe:
    """`serve` is tap-driven. It must refuse the one configuration that would
    look healthy and do nothing -- no ingest token, so no route for tap to
    reach -- and hand `_serve` exactly what it was given."""

    def test_it_refuses_without_an_ingest_token(self, tmp_path) -> None:
        db = tmp_path / "should-not-exist.duckdb"
        result = CliRunner().invoke(
            cli,
            ["serve", "--db", str(db), "--dev-auth"],
            env={"JUICE_INGEST_TOKEN": ""},
        )
        assert result.exit_code != 0
        assert "serve needs --ingest-token" in result.output
        assert not db.exists(), "the refusal must come before the database is touched"

    def test_it_hands_serve_what_it_was_given(self, tmp_path, monkeypatch) -> None:
        """The seam is `_serve`; what it is handed is what matters."""
        import juice.cli as cli_mod

        seen: dict = {}

        async def fake_serve(db, server_kwargs, **kw):
            seen["db"] = db
            seen["server_kwargs"] = server_kwargs
            seen.update(kw)

        monkeypatch.setattr(cli_mod, "_serve", fake_serve)
        db = tmp_path / "x.duckdb"
        result = CliRunner().invoke(
            cli,
            ["serve", "--db", str(db), "--dev-auth", "--port", "8123"],
            env={
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


class TestServeTapRuns:
    """`_serve` is the production entrypoint. Run it for
    real on an ephemeral port: the three seams into `create_app`, the gather,
    the shutdown order."""

    async def test_it_serves_a_floor_and_refuses_operations_with_no_tap(self, tmp_path) -> None:
        import asyncio
        import contextlib
        import socket

        import aiohttp

        from juice.cli import _serve

        db = tmp_path / "x.duckdb"
        with Store(str(db)) as store:
            plug = store.ensure_plug("STRIP1", "STRIP100", "Blackout - M0013")
            machine = store.ensure_machine("M0013", "Blackout")
            store.update_assignment(plug, machine, datetime.now(UTC))
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]

        task = asyncio.create_task(
            _serve(
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


class TestDoctor:
    """`juice doctor` reads the store and nothing else: what has gone quiet,
    what is drawing power under a label with no asset tag, and which machine
    the store thinks is on two outlets at once."""

    @staticmethod
    def _section(out: str, title: str) -> str:
        """The lines under one `=== title ... ===` heading."""
        body = out.split(f"=== {title}", 1)[1].split("===\n", 1)[1]
        return body.split("\n===", 1)[0]

    @staticmethod
    def _seed(db) -> None:
        now = datetime.now(UTC)
        with Store(str(db)) as store:
            live = store.ensure_plug("STRIP1", "STRIP100", "Blackout - M0013")
            dead = store.ensure_plug("DEADBEEF", "DEADBEEF00", "Star Trip - M0009")
            untagged = store.ensure_plug("STRIP1", "STRIP101", "Plug 2")
            never = store.ensure_plug("STRIP1", "STRIP102", "Plug 3")
            moved = store.ensure_plug("STRIP1", "STRIP103", "Star Trip - M0009")
            ep10 = store.ensure_plug("EP10A", "", "Snack Machine", has_emeter=False)
            meter_died = store.ensure_plug("STRIP1", "STRIP104", "Plug 5")
            blackout = store.ensure_machine("M0013", "Blackout")
            star_trip = store.ensure_machine("M0009", "Star Trip")
            store.update_assignment(live, blackout, now - timedelta(days=30))
            store.update_assignment(dead, star_trip, now - timedelta(days=30))
            store.update_assignment(moved, star_trip, now - timedelta(days=1))
            store.insert_readings(
                [
                    (now - timedelta(minutes=1), live, 120.0, 119.0, 1.0, 5.0),
                    (now - timedelta(days=20), dead, 110.0, 119.0, 0.9, 5.0),
                    (now - timedelta(minutes=1), untagged, 95.0, 119.0, 0.8, 1.0),
                    (now - timedelta(minutes=1), moved, 105.0, 119.0, 0.9, 1.0),
                    # A meterless outlet reports NULL watts; tap says whether it is on.
                    (now - timedelta(minutes=2), meter_died, 80.0, 119.0, 0.7, 1.0),
                ]
            )
            store._conn.execute(
                "INSERT INTO readings (ts, plug_id, watts, voltage, amps, total_kwh, relay_on) "
                "VALUES (?, ?, NULL, NULL, NULL, NULL, TRUE), (?, ?, NULL, NULL, NULL, NULL, NULL)",
                [now - timedelta(minutes=1), ep10, now - timedelta(minutes=1), meter_died],
            )
            assert never  # a plug the store knows that has never reported

    def test_it_names_the_quiet_the_untagged_and_the_doubled(self, tmp_path) -> None:
        db = tmp_path / "x.duckdb"
        self._seed(db)
        result = CliRunner().invoke(cli, ["doctor", "--db", str(db)])
        assert result.exit_code == 0, result.output
        out = result.output

        quiet = self._section(out, "Quiet outlets")
        assert "DEADBEEF/DEADBEEF00" in quiet and "20 days" in quiet
        assert "affects: Star Trip (M0009)" in quiet
        assert "STRIP1/STRIP102" in quiet and "never" in quiet
        assert "STRIP100" not in quiet, "an outlet that reported a minute ago is not quiet"

        relabel = self._section(out, "Relabel candidates")
        assert '"Plug 2"' in relabel and "95 W" in relabel
        assert "Plug 3" not in relabel, "an outlet that never reported is not drawing anything"
        assert '"Snack Machine"' in relabel and "on, unmetered" in relabel
        assert "Plug 5" not in relabel, (
            "the latest row says nothing about draw or relay; an older 80 W must not stand in"
        )

        doubled = self._section(out, "Machines on more than one outlet")
        assert "Star Trip (M0009)" in doubled
        assert "DEADBEEF/DEADBEEF00" in doubled and "STRIP1/STRIP103" in doubled
        assert "Blackout" not in doubled

    def test_a_healthy_store_says_none_three_times(self, tmp_path) -> None:
        db = tmp_path / "x.duckdb"
        now = datetime.now(UTC)
        with Store(str(db)) as store:
            plug = store.ensure_plug("STRIP1", "STRIP100", "Blackout - M0013")
            store.update_assignment(plug, store.ensure_machine("M0013", "Blackout"), now)
            store.insert_readings([(now, plug, 120.0, 119.0, 1.0, 5.0)])
        result = CliRunner().invoke(cli, ["doctor", "--db", str(db)])
        assert result.exit_code == 0, result.output
        assert result.output.count("  none") == 3

    def test_a_missing_database_is_an_error_not_a_clean_bill(self, tmp_path) -> None:
        """`Store` creates a file that does not exist; a doctor that did so
        would print "none" three times about a floor that was never there."""
        result = CliRunner().invoke(cli, ["doctor", "--db", str(tmp_path / "typo.duckdb")])
        assert result.exit_code != 0
        assert "does not exist" in result.output
        assert not (tmp_path / "typo.duckdb").exists()

    def test_the_window_is_adjustable(self, tmp_path) -> None:
        db = tmp_path / "x.duckdb"
        self._seed(db)
        result = CliRunner().invoke(cli, ["doctor", "--db", str(db), "--days", "30"])
        assert result.exit_code == 0, result.output
        quiet = self._section(result.output, "Quiet outlets")
        assert "DEADBEEF" not in quiet, "20 days quiet is inside a 30-day window"
        assert "STRIP102" in quiet, "never reported is quiet at any window"
