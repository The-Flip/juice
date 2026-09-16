"""Tests for juice.cli."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime
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
    reach -- and hand `_serve_tap` exactly what it was given."""

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

    def test_it_hands_serve_tap_what_it_was_given(self, tmp_path, monkeypatch) -> None:
        """The seam is `_serve_tap`; what it is handed is what matters."""
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
    """`_serve_tap` is the production entrypoint. Run it for
    real on an ephemeral port: the three seams into `create_app`, the gather,
    the shutdown order."""

    async def test_it_serves_a_floor_and_refuses_operations_with_no_tap(self, tmp_path) -> None:
        import asyncio
        import contextlib
        import socket

        import aiohttp

        from juice.cli import _serve_tap

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
