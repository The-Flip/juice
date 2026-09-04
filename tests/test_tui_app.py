"""Tests for juice.tui.app — the Textual layer.

Driven against a stub client so they assert on what the app does with a payload,
not on whether a server is up; `test_tui_client.py` covers the wire.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from juice.tui.app import MISSING, JuiceTui, activity_cell, age, pending_cell, watts
from juice.tui.client import ApiError, Frame

NOW = datetime(2026, 9, 1, 21, 30, tzinfo=UTC)


def _machine(**overrides):
    base = {
        "asset_id": "M0001",
        "name": "Blackout",
        "status": "attract",
        "activity": "attract",
        "activity_unknown_because": None,
        "relay": "on",
        "draw_watts": 180.0,
        "status_since": (NOW - timedelta(minutes=8)).isoformat(),
        "lock_mode": None,
        "pending_command": None,
        "plug_id": 1,
        "device_id": "dev-a",
        "strip": "Row 1",
        "outlet": 1,
        "calibration": {"calibrated": True},
    }
    return base | overrides


class StubClient:
    """Just enough of JuiceClient for the app, with no I/O."""

    base_url = "http://juice.test"

    def __init__(self, machines, *, authenticated=True):
        self.frames = []
        self.authenticated = authenticated
        self.who = "dev@localhost" if authenticated else "anon"
        self._machines = machines

    async def me(self):
        return {"authenticated": self.authenticated}

    def _view(self, machine):
        """Mirror the server's redaction, so an anonymous stub is anonymous."""
        if self.authenticated:
            return machine
        operator_only = {"plug_id", "device_id", "outlet", "strip", "calibration"}
        return {k: v for k, v in machine.items() if k not in operator_only}

    async def floor(self):
        machines = [self._view(m) for m in self._machines]
        problems = [
            {"asset_id": m["asset_id"], "name": m["name"], "status": m["status"], "since": None}
            for m in machines
            if m["status"] in {"no_draw", "abandoned"}
        ]
        return {
            "counts": {
                "total": len(machines),
                "powered": sum(1 for m in machines if m["status"] != "off"),
                "playing": sum(1 for m in machines if m["status"] == "playing"),
                "problems": len(problems),
            },
            "problems": problems,
            "infrastructure": [],
            "groups": [{"device_id": "dev-a", "name": "Row 1", "machines": machines}],
            "operation": None,
        }

    frames: list = []

    async def stream(self):
        """Replays `frames`, then idles — like a real stream, which never ends."""
        for frame in self.frames:
            yield frame
        await asyncio.sleep(3600)


# --------------------------------------------------------------------------
# Cell rendering
# --------------------------------------------------------------------------


def test_age_reads_at_a_glance():
    assert age((NOW - timedelta(seconds=12)).isoformat(), now=NOW) == "12s"
    assert age((NOW - timedelta(minutes=8)).isoformat(), now=NOW) == "8m"
    assert age((NOW - timedelta(hours=3)).isoformat(), now=NOW) == "3h"
    assert age((NOW - timedelta(days=5)).isoformat(), now=NOW) == "5d"
    assert age(None) == MISSING


def test_unmeasurable_draw_is_not_rendered_as_zero():
    """`draw_watts: null` means we cannot measure, which is not 0.0 W."""
    assert watts(None) == MISSING
    assert watts(0.0) == "0.0"


def test_activity_cell_says_why_it_is_unknown():
    cell = activity_cell(_machine(activity=None, activity_unknown_because="uncalibrated"))
    assert cell.plain == "uncalibrated"


def test_pending_command_is_shown_separately_from_status():
    cell = pending_cell({"command_id": "cb16", "kind": "reboot", "phase": "retrying", "attempt": 3})
    assert cell.plain == "reboot retrying #3"
    assert pending_cell(None).plain == MISSING


# --------------------------------------------------------------------------
# The app
# --------------------------------------------------------------------------


async def test_the_table_renders_a_row_per_machine():
    client = StubClient([_machine(), _machine(asset_id="M0002", name="Centaur", plug_id=2)])
    app = JuiceTui(client)
    async with app.run_test() as pilot:
        await pilot.pause()
        table = app.query_one("#table")
        assert table.row_count == 2
        assert app.by_plug == {1: "M0001", 2: "M0002"}
        assert set(app.machines) == {"M0001", "M0002"}


async def test_a_reading_tick_updates_the_row_it_names():
    client = StubClient([_machine()])
    app = JuiceTui(client)
    async with app.run_test() as pilot:
        await pilot.pause()
        await app._handle(
            Frame(
                kind="event",
                seq=2,
                type="reading_tick",
                raw="{}",
                data={
                    "machines": [
                        {
                            "plug_id": 1,
                            "asset_id": "M0001",
                            "status": "playing",
                            "activity": "playing",
                            "activity_unknown_because": None,
                            "status_since": NOW.isoformat(),
                            "relay": "on",
                            "draw_watts": 244.0,
                        }
                    ]
                },
            )
        )
        table = app.query_one("#table")
        assert table.get_cell("M0001", "status").plain == "playing"
        assert table.get_cell("M0001", "watts") == "244.0"
        assert app.machines["M0001"]["status"] == "playing"
        # The tick carries no name/strip; folding it in must not drop them.
        assert app.machines["M0001"]["name"] == "Blackout"
        assert app.machines["M0001"]["strip"] == "Row 1"


async def test_a_tick_for_an_unknown_machine_is_counted_not_crashed_on():
    client = StubClient([_machine()])
    app = JuiceTui(client)
    async with app.run_test() as pilot:
        await pilot.pause()
        await app._handle(
            Frame(kind="event", seq=2, type="reading_tick", data={"machines": [{"plug_id": 99}]})
        )
        assert app.query_one("#table").row_count == 1
        assert app.unjoined == 1


async def test_an_anonymous_client_joins_ticks_by_asset_id():
    """The whole point of asset_id on the tick: no plug_id is needed, so the
    public floor view updates live like the operator one."""
    client = StubClient([_machine()], authenticated=False)
    app = JuiceTui(client)
    async with app.run_test() as pilot:
        await pilot.pause()
        assert app.by_plug == {}  # redacted, as an anonymous caller sees it
        await app._handle(
            Frame(
                kind="event",
                seq=2,
                type="reading_tick",
                data={"machines": [{"asset_id": "M0001", "status": "playing", "draw_watts": 9.0}]},
            )
        )
        assert app.unjoined == 0
        assert app.query_one("#table").get_cell("M0001", "status").plain == "playing"


async def test_a_resync_frame_refetches_the_floor_and_is_rate_limited():
    client = StubClient([_machine()])
    app = JuiceTui(client)
    async with app.run_test() as pilot:
        await pilot.pause()
        calls = []
        original = app.refetch

        async def counting():
            calls.append(1)
            await original()

        app.refetch = counting
        await app._handle(Frame(kind="resync", seq=7, reason="gap", detail="seq 5 → 7"))
        assert calls == [1]
        # A second resync straight away must not become a refetch storm.
        await app._handle(Frame(kind="resync", seq=8, reason="gap", detail="seq 7 → 8"))
        assert calls == [1]
        assert app.resyncs == 2


async def test_unattributable_ticks_are_reported_in_the_banner():
    """A server that predates asset_id on ticks sends some we cannot apply.
    Saying so beats a table that silently stops updating."""
    client = StubClient([_machine()], authenticated=False)
    app = JuiceTui(client)
    async with app.run_test() as pilot:
        await pilot.pause()
        assert "could not be attributed" not in str(app.query_one("#banner").content)

        await app._handle(
            Frame(kind="event", seq=2, type="reading_tick", data={"machines": [{"plug_id": 99}]})
        )
        text = str(app.query_one("#banner").content)
        assert "could not be attributed" in text
        assert "anon" in text


async def test_raw_mode_shows_the_wire_text_it_was_given():
    client = StubClient([_machine()])
    app = JuiceTui(client)
    async with app.run_test() as pilot:
        await pilot.pause()
        app.action_toggle_raw()
        assert app.raw is True
        line = app._raw_line(Frame(kind="event", seq=3, type="hello", raw='{"seq":3}'))
        assert '{"seq":3}' in line


async def test_raw_lines_are_capped_so_one_tick_cannot_bury_the_pane():
    app = JuiceTui(StubClient([_machine()]))
    line = app._raw_line(Frame(kind="event", seq=1, raw="x" * 5000))
    assert len(line) < 500
    assert "+4680 chars" in line  # says how much was dropped


async def test_wire_text_is_escaped_so_a_payload_cannot_forge_markup():
    """The stream pane is a RichLog(markup=True), so bracketed wire text would
    otherwise be read as Rich tags — a machine named "[bold]" must print as
    itself, not restyle the pane."""
    app = JuiceTui(StubClient([_machine()]))
    line = app._raw_line(Frame(kind="event", seq=1, raw='{"name": "[bold]Trip[/]"}'))
    assert r"\[bold]Trip\[/]" in line
    # The client's own elision annotation stays live markup.
    long_line = app._raw_line(Frame(kind="event", seq=1, raw="[x" * 5000))
    assert "[grey42](+" in long_line


async def test_a_bracketed_machine_name_is_escaped_in_the_change_line():
    """The default (non-raw) view interpolates the machine name into markup.

    Only tag-shaped text needs escaping — Rich never reads `[Trip]` as a tag —
    so the case that matters is a name that would parse, like `[bold]`.
    """
    client = StubClient([_machine(name="Star [bold]Trip")])
    app = JuiceTui(client)
    async with app.run_test() as pilot:
        await pilot.pause()
        changes = app._apply_tick([{"asset_id": "M0001", "status": "playing"}])
    assert len(changes) == 1
    assert r"Star \[bold]Trip" in changes[0]
    assert "[bold]M0001[/]" in changes[0]  # client-generated markup survives


class AngryClient(StubClient):
    """A server whose error envelope carries markup-shaped text."""

    async def floor(self):
        raise ApiError(500, "boom", "outlet [bold]3[/] exploded")


async def test_a_server_error_message_cannot_forge_markup_either():
    """`message` is server text on the same RichLog(markup=True) path as the
    payload, and it reaches it through a different function."""
    app = JuiceTui(AngryClient([_machine()]))
    lines = []
    app.log_line = lines.append
    async with app.run_test() as pilot:
        await pilot.pause()
    failures = [line for line in lines if "floor failed" in line]
    assert failures
    assert r"outlet \[bold]3\[/] exploded" in failures[0]


class DeadClient(StubClient):
    """A server that has gone away mid-session."""

    def __init__(self):
        super().__init__([_machine()])
        self.dead = False
        self.frames = []

    async def floor(self):
        if self.dead:
            raise ApiError(0, "unreachable", "ClientConnectorError: refused")
        return await super().floor()


async def test_a_resync_against_a_dead_server_does_not_kill_the_stream():
    """Drives the real worker: a resync fires precisely when the server may
    have gone, and `refetch` raising there would end the session."""
    client = DeadClient()
    client.frames = [
        Frame(kind="event", seq=1, type="hello", data={"epoch": "e1"}),
        Frame(kind="disconnected", detail="gone"),
        Frame(kind="resync", reason="reconnect", detail="gone"),
        Frame(kind="event", seq=1, type="hello", data={"epoch": "e1"}),
    ]
    app = JuiceTui(client)
    async with app.run_test() as pilot:
        await pilot.pause()
        client.dead = True
        app.run_worker(app._pump())
        await pilot.pause()
        await asyncio.sleep(0.1)
        await pilot.pause()

        assert app.stale is True  # the floor refetch failed and says so
        assert app.query_one("#table").row_count == 1  # last good rows kept
        assert app.is_running  # the worker survived it


async def test_an_exception_on_one_frame_does_not_end_the_session():
    """A frame the client cannot render is a bad log line, not a dead app."""
    client = StubClient([_machine()])
    client.frames = [Frame(kind="event", seq=None, type="reading_tick", data={"machines": []})]
    app = JuiceTui(client)
    async with app.run_test() as pilot:
        await pilot.pause()
        app.run_worker(app._pump())
        await pilot.pause()
        await asyncio.sleep(0.1)
        await pilot.pause()
        assert app.is_running


async def test_the_dot_stays_red_while_the_client_is_backing_off():
    """The floor still refetches during a reconnect, and a green dot over a
    45-second backoff is the failure api_v2.md §5 asks clients to surface."""
    client = StubClient([_machine()])
    app = JuiceTui(client)
    async with app.run_test() as pilot:
        await pilot.pause()
        await app._handle(Frame(kind="connected", detail="x"))
        assert "[green]●" in str(app.query_one("#banner").content)

        await app._handle(Frame(kind="disconnected", detail="gone"))
        await app._handle(Frame(kind="resync", reason="reconnect", detail="gone"))
        text = str(app.query_one("#banner").content)
        assert app.connection == "reconnecting"
        assert "[red]●" in text


async def test_a_deferred_resync_is_retried_not_dropped():
    """Two resyncs inside the window is the normal overflow shape; dropping the
    second leaves the table's membership permanently behind."""
    client = StubClient([_machine()])
    app = JuiceTui(client)
    async with app.run_test() as pilot:
        await pilot.pause()
        await app._handle(Frame(kind="resync", seq=7, reason="gap", detail="a"))
        assert app.stale is False  # first one refetched

        client._machines.append(_machine(asset_id="M0002", name="Centaur", plug_id=2))
        await app._handle(Frame(kind="resync", seq=8, reason="gap", detail="b"))
        assert app._resync_pending is True
        assert app.query_one("#table").row_count == 1  # not yet

        app._last_resync = 0.0  # the window has passed
        await app._tick_ui()
        assert app._resync_pending is False
        assert app.query_one("#table").row_count == 2
        assert app.stale is False


async def test_login_failure_is_reported_rather_than_raised():
    client = DeadClient()
    app = JuiceTui(client)

    async def boom():
        raise ApiError(0, "unreachable", "ClientConnectorError: refused")

    client.login = boom
    async with app.run_test() as pilot:
        await pilot.pause()
        logged = []
        app.log_line = logged.append
        await app.action_login()
        assert any("login failed" in line and "unreachable" in line for line in logged)
        assert app.query_one("#table").row_count == 1
