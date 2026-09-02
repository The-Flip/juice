"""The TUI itself: a machines table and a stream pane.

Plain on purpose. This exists to evaluate `/api/v2`, so it shows what the wire
actually carries — including the operator-only keys as `—` when logged out, and
every sequence gap — rather than smoothing any of it over.
"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from typing import Any

from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.widgets import DataTable, Footer, RichLog, Static

from juice.tui.client import ApiError, Frame, JuiceClient

# api_v2.md §3's table, mapped onto a terminal. `powered` and `attract` share a
# colour deliberately — both are the good case; the difference is only whether
# we could measure it.
STATUS_STYLE = {
    "unreachable": "grey50",
    "off": "grey42",
    "no_draw": "bold dark_orange",
    "powered": "deep_sky_blue1",
    "attract": "deep_sky_blue1",
    "playing": "bold green",
    "abandoned": "bold yellow",
}

COLUMNS = [
    ("asset", "ASSET", 7),
    ("name", "NAME", 22),
    ("status", "STATUS", 12),
    ("activity", "ACTIVITY", 16),
    ("watts", "W", 8),
    ("since", "SINCE", 7),
    ("relay", "RELAY", 6),
    ("lock", "LOCK", 5),
    ("pending", "PENDING", 20),
    ("where", "STRIP / OUTLET", 20),
]

MISSING = "—"

# Identity keys on a tick: they address the row, they are not fields of it, and
# merging them over the floor payload would overwrite a redacted plug_id.
_TICK_KEYS = frozenset({"plug_id", "asset_id"})

# How much of a frame body the stream pane shows before eliding — see _compact.
RAW_MAX = 320

# Never refetch the floor more often than this, however many resyncs arrive.
# §5: a client resyncing every second is what overflows the queue.
RESYNC_MIN_INTERVAL = 2.0


def age(iso: str | None, *, now: datetime | None = None) -> str:
    """`status_since` as something readable at a glance."""
    if not iso:
        return MISSING
    try:
        started = datetime.fromisoformat(iso)
    except ValueError:
        return "?"
    if started.tzinfo is None:  # the doc promises a real offset; trust but cope
        started = started.replace(tzinfo=UTC)
    seconds = ((now or datetime.now(UTC)) - started).total_seconds()
    if seconds < 0:
        return "0s"
    if seconds < 60:
        return f"{int(seconds)}s"
    if seconds < 3600:
        return f"{int(seconds // 60)}m"
    if seconds < 86400:
        return f"{int(seconds // 3600)}h"
    return f"{int(seconds // 86400)}d"


def watts(value: Any) -> str:
    """`null` is not zero — an unmeasurable outlet must not read as 0.0 W."""
    return MISSING if value is None else f"{float(value):.1f}"


def activity_cell(machine: dict[str, Any]) -> Text:
    """Say *why* activity is unknown instead of leaving the cell blank."""
    if machine.get("activity"):
        return Text(machine["activity"])
    reason = machine.get("activity_unknown_because")
    return Text(reason or MISSING, style="grey42")


def status_cell(status: str | None) -> Text:
    return Text(status or MISSING, style=STATUS_STYLE.get(status or "", ""))


def _compact(body: Any) -> str:
    """A frame body short enough to sit on one line of the stream pane.

    A raw reading tick is ~4 KB of JSON; printed whole it buries everything
    around it. The head is enough to judge the shape, and the elision says how
    much was dropped so it is never mistaken for the whole payload.
    """
    text = body if isinstance(body, str) else json.dumps(body)
    if len(text) <= RAW_MAX:
        return text
    return f"{text[:RAW_MAX]}… [grey42](+{len(text) - RAW_MAX} chars)[/]"


def pending_cell(pending: dict[str, Any] | None) -> Text:
    """`pending_command` is intent; `status` is observation. Never merged."""
    if not pending:
        return Text(MISSING, style="grey42")
    label = f"{pending.get('kind')} {pending.get('phase')}"
    if (attempt := pending.get("attempt")) and attempt > 1:
        label += f" #{attempt}"
    return Text(label, style="bold magenta")


class JuiceTui(App):
    """A read-only client for `/api/v2`."""

    CSS = """
    Screen { layout: vertical; }
    #banner { height: auto; padding: 0 1; }
    #table { height: 1fr; }
    #stream { height: 12; border-top: solid $accent; padding: 0 1; }
    """

    BINDINGS = [
        Binding("q", "quit", "quit"),
        Binding("r", "toggle_raw", "raw/humanized"),
        Binding("f", "refetch", "refetch floor"),
        Binding("l", "login", "login"),
        Binding("L", "logout", "logout"),
        Binding("p", "toggle_pause", "pause scroll"),
    ]

    def __init__(self, client: JuiceClient) -> None:
        super().__init__()
        self.client = client
        self.raw = False
        self.paused = False
        self.floor: dict[str, Any] = {}
        self.machines: dict[str, dict[str, Any]] = {}
        self.by_plug: dict[int, str] = {}
        self.connection = "connecting"
        self.last_seq: int | None = None
        self.epoch: str | None = None
        self.ticks = 0
        self.resyncs = 0
        self.unjoined = 0
        self._last_resync = 0.0

    # --- layout ----------------------------------------------------------

    def compose(self) -> ComposeResult:
        with Vertical():
            yield Static(id="banner")
            yield DataTable(id="table", zebra_stripes=True, cursor_type="row")
            yield RichLog(id="stream", markup=True, wrap=True, max_lines=2000)
        yield Footer()

    async def on_mount(self) -> None:
        table = self.query_one("#table", DataTable)
        for key, label, width in COLUMNS:
            table.add_column(label, key=key, width=width)
        await self.client.me()
        await self.refetch()
        self.set_interval(1.0, self._retick_ages)
        self.run_worker(self._pump(), exclusive=True)

    # --- data ------------------------------------------------------------

    async def refetch(self) -> bool:
        """One request for the whole view, per §4. There is no polling timer.

        Returns whether it succeeded. A resync fires precisely when the server
        may have gone away, so this failing is ordinary, not exceptional — and
        letting it raise would kill the stream worker that asked for it.
        """
        try:
            self.floor = await self.client.floor()
        except ApiError as exc:
            self.log_line(f"[bold red]floor failed[/] {exc.code}: {exc.message}")
            return False

        self.machines = {}
        self.by_plug = {}
        rows: list[tuple[dict[str, Any], str]] = []
        for group in self.floor.get("groups", []):
            where = group.get("name") or MISSING
            for machine in group.get("machines", []):
                self.machines[machine["asset_id"]] = machine
                if (plug := machine.get("plug_id")) is not None:
                    self.by_plug[plug] = machine["asset_id"]
                rows.append((machine, where))

        table = self.query_one("#table", DataTable)
        table.clear()
        for machine, strip in rows:
            table.add_row(*self._row(machine, strip), key=machine["asset_id"])
        self._render_banner()
        return True

    def _row(self, machine: dict[str, Any], strip: str) -> list[Any]:
        outlet = machine.get("outlet")
        where = MISSING if strip == MISSING else f"{strip} / {outlet if outlet else '?'}"
        return [
            machine["asset_id"],
            machine.get("name") or MISSING,
            status_cell(machine.get("status")),
            activity_cell(machine),
            watts(machine.get("draw_watts")),
            age(machine.get("status_since")),
            machine.get("relay") or MISSING,
            machine.get("lock_mode") or MISSING,
            pending_cell(machine.get("pending_command")),
            where,
        ]

    def _apply_tick(self, machines: list[dict[str, Any]]) -> list[str]:
        """Fold a `reading_tick` into the table. Returns human-readable changes.

        Joins on `asset_id`, which every audience can see, and falls back to
        `plug_id` for a server predating that field — pointing this at an
        undeployed production while testing the fix is exactly the case. Ticks
        that match neither are counted, not dropped silently: a table that
        quietly stops updating is the failure this is meant to make visible.
        """
        table = self.query_one("#table", DataTable)
        changes: list[str] = []
        self.unjoined = 0
        for update in machines:
            asset_id = self._identify(update)
            if asset_id is None:
                self.unjoined += 1
                continue
            before = self.machines[asset_id]
            after = {**before, **{k: v for k, v in update.items() if k not in _TICK_KEYS}}
            self.machines[asset_id] = after

            if after.get("status") != before.get("status"):
                changes.append(
                    f"[bold]{asset_id}[/] {after.get('name')}: "
                    f"{before.get('status')} → {after.get('status')} "
                    f"({watts(after.get('draw_watts'))} W)"
                )
            for column, value in (
                ("status", status_cell(after.get("status"))),
                ("activity", activity_cell(after)),
                ("watts", watts(after.get("draw_watts"))),
                ("since", age(after.get("status_since"))),
                ("relay", after.get("relay") or MISSING),
            ):
                table.update_cell(asset_id, column, value)
        return changes

    def _identify(self, update: dict[str, Any]) -> str | None:
        asset_id = update.get("asset_id")
        if asset_id is None and (plug_id := update.get("plug_id")) is not None:
            asset_id = self.by_plug.get(plug_id)
        return asset_id if asset_id in self.machines else None

    def _retick_ages(self) -> None:
        """Re-render the relative ages. Local clock only — fetches nothing."""
        table = self.query_one("#table", DataTable)
        now = datetime.now(UTC)
        for asset_id, machine in self.machines.items():
            table.update_cell(asset_id, "since", age(machine.get("status_since"), now=now))

    # --- the stream ------------------------------------------------------

    async def _pump(self) -> None:
        async for frame in self.client.stream():
            await self._handle(frame)

    async def _handle(self, frame: Frame) -> None:
        if frame.seq is not None:
            self.last_seq = frame.seq

        match frame.kind:
            case "connected":
                self.connection = "live"
                self.log_line(f"[green]connected[/] {frame.detail}")
            case "disconnected":
                self.connection = "disconnected"
                self.log_line(f"[bold red]disconnected[/] {frame.detail}")
            case "comment":
                if self.raw:
                    self.log_line(f"[grey42]{frame.raw}[/]")
            case "resync":
                self.resyncs += 1
                self.connection = "stale"
                self.log_line(f"[bold yellow]RESYNC ({frame.reason})[/] {frame.detail}")
                await self._resync()
            case "event":
                self._log_event(frame)

        self._render_banner()

    async def _resync(self) -> None:
        elapsed = time.monotonic() - self._last_resync
        if elapsed < RESYNC_MIN_INTERVAL:
            self.log_line(f"[grey42]…resync suppressed ({elapsed:.1f}s since the last)[/]")
            return
        self._last_resync = time.monotonic()
        if await self.refetch():
            self.connection = "live"

    def _log_event(self, frame: Frame) -> None:
        if frame.type == "hello":
            self.epoch = frame.data.get("epoch")
            self.connection = "live"

        if frame.type == "reading_tick":
            self.ticks += 1
            changes = self._apply_tick(frame.data.get("machines", []))
            if self.raw:
                self.log_line(self._raw_line(frame))
            else:
                self.log_line(self._tick_summary(frame))
                for change in changes:
                    self.log_line(f"      {change}")
            return

        if self.raw:
            self.log_line(self._raw_line(frame))
        else:
            self.log_line(f"[grey62]{frame.seq:>5}[/] [bold]{frame.type}[/] {self._gist(frame)}")

    def _raw_line(self, frame: Frame) -> str:
        return f"[grey62]{frame.seq:>5}[/] {_compact(frame.raw or json.dumps(frame.data))}"

    def _tick_summary(self, frame: Frame) -> str:
        machines = frame.data.get("machines", [])
        drawing = [m["draw_watts"] for m in machines if m.get("draw_watts")]
        playing = sum(1 for m in machines if m.get("status") == "playing")
        joined = len(machines) - self.unjoined
        return (
            f"[grey62]{frame.seq:>5}[/] reading_tick · {len(machines)} machines "
            f"· {joined} joined · {playing} playing · Σ {sum(drawing) / 1000:.2f} kW"
        )

    def _gist(self, frame: Frame) -> str:
        match frame.type:
            case "hello":
                return f"epoch {str(frame.data.get('epoch'))[:8]}"
            case "command":
                return (
                    f"{str(frame.data.get('command_id'))[:8]} "
                    f"{frame.data.get('kind')} → {frame.data.get('phase')}"
                )
            case _:
                # `operation` and anything added later have no documented
                # payload in api_v2.md §6 — it shows the sentence to render,
                # not the fields. Printing the frame beats inventing key names
                # and quietly rendering "None/None".
                return _compact(frame.data)

    # --- chrome ----------------------------------------------------------

    def log_line(self, text: str) -> None:
        stream = self.query_one("#stream", RichLog)
        stream.auto_scroll = not self.paused
        stream.write(text)

    def _render_banner(self) -> None:
        counts = self.floor.get("counts", {})
        dots = {"live": "[green]●[/]", "stale": "[yellow]●[/]"}
        dot = dots.get(self.connection, "[red]●[/]")
        head = (
            f"{dot} [bold]{self.client.base_url}[/]/api/v2 · {self.client.who} · "
            f"{self.connection} · seq {self.last_seq} · epoch {str(self.epoch)[:8]} · "
            f"ticks {self.ticks} · resyncs {self.resyncs} · "
            f"[{'bold' if self.raw else 'grey42'}]raw[/]"
        )
        line = (
            f"{counts.get('total', 0)} machines · {counts.get('powered', 0)} powered · "
            f"{counts.get('playing', 0)} playing · "
            f"[dark_orange]{counts.get('problems', 0)} problems[/]"
        )
        for entry in self.floor.get("infrastructure", []):
            line += f" · [grey50]{entry['name']} unreachable ({len(entry['affects'])})[/]"

        notes = []
        if self.unjoined:
            # Ticks the client could not attribute to a row. On a current server
            # this should be zero for every audience; anything else means the
            # tick and the floor disagree about who is on the floor.
            notes.append(
                f"[yellow]{self.unjoined} of the last tick's machines could not be "
                "attributed[/] — no asset_id on the tick and no visible plug_id. "
                "Rows for those machines are not updating."
            )
        self.query_one("#banner", Static).update("\n".join([head, line, *notes]))

    # --- actions ---------------------------------------------------------

    def action_toggle_raw(self) -> None:
        self.raw = not self.raw
        self.log_line(f"[grey42]— {'raw frames' if self.raw else 'humanized'} —[/]")
        self._render_banner()

    def action_toggle_pause(self) -> None:
        self.paused = not self.paused
        self.log_line(f"[grey42]— scroll {'paused' if self.paused else 'resumed'} —[/]")

    async def action_refetch(self) -> None:
        await self.refetch()
        self.log_line("[grey42]— refetched /api/v2/floor —[/]")

    async def action_login(self) -> None:
        try:
            await self.client.login()
        except ApiError as exc:
            self.log_line(f"[bold red]login failed[/] {exc.code}: {exc.message}")
            return
        if not self.client.authenticated:
            self.log_line("[bold red]login failed[/] — dev-auth shim not installed?")
        await self.refetch()

    async def action_logout(self) -> None:
        try:
            await self.client.logout()
        except ApiError as exc:
            self.log_line(f"[bold red]logout failed[/] {exc.code}: {exc.message}")
            return
        await self.refetch()
