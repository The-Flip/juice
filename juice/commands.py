"""In-flight power commands: the pending-action contract, server-side.

Actuation is a WAN round-trip to the TP-Link cloud, so a power action takes
seconds — 4 to 30 for a reboot. The rule that makes that bearable already exists,
but it lives in the browser (`juice/web/power.js`): a write's response is never
treated as completion, the client waits for a corroborating *relay reading*, and
each page reimplements the settle logic with its own hardcoded timeouts.

This module moves that rule to the server and puts it on the wire, so every
client agrees on when an action finished and two operators watching the same
machine see the same outcome at the same instant.

The contract:

  * A write **opens** a command and returns its id. That is "we have started
    talking to the device", not "it worked".
  * Progress — retries, attempt counts — is announced as it happens.
  * `confirmed` is decided **only** by a fresh relay reading that matches what
    the command expected. Never by the write returning, never by the transient
    action event. This is `pcReduceReading`'s rule, unchanged.
  * A reboot additionally needs evidence the cycle actually happened: an observed
    off→on, or both cloud legs acknowledged. The pre-off "on" must not settle it.
  * Anything still unconfirmed at its deadline is `timed_out`, decided by the
    server so every client times out together.

Freshness is the part with no equivalent in the JS. `PlugReading` carries no
timestamp, so the reconciler is handed one explicitly and ignores any reading at
or before the command was issued. Without that, a `turn_on` sent to a plug whose
device has just gone dark confirms instantly against the stale cached
`is_on=True` — the button-lied failure this design exists to eliminate.

Pure and clock-injectable: no aiohttp, no DB, no sleeping.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Literal

Kind = Literal["turn_on", "turn_off", "reboot"]
Phase = Literal[
    "accepted",  # command opened; the device call is starting
    "dispatching",  # the cloud call is in flight
    "retrying",  # a transient failure; another attempt is queued
    "awaiting_relay",  # the cloud accepted; waiting for the relay to agree
    "confirmed",  # a fresh relay reading matched what we asked for
    "failed",  # the device call gave up
    "timed_out",  # the cloud accepted but the relay never agreed
    "refused",  # blocked before we called the device (e.g. a lock)
    "superseded",  # a later command for the same plug replaced this one
]

TERMINAL_PHASES: frozenset[str] = frozenset(
    {"confirmed", "failed", "timed_out", "refused", "superseded"}
)

# Phases during which a cloud call is actually in flight. Only these conflict:
# the hazard is two calls racing at the device, not a command sitting waiting for
# the relay to catch up. Refusing while merely `awaiting_relay` would block an
# operator from cutting power for the whole timeout — precisely wrong when a
# machine is smoking and someone wants it off now.
_DISPATCHING_PHASES: frozenset[str] = frozenset({"accepted", "dispatching", "retrying"})

# Worst-case time to actuate one plug. Derived rather than hardcoded so that
# changing the retry policy moves the contract with it — a client that gives up
# before the server has stopped trying is how a machine comes up after the UI
# already declared failure.
#
# Individual power control allows 6 attempts with 0.5/1/2/4/4s of backoff, plus a
# cloud round-trip per attempt.
_MAX_ATTEMPTS = 6
_BACKOFF_TOTAL_S = 11.5
_CLOUD_RTT_S = 2.0
_POWER_BUDGET_S = _BACKOFF_TOTAL_S + _MAX_ATTEMPTS * _CLOUD_RTT_S

# A reboot is two of those, plus the hold, plus the window we keep polling for a
# late-appearing load.
_REBOOT_HOLD_S = 3.0
_WATCH_WINDOW_S = 10.0

POWER_TIMEOUT_MS = int(_POWER_BUDGET_S * 1000)
REBOOT_TIMEOUT_MS = int((2 * _POWER_BUDGET_S + _REBOOT_HOLD_S + _WATCH_WINDOW_S) * 1000)

# How long a terminal command stays queryable before the sweep forgets it. Long
# enough that a client which reconnects right after an action can still learn how
# it ended; short enough that the registry can't grow without bound.
_RETENTION_S = 120.0


def timeout_ms_for(kind: Kind) -> int:
    return REBOOT_TIMEOUT_MS if kind == "reboot" else POWER_TIMEOUT_MS


@dataclass
class Command:
    """One in-flight power action against one plug."""

    id: str
    kind: Kind
    plug_id: int
    asset_id: str | None
    actor: str
    source: str  # 'individual' | 'reboot' | 'all_on' | 'all_off'
    issued_at: datetime
    deadline: datetime
    expect_relay: bool
    operation_id: str | None = None
    phase: Phase = "accepted"
    attempt: int = 1
    saw_off: bool = False  # reboot: the off leg was actually observed
    legs_acked: bool = False  # reboot: both cloud calls returned ok
    error: str | None = None
    confirmed_by: str | None = None  # 'relay_cycle' | 'ack_and_relay' | 'relay'
    terminal_at: datetime | None = field(default=None, repr=False)

    @property
    def terminal(self) -> bool:
        return self.phase in TERMINAL_PHASES

    def to_dict(self) -> dict:
        """The wire shape. `timeout_ms` is *remaining* budget, so a client that
        joins mid-flight (or sees a retry extend the deadline) always has the
        authoritative number rather than the one from the original response."""
        return {
            "type": "command",
            "command_id": self.id,
            "kind": self.kind,
            "plug_id": self.plug_id,
            "asset_id": self.asset_id,
            "actor": self.actor,
            "source": self.source,
            "operation_id": self.operation_id,
            "phase": self.phase,
            "attempt": self.attempt,
            "expect": {"relay": "on" if self.expect_relay else "off"},
            "issued_at": self.issued_at.isoformat(),
            "error": self.error,
            "confirmed_by": self.confirmed_by,
        }


class CommandRegistry:
    """Tracks in-flight commands and decides when they are done.

    `publish` receives wire-shaped events; `now` supplies the clock so timeout
    behaviour is testable without sleeping.
    """

    def __init__(
        self,
        publish: Callable[[dict], None] | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._commands: dict[str, Command] = {}
        self._publish = publish or (lambda _event: None)
        self._now = now or (lambda: datetime.now(UTC))

    # -- lifecycle ----------------------------------------------------------

    def open(
        self,
        *,
        kind: Kind,
        plug_id: int,
        actor: str,
        source: str,
        asset_id: str | None = None,
        operation_id: str | None = None,
    ) -> Command:
        """Open a command, or return the identical one already in flight.

        Prefer `open_ex` in handlers: knowing the command already existed is what
        stops a double-tap from dispatching twice.
        """
        command, _created = self.open_ex(
            kind=kind,
            plug_id=plug_id,
            actor=actor,
            source=source,
            asset_id=asset_id,
            operation_id=operation_id,
        )
        return command

    def open_ex(
        self,
        *,
        kind: Kind,
        plug_id: int,
        actor: str,
        source: str,
        asset_id: str | None = None,
        operation_id: str | None = None,
    ) -> tuple[Command, bool]:
        """Open a command; returns `(command, created)`.

        Repeating the *same* action on the same plug is idempotent, but returning
        the existing command is only half of that: the caller must also skip the
        device call, or a double-tap still sends two cloud requests and a
        repeated reboot spawns a second delayed power-on task. `created` is how
        the caller knows.

        A *conflicting* action is not resolved here — callers consult
        `conflicts()` and refuse.
        """
        existing = self.in_flight_for_plug(plug_id)
        if existing is not None and existing.kind == kind:
            return existing, False
        if existing is not None:
            # Reached only once the earlier command's cloud call has landed (a
            # still-dispatching one is refused by conflicts()). It is now waiting
            # on a relay that this command is about to change, so its own
            # confirmation would be meaningless — retire it honestly rather than
            # letting it confirm against our result or time out.
            self.advance(existing, "superseded", error=f"replaced by a {kind} command")

        now = self._now()
        cmd = Command(
            id=uuid.uuid4().hex,
            kind=kind,
            plug_id=plug_id,
            asset_id=asset_id,
            actor=actor,
            source=source,
            operation_id=operation_id,
            issued_at=now,
            deadline=now + timedelta(milliseconds=timeout_ms_for(kind)),
            expect_relay=kind != "turn_off",
        )
        self._commands[cmd.id] = cmd
        self._announce(cmd)
        return cmd, True

    def get(self, command_id: str) -> Command | None:
        return self._commands.get(command_id)

    def in_flight_for_plug(self, plug_id: int) -> Command | None:
        for cmd in self._commands.values():
            if cmd.plug_id == plug_id and not cmd.terminal:
                return cmd
        return None

    def conflicts(self, plug_id: int, kind: Kind) -> Command | None:
        """The in-flight command a new `kind` would collide with, if any.

        Two operators converging on one machine is the normal case, not the edge
        case (user_needs J6), so a conflicting command is refused with the
        in-flight one attached rather than both being accepted and racing at the
        device.
        """
        existing = self.in_flight_for_plug(plug_id)
        if existing is None or existing.kind == kind:
            return None
        if existing.phase not in _DISPATCHING_PHASES:
            # Waiting on the relay, not talking to the device — a new command
            # supersedes it instead of being refused.
            return None
        return existing

    # -- progress -----------------------------------------------------------

    def advance(self, cmd: Command, phase: Phase, **fields: object) -> None:
        for key, value in fields.items():
            setattr(cmd, key, value)
        cmd.phase = phase
        if cmd.terminal and cmd.terminal_at is None:
            cmd.terminal_at = self._now()
        self._announce(cmd)

    def record_retry(self, cmd: Command, *, attempt: int, error: str, delay: float) -> None:
        """A transient failure; another attempt is coming.

        Extends the deadline by the time the retry will actually consume. Without
        this a reboot's on-leg can still be retrying when the sweep declares it
        timed out — and then the machine comes up ten seconds later with nobody
        listening, so the operator taps reboot again and gets two cycles.
        """
        cmd.deadline += timedelta(seconds=delay + _CLOUD_RTT_S)
        self.advance(cmd, "retrying", attempt=attempt + 1, error=error)

    def record_dispatched(self, cmd: Command) -> None:
        """The cloud accepted the call; now we wait for the relay to agree."""
        self.advance(cmd, "awaiting_relay", error=None)

    def record_failure(self, cmd: Command, error: str) -> None:
        self.advance(cmd, "failed", error=error)

    def record_refusal(self, cmd: Command, error: str) -> None:
        self.advance(cmd, "refused", error=error)

    def mark_legs_acked(self, cmd: Command) -> None:
        """Both halves of a reboot returned ok. Permits — but does not itself
        cause — a confirm; a real relay reading is still required."""
        cmd.legs_acked = True

    # -- the settle rule ----------------------------------------------------

    def reconcile(self, plug_id: int, *, relay_on: bool, reading_ts: datetime) -> None:
        """Offer a relay reading as evidence for whatever is in flight on a plug.

        Called from the recorder's ~1 Hz tick with the timestamp of the reading
        it just took. A reading at or before `issued_at` is ignored: it predates
        the command and so cannot be evidence that the command worked.
        """
        cmd = self.in_flight_for_plug(plug_id)
        if cmd is None or reading_ts <= cmd.issued_at:
            return

        if cmd.kind == "reboot":
            if not relay_on:
                # The off leg runs (and the 3s hold elapses) while the command is
                # still pre-dispatch, so this evidence must be collected even
                # though it is far too early to confirm.
                cmd.saw_off = True
                return
            if cmd.phase != "awaiting_relay":
                return  # the power-on leg has not completed yet
            if cmd.saw_off:
                self.advance(cmd, "confirmed", confirmed_by="relay_cycle")
            elif cmd.legs_acked:
                # The 3s off window can fall between polls. Both cloud calls
                # returned ok and the relay now reads on, which is the best
                # evidence available without having sampled the gap.
                self.advance(cmd, "confirmed", confirmed_by="ack_and_relay")
            return

        # Only a dispatched command can be confirmed. Otherwise a plug that
        # already holds the requested relay state confirms while the handler is
        # still inside call_with_retry — and record_dispatched would then drag a
        # terminal command back to awaiting_relay, where it can later time out.
        if cmd.phase != "awaiting_relay":
            return
        if relay_on == cmd.expect_relay:
            self.advance(cmd, "confirmed", confirmed_by="relay")

    def sweep(self) -> None:
        """Expire overdue commands and forget long-terminal ones.

        Runs on the recorder tick, so it must stay cheap and must not raise into
        the poll loop.
        """
        now = self._now()
        for cmd in list(self._commands.values()):
            if not cmd.terminal and now > cmd.deadline:
                self.advance(cmd, "timed_out", error="relay never confirmed")
            elif (
                cmd.terminal
                and cmd.terminal_at is not None
                and (now - cmd.terminal_at).total_seconds() > _RETENTION_S
            ):
                del self._commands[cmd.id]

    # -- internals ----------------------------------------------------------

    def _announce(self, cmd: Command) -> None:
        event = cmd.to_dict()
        remaining = (cmd.deadline - self._now()).total_seconds()
        event["timeout_ms"] = max(0, int(remaining * 1000))
        self._publish(event)
