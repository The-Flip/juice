"""Power control over tap's `command` frame.

The cloud path actuates by calling the TP-Link cloud from `plug_objects`; the
tap path sends a `command` frame down the uplink socket and waits for tap's
`command_result`. Everything the power handlers already do -- the command
lifecycle, `call_with_retry`, confirmation from the next reading -- stays as
it is, because `TapPlug` is a drop-in for `plug_objects` and the frame round
trip hides behind `turn_on()` / `turn_off()`.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from juice.api.v2 import tap_wire as wire
from juice.collector_tap import (
    COMMAND_EXPIRES_S,
    COMMAND_RESULT_TIMEOUT_S,
    TapCommandFailedError,
    TapControl,
    TapPlug,
    TapUnavailableError,
    apply_devices,
)
from juice.control import is_retryable
from juice.floor_state import FloorState
from juice.store import Store

DEV = "STRIP1"
OTHER = "STRIP2"
NOW = datetime(2026, 9, 13, 16, 0, 0, tzinfo=UTC)


class Sender:
    """One connected tap's socket, as the registry sees it: a coroutine that
    takes a frame. Records what was sent and can answer on cue."""

    def __init__(self) -> None:
        self.sent: list[dict] = []

    async def __call__(self, frame: dict) -> None:
        self.sent.append(frame)

    def last_command_id(self) -> str:
        return self.sent[-1]["command_id"]


@pytest.fixture
def store():
    with Store(":memory:") as s:
        yield s


async def answer(control: TapControl, sender: Sender, status: str = "ok", error=None) -> None:
    """Let the send land, then reply as tap would."""
    await asyncio.sleep(0)
    control.resolve(
        wire.command_result_of(
            {"command_id": sender.last_command_id(), "status": status, "error": error}
        )
    )


class TestTheWire:
    def test_a_command_frame_has_what_tap_reads(self) -> None:
        frame = wire.command("c1", "turn_on", DEV, f"{DEV}03", NOW)
        assert frame == {
            "type": "command",
            "command_id": "c1",
            "kind": "turn_on",
            "device_id": DEV,
            "child_id": f"{DEV}03",
            "expires_at": "2026-09-13T16:00:00+00:00",
        }

    def test_a_result_is_decoded(self) -> None:
        assert wire.command_result_of(
            {"type": "command_result", "command_id": "c1", "status": "ok", "error": None}
        ) == ("c1", "ok", None)
        assert wire.command_result_of(
            {"type": "command_result", "command_id": "c1", "status": "error", "error": "expired"}
        ) == ("c1", "error", "expired")

    @pytest.mark.parametrize(
        "frame",
        [
            {"type": "command_result", "status": "ok"},
            {"type": "command_result", "command_id": "", "status": "ok"},
            {"type": "command_result", "command_id": "c1", "status": "maybe"},
            {"type": "command_result", "command_id": 7, "status": "ok"},
        ],
    )
    def test_an_unusable_result_is_refused(self, frame) -> None:
        with pytest.raises(wire.BadFrameError):
            wire.command_result_of(frame)


class TestRouting:
    async def test_a_command_goes_to_the_tap_that_reports_the_device(self) -> None:
        control = TapControl(now=lambda: NOW)
        a, b = Sender(), Sender()
        control.connect("tap-a", a)
        control.connect("tap-b", b)
        control.note_devices("tap-a", {DEV})
        control.note_devices("tap-b", {OTHER})

        task = asyncio.create_task(control.command("turn_on", OTHER, f"{OTHER}00"))
        await answer(control, b)
        await task

        assert a.sent == []
        assert b.sent[0]["kind"] == "turn_on"
        assert b.sent[0]["device_id"] == OTHER

    async def test_with_one_tap_connected_every_device_is_its(self) -> None:
        """A device tap has not (yet) listed in a roster still has exactly one
        place it could be."""
        control = TapControl(now=lambda: NOW)
        only = Sender()
        control.connect("tap-a", only)

        task = asyncio.create_task(control.command("turn_off", DEV, f"{DEV}00"))
        await answer(control, only)
        await task

        assert only.sent[0]["device_id"] == DEV

    async def test_no_tap_connected_refuses_immediately(self) -> None:
        control = TapControl(now=lambda: NOW)
        with pytest.raises(TapUnavailableError):
            await control.command("turn_on", DEV, f"{DEV}00")

    async def test_two_taps_and_an_unclaimed_device_refuses(self) -> None:
        """Guessing which of two taps can reach a strip is how a command lands
        on the wrong floor."""
        control = TapControl(now=lambda: NOW)
        control.connect("tap-a", Sender())
        control.connect("tap-b", Sender())
        with pytest.raises(TapUnavailableError):
            await control.command("turn_on", DEV, f"{DEV}00")

    async def test_a_roster_replaces_what_a_tap_reports(self) -> None:
        """The frame is the full roster: a strip that moved to another tap
        must stop being routed to the one that lost it."""
        control = TapControl(now=lambda: NOW)
        a, b = Sender(), Sender()
        control.connect("tap-a", a)
        control.connect("tap-b", b)
        control.note_devices("tap-a", {DEV, OTHER})
        control.note_devices("tap-b", set())
        assert control.owner_of(OTHER) == "tap-a"

        control.note_devices("tap-a", {DEV})
        control.note_devices("tap-b", {OTHER})
        assert control.owner_of(OTHER) == "tap-b"

    async def test_a_reconnecting_tap_replaces_its_old_session(self) -> None:
        control = TapControl(now=lambda: NOW)
        old, new = Sender(), Sender()
        control.connect("tap-a", old)
        control.note_devices("tap-a", {DEV})
        control.connect("tap-a", new)

        task = asyncio.create_task(control.command("turn_on", DEV, f"{DEV}00"))
        await answer(control, new)
        await task

        assert old.sent == [] and len(new.sent) == 1
        assert control.connected == ["tap-a"]

    async def test_a_stale_disconnect_does_not_drop_the_new_session(self) -> None:
        """The old socket's handler finishes *after* the new one connected;
        its disconnect must not take the live session with it."""
        control = TapControl(now=lambda: NOW)
        old, new = Sender(), Sender()
        control.connect("tap-a", old)
        control.connect("tap-a", new)
        control.disconnect("tap-a", old)
        assert control.connected == ["tap-a"]


class TestTheRoundTrip:
    async def test_ok_returns(self) -> None:
        control = TapControl(now=lambda: NOW)
        tap = Sender()
        control.connect("tap-a", tap)
        task = asyncio.create_task(control.command("turn_on", DEV, f"{DEV}00"))
        await answer(control, tap, "ok")
        assert await task == tap.last_command_id()
        assert control.pending == 0

    @pytest.mark.parametrize("error", ["expired", "unknown device STRIP1", "no devices"])
    async def test_a_refusal_fails_and_is_not_retried(self, error) -> None:
        """Nothing another attempt could change; `call_with_retry` must not
        spend 23 s finding that out."""
        control = TapControl(now=lambda: NOW)
        tap = Sender()
        control.connect("tap-a", tap)
        task = asyncio.create_task(control.command("turn_on", DEV, f"{DEV}00"))
        await answer(control, tap, "error", error)

        with pytest.raises(TapCommandFailedError) as info:
            await task
        assert not is_retryable(info.value)
        assert control.commands_failed == 1

    async def test_an_expired_refusal_points_at_the_clock(self) -> None:
        control = TapControl(now=lambda: NOW)
        tap = Sender()
        control.connect("tap-a", tap)
        task = asyncio.create_task(control.command("turn_on", DEV, f"{DEV}00"))
        await answer(control, tap, "error", "expired")
        with pytest.raises(TapCommandFailedError, match="clock on the tap box"):
            await task

    @pytest.mark.parametrize(
        "error",
        [
            "ConnectionError: 192.168.2.8 is not connected",
            "TimeoutError: ",
            "TransientError: strip busy",
        ],
    )
    async def test_a_device_error_is_retried(self, error) -> None:
        """tap's poller raises `ConnectionError` *before* its own retries when
        it has dropped the device -- a strip in a reconnect window. The cloud
        path retries "Device is offline" for the whole budget; so must this,
        and since tap does not cache failures the retry actuates again."""
        control = TapControl(now=lambda: NOW)
        tap = Sender()
        control.connect("tap-a", tap)
        task = asyncio.create_task(control.command("turn_on", DEV, f"{DEV}00"))
        await answer(control, tap, "error", error)

        with pytest.raises(TimeoutError, match=error.split(":")[0]) as info:
            await task
        assert is_retryable(info.value)

    async def test_silence_is_a_timeout_and_is_retried(self) -> None:
        """No result inside the attempt budget raises the one exception
        `call_with_retry` retries on, so the handler's existing backoff applies."""
        control = TapControl(now=lambda: NOW, result_timeout=0.05)
        tap = Sender()
        control.connect("tap-a", tap)

        with pytest.raises(TimeoutError) as info:
            await control.command("turn_on", DEV, f"{DEV}00")
        assert is_retryable(info.value)
        assert control.pending == 0, "the timed-out wait must not leak"

    async def test_a_late_result_is_ignored(self) -> None:
        control = TapControl(now=lambda: NOW, result_timeout=0.05)
        tap = Sender()
        control.connect("tap-a", tap)
        with pytest.raises(TimeoutError):
            await control.command("turn_on", DEV, f"{DEV}00")
        control.resolve((tap.last_command_id(), "ok", None))  # no future to hit
        assert control.pending == 0

    async def test_a_disconnect_mid_command_is_retried_not_refused(self) -> None:
        """tap reconnects within a second and its cache still holds the
        answer; the retry rides the reconnect with the same id. The refusal
        is for a tap that was never there (`owner_of` is None)."""
        control = TapControl(now=lambda: NOW)
        tap = Sender()
        control.connect("tap-a", tap)
        task = asyncio.create_task(control.command("turn_on", DEV, f"{DEV}00"))
        await asyncio.sleep(0)
        control.disconnect("tap-a", tap)
        with pytest.raises(TimeoutError, match="disconnected") as info:
            await task
        assert is_retryable(info.value)
        assert control.pending == 0

    async def test_a_disconnect_fails_only_that_taps_commands(self) -> None:
        control = TapControl(now=lambda: NOW)
        a, b = Sender(), Sender()
        control.connect("tap-a", a)
        control.connect("tap-b", b)
        control.note_devices("tap-a", {DEV})
        control.note_devices("tap-b", {OTHER})
        on_a = asyncio.create_task(control.command("turn_on", DEV, f"{DEV}00"))
        on_b = asyncio.create_task(control.command("turn_on", OTHER, f"{OTHER}00"))
        await asyncio.sleep(0)

        control.disconnect("tap-b", b)
        with pytest.raises(TimeoutError):
            await on_b
        await answer(control, a)
        await on_a  # unaffected

    async def test_the_frame_carries_an_expiry(self) -> None:
        control = TapControl(now=lambda: NOW)
        tap = Sender()
        control.connect("tap-a", tap)
        task = asyncio.create_task(control.command("turn_on", DEV, f"{DEV}00"))
        await answer(control, tap)
        await task
        expires = datetime.fromisoformat(tap.sent[0]["expires_at"])
        assert expires == NOW + timedelta(seconds=COMMAND_EXPIRES_S)

    async def test_a_send_that_raises_is_retried(self) -> None:
        control = TapControl(now=lambda: NOW)

        async def broken(_frame):
            raise ConnectionResetError("socket gone")

        control.connect("tap-a", broken)
        with pytest.raises(TimeoutError, match="socket gone"):
            await control.command("turn_on", DEV, f"{DEV}00")
        assert control.pending == 0


class TestRoundTripTiming:
    """Send -> result, measured on juice's side, so 'how long does a button
    take' is a number in the log and a percentile on the registry rather
    than two log lines on two boxes with two clocks."""

    async def test_an_answered_command_is_timed_and_logged(self, caplog) -> None:
        import logging

        control = TapControl(now=lambda: NOW)
        tap = Sender()
        control.connect("tap-a", tap)

        async def slow_answer() -> None:
            await asyncio.sleep(0.05)
            control.resolve((tap.last_command_id(), "ok", None))

        with caplog.at_level(logging.INFO, logger="juice.collector_tap"):
            task = asyncio.create_task(control.command("turn_on", DEV, f"{DEV}00"))
            await asyncio.sleep(0)
            await slow_answer()
            await task

        latency = control.latency()
        assert latency["n"] == 1
        assert latency["p50_ms"] >= 50
        assert latency["p50_ms"] == latency["p95_ms"] == latency["max_ms"]
        line = next(
            r.getMessage() for r in caplog.records if " ok from tap-a in " in r.getMessage()
        )
        assert line.endswith("ms"), line

    async def test_an_error_result_is_still_a_measurement(self) -> None:
        control = TapControl(now=lambda: NOW)
        tap = Sender()
        control.connect("tap-a", tap)
        task = asyncio.create_task(control.command("turn_on", DEV, f"{DEV}00"))
        await answer(control, tap, "error", "expired")
        with pytest.raises(TapCommandFailedError):
            await task
        assert control.latency()["n"] == 1

    async def test_silence_is_not_a_sample(self) -> None:
        control = TapControl(now=lambda: NOW, result_timeout=0.05)
        tap = Sender()
        control.connect("tap-a", tap)
        with pytest.raises(TimeoutError):
            await control.command("turn_on", DEV, f"{DEV}00")
        assert control.latency() == {"n": 0}
        assert control.snapshot()["commands"]["timed_out"] == 1

    def test_percentiles(self) -> None:
        control = TapControl(now=lambda: NOW)
        for ms in range(1, 101):
            control._latency_ms.append(float(ms))
        latency = control.latency()
        assert latency == {"n": 100, "p50_ms": 51.0, "p95_ms": 95.0, "max_ms": 100.0}

    async def test_the_snapshot_says_who_is_connected_and_what_they_report(self) -> None:
        control = TapControl(now=lambda: NOW)
        control.connect("bumper", Sender())
        control.note_devices("bumper", {OTHER, DEV})
        assert control.snapshot()["connected"] == [{"tap_id": "bumper", "devices": [DEV, OTHER]}]
        assert control.snapshot()["latency"] == {"n": 0}


class TestTapPlug:
    """The `plug_objects` entry. Its only job beyond forwarding is the
    redelivery id: a retry after silence re-sends the *same* command_id, so
    tap's cache answers a command it has already applied rather than throwing
    the relay a second time."""

    async def test_turn_on_and_off_send_the_right_kind(self) -> None:
        control = TapControl(now=lambda: NOW)
        tap = Sender()
        control.connect("tap-a", tap)
        plug = TapPlug(control, DEV, f"{DEV}00", "Blackout - M0013")
        assert plug.alias == "Blackout - M0013" and plug.child_id == f"{DEV}00"

        task = asyncio.create_task(plug.turn_on())
        await answer(control, tap)
        await task
        task = asyncio.create_task(plug.turn_off())
        await answer(control, tap)
        await task

        assert [f["kind"] for f in tap.sent] == ["turn_on", "turn_off"]
        assert tap.sent[0]["command_id"] != tap.sent[1]["command_id"]

    async def test_a_retry_after_silence_reuses_the_command_id(self) -> None:
        control = TapControl(now=lambda: NOW, result_timeout=0.5)
        tap = Sender()
        control.connect("tap-a", tap)
        plug = TapPlug(control, DEV, f"{DEV}00", "x")

        with pytest.raises(TimeoutError):
            await plug.turn_on()
        task = asyncio.create_task(plug.turn_on())
        await answer(control, tap)
        await task

        assert len(tap.sent) == 2
        assert tap.sent[0]["command_id"] == tap.sent[1]["command_id"]
        assert tap.sent[0]["expires_at"] == tap.sent[1]["expires_at"], (
            "a redelivery is the same command, expiry included"
        )

    async def test_a_result_ends_the_reuse(self) -> None:
        """Including an error after a silence: tap does not cache failures, so
        re-sending that id would actuate again, not replay."""
        control = TapControl(now=lambda: NOW, result_timeout=0.5)
        tap = Sender()
        control.connect("tap-a", tap)
        plug = TapPlug(control, DEV, f"{DEV}00", "x")

        with pytest.raises(TimeoutError):
            await plug.turn_on()
        task = asyncio.create_task(plug.turn_on())
        await answer(control, tap, "error", "expired")
        with pytest.raises(TapCommandFailedError):
            await task
        task = asyncio.create_task(plug.turn_on())
        await answer(control, tap)
        await task

        ids = [f["command_id"] for f in tap.sent]
        assert ids[0] == ids[1], "the retry after silence redelivered"
        assert ids[2] != ids[1], "the error ended the reuse"

    async def test_an_expired_open_command_is_not_reused(self) -> None:
        clock = [NOW]
        control = TapControl(now=lambda: clock[0], result_timeout=0.5)
        tap = Sender()
        control.connect("tap-a", tap)
        plug = TapPlug(control, DEV, f"{DEV}00", "x")

        with pytest.raises(TimeoutError):
            await plug.turn_on()
        clock[0] = NOW + timedelta(seconds=COMMAND_EXPIRES_S + 1)
        task = asyncio.create_task(plug.turn_on())
        await answer(control, tap)
        await task

        assert tap.sent[0]["command_id"] != tap.sent[1]["command_id"]

    async def test_a_different_kind_is_a_different_command(self) -> None:
        control = TapControl(now=lambda: NOW, result_timeout=0.5)
        tap = Sender()
        control.connect("tap-a", tap)
        plug = TapPlug(control, DEV, f"{DEV}00", "x")

        with pytest.raises(TimeoutError):
            await plug.turn_on()
        task = asyncio.create_task(plug.turn_off())
        await answer(control, tap)
        await task

        assert tap.sent[0]["command_id"] != tap.sent[1]["command_id"]

    async def test_the_opposite_kind_ends_the_reuse(self) -> None:
        """turn_on X unanswered; turn_off applied; turn_on again must be a new
        command -- re-sending X would get tap's cached "ok" for a relay that
        has since been opened, and the command would wait on it forever."""
        control = TapControl(now=lambda: NOW, result_timeout=0.5)
        tap = Sender()
        control.connect("tap-a", tap)
        plug = TapPlug(control, DEV, f"{DEV}00", "x")

        with pytest.raises(TimeoutError):
            await plug.turn_on()
        task = asyncio.create_task(plug.turn_off())
        await answer(control, tap)
        await task
        task = asyncio.create_task(plug.turn_on())
        await answer(control, tap)
        await task

        ids = [f["command_id"] for f in tap.sent]
        assert ids[2] != ids[0], "the turn_off in between made X stale"


class TestTheRosterInstallsThePlugs:
    def _machines(self) -> dict:
        return {"M0013": {"name": "Blackout", "year": 1980}}

    def test_apply_devices_with_control_installs_a_tap_plug(self, store: Store) -> None:
        state = FloorState()
        control = TapControl(now=lambda: NOW)
        apply_devices(
            state,
            store,
            [{"device_id": DEV, "child_id": f"{DEV}00", "alias": "Blackout - M0013"}],
            self._machines(),
            NOW,
            control=control,
        )
        plug_id = store.ensure_plug(DEV, f"{DEV}00", "Blackout - M0013")
        plug = state.plug_objects[plug_id]
        assert isinstance(plug, TapPlug)
        assert plug.alias == "Blackout - M0013"

    def test_without_control_nothing_is_installed(self, store: Store) -> None:
        """A roster projected without a command channel (a bare `create_app`,
        handler-level unit tests) must install no `TapPlug`, or a power button
        would send frames to nobody."""
        state = FloorState()
        apply_devices(
            state,
            store,
            [{"device_id": DEV, "child_id": f"{DEV}00", "alias": "Blackout - M0013"}],
            self._machines(),
            NOW,
        )
        assert state.plug_objects == {}

    def test_a_relabel_keeps_the_plug_but_renames_it(self, store: Store) -> None:
        """tap re-sends the roster every time it changes; the plug object must
        not be recreated under a command that is mid-flight on it."""
        state = FloorState()
        control = TapControl(now=lambda: NOW)
        entry = {"device_id": DEV, "child_id": f"{DEV}00", "alias": "Blackout - M0013"}
        apply_devices(state, store, [entry], self._machines(), NOW, control=control)
        plug_id = store.ensure_plug(DEV, f"{DEV}00", "Blackout - M0013")
        before = state.plug_objects[plug_id]

        apply_devices(
            state,
            store,
            [{**entry, "alias": "Lightning - M0099"}],
            self._machines(),
            NOW,
            control=control,
        )

        assert state.plug_objects[plug_id] is before
        assert before.alias == "Lightning - M0099"


class TestConstants:
    def test_the_attempt_budget_matches_the_command_contract(self) -> None:
        """`CommandRegistry` extends a command's deadline by the retry delay
        plus one attempt's budget on every retry; if a tap attempt could wait
        longer than that, the client would be told `timed_out` while the
        server was still trying."""
        from juice.commands import ATTEMPT_BUDGET_S

        assert COMMAND_RESULT_TIMEOUT_S == ATTEMPT_BUDGET_S
