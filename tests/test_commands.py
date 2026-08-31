"""Tests for juice.commands — the pending-action contract, moved server-side.

Today this rule lives in the browser (`juice/web/power.js`): a write's response is
never treated as completion; the client waits for a corroborating relay reading,
with a hardcoded per-action timeout, reimplemented per page. `pcReduceReading`'s
table is the spec, so these tests port it — if the contract shifted during the
move, that's a bug, and this is where it shows up.

The freshness guard is the part that has no equivalent in the JS version, and it
matters most: `PlugReading` carries no timestamp, so without it a `turn_on` can
"confirm" instantly against a stale cached `is_on=True` from a device that has
since gone dark. That is the button-lied failure the whole design exists to kill.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from juice.commands import (
    POWER_TIMEOUT_MS,
    REBOOT_TIMEOUT_MS,
    TERMINAL_PHASES,
    CommandRegistry,
)

T0 = datetime(2026, 8, 31, 12, 0, 0, tzinfo=UTC)


class _Clock:
    def __init__(self, now: datetime = T0) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


def _registry(clock: _Clock | None = None) -> tuple[CommandRegistry, list[dict]]:
    published: list[dict] = []
    reg = CommandRegistry(publish=published.append, now=clock or _Clock())
    return reg, published


def _open(reg: CommandRegistry, kind: str = "turn_on", plug_id: int = 7):
    return reg.open(kind=kind, plug_id=plug_id, asset_id="M0021", actor="dana", source="individual")


class TestOpen:
    def test_mints_an_id_and_announces_it(self) -> None:
        reg, published = _registry()
        cmd = _open(reg)

        assert cmd.id
        assert cmd.phase == "accepted"
        assert published[-1]["type"] == "command"
        assert published[-1]["command_id"] == cmd.id
        assert published[-1]["phase"] == "accepted"

    def test_expected_relay_follows_the_kind(self) -> None:
        reg, _ = _registry()
        assert _open(reg, "turn_on").expect_relay is True
        assert _open(reg, "turn_off", plug_id=8).expect_relay is False
        assert _open(reg, "reboot", plug_id=9).expect_relay is True

    def test_timeout_covers_the_worst_case_retry_budget(self) -> None:
        """The client must not give up before the server has stopped trying —
        otherwise a machine comes up after the UI declared failure."""
        assert POWER_TIMEOUT_MS > 11_500  # 0.5+1+2+4+4s of backoff across 6 attempts
        assert REBOOT_TIMEOUT_MS > POWER_TIMEOUT_MS  # two legs plus the hold


class TestInFlight:
    def test_finds_an_active_command(self) -> None:
        reg, _ = _registry()
        cmd = _open(reg)
        assert reg.in_flight_for_plug(7) is cmd

    def test_a_terminal_command_is_not_in_flight(self) -> None:
        reg, _ = _registry()
        cmd = _open(reg)
        reg.advance(cmd, "confirmed")
        assert reg.in_flight_for_plug(7) is None

    def test_other_plugs_are_unaffected(self) -> None:
        reg, _ = _registry()
        _open(reg, plug_id=7)
        assert reg.in_flight_for_plug(8) is None


class TestReconcileConfirms:
    def test_turn_on_confirms_on_a_fresh_relay_on_reading(self) -> None:
        clock = _Clock()
        reg, published = _registry(clock)
        cmd = _open(reg, "turn_on")
        reg.record_dispatched(cmd)  # the cloud call returned

        clock.advance(2)
        reg.reconcile(7, relay_on=True, reading_ts=clock.now)

        assert cmd.phase == "confirmed"
        assert published[-1]["phase"] == "confirmed"

    def test_turn_off_confirms_on_a_fresh_relay_off_reading(self) -> None:
        clock = _Clock()
        reg, _ = _registry(clock)
        cmd = _open(reg, "turn_off")
        reg.record_dispatched(cmd)

        clock.advance(2)
        reg.reconcile(7, relay_on=False, reading_ts=clock.now)

        assert cmd.phase == "confirmed"

    def test_the_wrong_relay_state_does_not_confirm(self) -> None:
        clock = _Clock()
        reg, _ = _registry(clock)
        cmd = _open(reg, "turn_on")

        clock.advance(2)
        reg.reconcile(7, relay_on=False, reading_ts=clock.now)

        assert cmd.phase != "confirmed"


class TestConfirmRequiresDispatch:
    """A command cannot confirm before the cloud call has completed.

    The handler is still inside call_with_retry when the first readings ticks
    arrive. If the plug already happens to hold the requested relay state, a
    naive reconcile confirms immediately — and then record_dispatched drags a
    *terminal* command back to awaiting_relay, where it can later time out.
    """

    def test_a_matching_reading_before_dispatch_does_not_confirm(self) -> None:
        clock = _Clock()
        reg, _ = _registry(clock)
        cmd = _open(reg, "turn_on")  # still 'accepted'; cloud call in flight

        clock.advance(1)
        reg.reconcile(7, relay_on=True, reading_ts=clock.now)

        assert cmd.phase == "accepted"

    def test_it_confirms_normally_once_dispatched(self) -> None:
        clock = _Clock()
        reg, _ = _registry(clock)
        cmd = _open(reg, "turn_on")
        clock.advance(1)
        reg.reconcile(7, relay_on=True, reading_ts=clock.now)

        reg.record_dispatched(cmd)
        clock.advance(1)
        reg.reconcile(7, relay_on=True, reading_ts=clock.now)

        assert cmd.phase == "confirmed"

    def test_a_retrying_command_does_not_confirm(self) -> None:
        clock = _Clock()
        reg, _ = _registry(clock)
        cmd = _open(reg, "turn_on")
        reg.record_retry(cmd, attempt=1, error="timeout", delay=0.5)

        clock.advance(1)
        reg.reconcile(7, relay_on=True, reading_ts=clock.now)

        assert cmd.phase == "retrying"

    def test_a_reboot_still_collects_saw_off_before_dispatch(self) -> None:
        """The off leg completes and the 3s hold runs while the command is still
        pre-dispatch, so the observed OFF must be recorded even though it is too
        early to confirm."""
        clock = _Clock()
        reg, _ = _registry(clock)
        cmd = _open(reg, "reboot")

        clock.advance(1)
        reg.reconcile(7, relay_on=False, reading_ts=clock.now)

        assert cmd.saw_off is True
        assert cmd.phase == "accepted"

    def test_a_reboot_does_not_confirm_until_the_on_leg_completes(self) -> None:
        clock = _Clock()
        reg, _ = _registry(clock)
        cmd = _open(reg, "reboot")
        clock.advance(1)
        reg.reconcile(7, relay_on=False, reading_ts=clock.now)  # off leg observed

        clock.advance(1)
        reg.reconcile(7, relay_on=True, reading_ts=clock.now)  # machine bounced early
        assert cmd.phase != "confirmed"

        reg.mark_legs_acked(cmd)
        reg.record_dispatched(cmd)
        clock.advance(1)
        reg.reconcile(7, relay_on=True, reading_ts=clock.now)
        assert cmd.phase == "confirmed"


class TestFreshnessGuard:
    """The guard with no equivalent in power.js — and the one that matters most."""

    def test_a_reading_older_than_the_command_is_ignored(self) -> None:
        clock = _Clock()
        reg, _ = _registry(clock)
        cmd = _open(reg, "turn_on")

        # A cached reading from before we asked. It says relay-on, but it cannot
        # be evidence that *our* command worked.
        reg.reconcile(7, relay_on=True, reading_ts=T0 - timedelta(seconds=5))

        assert cmd.phase != "confirmed"

    def test_a_reading_exactly_at_issue_time_is_ignored(self) -> None:
        clock = _Clock()
        reg, _ = _registry(clock)
        cmd = _open(reg, "turn_on")

        reg.reconcile(7, relay_on=True, reading_ts=cmd.issued_at)

        assert cmd.phase != "confirmed"

    def test_a_stale_reading_from_a_dead_device_never_confirms(self) -> None:
        """The concrete failure: a plug whose device just went offline still has
        a cached is_on=True. Without the guard, turn_on confirms instantly
        against a device that is not listening."""
        clock = _Clock()
        reg, _ = _registry(clock)
        cmd = _open(reg, "turn_on")
        stale = T0 - timedelta(minutes=3)

        for _ in range(10):
            clock.advance(1)
            reg.reconcile(7, relay_on=True, reading_ts=stale)

        assert cmd.phase != "confirmed"


class TestReboot:
    def test_relay_on_alone_does_not_confirm_a_reboot(self) -> None:
        """The pre-off 'on' would settle prematurely — pcReduceReading's sawOff
        rule, which is why the JS tracks it at all."""
        clock = _Clock()
        reg, _ = _registry(clock)
        cmd = _open(reg, "reboot")

        clock.advance(1)
        reg.reconcile(7, relay_on=True, reading_ts=clock.now)

        assert cmd.phase != "confirmed"

    def test_an_observed_off_then_on_confirms(self) -> None:
        clock = _Clock()
        reg, _ = _registry(clock)
        cmd = _open(reg, "reboot")

        clock.advance(1)
        reg.reconcile(7, relay_on=False, reading_ts=clock.now)  # the off leg
        assert cmd.saw_off is True

        reg.record_dispatched(cmd)  # the power-on leg returned
        clock.advance(1)
        reg.reconcile(7, relay_on=True, reading_ts=clock.now)
        assert cmd.phase == "confirmed"
        assert cmd.confirmed_by == "relay_cycle"

    def test_both_legs_acked_confirms_without_observing_the_off(self) -> None:
        """The 3s off window can fall between 1Hz polls. The server knows both
        cloud calls returned ok, which is better evidence than the client's
        onConfirmed hack — but it still waits for a real relay-on reading."""
        clock = _Clock()
        reg, _ = _registry(clock)
        cmd = _open(reg, "reboot")
        reg.mark_legs_acked(cmd)
        reg.record_dispatched(cmd)

        clock.advance(1)
        reg.reconcile(7, relay_on=True, reading_ts=clock.now)

        assert cmd.phase == "confirmed"
        assert cmd.confirmed_by == "ack_and_relay"

    def test_legs_acked_alone_is_not_enough(self) -> None:
        reg, _ = _registry()
        cmd = _open(reg, "reboot")
        reg.mark_legs_acked(cmd)
        assert cmd.phase != "confirmed"


class TestRetryAndFailure:
    def test_retry_raises_the_attempt_count_and_announces_it(self) -> None:
        reg, published = _registry()
        cmd = _open(reg)

        reg.record_retry(cmd, attempt=1, error="Device is offline", delay=0.5)

        assert cmd.attempt == 2
        assert cmd.phase == "retrying"
        assert published[-1]["attempt"] == 2
        assert published[-1]["error"] == "Device is offline"

    def test_retrying_extends_the_deadline(self) -> None:
        """A reboot's on-leg can still be retrying at the original deadline; if
        we time out there, the machine comes up later with nobody listening and
        the operator taps reboot again."""
        reg, _ = _registry()
        cmd = _open(reg)
        first = cmd.deadline

        reg.record_retry(cmd, attempt=1, error="timeout", delay=4.0)

        assert cmd.deadline > first

    def test_failure_is_terminal_and_carries_the_reason(self) -> None:
        reg, published = _registry()
        cmd = _open(reg)

        reg.record_failure(cmd, "Device is offline")

        assert cmd.phase == "failed"
        assert cmd.phase in TERMINAL_PHASES
        assert published[-1]["error"] == "Device is offline"


class TestSweep:
    def test_expires_a_command_past_its_deadline(self) -> None:
        clock = _Clock()
        reg, published = _registry(clock)
        cmd = _open(reg)

        clock.advance(POWER_TIMEOUT_MS / 1000 + 1)
        reg.sweep()

        assert cmd.phase == "timed_out"
        assert published[-1]["phase"] == "timed_out"
        assert published[-1]["attempt"] == cmd.attempt

    def test_does_not_expire_a_live_command(self) -> None:
        clock = _Clock()
        reg, _ = _registry(clock)
        cmd = _open(reg)

        clock.advance(1)
        reg.sweep()

        assert cmd.phase != "timed_out"

    def test_forgets_terminal_commands_so_the_registry_cannot_leak(self) -> None:
        clock = _Clock()
        reg, _ = _registry(clock)
        cmd = _open(reg)
        reg.advance(cmd, "confirmed")

        clock.advance(3600)
        reg.sweep()

        assert reg.get(cmd.id) is None

    def test_a_terminal_command_is_never_reconciled_again(self) -> None:
        clock = _Clock()
        reg, _ = _registry(clock)
        cmd = _open(reg, "turn_on")
        reg.record_failure(cmd, "boom")

        clock.advance(2)
        reg.reconcile(7, relay_on=True, reading_ts=clock.now)

        assert cmd.phase == "failed"


class TestIdempotency:
    def test_repeating_the_same_action_returns_the_existing_command(self) -> None:
        """A double-tap on a phone must not fire two reboots."""
        reg, _ = _registry()
        first = _open(reg, "reboot")
        second = _open(reg, "reboot")

        assert second is first

    def test_open_reports_whether_it_created_the_command(self) -> None:
        """Returning the existing command is not enough on its own — the caller
        must know not to dispatch again, or a double-tap still sends two cloud
        calls and spawns two reboot power-on tasks."""
        reg, _ = _registry()
        first, created_first = reg.open_ex(
            kind="reboot", plug_id=7, asset_id="M0021", actor="dana", source="individual"
        )
        second, created_second = reg.open_ex(
            kind="reboot", plug_id=7, asset_id="M0021", actor="dana", source="individual"
        )

        assert second is first
        assert created_first is True
        assert created_second is False

    def test_a_conflicting_action_conflicts_while_the_cloud_call_is_in_flight(self) -> None:
        reg, _ = _registry()
        on = _open(reg, "turn_on")
        assert reg.in_flight_for_plug(7) is on
        # The handler is expected to 409 rather than open a second command; the
        # registry's job is only to report the conflict.
        assert reg.conflicts(7, "turn_off") is on
        assert reg.conflicts(7, "turn_on") is None


class TestSupersede:
    """Refusal is scoped to the genuinely racy window.

    Once the cloud call has landed and we are only waiting for the relay to
    agree, a new opposing command is a legitimate operator decision — and
    blocking it would mean an operator could not cut power for the whole timeout,
    which is exactly wrong when a machine is smoking.
    """

    def test_awaiting_relay_does_not_conflict(self) -> None:
        reg, _ = _registry()
        cmd = _open(reg, "turn_on")
        reg.record_dispatched(cmd)

        assert reg.conflicts(7, "turn_off") is None

    def test_the_superseded_command_is_retired_not_left_hanging(self) -> None:
        reg, _ = _registry()
        first = _open(reg, "turn_on")
        reg.record_dispatched(first)

        second = reg.open(
            kind="turn_off", plug_id=7, asset_id="M0021", actor="sam", source="individual"
        )

        assert second is not first
        assert first.phase == "superseded"
        assert reg.in_flight_for_plug(7) is second

    def test_a_superseded_command_cannot_later_confirm(self) -> None:
        """Otherwise it would settle against the *new* command's relay change and
        report success for an action that was abandoned."""
        clock = _Clock()
        reg, _ = _registry(clock)
        first = _open(reg, "turn_on")
        reg.record_dispatched(first)
        reg.open(kind="turn_off", plug_id=7, asset_id="M0021", actor="sam", source="individual")

        clock.advance(2)
        reg.reconcile(7, relay_on=True, reading_ts=clock.now)

        assert first.phase == "superseded"

    def test_a_still_dispatching_command_is_refused_not_superseded(self) -> None:
        reg, _ = _registry()
        first = _open(reg, "turn_on")

        assert reg.conflicts(7, "turn_off") is first
        assert first.phase == "accepted"
