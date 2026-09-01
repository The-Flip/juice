"""Tests for juice.identity — resolving an asset_id to the plug it's on now.

v2 addresses machines by asset_id (M0021) because that's durable across outlet
moves and is what operators actually know. But `state.assignments` is keyed by
plug_id, and a machine that has just moved has **two** open assignments: a stale
one on the old, now-offline outlet and a live one on the new outlet.

`handle_machines` already dedupes that, but only for display. A naive resolution
for a *write* would pick whichever came first in dict order and, half the time,
reboot the dead outlet — silently doing nothing, in precisely the situation
(a machine was just moved) where operators are already confused.
"""

from __future__ import annotations

from datetime import UTC, datetime

from juice.identity import resolve_asset
from juice.server import RecorderState

DEV_A = "AAAA"
DEV_B = "BBBB"


def _state() -> RecorderState:
    return RecorderState()


def _assign(state: RecorderState, plug_id: int, device_id: str, asset_id: str) -> None:
    state.plugs[plug_id] = (device_id, f"{device_id}{plug_id:02d}", f"Thing - {asset_id}")
    state.assignments[plug_id] = (f"Machine {asset_id}", asset_id, 1980)


class TestResolve:
    def test_finds_the_single_assignment(self) -> None:
        state = _state()
        _assign(state, 5, DEV_A, "M0021")

        res = resolve_asset(state, "M0021")

        assert res.found
        assert res.plug_id == 5
        assert res.ambiguous is False

    def test_unknown_asset_is_not_found(self) -> None:
        res = resolve_asset(_state(), "M9999")
        assert res.found is False
        assert res.plug_id is None

    def test_accepts_non_m_prefixed_asset_ids(self) -> None:
        """FlipFix owns the format. The e2e fixture mints S0001-style ids for
        no-emeter machines, so anything assuming M\\d+ breaks on real data."""
        state = _state()
        _assign(state, 3, DEV_A, "S0001")
        assert resolve_asset(state, "S0001").plug_id == 3


class TestMovedMachine:
    def test_prefers_the_online_outlet_over_the_stale_offline_one(self) -> None:
        """The whole point. Same rule handle_machines uses for display, applied
        to resolution so a write can't land on the dead outlet."""
        state = _state()
        _assign(state, 5, DEV_A, "M0021")  # old outlet, device now offline
        _assign(state, 9, DEV_B, "M0021")  # new outlet
        state.offline_since[DEV_A] = datetime.now(UTC)

        res = resolve_asset(state, "M0021")

        assert res.plug_id == 9
        assert res.ambiguous is False

    def test_order_of_assignments_does_not_matter(self) -> None:
        """dict order must not decide which outlet a reboot lands on."""
        state = _state()
        _assign(state, 9, DEV_B, "M0021")  # live one inserted first this time
        _assign(state, 5, DEV_A, "M0021")
        state.offline_since[DEV_A] = datetime.now(UTC)

        assert resolve_asset(state, "M0021").plug_id == 9

    def test_all_offline_still_resolves_deterministically(self) -> None:
        """A machine whose device is down must stay addressable for reads —
        the tile has to render OFFLINE rather than 404."""
        state = _state()
        _assign(state, 9, DEV_B, "M0021")
        _assign(state, 5, DEV_A, "M0021")
        state.offline_since[DEV_A] = datetime.now(UTC)
        state.offline_since[DEV_B] = datetime.now(UTC)

        res = resolve_asset(state, "M0021")

        assert res.found
        assert res.plug_id == 5  # lowest, so it's stable across restarts
        assert res.ambiguous is False


class TestAmbiguous:
    def test_two_online_claimants_is_ambiguous(self) -> None:
        """A Kasa label typo puts the same tag on two live outlets. Guessing
        would silently act on the wrong machine; better to say which two."""
        state = _state()
        _assign(state, 5, DEV_A, "M0021")
        _assign(state, 9, DEV_B, "M0021")

        res = resolve_asset(state, "M0021")

        assert res.ambiguous is True
        assert res.plug_id is None
        assert set(res.candidates) == {5, 9}

    def test_ambiguity_is_reported_even_though_it_is_findable(self) -> None:
        state = _state()
        _assign(state, 5, DEV_A, "M0021")
        _assign(state, 9, DEV_B, "M0021")

        res = resolve_asset(state, "M0021")

        assert res.found is True  # the asset exists...
        assert res.plug_id is None  # ...but we will not guess which outlet
