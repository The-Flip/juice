"""Resolving an asset_id to the outlet a machine is on right now.

v2 addresses machines by `asset_id` (`M0021`) rather than `plug_id`, because the
asset tag is durable across outlet moves and is what operators actually know —
it's printed on the machine. See domain_model.md §7.4.

The catch is that `RecorderState.assignments` is keyed by `plug_id`, and a
machine that has just moved has **two** open assignments: a stale one on the old,
now-offline outlet, and a live one on the new outlet. `handle_machines` already
drops the stale copy, but only for display. Resolution for a *write* needs the
same rule, or a reboot lands on the dead outlet and silently does nothing — in
exactly the situation where operators are already confused.

Note `asset_id` is not always `M\\d+`. The recorder extracts tags with that
pattern from Kasa aliases, but FlipFix owns the format and the e2e fixture mints
`S0001`-style ids for no-emeter machines. Nothing here assumes a shape.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids importing the HTTP layer
    from juice.server import RecorderState


@dataclass(frozen=True)
class Resolution:
    """Where an asset_id points, and how confident we are.

    `found` and `plug_id` are deliberately independent: an ambiguous asset
    exists but has no single answer, and saying so beats guessing.
    """

    asset_id: str
    plug_id: int | None
    candidates: tuple[int, ...]
    ambiguous: bool

    @property
    def found(self) -> bool:
        return bool(self.candidates)


def resolve_asset(state: RecorderState, asset_id: str) -> Resolution:
    """Find the outlet currently hosting `asset_id`.

    Rules, in order:

    * Exactly one online claimant wins — the ordinary case, and the case just
      after a move, where the stale assignment sits on an offline device.
    * Several online claimants is **ambiguous**: two outlets carry the same tag,
      which means a Kasa label typo. Guessing would act on the wrong machine, so
      callers should report both and ask for the label to be fixed.
    * No online claimant falls back to the lowest offline plug id, so a machine
      whose device is down stays addressable for reads and renders as offline
      instead of 404-ing. Lowest-id rather than dict order, so the answer is
      stable across restarts.
    """
    candidates = sorted(
        plug_id
        for plug_id, (_name, candidate_asset, _year) in state.assignments.items()
        if candidate_asset == asset_id
    )
    if not candidates:
        return Resolution(asset_id=asset_id, plug_id=None, candidates=(), ambiguous=False)

    online = [
        plug_id
        for plug_id in candidates
        if (info := state.plugs.get(plug_id)) is None or info[0] not in state.offline_since
    ]

    if len(online) > 1:
        return Resolution(
            asset_id=asset_id, plug_id=None, candidates=tuple(candidates), ambiguous=True
        )

    plug_id = online[0] if online else candidates[0]
    return Resolution(
        asset_id=asset_id, plug_id=plug_id, candidates=tuple(candidates), ambiguous=False
    )
