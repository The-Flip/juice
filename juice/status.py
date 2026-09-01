"""The single derivation of a machine's or outlet's displayed status.

`status_vocabulary.md` is the authority for this module. Its central point: the
five ways juice used to say "on" — `is_on`, `watts >= OFF_WATTS`, `power_status`,
`State.OFF`, `offline` — are not redundant. They are a **cascade of four
independent questions** that the code kept flattening into one enum, separately,
at each render site. Six such sites existed and they disagreed.

So: four named axes, and exactly one derived status.

    reachable  Did we hear from the device recently?
    relay      Is the outlet energized? A hardware fact.
    draw       How many watts? None means unmeasurable, not zero.
    activity   What is the machine doing? None means we can't say.

`activity_unknown_because` always accompanies a null activity, so a surface can
say "uncalibrated — play time not measurable" instead of a confident blank.

This module is pure: no aiohttp, no DB, no clock. That is deliberate — it makes
the cascade exhaustively testable, and it means both the v1 and v2 HTTP layers
depend on it rather than on each other.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, get_args

from juice.collector import PlugReading
from juice.state import OFF_WATTS, Activity

Status = Literal[
    "unreachable",  # device unreachable — we know nothing current
    "off",  # relay off
    "no_draw",  # relay on, metered, drawing < OFF_WATTS
    "powered",  # drawing (or unmeasurable) but activity unknown
    "attract",  # drawing, waiting for a player
    "playing",  # drawing, game in progress
    "abandoned",  # drawing, game in progress, player walked away
]

UnknownBecause = Literal[
    "not_drawing",  # relay on but below OFF_WATTS
    "uncalibrated",  # no usable calibration — play is not measurable
    "unmetered",  # this outlet has no energy meter at all
    "no_measurement",  # metered, but this reading carried no watts
    "unreachable",  # device is offline
]

STATUSES: tuple[str, ...] = get_args(Status)

# v1's four-value `power_status` is a strict projection of the seven above, so
# there is no second cascade to drift. Delete with the v1 routes.
_V1_PROJECTION: dict[str, str] = {
    "unreachable": "offline",
    "off": "off",
    "no_draw": "no_draw",
    "powered": "on",
    "attract": "on",
    "playing": "on",
    "abandoned": "on",
}


@dataclass(frozen=True, slots=True)
class Axes:
    """The four independent facts a status is derived from."""

    reachable: bool
    relay: Literal["on", "off"]
    draw: float | None
    activity: Activity | None
    activity_unknown_because: UnknownBecause | None

    @property
    def drawing(self) -> bool:
        """A real load is present. False when unmeasurable — see `draw`."""
        return self.draw is not None and self.draw >= OFF_WATTS


def read_axes(
    reading: PlugReading | None,
    *,
    has_emeter: bool,
    offline: bool,
    activity: Activity | None = None,
    calibrated: bool = True,
) -> Axes:
    """Read the four axes off a live plug reading.

    `activity` is the classifier's latest output; pass `calibrated=False` when the
    machine has no usable calibration, in which case the activity is discarded.
    That matters: v1 forced uncalibrated machines through ATTRACT so they wouldn't
    render as an unexplained gray tile (#74). v2 keeps the same colour but reports
    `powered`, because "drawing, don't know what it's doing" is what we actually
    know.
    """
    reachable = not offline
    relay: Literal["on", "off"] = "on" if reading is not None and reading.is_on else "off"

    # `draw is None` means unmeasurable — an unmetered outlet, or a metered one
    # whose reading carried no watts. Distinct from a measured zero. The two
    # causes are reported separately below: telling an operator an outlet is
    # "unmetered" when it has a meter and merely missed a sample explains the
    # wrong thing.
    draw = reading.watts if (has_emeter and reading is not None) else None

    # An activity is something a *drawing* machine does, so anything that rules
    # out a measured load also rules out the activity — whatever the classifier
    # says. This matters in practice: the rolling watt buffer keeps a machine's
    # last busy minute after its draw collapses, so classify() will report
    # PLAYING for an outlet now reading 0 W. Passing that through would emit a
    # payload that contradicts its own status.
    because: UnknownBecause | None = None
    if not reachable:
        activity, because = None, "unreachable"
    elif relay == "off":
        activity, because = None, "not_drawing"
    elif draw is None:
        activity, because = None, ("unmetered" if not has_emeter else "no_measurement")
    elif draw < OFF_WATTS:
        activity, because = None, "not_drawing"
    elif not calibrated:
        activity, because = None, "uncalibrated"
    elif activity is None:
        because = "uncalibrated"

    return Axes(
        reachable=reachable,
        relay=relay,
        draw=draw,
        activity=activity,
        activity_unknown_because=because,
    )


def derive_status(axes: Axes) -> str:
    """Collapse the axes to one displayed status. Total: never raises, never None.

    Order matters and encodes real precedence. Unreachable outranks everything
    because a stale cached reading must never be presented as current — that is
    the "the button lied" failure mode. Relay-off outranks draw because an outlet
    we switched off has nothing meaningful to measure.
    """
    if not axes.reachable:
        return "unreachable"
    if axes.relay == "off":
        return "off"
    if axes.draw is not None and axes.draw < OFF_WATTS:
        return "no_draw"
    if axes.activity is None:
        return "powered"
    return axes.activity.value


def legacy_power_status(axes: Axes) -> str:
    """v1's `power_status` vocabulary, as a projection of `derive_status`."""
    return _V1_PROJECTION[derive_status(axes)]
