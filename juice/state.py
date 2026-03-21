"""Detect pinball machine states from power readings.

States:
  OFF     — machine unpowered (< 5W)
  ATTRACT — on, running attract mode
  PLAYING — active game, solenoids firing
  IDLE    — game started but player walked away (ultra-stable power)
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum


class State(Enum):
    OFF = "OFF"
    ATTRACT = "ATTRACT"
    PLAYING = "PLAYING"
    IDLE = "IDLE"


@dataclass(frozen=True)
class Calibration:
    """Per-machine thresholds derived from observed power signatures."""

    idle_max_rsd: float | None  # Max RSD% for IDLE. None = IDLE impossible.
    play_min_rsd: float  # Min RSD% for PLAYING.


DEFAULT_CALIBRATION = Calibration(idle_max_rsd=None, play_min_rsd=10.0)


def _rolling_ma_sd(
    watts: list[float], window: int
) -> list[tuple[float, float, int]]:
    """Compute rolling mean, std dev, and buffer size, skipping zero-watt readings."""
    result: list[tuple[float, float, int]] = []
    buf: list[float] = []
    total = 0.0
    total_sq = 0.0
    for w in watts:
        if w > 0:
            buf.append(w)
            total += w
            total_sq += w * w
            if len(buf) > window:
                old = buf.pop(0)
                total -= old
                total_sq -= old * old
        n = len(buf)
        if n > 0:
            mean = total / n
            variance = max(0.0, total_sq / n - mean * mean)
            result.append((mean, math.sqrt(variance), n))
        else:
            result.append((0.0, 0.0, 0))
    return result


def classify(
    watts: list[float],
    calibration: Calibration,
    window: int = 30,
) -> list[State]:
    """Classify each reading into a machine state."""
    stats = _rolling_ma_sd(watts, window)
    states: list[State] = []
    for w, (mean, sd, buf_size) in zip(watts, stats):
        # Check raw watt value first — a zero reading is OFF regardless
        # of what the rolling window says.
        if w < 5:
            states.append(State.OFF)
        else:
            rsd = (sd / mean) * 100 if mean > 0 else 0.0
            # Only classify as IDLE once the buffer is full — partial
            # windows have artificially low RSD.
            if (
                calibration.idle_max_rsd is not None
                and rsd < calibration.idle_max_rsd
                and buf_size >= min(window, 10)
            ):
                states.append(State.IDLE)
            elif rsd > calibration.play_min_rsd:
                states.append(State.PLAYING)
            else:
                states.append(State.ATTRACT)
    return states
