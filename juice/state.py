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


class CalibrationError(Exception):
    """Raised when auto-calibration cannot derive valid thresholds."""


def _despike(watts: list[float], half: int = 5, threshold: float = 0.25) -> list[float]:
    """Replace brief downward power dips with the local median.

    For each non-zero reading, if it falls more than *threshold* (fractionally)
    below the median of a surrounding window (2*half+1 readings), replace it
    with that median.  This removes isolated power dips (e.g. Blackout's
    periodic attract-mode glitch) without suppressing the upward solenoid
    spikes that characterise real gameplay.
    """
    result = list(watts)
    for i in range(len(watts)):
        if watts[i] <= 0:
            continue
        lo = max(0, i - half)
        hi = min(len(watts), i + half + 1)
        neighbors = sorted(w for w in watts[lo:hi] if w > 0)
        if not neighbors:
            continue
        med = neighbors[len(neighbors) // 2]
        # Only despike when the upper half of the window is stable.
        # During gameplay the upper readings are spread out; during attract
        # with a dip they stay tight (the dip only affects the lower tail).
        upper = [v for v in neighbors if v >= med]
        if med > 0 and len(upper) >= 2:
            upper_spread = (upper[-1] - upper[0]) / med  # already sorted
            if upper_spread < 0.10 and (med - watts[i]) / med > threshold:
                result[i] = med
    return result


def _rolling_ma_sd(watts: list[float], window: int) -> list[tuple[float, float, int]]:
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
    watts = _despike(watts)
    stats = _rolling_ma_sd(watts, window)
    states: list[State] = []
    for w, (mean, sd, buf_size) in zip(watts, stats, strict=False):
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


def auto_calibrate(watts: list[float], window: int = 30) -> Calibration:
    """Derive calibration thresholds from ~1 hour of power data.

    Expects the data to contain at least 1 minute each of attract and play.
    Returns a Calibration with derived idle_max_rsd and play_min_rsd.
    Raises CalibrationError if the data is insufficient or ambiguous.

    Algorithm: builds a 1%-wide histogram of rolling RSD values, smooths it,
    finds the ATTRACT peak, then locates the valley (trough) between ATTRACT
    and PLAYING by scanning right for the first increase after a decrease.
    IDLE is detected by scanning left from the peak for a similar trough
    above a low-RSD cluster.
    """
    watts = _despike(watts)
    # Filter OFF readings
    on_watts = [w for w in watts if w >= 5]
    if len(on_watts) < 60:
        raise CalibrationError(f"Not enough non-OFF readings (need 60, got {len(on_watts)})")

    # Compute rolling RSD for each point
    stats = _rolling_ma_sd(on_watts, window)
    rsds: list[float] = []
    for mean, sd, buf_size in stats:
        if buf_size >= 10 and mean > 0:
            rsds.append((sd / mean) * 100)

    if len(rsds) < 60:
        raise CalibrationError(f"Not enough valid RSD samples (need 60, got {len(rsds)})")

    # Build histogram with 1% bins
    bin_w = 1.0
    n_bins = int(max(rsds) / bin_w) + 1
    counts = [0] * n_bins
    for r in rsds:
        counts[min(int(r / bin_w), n_bins - 1)] += 1

    # Smooth with 3-bin moving average
    sm = [0.0] * n_bins
    for i in range(n_bins):
        lo, hi = max(0, i - 1), min(n_bins - 1, i + 1)
        sm[i] = sum(counts[lo : hi + 1]) / (hi - lo + 1)

    # Find primary peak (ATTRACT mode — the densest RSD region)
    peak_idx = max(range(n_bins), key=lambda i: sm[i])
    peak_count = sm[peak_idx]

    # ATTRACT/PLAYING boundary: scan right from peak, find the first trough
    # (smoothed count increases after having dropped below 50% of peak)
    valley_idx: int | None = None
    for i in range(peak_idx + 1, n_bins - 1):
        if sm[i + 1] > sm[i] and sm[i] < peak_count * 0.5:
            valley_idx = i
            break
    # Fallback: first bin below 15% of peak
    if valley_idx is None:
        for i in range(peak_idx + 1, n_bins):
            if sm[i] < peak_count * 0.15:
                valley_idx = i
                break

    if valley_idx is None:
        raise CalibrationError("No clear separation between attract and playing")

    play_min_rsd = valley_idx * bin_w

    # Verify enough playing readings exist beyond the valley
    playing_count = sum(counts[valley_idx:])
    if playing_count < 30:
        raise CalibrationError(f"Too few playing readings ({playing_count}, need 30)")

    # Phase 2: IDLE detection on attract+idle subset only.
    # Filtering out playing readings makes the idle cluster visible even when
    # it's a small fraction of total readings.
    idle_max_rsd: float | None = None
    attract_idle_rsds = [r for r in rsds if r < play_min_rsd]

    if len(attract_idle_rsds) >= 30:
        ai_bin_w = 0.5
        ai_n_bins = int(max(attract_idle_rsds) / ai_bin_w) + 1
        ai_counts = [0] * ai_n_bins
        for r in attract_idle_rsds:
            ai_counts[min(int(r / ai_bin_w), ai_n_bins - 1)] += 1

        ai_sm = [0.0] * ai_n_bins
        for i in range(ai_n_bins):
            lo, hi = max(0, i - 1), min(ai_n_bins - 1, i + 1)
            ai_sm[i] = sum(ai_counts[lo : hi + 1]) / (hi - lo + 1)

        ai_peak_idx = max(range(ai_n_bins), key=lambda i: ai_sm[i])
        ai_peak_count = ai_sm[ai_peak_idx]

        if ai_peak_idx > 2:
            for i in range(ai_peak_idx - 1, 0, -1):
                if ai_sm[i - 1] > ai_sm[i] and ai_sm[i] < ai_peak_count * 0.5:
                    idle_boundary = (i + 1) * ai_bin_w
                    idle_count = sum(ai_counts[: i + 1])
                    if idle_boundary <= 5.0 and idle_count >= 10:
                        idle_max_rsd = idle_boundary
                    break

    return Calibration(idle_max_rsd=idle_max_rsd, play_min_rsd=play_min_rsd)
