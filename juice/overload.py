"""Sustained-overload detection.

A stuck solenoid (or similar fault) holds a coil energized, so the machine draws
an abnormally high load *continuously* for minutes — unlike normal gameplay,
which only spikes briefly as individual solenoids fire. We detect the former by
watching a trailing time-window of power readings and firing when the *average*
over the whole window exceeds a per-machine threshold.

The threshold is relative to each machine's own baseline (machines vary widely —
some normally sustain 200W+), with an absolute floor so low-baseline machines
aren't tripped by a modest bump.

This module is pure (no I/O, no clock): the caller supplies timestamps and the
baseline. The recorder feeds live readings through it; the `overload-report` CLI
replays historical readings through the *same* logic so the backtest matches
production exactly.
"""

from __future__ import annotations

from collections import deque
from datetime import datetime

# Fire when the trailing-window average exceeds REL_MULTIPLIER x the machine's
# baseline, but never below FLOOR_WATTS. Validated against production data: real
# incidents ran at 3.6-3.9x baseline; the highest sustained level of any healthy
# machine was ~2.0x, so 2.5x sits in open space with zero historical false
# positives.
REL_MULTIPLIER = 2.5
FLOOR_WATTS = 80.0

# The load must stay high for this long (seconds) before we act, so transient
# solenoid spikes and power-on inrush never trigger a shutdown.
SUSTAIN_SECONDS = 120

# Baseline = this quantile of per-minute average watts over the trailing window
# of days. Minute-averaging removes transient spikes; the high quantile absorbs
# brief past incidents. A machine needs at least MIN_BASELINE_MINUTES of "on"
# history before it's armed (otherwise it's never auto-shut-down — fail-safe).
BASELINE_DAYS = 30
BASELINE_QUANTILE = 0.99
MIN_BASELINE_MINUTES = 500

# Auto-shutdown behavior, set via JUICE_OVERLOAD_PROTECTION:
#   'live'   — detect and shut machines down (default)
#   'shadow' — detect and log/audit only, no power action
#   'off'    — disable detection entirely
OVERLOAD_MODES = ("live", "shadow", "off")


def threshold_for(baseline: float) -> float:
    """Watts above which a sustained load is an overload for this machine."""
    return max(REL_MULTIPLIER * baseline, FLOOR_WATTS)


def resolve_overload_mode(raw: str | None) -> str:
    """Normalize a JUICE_OVERLOAD_PROTECTION value to a valid mode.

    Unrecognized values (typos) fall back to 'live' rather than silently
    disabling protection — the safety feature fails toward protecting machines.
    """
    mode = (raw or "live").lower()
    return mode if mode in OVERLOAD_MODES else "live"


class OverloadWindow:
    """Trailing time-window of (timestamp, watts) for one plug.

    `verdict` fires only once the window covers a full SUSTAIN_SECONDS *and* the
    mean watts over it exceeds the machine's threshold — so it can't fire on a
    partially-filled window right after power-on.
    """

    def __init__(self, sustain_seconds: float = SUSTAIN_SECONDS) -> None:
        self._sustain = sustain_seconds
        self._samples: deque[tuple[datetime, float]] = deque()

    def add(self, ts: datetime, watts: float) -> None:
        """Append a reading and trim to just cover the trailing sustain window.

        Keeps one sample at/just before the cutoff (the "straddler") so the
        retained samples actually *bracket* a full SUSTAIN_SECONDS of history —
        otherwise the span would always fall just short of the window and never
        satisfy `verdict`'s coverage check on real, unaligned timestamps.
        """
        # A gap longer than the window means we have no idea what the load did in
        # between — start fresh rather than bridging stale watts across the gap
        # (which could look "full" with only a couple of samples and misfire).
        if self._samples and ts.timestamp() - self._samples[-1][0].timestamp() > self._sustain:
            self._samples.clear()
        self._samples.append((ts, watts))
        cutoff = ts.timestamp() - self._sustain
        while len(self._samples) >= 2 and self._samples[1][0].timestamp() <= cutoff:
            self._samples.popleft()

    def reset(self) -> None:
        """Forget all buffered samples (e.g. after acting on an overload)."""
        self._samples.clear()

    def peak(self) -> float:
        """Highest watts currently in the window (0 if empty)."""
        return max((w for _, w in self._samples), default=0.0)

    def verdict(self, baseline: float) -> tuple[bool, float]:
        """Return (fire, window_mean_watts) for the current window.

        Fires when the buffered samples span at least the full sustain window and
        their mean exceeds `threshold_for(baseline)`.
        """
        if len(self._samples) < 2:
            return False, 0.0
        span = self._samples[-1][0].timestamp() - self._samples[0][0].timestamp()
        if span < self._sustain:
            return False, 0.0
        mean = sum(w for _, w in self._samples) / len(self._samples)
        return mean > threshold_for(baseline), mean
