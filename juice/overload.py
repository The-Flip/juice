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

# The largest hole a window may have and still be believed. A window that
# spans two minutes but *observed* six seconds of them is not evidence of a
# sustained load; the verdict is refused until the hole has aged out, which
# delays a real overload by the window at most -- provided holes come rarer
# than one per window; a link that hiccups every minute keeps it refused, and
# that is the honest answer to a link like that. The bound is a fact about
# the collector's cadence, so there are two:
#
# - tap sends live frames at 1 Hz, and the frame re-stamps a device's last
#   values for the one or two timed-out sweeps (~15 s) before it parks the
#   device and the outlets vanish, so no juice-side bound sees meter staleness
#   below that; what this bounds is the uplink. Measured on the museum LAN
#   against a fake server (49 outlets, 11 min, n=28,244): inter-arrival p50
#   1.001 s, p99 1.003 s, max 1.006 s; a dropped socket and reconnect 2.38 s.
#   The real path adds a WAN reconnect on the second attempt (backoff, TLS,
#   hello, one live interval: ~5-6 s) and any event-loop stall here, so 10 s.
#   At 10 s one held sample is 8% of the window: to fire alone on the lowest
#   threshold on the floor it would have to read ~900 W, which nothing draws.
#   Shadow mode measures the real path (`ShadowProjector.describe_gaps`);
#   read it before overload leaves shadow under tap.
# - the cloud recorder polls devices sequentially over the WAN. On a
#   production week (Aug 29 - Sep 5, 1.04M readings on drawing outlets) the
#   gap between consecutive readings was p50 6.9 s, p99 16.3 s, p99.9 21.6 s;
#   0.02% exceeded 30 s, in ~30 fleet-wide stalls. 30 s refuses almost
#   nothing and the 120-day backtest shows it refused nothing that fired.
#
# The default is the cloud's: the collector running in production today, and
# the safe error for a caller that forgets to say which one feeds it.
TAP_MAX_GAP_S = 10.0
CLOUD_MAX_GAP_S = 30.0

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

    `verdict` fires only once the window covers a full SUSTAIN_SECONDS with no
    hole wider than `max_gap_seconds` *and* the time-weighted mean watts over it
    exceeds the machine's threshold — so it can't fire on a partially-filled
    window right after power-on, nor on a handful of samples straddling a gap.
    """

    def __init__(
        self,
        sustain_seconds: float = SUSTAIN_SECONDS,
        max_gap_seconds: float = CLOUD_MAX_GAP_S,
    ) -> None:
        self._sustain = sustain_seconds
        self.max_gap_seconds = max_gap_seconds
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

        Fires when the buffered samples span at least the full sustain window,
        no two consecutive samples are further apart than `max_gap_seconds`,
        and the time-weighted mean exceeds `threshold_for(baseline)`. The mean
        weights each sample by how long it held -- until the next one -- so
        uneven sampling cannot bias it: five readings inside one second of a
        spike are one second of spike, not five samples' worth.
        """
        if len(self._samples) < 2:
            return False, 0.0
        times = [t.timestamp() for t, _ in self._samples]
        span = times[-1] - times[0]
        if span < self._sustain:
            return False, 0.0
        # Each sample holds until the next; the last one holds nothing yet. The
        # first sample is the straddler `add()` keeps at or before the cutoff,
        # and only the part of its hold *inside* the window counts -- weighting
        # all of it would let a high reading from before the window push the
        # mean over the threshold from outside the two minutes it claims.
        start = times[-1] - self._sustain
        held = [b - a for a, b in zip(times[:-1], times[1:], strict=True)]
        watts = [w for _, w in self._samples]
        inside = [times[1] - max(times[0], start), *held[1:]]
        mean = sum(w * h for w, h in zip(watts[:-1], inside, strict=True)) / (times[-1] - start)
        if max(held) > self.max_gap_seconds:
            return False, mean
        return mean > threshold_for(baseline), mean
