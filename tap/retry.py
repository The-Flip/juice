"""Retry with capped, jittered backoff and an interruptible sleep.

Ported from juice's `call_with_retry` (`juice/collector.py`), which got three
things right and one thing wrong:

Right: the backoff is capped rather than unbounded; the sleep is chunked so a
cancel isn't stuck behind a four-second wait; and `on_retry` is a hook rather
than a `log.warning` buried in the primitive, so the caller decides what a retry
means.

Wrong: no jitter. A dozen devices retrying the same network blip synchronise on
0.5/1/2/4 and hammer in lockstep, which is exactly when you least want a
thundering herd. Jitter is added here.

The retryable predicate is injected rather than hardcoded so this module stays
free of any device-library import.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable

from tap.errors import TransientError

# Backoff schedule: 0.5, 1, 2, 4, 4, ... capped, then jittered down by up to 50%.
BASE_DELAY = 0.5
MAX_DELAY = 4.0
# Granularity at which an in-flight backoff polls should_stop(). Lower = more
# responsive cancel, higher = fewer wakeups.
SLEEP_TICK = 0.1


def default_retryable(exc: BaseException) -> bool:
    """Retry transient transport failures; let everything else propagate."""
    return isinstance(exc, TransientError | asyncio.TimeoutError | ConnectionError | OSError)


def backoff_delay(attempt: int, *, base: float = BASE_DELAY, cap: float = MAX_DELAY) -> float:
    """Delay before attempt N+1, capped and jittered.

    Jitter is subtractive over the top half of the interval ("equal jitter"):
    always at least half the nominal delay, so backoff still grows, but never
    the same value on two hosts at once.
    """
    nominal = min(base * (2 ** (attempt - 1)), cap)
    return nominal / 2 + random.uniform(0, nominal / 2)  # noqa: S311 — spreading load, not a secret


async def call_with_retry[T](
    fn: Callable[[], Awaitable[T]],
    *,
    retryable: Callable[[BaseException], bool] = default_retryable,
    should_stop: Callable[[], bool] | None = None,
    max_attempts: int | None = None,
    on_retry: Callable[[int, BaseException, float], None] | None = None,
) -> T:
    """Call `fn()`, retrying transient failures with jittered backoff.

    Re-raises the last exception when `should_stop()` goes true or `max_attempts`
    is exhausted; non-retryable errors propagate on the first attempt.

    `on_retry(attempt, exc, delay)` reports the just-failed attempt (1-based);
    the next one to run is `attempt + 1`.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            return await fn()
        except BaseException as e:
            if not retryable(e):
                raise
            if max_attempts is not None and attempt >= max_attempts:
                raise
            if should_stop is not None and should_stop():
                raise

            delay = backoff_delay(attempt)
            if on_retry is not None:
                on_retry(attempt, e, delay)

            # Interruptible sleep: wake every SLEEP_TICK to check should_stop.
            remaining = delay
            while remaining > 0:
                if should_stop is not None and should_stop():
                    raise
                step = min(SLEEP_TICK, remaining)
                await asyncio.sleep(step)
                remaining -= step
