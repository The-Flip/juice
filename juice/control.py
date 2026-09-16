"""What a power handler needs from a plug object, and nothing else.

`RecorderState.plug_objects` holds one of these per outlet. The cloud collector
puts a `juice.collector.Plug` (or `_SelfPlug`) there; the tap collector a
`juice.collector_tap.TapPlug`; the e2e fixture a fake. The handlers -- power,
reboot, all-on/all-off, the overload shutdown -- only ever call `turn_on()` /
`turn_off()` through `call_with_retry` and read `.alias` for the log line, so
that is the whole contract. Kept as a protocol in a module that names no
collector, because the handlers must not care which one is on duty.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Protocol, runtime_checkable

import aiohttp


@runtime_checkable
class Controllable(Protocol):
    alias: str

    async def turn_on(self) -> None: ...

    async def turn_off(self) -> None: ...


# Transient cloud-API messages worth retrying. Matches the three error patterns
# observed in production audit logs (the second covers "Device is offline" and
# "Device is offline during processing").
_RETRYABLE_PASSTHROUGH_MESSAGES = ("Request timeout", "Device is offline")

# Backoff schedule for call_with_retry: 0.5, 1, 2, 4, 4, 4, ... capped at _MAX_DELAY.
_RETRY_BASE_DELAY = 0.5
_RETRY_MAX_DELAY = 4.0
# Granularity at which an in-flight backoff polls should_stop(). Lower = more
# responsive cancel, higher = fewer wakeups.
_RETRY_SLEEP_TICK = 0.1


def is_retryable_passthrough_error(exc: BaseException) -> bool:
    """True for transient power-control failures that deserve another attempt."""
    if isinstance(exc, asyncio.TimeoutError | aiohttp.ClientError):
        return True
    if isinstance(exc, RuntimeError):
        msg = str(exc)
        if msg.startswith("Passthrough failed: "):
            return any(m in msg for m in _RETRYABLE_PASSTHROUGH_MESSAGES)
    return False


async def call_with_retry[T](
    fn: Callable[[], Awaitable[T]],
    *,
    should_stop: Callable[[], bool] | None = None,
    max_attempts: int | None = None,
    on_retry: Callable[[int, BaseException, float], None] | None = None,
) -> T:
    """Call fn() with retries on transient passthrough errors.

    Delays double each attempt up to _RETRY_MAX_DELAY. Each backoff is chunked
    into _RETRY_SLEEP_TICK slices so should_stop() is polled while sleeping.
    Re-raises the last exception when should_stop returns True or max_attempts
    is exhausted; non-retryable errors propagate immediately.

    on_retry(attempt, exc, delay) is invoked between attempts so callers can
    observe progress (logging, SSE events). `attempt` is the just-failed
    attempt (1-based); the next attempt about to run is `attempt + 1`.
    """
    attempt = 0
    last_exc: BaseException | None = None
    while True:
        attempt += 1
        try:
            return await fn()
        except BaseException as e:
            if not is_retryable_passthrough_error(e):
                raise
            last_exc = e
            if max_attempts is not None and attempt >= max_attempts:
                raise
            if should_stop is not None and should_stop():
                raise

            delay = min(_RETRY_BASE_DELAY * (2 ** (attempt - 1)), _RETRY_MAX_DELAY)
            if on_retry is not None:
                on_retry(attempt, e, delay)

            # Interruptible sleep: wake every _RETRY_SLEEP_TICK to check should_stop.
            remaining = delay
            while remaining > 0:
                if should_stop is not None and should_stop():
                    raise last_exc from None
                step = min(_RETRY_SLEEP_TICK, remaining)
                await asyncio.sleep(step)
                remaining -= step
