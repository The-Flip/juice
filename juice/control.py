"""What a power handler needs from a plug object, and nothing else.

`RecorderState.plug_objects` holds one of these per outlet: the tap collector
puts a `juice.collector_tap.TapPlug` there, the e2e fixture a fake. The
handlers -- power, reboot, all-on/all-off, the overload shutdown -- only ever
call `turn_on()` / `turn_off()` through `call_with_retry` and read `.alias`
for the log line, so that is the whole contract. Kept as a protocol in a
module that names no collector, because the handlers must not care which one
is on duty.

`call_with_retry` is the retry policy they all actuate through, and
`is_retryable` is its whole contract: a `TimeoutError` is worth another attempt
(tap's `TapControl` raises one for silence, a dropped socket, or a device error
tap names as transient), a `aiohttp.ClientError` too (the Qingping poller
retries through the same helper), and anything else is a refusal that another
try cannot change.
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


# Backoff schedule for call_with_retry: 0.5, 1, 2, 4, 4, 4, ... capped at _MAX_DELAY.
_RETRY_BASE_DELAY = 0.5
_RETRY_MAX_DELAY = 4.0
# Granularity at which an in-flight backoff polls should_stop(). Lower = more
# responsive cancel, higher = fewer wakeups.
_RETRY_SLEEP_TICK = 0.1


def is_retryable(exc: BaseException) -> bool:
    """True for a transient failure that deserves another attempt.

    Deliberately a type check and nothing else: a caller that wants a retry
    raises `TimeoutError`, one that wants a refusal raises anything else. No
    message matching, so no error text can accidentally buy itself six more
    tries.
    """
    return isinstance(exc, TimeoutError | aiohttp.ClientError)


async def call_with_retry[T](
    fn: Callable[[], Awaitable[T]],
    *,
    should_stop: Callable[[], bool] | None = None,
    max_attempts: int | None = None,
    on_retry: Callable[[int, BaseException, float], None] | None = None,
) -> T:
    """Call fn() with retries on transient errors (`is_retryable`).

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
            if not is_retryable(e):
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
