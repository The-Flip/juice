"""`call_with_retry`: backoff shape, cancellation, and what counts as retryable.

Ported from juice's version, which had no jitter — a dozen devices retrying the
same blip would synchronise on 0.5/1/2/4 and hammer in lockstep.
"""

from __future__ import annotations

import asyncio

import pytest

from tap.errors import DeviceAuthError, TransientError
from tap.retry import (
    BASE_DELAY,
    MAX_DELAY,
    backoff_delay,
    call_with_retry,
    default_retryable,
)


class TestBackoff:
    def test_it_grows_and_then_caps(self):
        # Jitter makes each value a range, so compare bounds rather than values.
        for attempt in range(1, 8):
            nominal = min(BASE_DELAY * 2 ** (attempt - 1), MAX_DELAY)
            delay = backoff_delay(attempt)
            assert nominal / 2 <= delay <= nominal

    def test_it_never_exceeds_the_cap(self):
        assert all(backoff_delay(a) <= MAX_DELAY for a in range(1, 30))

    def test_it_is_jittered(self):
        """The whole point: N devices must not retry in lockstep."""
        values = {backoff_delay(4) for _ in range(200)}
        assert len(values) > 50, "delays look deterministic"

    def test_it_still_grows_despite_jitter(self):
        """Equal jitter: never less than half the nominal delay."""
        assert min(backoff_delay(1) for _ in range(200)) >= BASE_DELAY / 2


class TestRetryable:
    @pytest.mark.parametrize(
        "exc", [TransientError("x"), TimeoutError(), ConnectionError(), OSError("refused")]
    )
    def test_transport_failures_are_retried(self, exc):
        assert default_retryable(exc) is True

    @pytest.mark.parametrize("exc", [DeviceAuthError("nope"), ValueError("bad"), KeyError("k")])
    def test_everything_else_is_not(self, exc):
        assert default_retryable(exc) is False

    def test_cancellation_is_never_retryable(self):
        """Retrying a cancel would defeat every timeout in the process."""
        assert default_retryable(asyncio.CancelledError()) is False


class TestCallWithRetry:
    async def test_it_returns_the_first_success(self):
        calls = []

        async def once():
            calls.append(1)
            return "ok"

        assert await call_with_retry(once) == "ok"
        assert len(calls) == 1

    async def test_it_retries_until_it_succeeds(self):
        calls = []

        async def flaky():
            calls.append(1)
            if len(calls) < 3:
                raise TransientError("not yet")
            return "ok"

        assert await call_with_retry(flaky, max_attempts=5) == "ok"
        assert len(calls) == 3

    async def test_a_non_retryable_error_propagates_immediately(self):
        calls = []

        async def refuses():
            calls.append(1)
            raise DeviceAuthError("nope")

        with pytest.raises(DeviceAuthError):
            await call_with_retry(refuses, max_attempts=5)
        assert len(calls) == 1

    async def test_max_attempts_is_honoured(self):
        calls = []

        async def always_fails():
            calls.append(1)
            raise TransientError("no")

        with pytest.raises(TransientError):
            await call_with_retry(always_fails, max_attempts=3)
        assert len(calls) == 3

    async def test_on_retry_observes_each_attempt(self):
        seen = []

        async def always_fails():
            raise TransientError("no")

        with pytest.raises(TransientError):
            await call_with_retry(
                always_fails,
                max_attempts=3,
                on_retry=lambda attempt, exc, delay: seen.append((attempt, delay)),
            )
        assert [a for a, _ in seen] == [1, 2]
        assert all(d > 0 for _, d in seen)

    async def test_should_stop_interrupts_a_backoff_promptly(self):
        """The sleep is chunked so a cancel is not stuck behind a 4s wait."""
        stop = False

        async def always_fails():
            raise TransientError("no")

        async def flip():
            await asyncio.sleep(0.05)
            nonlocal stop
            stop = True

        asyncio.create_task(flip())
        started = asyncio.get_running_loop().time()
        with pytest.raises(TransientError):
            await call_with_retry(always_fails, should_stop=lambda: stop)
        elapsed = asyncio.get_running_loop().time() - started
        assert elapsed < 1.0, f"took {elapsed:.2f}s to notice should_stop"

    async def test_cancellation_is_not_retried(self):
        calls = []

        async def cancelled():
            calls.append(1)
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await call_with_retry(cancelled, max_attempts=5)
        assert len(calls) == 1
