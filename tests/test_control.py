"""Tests for juice.control — the retry policy every power handler actuates through."""

from __future__ import annotations

import aiohttp
import pytest

from juice.control import call_with_retry, is_retryable


class TestIsRetryable:
    """The predicate is a type check and nothing else -- see `is_retryable`."""

    def test_a_timeout_is_retried(self) -> None:
        assert is_retryable(TimeoutError("tap bumper: no command_result within 2.0s"))

    def test_asyncio_timeout_is_the_same_type(self) -> None:

        assert is_retryable(TimeoutError())

    def test_an_aiohttp_client_error_is_retried(self) -> None:
        assert is_retryable(aiohttp.ClientError())

    def test_a_refusal_is_not(self) -> None:
        assert not is_retryable(RuntimeError("tap refused the command as expired"))

    def test_no_message_buys_a_retry(self) -> None:
        """The cloud client used to retry on error *text*; a `RuntimeError`
        that merely says the right words must not get six attempts."""
        assert not is_retryable(RuntimeError("Passthrough failed: Device is offline"))
        assert not is_retryable(RuntimeError("Request timeout"))

    def test_a_value_error_is_not(self) -> None:
        assert not is_retryable(ValueError("nope"))


@pytest.fixture
def fast_sleep(monkeypatch):
    """Replace asyncio.sleep with a no-op for retry tests (kept awaitable)."""

    async def _noop(_):
        return None

    monkeypatch.setattr("juice.control.asyncio.sleep", _noop)


class TestCallWithRetry:
    @pytest.mark.asyncio
    async def test_succeeds_first_try(self, fast_sleep) -> None:
        calls = []

        async def fn():
            calls.append(1)
            return "ok"

        retries: list = []
        result = await call_with_retry(fn, on_retry=lambda a, e, d: retries.append((a, e, d)))
        assert result == "ok"
        assert calls == [1]
        assert retries == []

    @pytest.mark.asyncio
    async def test_two_transient_failures_then_success(self, fast_sleep) -> None:
        attempts = {"n": 0}

        async def fn():
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise TimeoutError("tap bumper: no command_result within 2.0s")
            return "ok"

        retries: list = []
        result = await call_with_retry(fn, on_retry=lambda a, e, d: retries.append((a, d)))
        assert result == "ok"
        assert attempts["n"] == 3
        assert [a for a, _ in retries] == [1, 2]

    @pytest.mark.asyncio
    async def test_non_retryable_raises_immediately(self, fast_sleep) -> None:
        attempts = {"n": 0}

        async def fn():
            attempts["n"] += 1
            raise RuntimeError("tap refused the command as expired")

        with pytest.raises(RuntimeError, match="expired"):
            await call_with_retry(fn)
        assert attempts["n"] == 1

    @pytest.mark.asyncio
    async def test_should_stop_raises_last_error(self, fast_sleep) -> None:
        attempts = {"n": 0}
        should_stop_after = 2

        async def fn():
            attempts["n"] += 1
            raise TimeoutError("tap bumper: ConnectionError: strip gone")

        # should_stop returns True after the first retry's backoff.
        def should_stop():
            return attempts["n"] >= should_stop_after

        with pytest.raises(TimeoutError, match="strip gone"):
            await call_with_retry(fn, should_stop=should_stop)
        # Stopped before completing many attempts.
        assert attempts["n"] <= 3

    @pytest.mark.asyncio
    async def test_max_attempts_bounds(self, fast_sleep) -> None:
        attempts = {"n": 0}

        async def fn():
            attempts["n"] += 1
            raise TimeoutError("tap bumper: no command_result within 2.0s")

        with pytest.raises(TimeoutError, match="no command_result"):
            await call_with_retry(fn, max_attempts=3)
        assert attempts["n"] == 3

    @pytest.mark.asyncio
    async def test_backoff_schedule(self, fast_sleep) -> None:
        async def fn():
            raise TimeoutError("tap bumper: no command_result within 2.0s")

        delays: list[float] = []
        with pytest.raises(TimeoutError):
            await call_with_retry(
                fn,
                max_attempts=6,
                on_retry=lambda a, e, d: delays.append(d),
            )
        # 5 retries after 6 attempts: 0.5, 1.0, 2.0, 4.0, 4.0
        assert delays == [0.5, 1.0, 2.0, 4.0, 4.0]
