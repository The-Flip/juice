"""Tests for juice.control — the retry policy every power handler actuates through."""

from __future__ import annotations

import aiohttp
import pytest

from juice.control import call_with_retry, is_retryable_passthrough_error


class TestIsRetryablePassthroughError:
    def test_request_timeout(self) -> None:
        assert is_retryable_passthrough_error(RuntimeError("Passthrough failed: Request timeout"))

    def test_device_offline(self) -> None:
        assert is_retryable_passthrough_error(RuntimeError("Passthrough failed: Device is offline"))

    def test_device_offline_during_processing(self) -> None:
        assert is_retryable_passthrough_error(
            RuntimeError("Passthrough failed: Device is offline during processing")
        )

    def test_asyncio_timeout(self) -> None:
        assert is_retryable_passthrough_error(TimeoutError())

    def test_aiohttp_client_error(self) -> None:
        assert is_retryable_passthrough_error(aiohttp.ClientError())

    def test_token_invalid_not_retryable(self) -> None:
        assert not is_retryable_passthrough_error(RuntimeError("Passthrough failed: Token invalid"))

    def test_unrelated_runtime_error_not_retryable(self) -> None:
        assert not is_retryable_passthrough_error(RuntimeError("something else"))

    def test_value_error_not_retryable(self) -> None:
        assert not is_retryable_passthrough_error(ValueError("nope"))


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
                raise RuntimeError("Passthrough failed: Request timeout")
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
            raise RuntimeError("Passthrough failed: Token invalid")

        with pytest.raises(RuntimeError, match="Token invalid"):
            await call_with_retry(fn)
        assert attempts["n"] == 1

    @pytest.mark.asyncio
    async def test_should_stop_raises_last_error(self, fast_sleep) -> None:
        attempts = {"n": 0}
        should_stop_after = 2

        async def fn():
            attempts["n"] += 1
            raise RuntimeError("Passthrough failed: Device is offline")

        # should_stop returns True after the first retry's backoff.
        def should_stop():
            return attempts["n"] >= should_stop_after

        with pytest.raises(RuntimeError, match="Device is offline"):
            await call_with_retry(fn, should_stop=should_stop)
        # Stopped before completing many attempts.
        assert attempts["n"] <= 3

    @pytest.mark.asyncio
    async def test_max_attempts_bounds(self, fast_sleep) -> None:
        attempts = {"n": 0}

        async def fn():
            attempts["n"] += 1
            raise RuntimeError("Passthrough failed: Request timeout")

        with pytest.raises(RuntimeError, match="Request timeout"):
            await call_with_retry(fn, max_attempts=3)
        assert attempts["n"] == 3

    @pytest.mark.asyncio
    async def test_backoff_schedule(self, fast_sleep) -> None:
        async def fn():
            raise RuntimeError("Passthrough failed: Request timeout")

        delays: list[float] = []
        with pytest.raises(RuntimeError):
            await call_with_retry(
                fn,
                max_attempts=6,
                on_retry=lambda a, e, d: delays.append(d),
            )
        # 5 retries after 6 attempts: 0.5, 1.0, 2.0, 4.0, 4.0
        assert delays == [0.5, 1.0, 2.0, 4.0, 4.0]
