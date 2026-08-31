"""Tests for SSE stream reliability — sequence numbers, resync, heartbeat.

The problem these solve: `_publish` used `put_nowait` on a bounded queue and
dropped silently on overflow, so a client could never know it had missed
something. That is why every page in the old UI also blind-polls on a 5-30s
timer. With a dense per-connection `seq` and an explicit `resync_required`, a
gap becomes detectable and the polling can go away.
"""

from __future__ import annotations

import asyncio

import pytest

from juice.server import (
    SSE_HEARTBEAT_SECONDS,
    SSE_QUEUE_MAXSIZE,
    RecorderState,
    _publish,
    _sse_stream,
)


async def _drain(captured: list, want: int, ticks: int = 50) -> None:
    """Let the stream task move events into `captured`."""
    for _ in range(ticks):
        await asyncio.sleep(0)
        if len(captured) >= want:
            return


async def _run_stream(state: RecorderState, captured: list, **kw) -> asyncio.Task:
    async def write(event: dict) -> None:
        captured.append(event)

    task = asyncio.create_task(_sse_stream(state, write, **kw))
    await _drain(captured, 1)
    return task


async def _stop(task: asyncio.Task) -> None:
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


class TestSequenceNumbers:
    @pytest.mark.asyncio
    async def test_every_frame_carries_a_dense_seq(self) -> None:
        state = RecorderState()
        captured: list = []
        task = await _run_stream(state, captured)

        for i in range(3):
            _publish(state, {"type": "power_change", "plug_id": i})
        await _drain(captured, 4)
        await _stop(task)

        assert [e["seq"] for e in captured] == [1, 2, 3, 4]

    @pytest.mark.asyncio
    async def test_hello_is_seq_one_and_carries_the_epoch(self) -> None:
        state = RecorderState()
        captured: list = []
        task = await _run_stream(state, captured)
        await _stop(task)

        assert captured[0]["type"] == "hello"
        assert captured[0]["seq"] == 1
        assert captured[0]["epoch"]

    @pytest.mark.asyncio
    async def test_filtered_events_do_not_consume_a_seq(self) -> None:
        """A public subscriber drops operator events, so counting them would
        make every public client think it had missed something."""
        state = RecorderState()
        captured: list = []
        task = await _run_stream(state, captured, public=True)

        _publish(state, {"type": "power_change", "plug_id": 1})  # operator-only
        _publish(state, {"type": "readings", "machines": []})  # public
        await _drain(captured, 2)
        await _stop(task)

        assert [e["type"] for e in captured] == ["hello", "readings"]
        assert [e["seq"] for e in captured] == [1, 2]

    @pytest.mark.asyncio
    async def test_two_connections_have_independent_counters(self) -> None:
        state = RecorderState()
        a: list = []
        b: list = []
        task_a = await _run_stream(state, a)
        _publish(state, {"type": "power_change", "plug_id": 1})
        await _drain(a, 2)
        task_b = await _run_stream(state, b)

        _publish(state, {"type": "power_change", "plug_id": 2})
        await _drain(a, 3)
        await _drain(b, 2)
        await _stop(task_a)
        await _stop(task_b)

        assert [e["seq"] for e in a] == [1, 2, 3]
        assert [e["seq"] for e in b] == [1, 2]  # not 1, 4


class TestOverflowResync:
    @pytest.mark.asyncio
    async def test_overflow_collapses_to_a_single_resync(self) -> None:
        """A stuck subscriber must learn it fell behind, not silently miss events."""
        state = RecorderState()
        queue: asyncio.Queue = asyncio.Queue(maxsize=SSE_QUEUE_MAXSIZE)
        state.event_subscribers.add(queue)

        for i in range(SSE_QUEUE_MAXSIZE + 10):
            _publish(state, {"type": "power_change", "plug_id": i})

        items = []
        while not queue.empty():
            items.append(queue.get_nowait())

        # The backlog the client hadn't read is gone, replaced by one sentinel,
        # and the queue has room again — so the events published after the
        # overflow are still delivered normally.
        assert items[0] == {"type": "resync_required"}
        assert sum(1 for i in items if i["type"] == "resync_required") == 1
        assert len(items) < SSE_QUEUE_MAXSIZE

    @pytest.mark.asyncio
    async def test_resync_is_delivered_and_seq_stays_dense(self) -> None:
        state = RecorderState()
        captured: list = []

        async def write(event: dict) -> None:
            captured.append(event)
            # Block after hello so the queue backs up behind us.
            if event["type"] == "hello":
                await asyncio.sleep(0.05)

        task = asyncio.create_task(_sse_stream(state, write))
        await _drain(captured, 1)
        for i in range(SSE_QUEUE_MAXSIZE + 10):
            _publish(state, {"type": "power_change", "plug_id": i})
        await asyncio.sleep(0.1)
        await _stop(task)

        types = [e["type"] for e in captured]
        assert "resync_required" in types
        assert [e["seq"] for e in captured] == list(range(1, len(captured) + 1))

    @pytest.mark.asyncio
    async def test_publisher_never_raises_on_a_full_queue(self) -> None:
        state = RecorderState()
        queue: asyncio.Queue = asyncio.Queue(maxsize=1)
        state.event_subscribers.add(queue)
        for i in range(20):
            _publish(state, {"type": "readings", "n": i})  # must not raise


class TestHeartbeat:
    @pytest.mark.asyncio
    async def test_ping_fires_when_the_stream_is_idle(self) -> None:
        """A proxy-killed-but-not-closed connection is invisible without this:
        the client waits forever for a readings tick that will never come."""
        state = RecorderState()
        captured: list = []
        pings: list[int] = []

        async def write(event: dict) -> None:
            captured.append(event)

        async def ping() -> None:
            pings.append(1)

        task = asyncio.create_task(_sse_stream(state, write, ping=ping, heartbeat_s=0.01))
        await _drain(captured, 1)
        await asyncio.sleep(0.05)
        await _stop(task)

        assert pings, "expected at least one heartbeat while idle"

    @pytest.mark.asyncio
    async def test_heartbeat_does_not_consume_a_seq(self) -> None:
        state = RecorderState()
        captured: list = []

        async def write(event: dict) -> None:
            captured.append(event)

        async def ping() -> None:
            pass

        task = asyncio.create_task(_sse_stream(state, write, ping=ping, heartbeat_s=0.01))
        await _drain(captured, 1)
        await asyncio.sleep(0.05)
        _publish(state, {"type": "readings", "machines": []})
        await _drain(captured, 2)
        await _stop(task)

        assert [e["seq"] for e in captured] == [1, 2]


class TestDefaults:
    def test_queue_is_large_enough_for_a_full_sweep(self) -> None:
        """An all-on over ~33 machines emits several events per machine on top
        of the 1 Hz readings tick; 64 was genuinely tight."""
        assert SSE_QUEUE_MAXSIZE >= 256

    def test_heartbeat_is_shorter_than_common_proxy_idle_timeouts(self) -> None:
        assert 0 < SSE_HEARTBEAT_SECONDS <= 30
