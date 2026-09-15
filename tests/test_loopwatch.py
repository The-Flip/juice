import asyncio
import logging
import time

import pytest

from juice.loopwatch import stall_monitor


@pytest.mark.asyncio
async def test_a_blocking_call_on_the_loop_is_logged_with_its_length(caplog):
    caplog.set_level(logging.WARNING, logger="juice.loopwatch")
    task = asyncio.create_task(stall_monitor(threshold_s=0.2, tick_s=0.05))
    await asyncio.sleep(0.1)  # let the first tick be scheduled
    time.sleep(0.5)  # block the loop thread itself
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    stalls = [r for r in caplog.records if "event loop stalled" in r.getMessage()]
    assert len(stalls) == 1
    assert 0.45 <= float(stalls[0].getMessage().split("stalled for ")[1].rstrip("s")) < 1.0


@pytest.mark.asyncio
async def test_a_healthy_loop_logs_nothing(caplog):
    caplog.set_level(logging.WARNING, logger="juice.loopwatch")
    task = asyncio.create_task(stall_monitor(threshold_s=0.2, tick_s=0.02))
    await asyncio.sleep(0.2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not [r for r in caplog.records if "stalled" in r.getMessage()]
