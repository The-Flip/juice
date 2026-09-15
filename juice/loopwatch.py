"""Log when the event loop stops turning.

A blocking call on the loop thread -- a DuckDB statement on `Store._conn`, a
synchronous file write, a GIL-holding extension -- stalls every coroutine at
once, and nothing in the logs says so: each task just resumes late. The
symptom that surfaced this was tap's uplink reconnecting at fixed seconds of
the minute while its own logs showed no error (the WebSocket heartbeat on
either side gives up after 10-15 s of no pong, and a stalled loop answers
no pings). This task sleeps `tick_s` and reports every wake-up that came
more than `threshold_s` late, with the stall's length, so a stall can be
matched against whatever else happened at that moment.

Cheap: one timer per second and no work between wake-ups.
"""

from __future__ import annotations

import asyncio
import logging

log = logging.getLogger(__name__)

TICK_S = 1.0
# Late by this much is a stall worth a log line. Scheduling jitter on a busy
# loop is tens of milliseconds; a DuckDB statement that hurts is seconds.
STALL_THRESHOLD_S = 2.0


async def stall_monitor(threshold_s: float = STALL_THRESHOLD_S, tick_s: float = TICK_S) -> None:
    """Run forever, logging each wake-up that is more than `threshold_s` late."""
    loop = asyncio.get_running_loop()
    while True:
        before = loop.time()
        await asyncio.sleep(tick_s)
        late = loop.time() - before - tick_s
        if late >= threshold_s:
            log.warning("event loop stalled for %.1fs", late + tick_s)
