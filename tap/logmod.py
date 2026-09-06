"""Logging setup and the rate limiter that keeps a 1 Hz daemon readable.

The format string matches juice's (`juice/cli.py`) so the two are greppable the
same way. It lives in a function here because juice copy-pastes its
`basicConfig` into every long-running command and then has none at all in the
short ones; one helper avoids repeating that.

The discipline this module exists to support: **one line per state transition,
never per tick.** At 1 Hz across a dozen devices, a single careless per-sweep
INFO is about a million lines a day, which is the same as having no logs.
"""

from __future__ import annotations

import logging
import time

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def setup_logging(level: str | int = "INFO") -> None:
    """Configure root logging once, idempotently.

    `force=True` so a reload (or a test that reconfigures) actually takes
    effect; `basicConfig` is otherwise a no-op once a handler exists.
    """
    if isinstance(level, str):
        level = logging.getLevelNamesMapping().get(level.upper(), logging.INFO)
    logging.basicConfig(level=level, format=LOG_FORMAT, force=True)


def set_level(level: str | int) -> None:
    """Change the root log level in place (used by SIGHUP config reload)."""
    if isinstance(level, str):
        level = logging.getLevelNamesMapping().get(level.upper(), logging.INFO)
    logging.getLogger().setLevel(level)


class RateLimited:
    """Emit at most one log line per `interval` seconds, counting the rest.

    For failures that are individually uninteresting but collectively alarming —
    a full write queue, say. The suppressed count rides along on the next line
    that does get through, so the volume is visible without being printed.
    """

    def __init__(self, log: logging.Logger, interval: float = 60.0) -> None:
        self._log = log
        self._interval = interval
        # None, not 0.0: `time.monotonic()` counts from boot, so 0.0 reads as
        # "emitted at boot" and swallowed every line for the first `interval`
        # seconds of a machine's life. On a collector that restarts with its
        # host, that is exactly the window where the first failures happen —
        # and it is silent. `None` means never emitted, which is different.
        self._last: float | None = None
        self._suppressed = 0

    def warning(self, msg: str, *args: object) -> None:
        now = time.monotonic()
        if self._last is not None and now - self._last < self._interval:
            self._suppressed += 1
            return
        if self._suppressed:
            self._log.warning(f"{msg} (+%d more since last report)", *args, self._suppressed)
        else:
            self._log.warning(msg, *args)
        self._last = now
        self._suppressed = 0
