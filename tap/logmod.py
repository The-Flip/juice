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
import os
import re
import sys
import time
from collections.abc import Callable
from datetime import UTC, date, datetime
from pathlib import Path
from typing import IO

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
DEFAULT_LOG_RETENTION_DAYS = 365

_DAY_FILE = re.compile(r"^tap-(\d{4}-\d{2}-\d{2})\.log$")


def setup_logging(
    level: str | int = "INFO",
    *,
    log_dir: str | os.PathLike[str] | None = None,
    log_retention_days: int = DEFAULT_LOG_RETENTION_DAYS,
) -> None:
    """Configure root logging once, idempotently.

    `force=True` so a reload (or a test that reconfigures) actually takes
    effect; `basicConfig` is otherwise a no-op once a handler exists. With a
    `log_dir`, a `DayFileHandler` writes the same lines to one file per UTC
    day beside the stderr stream — stderr is what `docker logs` shows, the
    files are what survives a container being replaced.
    """
    if isinstance(level, str):
        level = logging.getLevelNamesMapping().get(level.upper(), logging.INFO)
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_dir is not None:
        day = DayFileHandler(log_dir, retention_days=log_retention_days)
        # The files are cut on UTC days, so the stamps inside them are UTC too;
        # otherwise a laptop run in Central time files its evening under
        # tomorrow. stderr keeps local time, like juice.
        formatter = logging.Formatter(LOG_FORMAT)
        formatter.converter = time.gmtime
        day.setFormatter(formatter)
        handlers.append(day)
    logging.basicConfig(level=level, format=LOG_FORMAT, handlers=handlers, force=True)


class DayFileHandler(logging.Handler):
    """One log file per UTC day, `tap-YYYY-MM-DD.log`, pruned by age.

    The same shape as the buffer's day files, for the same reasons: a rollover
    is just opening the next file, retention is `os.unlink`, and a restart
    appends to today's file instead of starting a fresh one. The log is a
    diagnostic record of months of unattended running, so it is flushed line by
    line — the lines just before a crash are the ones worth having.
    """

    def __init__(
        self,
        directory: str | os.PathLike[str],
        *,
        retention_days: int = DEFAULT_LOG_RETENTION_DAYS,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if retention_days < 1:
            raise ValueError("retention_days must be at least 1")
        super().__init__()
        self._dir = Path(directory)
        self._retention_days = retention_days
        self._clock = clock or (lambda: datetime.now(UTC))
        self._day: date | None = None
        self._stream: IO[str] | None = None

    def emit(self, record: logging.LogRecord) -> None:
        try:
            today = self._clock().date()
            if self._stream is None or today != self._day:
                self._roll_to(today)
            assert self._stream is not None
            self._stream.write(self.format(record) + "\n")
            self._stream.flush()
        except Exception:
            self.handleError(record)

    def _roll_to(self, day: date) -> None:
        self._close_stream()
        self._dir.mkdir(parents=True, exist_ok=True)
        self._stream = open(
            self._dir / f"tap-{day.isoformat()}.log",
            "a",
            encoding="utf-8",
            errors="backslashreplace",
        )
        self._day = day
        try:
            self._prune(day)
        except OSError as e:
            # A prune that fails is a disk problem worth a line on stderr; it
            # must not cost the record that caused the rollover.
            sys.stderr.write(f"tap: could not prune old logs in {self._dir}: {e}\n")

    def _prune(self, today: date) -> None:
        """Unlink day files outside the retention window. Anything else in the
        directory is not ours and is left alone."""
        for path in self._dir.iterdir():
            m = _DAY_FILE.match(path.name)
            if m is None:
                continue
            try:
                file_day = date.fromisoformat(m.group(1))
            except ValueError:
                continue
            if (today - file_day).days >= self._retention_days:
                path.unlink(missing_ok=True)

    def _close_stream(self) -> None:
        if self._stream is not None:
            self._stream.close()
            self._stream = None

    def close(self) -> None:
        self.acquire()
        try:
            self._close_stream()
        finally:
            self.release()
        super().close()


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
