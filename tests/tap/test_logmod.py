"""The day-partitioned log file.

One file per UTC day, like the buffer: a rollover is a new file, retention is
an unlink, and a restart reopens today's file rather than starting a new one.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest

from tap.logmod import DayFileHandler, setup_logging

DAY1 = datetime(2026, 9, 13, 23, 59, 30, tzinfo=UTC)
DAY2 = datetime(2026, 9, 14, 0, 0, 30, tzinfo=UTC)


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


def _record(msg: str) -> logging.LogRecord:
    return logging.LogRecord("tap.test", logging.INFO, __file__, 1, msg, None, None)


@pytest.fixture
def clock() -> Clock:
    return Clock(DAY1)


@pytest.fixture
def handler(tmp_path: Path, clock: Clock):
    h = DayFileHandler(tmp_path / "logs", retention_days=3, clock=clock)
    h.setFormatter(logging.Formatter("%(message)s"))
    yield h
    h.close()


class TestDayFiles:
    def test_creates_the_directory_and_todays_file(self, tmp_path, handler):
        handler.emit(_record("hello"))
        path = tmp_path / "logs" / "tap-2026-09-13.log"
        assert path.read_text() == "hello\n"

    def test_midnight_utc_starts_a_new_file(self, tmp_path, handler, clock):
        handler.emit(_record("before"))
        clock.now = DAY2
        handler.emit(_record("after"))
        assert (tmp_path / "logs" / "tap-2026-09-13.log").read_text() == "before\n"
        assert (tmp_path / "logs" / "tap-2026-09-14.log").read_text() == "after\n"

    def test_a_restart_appends_to_todays_file(self, tmp_path, handler, clock):
        handler.emit(_record("first process"))
        handler.close()
        again = DayFileHandler(tmp_path / "logs", retention_days=3, clock=clock)
        again.setFormatter(logging.Formatter("%(message)s"))
        again.emit(_record("second process"))
        again.close()
        text = (tmp_path / "logs" / "tap-2026-09-13.log").read_text()
        assert text == "first process\nsecond process\n"

    def test_lines_are_flushed_as_written(self, tmp_path, handler):
        """A crash must not take the last lines with it; they are the useful ones."""
        handler.emit(_record("about to die"))
        assert (tmp_path / "logs" / "tap-2026-09-13.log").read_text() == "about to die\n"


class TestRetention:
    def test_rollover_unlinks_files_older_than_retention(self, tmp_path, handler, clock):
        logs = tmp_path / "logs"
        logs.mkdir()
        (logs / "tap-2026-09-09.log").write_text("too old\n")
        (logs / "tap-2026-09-10.log").write_text("too old\n")
        (logs / "tap-2026-09-11.log").write_text("kept\n")
        (logs / "notes.txt").write_text("not ours\n")
        (logs / "tap-garbage.log").write_text("not ours either\n")
        handler.emit(_record("today"))  # retention 3: keeps 11, 12, 13
        names = sorted(p.name for p in logs.iterdir())
        assert names == ["notes.txt", "tap-2026-09-11.log", "tap-2026-09-13.log", "tap-garbage.log"]

    def test_retention_is_checked_again_at_each_rollover(self, tmp_path, handler, clock):
        logs = tmp_path / "logs"
        handler.emit(_record("day one"))
        (logs / "tap-2026-09-11.log").write_text("kept on day one\n")
        clock.now = DAY2
        handler.emit(_record("day two"))  # keeps 12, 13, 14
        assert not (logs / "tap-2026-09-11.log").exists()
        assert (logs / "tap-2026-09-13.log").exists()

    def test_retention_below_one_is_refused(self, tmp_path):
        with pytest.raises(ValueError):
            DayFileHandler(tmp_path / "logs", retention_days=0)


class TestSetupLogging:
    def test_a_log_dir_adds_the_day_file_beside_stderr(self, tmp_path):
        logs = tmp_path / "logs"
        logs.mkdir()
        (logs / "tap-2000-01-01.log").write_text("ancient\n")
        setup_logging("INFO", log_dir=logs, log_retention_days=7)
        try:
            handlers = logging.getLogger().handlers
            day = [h for h in handlers if isinstance(h, DayFileHandler)]
            assert len(day) == 1 and len(handlers) == 2
            logging.getLogger("tap.test").info("configured")
            (path,) = logs.glob("tap-*.log")  # the ancient file was pruned by retention
            assert "INFO tap.test: configured" in path.read_text()
        finally:
            setup_logging("INFO")

    def test_file_lines_are_stamped_in_utc(self, tmp_path, monkeypatch):
        """The file boundaries are UTC days, so the lines inside must agree with
        them — a laptop in Central time must not file an 18:59 line under today
        when the handler has already rolled to tomorrow."""
        original_tz = os.environ.get("TZ")
        monkeypatch.setenv("TZ", "America/Chicago")
        time.tzset()
        setup_logging("INFO", log_dir=tmp_path / "logs")
        try:
            (day,) = [h for h in logging.getLogger().handlers if isinstance(h, DayFileHandler)]
            record = _record("stamped")
            record.created = DAY1.timestamp()  # 23:59:30 UTC, 18:59:30 in Chicago
            day.emit(record)
            (path,) = (tmp_path / "logs").glob("tap-*.log")
            assert path.read_text().splitlines()[-1].startswith("2026-09-13 23:59:30")
        finally:
            setup_logging("INFO")
            if original_tz is None:
                monkeypatch.delenv("TZ")
            else:
                monkeypatch.setenv("TZ", original_tz)
            time.tzset()


class TestFailures:
    """Months unattended: a bad day for the filesystem must not become a crash
    or a silent stop, and lines must flow again once the fault clears."""

    def test_an_unwritable_dir_does_not_raise_and_recovers(self, tmp_path, clock, capsys):
        if os.geteuid() == 0:
            pytest.skip("root can write anywhere")
        parent = tmp_path / "ro"
        parent.mkdir()
        parent.chmod(0o500)
        h = DayFileHandler(parent / "logs", retention_days=3, clock=clock)
        h.setFormatter(logging.Formatter("%(message)s"))
        try:
            h.emit(_record("lost"))  # handleError -> stderr, no exception
            assert "Logging error" in capsys.readouterr().err
            parent.chmod(0o700)
            h.emit(_record("back"))
            assert (parent / "logs" / "tap-2026-09-13.log").read_text() == "back\n"
        finally:
            parent.chmod(0o700)
            h.close()

    def test_a_failed_prune_does_not_lose_the_line(self, tmp_path, handler, clock, monkeypatch):
        def boom(self, today):
            raise OSError("disk says no")

        monkeypatch.setattr(DayFileHandler, "_prune", boom)
        handler.emit(_record("kept anyway"))
        assert (tmp_path / "logs" / "tap-2026-09-13.log").read_text() == "kept anyway\n"

    def test_no_log_dir_means_stderr_only(self):
        setup_logging("INFO")
        assert not any(isinstance(h, DayFileHandler) for h in logging.getLogger().handlers)
