"""The wire envelope, and the welcome frame's refusal to accept nonsense."""

from __future__ import annotations

import pytest

from tap import wire


class TestFrames:
    def test_reading_rows_are_positional(self):
        frame = wire.readings("b1", "20260903:0000000000000005", [[1, "D", "C", 1, 2, 3, 4, 5]])
        assert frame["type"] == wire.READINGS
        assert frame["rows"][0] == [1, "D", "C", 1, 2, 3, 4, 5]
        assert len(wire.ROW_FIELDS) == len(frame["rows"][0])

    def test_hello_carries_the_buffer_extent(self):
        frame = wire.hello("museum-1", "0.1.0", "a", "b")
        assert frame["tap_id"] == "museum-1"
        assert frame["buffer_oldest"] == "a"
        assert frame["protocol"] == wire.PROTOCOL_VERSION


class TestWelcome:
    def test_defaults_when_the_server_says_nothing(self):
        w = wire.Welcome({"type": wire.WELCOME})
        assert w.max_batch_rows == wire.DEFAULT_MAX_BATCH_ROWS
        assert w.window == wire.DEFAULT_WINDOW
        assert w.resume_from is None

    def test_server_overrides_are_honoured(self):
        w = wire.Welcome(
            {"type": wire.WELCOME, "max_batch_rows": 10, "window": 1, "resume_from": "x"}
        )
        assert (w.max_batch_rows, w.window, w.resume_from) == (10, 1, "x")

    def test_protocol_mismatch_is_refused(self):
        with pytest.raises(wire.WelcomeError, match="protocol"):
            wire.Welcome({"type": wire.WELCOME, "protocol": 999})

    def test_wrong_frame_type_is_refused(self):
        with pytest.raises(wire.WelcomeError):
            wire.Welcome({"type": "something-else"})

    @pytest.mark.parametrize("bad", [{"window": 0}, {"max_batch_rows": 0}])
    def test_non_positive_limits_are_refused(self, bad):
        with pytest.raises(wire.WelcomeError):
            wire.Welcome({"type": wire.WELCOME, **bad})
