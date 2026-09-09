"""`juice` must not import `tap`, and the two wire copies must not drift.

`tap/wire.py:3-6` makes the rule explicit: the protocol module is "deliberately
duplicated on the server side rather than imported: neither codebase should be
able to reach into the other to 'just check one field'." `tests/tap/test_isolation.py`
enforces that direction. This is the other one, which until now was only prose.

The second test covers a gap the protocol's own version negotiation cannot.
A bumped `PROTOCOL_VERSION` is caught by the `hello`/`welcome` handshake and the
connection is refused. A *reordered* `ROW_FIELDS` at the same version is not
caught by anything: both sides agree they speak protocol 1, and every reading is
silently written into the wrong column.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
JUICE = ROOT / "juice"


def _modules():
    return sorted(JUICE.rglob("*.py"))


def test_the_package_is_where_we_think_it_is():
    """Guards against the glob below silently matching nothing."""
    names = {p.name for p in _modules()}
    assert {"store.py", "server.py", "auth.py", "recorder.py"} <= names


@pytest.mark.parametrize("path", _modules(), ids=lambda p: str(p.relative_to(JUICE)))
def test_no_juice_module_imports_tap(path):
    tree = ast.parse(path.read_text())
    offenders = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            offenders += [a.name for a in node.names if a.name.split(".")[0] == "tap"]
        elif isinstance(node, ast.ImportFrom) and node.module:
            if node.module.split(".")[0] == "tap":
                offenders.append(node.module)
    assert not offenders, f"{path.relative_to(ROOT)} imports {offenders}"


class TestTheTwoWireCopiesAgree:
    """Importing both is legitimate *here* — a test is exactly where the two
    copies are supposed to be compared."""

    def test_the_row_layout_is_identical(self):
        from juice.api.v2 import tap_wire as juice_wire
        from tap import wire as tap_wire

        assert juice_wire.ROW_FIELDS == tap_wire.ROW_FIELDS

    def test_the_store_uses_the_same_row_layout(self):
        """`juice.store` keeps a third copy, because it must not import the API
        layer (the dependency runs server -> api.v2, never back). A reordering
        there would be invisible to protocol negotiation -- both sides would
        still agree they speak version 1 -- while every reading landed in the
        wrong column, so this is the only thing that can catch it."""
        from juice.store import _WIRE_ROW_FIELDS
        from tap import wire as tap_wire

        assert _WIRE_ROW_FIELDS == tap_wire.ROW_FIELDS

    def test_the_protocol_version_is_identical(self):
        from juice.api.v2 import tap_wire as juice_wire
        from tap import wire as tap_wire

        assert juice_wire.PROTOCOL_VERSION == tap_wire.PROTOCOL_VERSION

    def test_the_frame_type_names_are_identical(self):
        from juice.api.v2 import tap_wire as juice_wire
        from tap import wire as tap_wire

        for name in (
            "HELLO",
            "READINGS",
            "LIVE",
            "DEVICES",
            "COMMAND_RESULT",
            "PONG",
            "WELCOME",
            "ACK",
            "NACK",
            "COMMAND",
            "PING",
            "NACK_TRANSIENT",
            "NACK_BAD_BATCH",
        ):
            assert getattr(juice_wire, name) == getattr(tap_wire, name), name

    def test_the_negotiated_defaults_are_identical(self):
        """tap falls back to these when the welcome omits them, so a server
        that "defaults" to something else is not defaulting at all."""
        from juice.api.v2 import tap_wire as juice_wire
        from tap import wire as tap_wire

        assert juice_wire.DEFAULT_MAX_BATCH_ROWS == tap_wire.DEFAULT_MAX_BATCH_ROWS
        assert juice_wire.DEFAULT_WINDOW == tap_wire.DEFAULT_WINDOW
        assert juice_wire.DEFAULT_LIVE_MAX_LAG_S == tap_wire.DEFAULT_LIVE_MAX_LAG_S
