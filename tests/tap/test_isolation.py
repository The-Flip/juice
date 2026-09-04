"""`tap` must not import `juice`. This makes that an invariant, not a convention.

juice/tui/ has the same rule stated in prose and nothing enforcing it. The rule
matters more here: tap is meant to be deployable to an edge box without juice's
dependencies, and every shortcut into `juice.server` for "just one payload
shape" is a coupling that makes the wire contract a fiction.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

TAP = pathlib.Path(__file__).resolve().parents[2] / "tap"


def _modules():
    return sorted(TAP.glob("*.py"))


def test_the_package_is_where_we_think_it_is():
    """Guards against the glob below silently matching nothing."""
    names = {p.name for p in _modules()}
    assert {"buffer.py", "poller.py", "uplink.py", "cli.py"} <= names


@pytest.mark.parametrize("path", _modules(), ids=lambda p: p.name)
def test_no_module_imports_juice(path):
    tree = ast.parse(path.read_text())
    offenders = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            offenders += [a.name for a in node.names if a.name.split(".")[0] == "juice"]
        elif isinstance(node, ast.ImportFrom) and node.module:
            if node.module.split(".")[0] == "juice":
                offenders.append(node.module)
    assert not offenders, f"{path.name} imports {offenders}"
