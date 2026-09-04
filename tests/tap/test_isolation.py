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


def test_the_core_modules_import_without_python_kasa():
    """python-kasa is an optional extra, so most of tap must not need it.

    Only `tap.kasa_common` and the two adapters may import it, and they are
    imported lazily from `build_device`, `discover` and the CLI command bodies.
    A regression here means `tap --help` breaks on a machine that installed the
    base package, and that juice's own image could no longer import anything
    from tap.
    """
    import subprocess
    import sys

    script = (
        "import sys\n"
        "class Block:\n"
        "    def find_spec(self, name, path=None, target=None):\n"
        "        if name == 'kasa' or name.startswith('kasa.'):\n"
        "            raise ImportError('python-kasa is blocked for this test')\n"
        "        return None\n"
        "sys.meta_path.insert(0, Block())\n"
        "import tap, tap.cli, tap.config, tap.buffer, tap.poller, tap.supervise\n"
        "import tap.uplink, tap.webui, tap.wire, tap.health, tap.discovery, tap.retry\n"
        "from click.testing import CliRunner\n"
        "result = CliRunner().invoke(tap.cli.cli, ['--help'])\n"
        "assert result.exit_code == 0, result.output\n"
        "print('ok')\n"
    )
    proc = subprocess.run(  # noqa: S603 — our own interpreter, our own script
        [sys.executable, "-c", script], capture_output=True, text=True, timeout=120, check=False
    )
    assert proc.returncode == 0, proc.stderr
    assert "ok" in proc.stdout
