"""Integration guard for the inline-from-file JS mechanism.

The frontend modules in ``juice/web/`` are unit-tested in isolation by
``node --test``. This file guards the *assembly*: it renders every ``*_HTML``
template through ``_render_page`` and, for the fully-substituted page, checks that

1. the inline ``<script>`` is syntactically valid (``node --check``), and
2. every helper provided by an extracted module is actually *defined* in any
   template that *calls* it — catching a dropped duplicate or a missing
   ``{{JS_*}}`` marker, which ``node --check`` cannot (an undefined global is a
   runtime ``ReferenceError``, not a syntax error).

Skips if ``node`` is unavailable; CI installs node so it runs there.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

import juice.server as server

_NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(_NODE is None, reason="node not available to check inline JS")

# Names exported by juice/web modules become page-scope globals once inlined, and
# are also what one module `import`s from another. Auto-derive them (rather than
# hand-maintaining a list) so any new extracted/imported helper is automatically
# guarded define-if-called — closing the "forgot to add it to the list" hole.
_WEB_DIR = Path(__file__).resolve().parent.parent / "juice" / "web"


def _provided_helpers() -> list[str]:
    names: set[str] = set()
    for mod in _WEB_DIR.glob("*.js"):
        if mod.name.endswith(".test.js"):
            continue
        names.update(
            re.findall(r"^export\s+(?:function|const|let|class)\s+(\w+)", mod.read_text(), re.M)
        )
    return sorted(names)


_PROVIDED_HELPERS = _provided_helpers()

_TEMPLATES = sorted(
    name
    for name in dir(server)
    if name.endswith("_HTML") and isinstance(getattr(server, name), str)
)


class _FakeApp(dict):
    pass


class _FakeRequest:
    """Minimal stand-in for aiohttp's Request that _render_page needs (authed)."""

    def __init__(self) -> None:
        self.app = _FakeApp()

    def get(self, key, default=None):  # noqa: ANN001
        return default


def _render(name: str) -> str:
    return server._render_page(getattr(server, name), _FakeRequest()).text


def _inline_scripts(html: str) -> str:
    # `<script>...</script>` with no attributes — excludes the d3 `<script src=...>`.
    return "\n".join(b for b in re.findall(r"<script>(.*?)</script>", html, re.S) if b.strip())


def test_templates_render_and_resolve() -> None:
    assert _TEMPLATES, "no *_HTML templates found"
    for name in _TEMPLATES:
        html = _render(name)
        # No template marker ({{PUBLIC_MODE}}, {{NAV}}, {{AUTH_CORNER}}, {{JS_*}}, …)
        # may survive a render anywhere in the page.
        leftover = re.findall(r"\{\{[A-Z_]+\}\}", html)
        assert not leftover, f"{name}: unsubstituted markers remain: {leftover}"
        js = _inline_scripts(html)
        if not js:
            continue
        # 1) Syntax-valid assembled script.
        with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
            f.write(js)
            path = f.name
        try:
            proc = subprocess.run(  # noqa: S603
                [_NODE, "--check", path], capture_output=True, text=True, timeout=10
            )
        finally:
            Path(path).unlink(missing_ok=True)
        assert proc.returncode == 0, f"{name}: inline JS failed node --check:\n{proc.stderr}"

        # 2) Define-if-called for every extracted helper. Match the loader
        # contract: helpers may be `function NAME` or `const NAME` (after the
        # `export ` strip), per juice/web/README.md.
        for helper in _PROVIDED_HELPERS:
            if re.search(rf"\b{helper}\s*\(", js):
                assert re.search(rf"\b(?:function|const|let|class) {helper}\b", js), (
                    f"{name}: calls {helper}() but no definition is inlined "
                    f"(missing {{{{JS_*}}}} marker?)"
                )


def test_power_helpers_inlined_into_detail() -> None:
    """The DETAIL page must carry the power state machine inlined from power.js."""
    js = _inline_scripts(_render("DETAIL_HTML"))
    assert "function pcReduceReading" in js
    assert "function pcPowerButton" in js
    # The old sentinel scrape is gone; the marker must have been substituted.
    assert "{{JS_POWER}}" not in js
    assert "__PC_STATE_MACHINE_START__" not in js
