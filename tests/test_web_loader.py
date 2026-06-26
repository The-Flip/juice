"""Unit tests for `_web_js`, the juice/web module loader.

It turns an ESM module into browser-inlinable JS (strip `export `, strip
single-line named `import`s) and fails fast on anything outside the supported
forms so a bad module can't silently corrupt an inlined page. These are pure
Python (no node needed) so they always run.
"""

from __future__ import annotations

import pytest

import juice.server as server


def _mod(tmp_path, monkeypatch, src: str) -> str:
    monkeypatch.setattr(server, "_WEB_DIR", tmp_path)
    (tmp_path / "m.js").write_text(src)
    return server._web_js("m.js")


def test_strips_export_and_single_line_import(tmp_path, monkeypatch) -> None:
    out = _mod(
        tmp_path,
        monkeypatch,
        "import { escapeHtml } from './format.js';\n"
        "export function f(x) { return escapeHtml(x); }\n"
        "const LOCAL = 1;\n",
    )
    assert "import" not in out
    assert "export" not in out
    assert "function f(x)" in out  # declaration kept, becomes a page global
    assert "const LOCAL = 1;" in out  # non-exported decl untouched


@pytest.mark.parametrize(
    "names",
    ["escapeHtml as esc", "escapeHtml  as  esc", "escapeHtml as\tesc", "a, b as c"],
)
def test_rejects_aliased_import(tmp_path, monkeypatch, names: str) -> None:
    with pytest.raises(ValueError, match="aliased"):
        _mod(tmp_path, monkeypatch, f"import {{ {names} }} from './format.js';\n")


@pytest.mark.parametrize(
    "src",
    [
        "import {\n  a,\n  b,\n} from './x.js';\n",  # multi-line
        "import x from './x.js';\n",  # default
        "import * as ns from './x.js';\n",  # namespace
    ],
)
def test_rejects_unsupported_import_forms(tmp_path, monkeypatch, src: str) -> None:
    with pytest.raises(ValueError, match="import"):
        _mod(tmp_path, monkeypatch, src)


def test_rejects_export_default(tmp_path, monkeypatch) -> None:
    with pytest.raises(ValueError, match="export"):
        _mod(tmp_path, monkeypatch, "export default function () {}\n")


def test_rejects_literal_marker(tmp_path, monkeypatch) -> None:
    # A {{MARKER}} in module text would be re-substituted by _render_page.
    with pytest.raises(ValueError, match="MARKER"):
        _mod(tmp_path, monkeypatch, "// see the {{JS_FORMAT}} marker\n")
