# juice/web — testable frontend modules

The juice frontend is otherwise inline JavaScript inside the `*_HTML` string
templates in `juice/server.py`. This directory holds the pieces we've extracted
into real `.js` files so they can be **unit-tested** (`node --test`) and **shared**
(one definition, inlined where used) instead of copy-pasted across templates.

There is **no bundler and no npm dependency.** Modules are written as ESM (so the
tests can `import` them) and inlined into the page at serve time by `_web_js()` in
`server.py`, which strips the leading `export ` so each declaration becomes a
page-scope global — exactly how the surrounding inline code already calls them.

## How serving works

`server.py` reads each module once at import, strips `export `, and substitutes it
into the templates via a `{{JS_<NAME>}}` marker (placed at the **end** of
`_render_page`'s replace-chain so injected code isn't re-scanned for `{{…}}`).
A page's `<script>` lists only the markers it needs, e.g. `{{JS_POWER}}`.

## Conventions (required — the loader and tests depend on them)

- Export only with **`export function name(...)`** or **`export const NAME = ...`**.
  No `export default`, no `export { ... }` blocks, no re-exports.
- **No cross-module `import`.** The loader only strips `export`, not `import`; a
  page that needs a shared helper inlines that module first and relies on the
  global. (When this becomes painful — e.g. render functions needing `escapeHtml`
  — the rule is still "inline `format` first, use the global", not `import`.)
- **Unique top-level identifier names** across any modules co-inlined into the
  same page (top-level `const` collides → page-wide `SyntaxError`).
- Anything referenced from inline `onclick="..."` must be an **`export function`**
  (a `const` arrow is not a `window` global, so onclick can't see it).

## Testing

- `make test-js` → `node --test 'juice/web/**/*.test.js'`. Tests live next to the
  module as `<name>.test.js` and import it directly.
- `tests/test_inline_js.py` (pytest) is the integration guard: it renders every
  template through `_render_page` and `node --check`s the assembled inline script,
  **and** asserts define-if-called for each injected helper — so a dropped
  duplicate or a missing `{{JS_*}}` marker fails CI instead of silently shipping a
  `ReferenceError`.

## Roadmap (this is the foundation; later phases)

- **Phase 2 — DOM rendering** (`render*`, `showToast`, `drawSparkline`): extract and
  test under a DOM environment (decide jsdom-under-node vs Playwright then).
- **Phase 3 — charting**: extract the d3 data/scale shaping out of the draw calls
  and unit-test that (not pixels).
- **Phase 4 — integration/e2e**: Playwright against `JUICE_DEV_AUTH=1 juice serve
  --dev-auth` to cover SSE/glue/charts end-to-end.

Track coverage with `node --test --experimental-test-coverage`.
