# juice/web — testable frontend modules

The juice frontend is otherwise inline JavaScript inside the `*_HTML` string
templates in `juice/server.py`. This directory holds the pieces we've extracted
into real `.js` files so they can be **unit-tested** (`node --test`) and **shared**
(one definition, inlined where used) instead of copy-pasted across templates.

There is **no bundler.** Modules are written as ESM (so the tests can `import`
them) and inlined into the page at serve time by `_web_js()` in `server.py`, which
strips the leading `export` keyword (and single-line `import` lines, see below) so
each declaration becomes a page-scope global — exactly how the surrounding inline code
already calls them. The only npm dependency is **jsdom**, a dev-only tool for
DOM-level unit tests (`node --test`); nothing ships to the browser via npm.

## How serving works

`server.py` reads each module once at import, strips `export `, and substitutes it
into the templates via a `{{JS_<NAME>}}` marker (placed at the **end** of
`_render_page`'s replace-chain so injected code isn't re-scanned for `{{…}}`).
A page's `<script>` lists only the markers it needs, e.g. `{{JS_POWER}}`.

## Conventions (required — the loader and tests depend on them)

- Export only with **`export function name(...)`** or **`export const NAME = ...`**.
  No `export default`, no `export { ... }` blocks, no re-exports.
- **Cross-module deps use a single-line named import**, e.g.
  `import { escapeHtml } from './format.js';` — the loader strips it for the
  browser (the imported name is a global from another inlined module) while node
  resolves it for tests. **No aliasing** (`import { x as y }` has no matching
  global → rejected), no default/namespace/multi-line imports. The page must also
  inline the providing module's marker; `tests/test_inline_js.py` enforces that
  every *exported* name is defined on any page that calls it.
- **Unique top-level identifier names** across any modules co-inlined into the
  same page (top-level `const` collides → page-wide `SyntaxError`).
- Anything referenced from inline `onclick="..."` must be an **`export function`**
  (a `const` arrow is not a `window` global, so onclick can't see it).
- **Pure builder + thin glue.** Prefer an exported `build*(data, …) -> string`
  that takes inputs as parameters and returns HTML (trivially testable). Keep the
  DOM glue — reading page state, `el.innerHTML = …`, attaching listeners — inline
  in the template. Don't stub the whole page-global surface in tests.

## Testing

- `make test-js` → `npm ci` then `node --test 'juice/web/**/*.test.js'`. Tests live
  next to the module as `<name>.test.js` and import it directly. Pure-logic tests
  use `node:assert`; DOM tests parse the built HTML with **jsdom**
  (`import { JSDOM } from 'jsdom'`) and assert on the resulting DOM. (Run
  `npm ci` once if invoking `node --test` directly.)
- `tests/test_inline_js.py` (pytest) is the integration guard: it renders every
  template through `_render_page` and `node --check`s the assembled inline script,
  **and** asserts define-if-called for each exported helper (auto-derived from the
  modules) — so a dropped duplicate or a missing `{{JS_*}}` marker fails CI instead
  of silently shipping a `ReferenceError`.

## Roadmap (later phases)

- **DOM rendering** (in progress): extract more `build*` HTML builders
  (`renderMeta`/meta-bar buttons, dashboard tiles, `renderCards`, `renderRecentEvent`)
  and test their output with jsdom.
- **Charting**: extract the d3 data/scale shaping out of the draw calls and
  unit-test that (not pixels).
- **Integration/e2e**: Playwright against `JUICE_DEV_AUTH=1 juice serve
  --dev-auth` to cover SSE/glue/charts end-to-end.

Track coverage with `node --test --experimental-test-coverage`.
