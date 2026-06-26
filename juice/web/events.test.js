import { test } from 'node:test';
import assert from 'node:assert/strict';
import { JSDOM } from 'jsdom';
import { buildEventRow } from './events.js';

// Parse the built row into a real DOM (this is what jsdom buys us over string
// matching: structural + escaping assertions). A fresh JSDOM per call avoids any
// cross-test global state.
function rowEl(e) {
  const dom = new JSDOM(`<table><tbody id="t"></tbody></table>`);
  dom.window.document.getElementById('t').innerHTML = buildEventRow(e);
  return dom.window.document.querySelector('tr');
}

test('turn_on renders an ON action cell', () => {
  const tr = rowEl({
    ts: '2024-01-01T12:00:00Z', actor: 'will', machine_name: 'Tron',
    action: 'turn_on', source: 'individual', result: 'ok',
  });
  const cell = tr.querySelector('td.action-on');
  assert.ok(cell, 'expected an action-on cell');
  assert.equal(cell.textContent, 'ON');
});

test('turn_off renders an OFF cell and an underscore-source becomes a space', () => {
  const tr = rowEl({
    ts: '2024-01-01T12:00:00Z', actor: 'a', plug_alias: 'P1',
    action: 'turn_off', source: 'all_off', result: 'ok',
  });
  assert.ok(tr.querySelector('td.action-off'));
  assert.equal(tr.querySelector('.source-all_off').textContent, 'all off');
});

test('error detail is HTML-escaped (no element injection) and marks the result cell', () => {
  const tr = rowEl({
    ts: '2024-01-01T12:00:00Z', actor: 'a', plug_alias: 'P1',
    action: 'turn_off', source: 'individual', result: 'error', error: '<img src=x>&"',
  });
  const cell = tr.querySelector('td.result-error');
  assert.ok(cell, 'expected a result-error cell');
  // Decoded text is the original string; no real <img> was injected.
  assert.match(cell.textContent, /error — <img src=x>&"/);
  assert.equal(cell.querySelectorAll('*').length, 0);
});

test('target falls back to "Plug <id>" when no machine name or alias', () => {
  const tr = rowEl({
    ts: '2024-01-01T12:00:00Z', actor: 'a', plug_id: 7,
    action: 'turn_on', source: 'individual', result: 'ok',
  });
  assert.match(tr.children[2].textContent, /^Plug 7$/);
});
