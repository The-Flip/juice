import { test } from 'node:test';
import assert from 'node:assert/strict';
import { JSDOM } from 'jsdom';
import { buildCircuitHeader, buildMemberRows, buildAddStripOptions } from './circuit_page.js';

const parse = (html) => new JSDOM(`<div id="d">${html}</div>`).window.document.getElementById('d');

test('buildCircuitHeader: labelled name span + edit button', () => {
  const el = parse(buildCircuitHeader({ panel: 'P1', breaker: 'B20', description: 'Backline' }));
  assert.equal(el.querySelector('#circuit-name').textContent, 'P1 B20 — Backline');
  assert.ok(el.querySelector('button.edit-name-btn.private-only'));
});

test('buildCircuitHeader: escapes the label', () => {
  const el = parse(buildCircuitHeader({ panel: 'P1', breaker: 'B1', description: '<b>x</b>' }));
  assert.equal(el.querySelector('#circuit-name b'), null);
  assert.match(el.querySelector('#circuit-name').textContent, /<b>x<\/b>/);
});

test('buildMemberRows: empty → no-data placeholder', () => {
  const el = parse(buildMemberRows([]));
  assert.ok(el.querySelector('.no-data'));
  assert.equal(el.querySelector('.member-row'), null);
});

test('buildMemberRows: one row links the strip page and has a Remove button', () => {
  const el = parse(buildMemberRows([{ device_id: 'dev 1', display_name: 'Backline' }]));
  const row = el.querySelector('.member-row');
  assert.equal(row.querySelector('a').getAttribute('href'), '/strip/dev%201');  // encoded
  assert.match(row.querySelector('a').textContent, /Backline/);
  const btn = row.querySelector('button.btn-cancel.private-only');
  assert.match(btn.textContent, /Remove/);
  assert.match(btn.getAttribute('onclick'), /assignStrip\('dev%201', null\)/);
});

test('buildMemberRows: falls back to device_id and escapes the name', () => {
  const el = parse(buildMemberRows([{ device_id: 'dev1', display_name: '' }]));
  assert.match(el.querySelector('.member-row a').textContent, /dev1/);
  const el2 = parse(buildMemberRows([{ device_id: 'dev1', display_name: '<img src=x>' }]));
  assert.equal(el2.querySelector('.member-row a img'), null);
});

test('buildAddStripOptions: prompt + only strips not already assigned', () => {
  const el = parse(`<select>${buildAddStripOptions(
    [{ device_id: 'a', display_name: 'A' }, { device_id: 'b', display_name: 'B' }],
    [{ device_id: 'a' }],
  )}</select>`);
  const opts = [...el.querySelectorAll('option')];
  assert.equal(opts[0].value, '');
  assert.match(opts[0].textContent, /Add a strip/);
  const vals = opts.map((o) => o.value);
  assert.ok(!vals.includes('a'));   // already a member
  assert.ok(vals.includes('b'));    // available
});

test('buildAddStripOptions: escapes device_id and display_name; falls back to device_id', () => {
  const el = parse(`<select>${buildAddStripOptions(
    [{ device_id: 'dev1', display_name: '' }, { device_id: '"x"', display_name: '<b>y</b>' }],
    [],
  )}</select>`);
  const opts = [...el.querySelectorAll('option')];
  assert.match(opts[1].textContent, /dev1/);     // display_name fell back to device_id
  assert.equal(opts[2].value, '"x"');            // attribute round-trips after escaping
  assert.equal(opts[2].querySelector('b'), null);
});
