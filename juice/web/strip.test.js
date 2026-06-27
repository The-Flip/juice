import { test } from 'node:test';
import assert from 'node:assert/strict';
import { JSDOM } from 'jsdom';
import { buildStripHeader, buildOutletRows, buildCircuitLine } from './strip.js';

const parse = (html) => new JSDOM(`<div id="d">${html}</div>`).window.document.getElementById('d');

test('buildStripHeader: display_name wins, with rename button', () => {
  const el = parse(buildStripHeader({ display_name: 'Backline', device_id: 'dev1' }));
  assert.equal(el.querySelector('#strip-name').textContent, 'Backline');
  assert.ok(el.querySelector('button.edit-name-btn.private-only'));
  assert.equal(el.querySelector('.alias-hint'), null);
});

test('buildStripHeader: falls back to device_id when no display_name', () => {
  const el = parse(buildStripHeader({ display_name: '', device_id: 'dev1' }));
  assert.equal(el.querySelector('#strip-name').textContent, 'dev1');
});

test('buildStripHeader: alias hint shows only when a name shadows a different alias', () => {
  // name set + alias differs → hint
  let el = parse(buildStripHeader({ display_name: 'Backline', name: 'Backline', alias: 'HS300-7' }));
  assert.match(el.querySelector('.alias-hint').textContent, /alias: HS300-7/);
  // alias equals name → no hint
  el = parse(buildStripHeader({ display_name: 'X', name: 'X', alias: 'X' }));
  assert.equal(el.querySelector('.alias-hint'), null);
  // no custom name → no hint
  el = parse(buildStripHeader({ display_name: 'X', name: '', alias: 'HS300-7' }));
  assert.equal(el.querySelector('.alias-hint'), null);
});

test('buildStripHeader: escapes display and alias', () => {
  const el = parse(buildStripHeader({ display_name: '<img src=x>', name: '<img src=x>', alias: '<b>a</b>' }));
  assert.equal(el.querySelector('#strip-name img'), null);
  assert.match(el.querySelector('#strip-name').textContent, /<img src=x>/);
  assert.equal(el.querySelector('.alias-hint b'), null);
});

test('buildOutletRows: empty outlets → no-data', () => {
  const el = parse(buildOutletRows({ outlets: [] }));
  assert.ok(el.querySelector('.no-data'));
});

test('buildOutletRows: machine row links to the plug page with asset id', () => {
  const el = parse(buildOutletRows({
    offline: false,
    outlets: [{ outlet_number: 1, plug_id: 9, watts: 42.5, is_on: true,
      machine: { name: 'Star Trip', asset_id: 'M0009' } }],
  }));
  const row = el.querySelector('.outlet-row');
  assert.equal(row.querySelector('.outlet-num').textContent, '1');
  assert.ok(row.querySelector('a[href="/machine/9"]'));
  assert.match(row.querySelector('.outlet-machine').textContent, /Star Trip/);
  assert.match(row.querySelector('.outlet-machine').textContent, /M0009/);
  assert.match(row.querySelector('.outlet-watts').textContent, /42\.5 W/);
});

test('buildOutletRows: unassigned outlet shows alias (or em dash), no machine link', () => {
  let el = parse(buildOutletRows({ outlets: [{ outlet_number: 2, alias: 'Spare' }] }));
  assert.equal(el.querySelector('.outlet-machine a'), null);
  assert.match(el.querySelector('.outlet-empty').textContent, /Spare/);
  el = parse(buildOutletRows({ outlets: [{ outlet_number: 2, alias: '' }] }));
  assert.match(el.querySelector('.outlet-empty').textContent, /—/);
});

test('buildOutletRows: offline strip dims watts to OFFLINE and dot to offline', () => {
  const el = parse(buildOutletRows({ offline: true, outlets: [{ outlet_number: 1, watts: 10, is_on: true }] }));
  assert.match(el.querySelector('.outlet-watts').textContent, /OFFLINE/);
  assert.ok(el.querySelector('.outlet-dot.offline'));
});

test('buildOutletRows: no_draw dot carries the explanatory title', () => {
  const el = parse(buildOutletRows({ outlets: [{ outlet_number: 1, power_status: 'no_draw' }] }));
  const dot = el.querySelector('.outlet-dot.no_draw');
  assert.ok(dot);
  assert.match(dot.getAttribute('title'), /no draw|faulted/);
});

test('buildOutletRows: missing outlet number renders the placeholder dot', () => {
  const el = parse(buildOutletRows({ outlets: [{ alias: 'x' }] }));
  assert.equal(el.querySelector('.outlet-num').textContent, '·');
});

// -- per-outlet power buttons (operator) ------------------------------------

test('buildOutletRows: operator gets a Turn Off button on an on outlet', () => {
  const el = parse(buildOutletRows({
    offline: false,
    outlets: [{ outlet_number: 1, plug_id: 9, is_on: true,
      machine: { name: 'Star Trip', asset_id: 'M0009' } }],
  }, { publicMode: false, pendingPlugs: new Map() }));
  const btn = el.querySelector('.outlet-ctl button');
  assert.ok(btn);
  assert.match(btn.textContent, /Turn Off/);
  assert.match(btn.getAttribute('onclick'), /togglePlug\(event, 9, false\)/);
});

test('buildOutletRows: off outlet gets a Turn On button', () => {
  const el = parse(buildOutletRows({
    outlets: [{ outlet_number: 1, plug_id: 9, is_on: false }],
  }, { publicMode: false, pendingPlugs: new Map() }));
  const btn = el.querySelector('.outlet-ctl button');
  assert.match(btn.textContent, /Turn On/);
  assert.match(btn.getAttribute('onclick'), /togglePlug\(event, 9, true\)/);
});

test('buildOutletRows: public mode renders no power button', () => {
  const el = parse(buildOutletRows({
    outlets: [{ outlet_number: 1, plug_id: 9, is_on: true }],
  }, { publicMode: true, pendingPlugs: new Map() }));
  assert.equal(el.querySelector('.outlet-ctl button'), null);
});

test('buildOutletRows: offline strip and offline outlet render no button', () => {
  let el = parse(buildOutletRows({
    offline: true, outlets: [{ outlet_number: 1, plug_id: 9, is_on: true }],
  }, { publicMode: false, pendingPlugs: new Map() }));
  assert.equal(el.querySelector('.outlet-ctl button'), null);
  el = parse(buildOutletRows({
    outlets: [{ outlet_number: 1, plug_id: 9, is_on: true, power_status: 'offline' }],
  }, { publicMode: false, pendingPlugs: new Map() }));
  assert.equal(el.querySelector('.outlet-ctl button'), null);
});

test('buildOutletRows: pending outlet shows a disabled neutral button, no togglePlug', () => {
  const el = parse(buildOutletRows({
    outlets: [{ outlet_number: 1, plug_id: 9, is_on: false }],
  }, { publicMode: false, pendingPlugs: new Map([[9, 'turn_on']]) }));
  const btn = el.querySelector('.outlet-ctl button');
  assert.ok(btn.disabled);
  assert.match(btn.textContent, /Turning on/);
  assert.equal(btn.getAttribute('onclick'), null);
});

test('buildOutletRows: locked machine shows a disabled Locked button', () => {
  // locked on + currently on → can't turn off
  const el = parse(buildOutletRows({
    outlets: [{ outlet_number: 1, plug_id: 9, is_on: true, lock_mode: 'on',
      machine: { name: 'X', asset_id: 'M1' } }],
  }, { publicMode: false, pendingPlugs: new Map() }));
  const btn = el.querySelector('.outlet-ctl button');
  assert.ok(btn.disabled);
  assert.match(btn.textContent, /Locked/);
  assert.equal(btn.getAttribute('onclick'), null);
});

test('buildOutletRows: non-machine outlet with a plug id still gets a button', () => {
  const el = parse(buildOutletRows({
    outlets: [{ outlet_number: 2, plug_id: 4, is_on: false, alias: 'Sign' }],
  }, { publicMode: false, pendingPlugs: new Map() }));
  assert.ok(el.querySelector('.outlet-ctl button'));
});

test('buildOutletRows: single-arg call is safe and renders no button without a plug id', () => {
  const el = parse(buildOutletRows({ outlets: [{ outlet_number: 1, alias: 'x' }] }));
  assert.ok(el.querySelector('.outlet-row'));
  assert.equal(el.querySelector('.outlet-ctl button'), null);
});

test('buildCircuitLine: unassigned strip shows "none" and the option list', () => {
  const el = parse(buildCircuitLine([{ circuit_id: 1, panel: 'P1', breaker: 'B1', device_ids: ['other'] }], 'me'));
  assert.equal(el.querySelector('a[href^="/circuit/"]'), null);  // not linked when unassigned
  assert.match(el.textContent, /none/);
  const vals = [...el.querySelectorAll('option')].map((o) => o.value);
  assert.ok(vals.includes('none'));
  assert.ok(vals.includes('new'));
  assert.ok(vals.includes('1'));
});

test('buildCircuitLine: assigned strip links its circuit and pre-selects it', () => {
  const el = parse(buildCircuitLine([
    { circuit_id: 7, panel: 'P1', breaker: 'B20', description: 'Backline', device_ids: ['me'] },
  ], 'me'));
  assert.ok(el.querySelector('a[href="/circuit/7"]'));
  assert.match(el.querySelector('a[href="/circuit/7"]').textContent, /P1 B20 — Backline/);
  const selected = el.querySelector('option[value="7"]');
  assert.ok(selected.selected);
});

test('buildCircuitLine: escapes the circuit label', () => {
  const el = parse(buildCircuitLine([
    { circuit_id: 1, panel: 'P1', breaker: 'B1', description: '<b>x</b>', device_ids: ['me'] },
  ], 'me'));
  assert.equal(el.querySelector('a[href="/circuit/1"] b'), null);
});
