import { test } from 'node:test';
import assert from 'node:assert/strict';
import { JSDOM } from 'jsdom';
import { groupByStrip, buildTiles } from './tiles.js';

const NO_PENDING = new Map();

function grid(machines, outlets = [], opts = {}) {
  const o = { publicMode: false, pendingPlugs: NO_PENDING, ...opts };
  const dom = new JSDOM('<div id="c"></div>');
  const el = dom.window.document.getElementById('c');
  el.innerHTML = buildTiles(groupByStrip(machines), outlets, o);
  return el;
}

const emeter = (over) => ({
  name: 'M', has_emeter: true, power_status: 'on', is_on: true, state: 'PLAYING',
  power: { watts: 120 }, plug: { plug_id: 1 }, strip_device_id: 'd1', strip_alias: 'S1',
  sparkline: [1, 2], ...over,
});

test('groupByStrip preserves first-seen strip order and groups members', () => {
  const strips = groupByStrip([
    emeter({ strip_device_id: 'a', name: 'A1' }),
    emeter({ strip_device_id: 'b', name: 'B1' }),
    emeter({ strip_device_id: 'a', name: 'A2' }),
  ]);
  assert.deepEqual(strips.map((s) => s.deviceId), ['a', 'b']);
  assert.deepEqual(strips[0].machines.map((m) => m.name), ['A1', 'A2']);
});

test('emeter tile: strip link, machine name, watts, and a spark canvas', () => {
  const el = grid([emeter({ name: 'Tron', power: { watts: 42.5 } })]);
  assert.ok(el.querySelector('a.strip-label[href="/strip/d1"]'));
  assert.match(el.querySelector('.machine-name').textContent, /Tron/);
  assert.match(el.querySelector('.tile-watts').textContent, /42\.5W/);
  assert.ok(el.querySelector('canvas#spark-0'));
});

test('spark idx advances for non-canvas tiles too (offline/no-emeter)', () => {
  // online emeter (idx 0, canvas), no-emeter (idx 1, NO canvas), online emeter
  // (idx 2, canvas), offline emeter (idx 3, NO canvas). The canvases must be
  // spark-0 and spark-2 — proving idx advances even for tiles with no canvas, so
  // the draw loop (which walks the same order) stays aligned.
  const el = grid([
    emeter({ name: 'A' }),
    emeter({ name: 'B', has_emeter: false, sparkline: null }),
    emeter({ name: 'C' }),
    emeter({ name: 'D', offline: true, sparkline: null }),
  ]);
  const ids = [...el.querySelectorAll('canvas')].map((c) => c.id);
  assert.deepEqual(ids, ['spark-0', 'spark-2']);
});

test('spark canvas ids match the strip-grouped flattened order', () => {
  // Interleaved strips: grouping reorders to [a:A1,a:A2, b:B1]; spark ids follow.
  const el = grid([
    emeter({ strip_device_id: 'a', name: 'A1' }),
    emeter({ strip_device_id: 'b', name: 'B1' }),
    emeter({ strip_device_id: 'a', name: 'A2' }),
  ]);
  const ids = [...el.querySelectorAll('canvas')].map((c) => c.id);
  assert.deepEqual(ids, ['spark-0', 'spark-1', 'spark-2']);
  // The strips array the caller iterates for drawing is the same order:
  const strips = groupByStrip([
    emeter({ strip_device_id: 'a', name: 'A1' }),
    emeter({ strip_device_id: 'b', name: 'B1' }),
    emeter({ strip_device_id: 'a', name: 'A2' }),
  ]);
  const flat = strips.flatMap((s) => s.machines.map((m) => m.name));
  assert.deepEqual(flat, ['A1', 'A2', 'B1']); // idx 0,1,2 → A1,A2,B1
});

test('no-emeter tile: ON/OFF + a toggle; offline tile shows OFFLINE, no toggle', () => {
  const on = grid([emeter({ has_emeter: false, is_on: true, sparkline: null })]);
  assert.match(on.querySelector('.tile-onoff.on').textContent, /ON/);
  assert.match(on.querySelector('.tile-toggle').textContent, /Turn Off/);

  const off = grid([emeter({ has_emeter: false, offline: true, sparkline: null })]);
  assert.match(off.querySelector('.tile-offline').textContent, /OFFLINE/);
  assert.equal(off.querySelector('.tile-toggle'), null);
});

test('pending plug: toggle is disabled with a neutral label', () => {
  const pending = new Map([[1, 'turn_on']]);
  const el = grid([emeter({ has_emeter: false, is_on: false, sparkline: null })], [], { pendingPlugs: pending });
  const btn = el.querySelector('.tile-toggle');
  assert.equal(btn.disabled, true);
  assert.match(btn.textContent, /Turning on/);
});

test('public mode: no strip label and no toggles', () => {
  const el = grid([emeter({ has_emeter: false, is_on: true, sparkline: null })], [], { publicMode: true });
  assert.equal(el.querySelector('.strip-label'), null);
  assert.equal(el.querySelector('.tile-toggle'), null);
});

test('outlets section renders toggles and escapes the alias', () => {
  const el = grid([], [{ plug_id: 9, alias: '<x>Snack', is_on: false }]);
  assert.match(el.querySelector('.outlets-section .outlet-alias').textContent, /<x>Snack/);
  assert.equal(el.querySelector('.outlets-section img'), null); // escaped, not injected
  assert.match(el.querySelector('.outlets-section .tile-toggle').textContent, /Turn On/);
});
