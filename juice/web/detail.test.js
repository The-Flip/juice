import { test } from 'node:test';
import assert from 'node:assert/strict';
import { JSDOM } from 'jsdom';
import { buildMeta } from './detail.js';

function bar(m, opts = {}) {
  const o = { publicMode: false, pending: null, peakWatts: null, ...opts };
  const dom = new JSDOM('<div id="b"></div>');
  const el = dom.window.document.getElementById('b');
  el.innerHTML = buildMeta(m, o);
  return el;
}

const ONLINE = {
  name: 'Tron', has_emeter: true, power_status: 'on', is_on: true, state: 'PLAYING',
  power: { watts: 120, voltage: 120, amps: 1.5, total_kwh: 5 },
  asset_id: 'M0001', calibrated: true,
  plug: { plug_id: 1, alias: 'Outlet A', device_id: 'dev1', outlet_number: 2 },
  strip_alias: 'Main Strip',
};

test('authed + relay on: Turn Off (enabled), reboot enabled, badge from state', () => {
  const el = bar(ONLINE);
  const power = el.querySelector('#power-btn');
  assert.equal(power.textContent, 'Turn Off');
  assert.ok(power.classList.contains('btn-power-off'));
  assert.equal(power.disabled, false);
  assert.equal(el.querySelector('#reboot-btn').disabled, false);
  assert.match(el.querySelector('#cal-btn').textContent, /Recalibrate/);
  assert.ok(el.querySelector('.state-PLAYING'));
  assert.match(el.querySelector('.meta-item.num .val').textContent, /120\.0 W/);
  assert.match(el.textContent, /Plug/);
  assert.match(el.textContent, /Strip/);
});

test('authed + relay off: Turn On (green), reboot disabled', () => {
  const el = bar({ ...ONLINE, is_on: false, power_status: 'off', state: 'OFF', power: null });
  const power = el.querySelector('#power-btn');
  assert.equal(power.textContent, 'Turn On');
  assert.ok(power.classList.contains('btn-power-on'));
  assert.equal(power.disabled, false);
  assert.equal(el.querySelector('#reboot-btn').disabled, true); // can't reboot an off outlet
});

test('pending turn_on: every control disabled, neutral label', () => {
  const el = bar(ONLINE, { pending: { action: 'turn_on', sawOff: false } });
  const power = el.querySelector('#power-btn');
  assert.equal(power.disabled, true);
  assert.match(power.textContent, /Turning on/);
  for (const id of ['#reboot-btn', '#lock-btn', '#cal-btn']) {
    assert.equal(el.querySelector(id).disabled, true, `${id} should be disabled while pending`);
  }
});

test('offline: power button reads Offline (disabled), OFFLINE badge', () => {
  const el = bar({ ...ONLINE, power_status: 'offline' });
  const power = el.querySelector('#power-btn');
  assert.equal(power.textContent, 'Offline');
  assert.equal(power.disabled, true);
  assert.ok(el.querySelector('.state-OFFLINE'));
});

test('locked on: power reads Locked (disabled) and a lock badge shows', () => {
  const el = bar({ ...ONLINE, lock_mode: 'on', locked: true });
  const power = el.querySelector('#power-btn');
  assert.equal(power.textContent, 'Locked');
  assert.equal(power.disabled, true);
  assert.ok(el.querySelector('.lock-badge'));
});

test('no_draw: shows the "No draw" badge + hint, reboot stays enabled (relay on)', () => {
  const el = bar({ ...ONLINE, power_status: 'no_draw' });
  assert.ok(el.querySelector('.state-NO_DRAW'));
  assert.ok(el.querySelector('.no-draw-hint'));
  assert.equal(el.querySelector('#reboot-btn').disabled, false); // relayOn (is_on && !offline)
});

test('locked off: power reads Locked and a "Locked off" badge shows', () => {
  const el = bar({ ...ONLINE, is_on: false, power_status: 'off', lock_mode: 'off', locked: true });
  assert.equal(el.querySelector('#power-btn').textContent, 'Locked');
  const badge = el.querySelector('.lock-badge');
  assert.match(badge.textContent, /Locked off/);
});

test('plug without outlet_number/device_id: plain alias, non-link strip', () => {
  const el = bar({ ...ONLINE, plug: { plug_id: 1, alias: 'Outlet A' }, strip_alias: 'S' });
  assert.match(el.textContent, /Outlet A/);
  assert.doesNotMatch(el.textContent, /#2 —/); // no outlet number prefix
  assert.equal(el.querySelector('a[href^="/strip/"]'), null); // no device_id → no link
});

test('public mode: no controls and no plug/strip rows', () => {
  const el = bar(ONLINE, { publicMode: true });
  assert.equal(el.querySelector('#power-btn'), null);
  assert.equal(el.querySelector('#reboot-btn'), null);
  assert.equal(el.querySelector('#lock-btn'), null);
  assert.equal(el.querySelector('#cal-btn'), null);
  assert.doesNotMatch(el.textContent, /Plug/);
  assert.doesNotMatch(el.textContent, /Strip/);
});

test('no-emeter machine: no Calibrate button, "no data" watts', () => {
  const el = bar({ ...ONLINE, has_emeter: false, power: null });
  assert.equal(el.querySelector('#cal-btn'), null);
  assert.match(el.textContent, /no data/);
  assert.ok(el.querySelector('.state-PLAYING')); // relay on + no emeter → PLAYING
});

test('asset id is HTML-escaped, not injected', () => {
  const el = bar({ ...ONLINE, asset_id: '<img src=x>' });
  // The asset value lands in a text/attr context; no real element is injected.
  assert.equal(el.querySelector('img'), null);
  assert.match(el.textContent, /<img src=x>/);
});
