import { test } from 'node:test';
import assert from 'node:assert/strict';
import { JSDOM } from 'jsdom';
import { buildMeta, buildDetailStats } from './detail.js';

function bar(m, opts = {}) {
  const o = { publicMode: false, pending: null, ...opts };
  const dom = new JSDOM('<div id="b"></div>');
  const el = dom.window.document.getElementById('b');
  el.innerHTML = buildMeta(m, o);
  return el;
}

function stats(m, opts = {}) {
  const o = { publicMode: false, peakWatts: null, avgDailyCost: null, ...opts };
  const dom = new JSDOM('<div id="s"></div>');
  const el = dom.window.document.getElementById('s');
  el.innerHTML = buildDetailStats(m, o);
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
  // The numeric readouts + plug/strip now live in the Details table, not the meta bar.
  assert.equal(el.querySelector('.meta-item.num'), null);
  assert.doesNotMatch(el.textContent, /Plug|Strip/);
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

test('stats: plug without outlet_number/device_id: plain alias, non-link strip', () => {
  const el = stats({ ...ONLINE, plug: { plug_id: 1, alias: 'Outlet A' }, strip_alias: 'S' });
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

test('no-emeter machine: no Calibrate button, PLAYING badge', () => {
  const el = bar({ ...ONLINE, has_emeter: false, power: null });
  assert.equal(el.querySelector('#cal-btn'), null);
  assert.ok(el.querySelector('.state-PLAYING')); // relay on + no emeter → PLAYING
});

// -- Details table (buildDetailStats) -----------------------------------------

test('stats: operator sees readouts + plug/strip/calibration/cost rows', () => {
  const m = { ...ONLINE, calibration: { idle_max_rsd: 2.5, play_min_rsd: 10.0 } };
  const el = stats(m, { peakWatts: 312.3, avgDailyCost: 1.86 });
  assert.match(el.textContent, /120\.0 W/);      // watts
  assert.match(el.textContent, /312\.3 W/);      // peak
  assert.match(el.textContent, /Plug/);
  assert.match(el.textContent, /Strip/);
  assert.match(el.textContent, /play/);          // calibration thresholds
  assert.match(el.textContent, /10\.0%/);
  assert.match(el.textContent, /idle/);
  assert.match(el.textContent, /2\.5%/);
  assert.match(el.textContent, /\$1\.86\/day/);  // avg daily cost
  assert.match(el.querySelector('.detail-stats-header').textContent, /Details/);
});

test('stats: uncalibrated machine reads "uncalibrated", missing cost shows —', () => {
  const el = stats({ ...ONLINE, calibration: null });  // no peak/cost loaded yet
  assert.match(el.textContent, /uncalibrated/);
  // Avg daily cost cell is a dash until the fetch lands.
  const cells = [...el.querySelectorAll('td')].map(td => td.textContent);
  assert.ok(cells.includes('—'));
});

test('stats: public viewer sees readouts but no plug/strip/calibration/cost', () => {
  const el = stats(ONLINE, { publicMode: true, peakWatts: 100 });
  assert.match(el.textContent, /120\.0 W/);   // readouts are public
  assert.match(el.textContent, /100\.0 W/);   // peak is public
  assert.doesNotMatch(el.textContent, /Plug|Strip|Calibration|daily cost/);
});

test('stats: no-emeter machine shows "no data" watts', () => {
  const el = stats({ ...ONLINE, has_emeter: false, power: null });
  assert.match(el.textContent, /no data/);
});

test('stats: asset id is HTML-escaped, not injected', () => {
  const el = stats({ ...ONLINE, asset_id: '<img src=x>',
    calibration: { idle_max_rsd: null, play_min_rsd: 10.0 } });
  assert.equal(el.querySelector('img'), null);
  assert.match(el.textContent, /<img src=x>/);
});

// -- strip-outlet map (buildOutletMapHeader / buildDetailOutletRows) -----------

import { buildOutletMapHeader, buildDetailOutletRows } from './detail.js';

const parse = (html) => new JSDOM(`<div id="d">${html}</div>`).window.document.getElementById('d');

const STRIP = {
  device_id: 'dev 1', display_name: 'Main Strip', offline: false,
  outlets: [
    { plug_id: 1, outlet_number: 1, watts: 10, is_on: true, machine: { name: 'Tron' } },
    { plug_id: 2, outlet_number: 2, watts: 0, power_status: 'no_draw', machine: { name: 'Star Trek' } },
    { plug_id: 3, outlet_number: 3, alias: 'Spare' },
  ],
};

test('buildOutletMapHeader: "Plug N of M on <strip link>" with the current outlet number', () => {
  const el = parse(buildOutletMapHeader(STRIP, 2));
  assert.match(el.textContent, /Plug 2 of 3 on/);
  const a = el.querySelector('a');
  assert.equal(a.getAttribute('href'), '/strip/dev%201');  // device_id encoded
  assert.match(a.textContent, /Main Strip/);
});

test('buildOutletMapHeader: falls back to "?" and device_id when the plug is absent / unnamed', () => {
  const el = parse(buildOutletMapHeader(
    { device_id: 'dev1', display_name: '', offline: false, outlets: [{ plug_id: 9 }] }, 1));
  assert.match(el.textContent, /Plug \? of 1 on/);
  assert.match(el.querySelector('a').textContent, /dev1/);  // display_name fell back
});

test('buildOutletMapHeader: escapes the strip name', () => {
  const el = parse(buildOutletMapHeader({ ...STRIP, display_name: '<b>x</b>' }, 1));
  assert.equal(el.querySelector('a b'), null);
});

test('buildDetailOutletRows: the current plug is a non-link span + "current" row + "this machine" tag', () => {
  const el = parse(buildDetailOutletRows(STRIP, 1));
  const rows = el.querySelectorAll('.outlet-row');
  assert.equal(rows.length, 3);
  const current = el.querySelector('.outlet-row.current');
  assert.ok(current);
  assert.equal(current.querySelector('.outlet-machine a'), null);   // self is not a link
  assert.match(current.querySelector('.outlet-machine span').textContent, /Tron/);
  assert.match(current.querySelector('.outlet-this').textContent, /this machine/);
});

test('buildDetailOutletRows: other machines link to their plug page; current does not', () => {
  const el = parse(buildDetailOutletRows(STRIP, 1));
  assert.ok(el.querySelector('a[href="/machine/2"]'));      // the other machine
  assert.equal(el.querySelector('a[href="/machine/1"]'), null);  // current is a span
});

test('buildDetailOutletRows: no_draw dot carries the title; unassigned outlet shows alias', () => {
  const el = parse(buildDetailOutletRows(STRIP, 1));
  const noDraw = el.querySelector('.outlet-dot.no_draw');
  assert.ok(noDraw);
  assert.match(noDraw.getAttribute('title'), /no draw|faulted/);
  assert.match(el.querySelector('.outlet-empty').textContent, /Spare/);
});

test('buildDetailOutletRows: offline strip dims watts to OFFLINE and dot to offline', () => {
  const el = parse(buildDetailOutletRows({ ...STRIP, offline: true }, 1));
  assert.match(el.querySelector('.outlet-watts').textContent, /OFFLINE/);
  assert.ok(el.querySelector('.outlet-dot.offline'));
});

test('buildDetailOutletRows: escapes machine name and outlet alias', () => {
  const el = parse(buildDetailOutletRows({
    device_id: 'd', offline: false,
    outlets: [
      { plug_id: 1, outlet_number: 1, machine: { name: '<img src=x>' } },
      { plug_id: 2, outlet_number: 2, alias: '<b>y</b>' },
    ],
  }, 9));  // plugId 9 → neither row is "current", so name renders as a link
  assert.equal(el.querySelector('.outlet-machine img'), null);
  assert.equal(el.querySelector('.outlet-empty b'), null);
});

test('buildDetailOutletRows: escapes the CURRENT machine name (span branch)', () => {
  const el = parse(buildDetailOutletRows({
    device_id: 'd', offline: false,
    outlets: [{ plug_id: 1, outlet_number: 1, machine: { name: '<img src=x>' } }],
  }, 1));  // plugId 1 → this row is current → name renders in the <span> branch
  const cur = el.querySelector('.outlet-row.current .outlet-machine');
  assert.equal(cur.querySelector('a'), null);
  assert.equal(cur.querySelector('img'), null);
  assert.match(cur.textContent, /<img src=x>/);
});

test('buildDetailOutletRows: null outlet_number → "·", empty alias → "—"', () => {
  const el = parse(buildDetailOutletRows({
    device_id: 'd', offline: false,
    outlets: [{ plug_id: 2, alias: '' }],  // no outlet_number, no machine, blank alias
  }, 1));
  assert.equal(el.querySelector('.outlet-num').textContent, '·');
  assert.match(el.querySelector('.outlet-empty').textContent, /—/);
});
