import { test } from 'node:test';
import assert from 'node:assert/strict';
import { JSDOM } from 'jsdom';
import {
  sensorRank, roleOf, orderSensors, closedIntervals,
  buildMetricChips, buildRangeChips, buildLegend,
  fmt, staleLabel, buildSensorCards,
} from './air.js';

const parse = (html) => new JSDOM(`<div id="d">${html}</div>`).window.document.getElementById('d');

test('roleOf classifies by name substring (case-insensitive), else null', () => {
  assert.equal(roleOf({ name: 'Front Room' }), 'front');
  assert.equal(roleOf({ name: 'the BACK' }), 'back');
  assert.equal(roleOf({ name: 'Workshop bench' }), 'workshop');
  assert.equal(roleOf({ name: 'Lobby' }), null);
  assert.equal(roleOf({}), null);
  assert.equal(roleOf(null), null);
});

test('sensorRank orders front < back < workshop < other', () => {
  assert.equal(sensorRank({ name: 'Front' }), 0);
  assert.equal(sensorRank({ name: 'Back' }), 1);
  assert.equal(sensorRank({ name: 'Workshop' }), 2);
  assert.equal(sensorRank({ name: 'Lobby' }), 3);
});

test('orderSensors sorts by role then name, without mutating the input', () => {
  const input = [
    { name: 'Zeta' }, { name: 'Workshop' }, { name: 'Back' },
    { name: 'Front' }, { name: 'Alpha' },
  ];
  const out = orderSensors(input);
  assert.deepEqual(out.map((s) => s.name), ['Front', 'Back', 'Workshop', 'Alpha', 'Zeta']);
  assert.equal(input[0].name, 'Zeta'); // original order untouched
});

// Build dates with local-time components so assertions hold regardless of the
// runner's timezone (closedIntervals works in local time, like the museum hours).
const hrs = (intervals) => intervals.map(([a, b]) => [a.getHours(), b.getHours()]);

test('closedIntervals: a weekday is closed before 10:00 and after 20:00', () => {
  const day = new Date(2024, 0, 1); // Mon 2024-01-01, local midnight
  const next = new Date(2024, 0, 2);
  const out = closedIntervals(day, next);
  assert.equal(out.length, 2);
  assert.deepEqual(hrs(out), [[0, 10], [20, 0]]); // [00:00,10:00] and [20:00,24:00]
});

test('closedIntervals: Sunday uses 11:00–18:00 hours', () => {
  const sun = new Date(2023, 11, 31); // Sun 2023-12-31
  const out = closedIntervals(sun, new Date(2024, 0, 1));
  assert.deepEqual(hrs(out), [[0, 11], [18, 0]]);
});

test('closedIntervals: a window entirely within open hours is empty', () => {
  const open = new Date(2024, 0, 1, 12); // Mon noon
  const stillOpen = new Date(2024, 0, 1, 15);
  assert.deepEqual(closedIntervals(open, stillOpen), []);
});

test('closedIntervals: clamps to the requested window', () => {
  const from = new Date(2024, 0, 1, 8); // 08:00, before open
  const to = new Date(2024, 0, 1, 11); // 11:00, after open
  const out = closedIntervals(from, to);
  assert.equal(out.length, 1);
  assert.equal(out[0][0].getHours(), 8); // clamped start
  assert.equal(out[0][1].getHours(), 10); // open at 10:00
});

const METRICS = { noise: { label: 'Noise' }, co2: { label: 'CO₂' } };

test('buildMetricChips: one chip per metric, selected ones active', () => {
  const el = parse(buildMetricChips(['noise', 'co2'], new Set(['noise']), METRICS));
  const chips = el.querySelectorAll('.chip');
  assert.equal(chips.length, 2);
  assert.equal(chips[0].dataset.metric, 'noise');
  assert.equal(chips[0].textContent, 'Noise');
  assert.ok(chips[0].classList.contains('active'));
  assert.equal(chips[0].getAttribute('aria-pressed'), 'true');
  assert.equal(chips[1].classList.contains('active'), false);
});

test('buildRangeChips: the current range is active', () => {
  const el = parse(buildRangeChips([{ label: '1D', days: 1 }, { label: '7D', days: 7 }], 7));
  const chips = el.querySelectorAll('.chip');
  assert.equal(chips[1].dataset.days, '7');
  assert.ok(chips[1].classList.contains('active'));
  assert.equal(chips[0].classList.contains('active'), false);
});

test('buildLegend: swatch uses colorFor; name is escaped', () => {
  const el = parse(buildLegend(
    [{ mac: 'a', name: 'Front' }, { mac: 'b', name: '<x>' }],
    (mac) => (mac === 'a' ? '#34c759' : '#000'),
  ));
  const items = el.querySelectorAll('.item');
  assert.match(items[0].querySelector('.swatch').getAttribute('style'), /#34c759/);
  assert.match(items[0].textContent, /Front/);
  assert.equal(items[1].querySelector('x'), null); // escaped, not injected
});

test('fmt: em-dash for missing, rounds without decimals, fixes with', () => {
  assert.equal(fmt(null), '—');
  assert.equal(fmt(undefined), '—');
  assert.equal(fmt(12.7), '13'); // rounded
  assert.equal(fmt(12.34, 1), '12.3'); // toFixed(1)
});

test('staleLabel: null when fresh, minutes then hours when stale', () => {
  const ago = (min) => new Date(Date.now() - min * 60000).toISOString();
  assert.equal(staleLabel(ago(10)), null); // < 45 min
  assert.equal(staleLabel(null), null);
  assert.equal(staleLabel(ago(60)), '60 min ago');
  assert.equal(staleLabel(ago(180)), '3 h ago');
});

const CARD_METRICS = {
  noise: { label: 'Noise', unit: 'dB' }, co2: { label: 'CO₂', unit: 'ppm' },
  tvoc: { label: 'TVOC', unit: 'ppb' }, battery: { label: 'Bat', unit: '%' },
};
const cardDeps = (selected) => ({
  primary: ['noise', 'co2'], metrics: CARD_METRICS,
  selectedDevices: new Set(selected), colorFor: () => '#34c759', bandClass: () => 'good',
});

test('buildSensorCards: metrics, swatch, badge, secondary (filtered), selected state', () => {
  const s = { mac: 'a', name: 'Front', online: true, noise: 50, co2: 800, tvoc: null, battery: 90, ts: new Date().toISOString() };
  const card = parse(buildSensorCards([s], cardDeps(['a']))).querySelector('.card');
  assert.equal(card.dataset.mac, 'a');
  assert.match(card.querySelector('.card-name').textContent, /Front/);
  assert.match(card.querySelector('.card-swatch').getAttribute('style'), /#34c759/);
  assert.equal(card.classList.contains('excluded'), false); // selected
  assert.ok(card.querySelector('.badge.online'));
  assert.ok(card.querySelector('.value.good')); // bandClass applied
  // secondary: battery shown, tvoc (null) filtered out
  assert.match(card.querySelector('.secondary').textContent, /Bat: 90 %/);
  assert.doesNotMatch(card.querySelector('.secondary').textContent, /TVOC/);
  assert.equal(card.querySelector('.stale'), null); // fresh
});

test('buildSensorCards: unselected → excluded; offline badge; name escaped; stale line', () => {
  const s = { mac: 'b', name: '<x>', online: false, noise: 50, co2: 800, ts: new Date(Date.now() - 3 * 3600000).toISOString() };
  const card = parse(buildSensorCards([s], cardDeps([]))).querySelector('.card');
  assert.ok(card.classList.contains('excluded'));
  assert.equal(card.getAttribute('aria-pressed'), 'false');
  assert.ok(card.querySelector('.badge.offline'));
  assert.equal(card.querySelector('.card-name x'), null); // escaped
  assert.match(card.querySelector('.stale').textContent, /Last reading 3 h ago/);
});
