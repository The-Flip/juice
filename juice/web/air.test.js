import { test } from 'node:test';
import assert from 'node:assert/strict';
import { sensorRank, roleOf, orderSensors, closedIntervals } from './air.js';

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
