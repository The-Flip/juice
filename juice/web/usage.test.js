import { test } from 'node:test';
import assert from 'node:assert/strict';
import { busyWeekdayIdx, busyWeekAggregate } from './usage.js';

test('busyWeekdayIdx maps Mon→0 … Sun→6', () => {
  assert.equal(busyWeekdayIdx('2024-01-01'), 0); // Monday
  assert.equal(busyWeekdayIdx('2024-01-06'), 5); // Saturday
  assert.equal(busyWeekdayIdx('2024-01-07'), 6); // Sunday
});

test('busyWeekAggregate pools same weekday+hour across dates and recomputes ratio', () => {
  const cells = [
    // two Mondays, hour 14 — should pool into one cell
    { date: '2024-01-01', hour: 14, play_hours: 1, on_hours: 4 },
    { date: '2024-01-08', hour: 14, play_hours: 2, on_hours: 6 },
    // a Monday hour 9
    { date: '2024-01-01', hour: 9, play_hours: 0, on_hours: 0 },
    // a Tuesday hour 14
    { date: '2024-01-02', hour: 14, play_hours: 3, on_hours: 3 },
  ];
  const { cells: out, hours } = busyWeekAggregate(cells);

  assert.deepEqual(hours, [9, 14]); // ascending, de-duplicated

  const monday14 = out.find((c) => c.col === 0 && c.hour === 14);
  assert.deepEqual(
    [monday14.play_hours, monday14.on_hours, monday14.ratio],
    [3, 10, 0.3], // (1+2)/(4+6)
  );

  const monday9 = out.find((c) => c.col === 0 && c.hour === 9);
  assert.equal(monday9.ratio, 0); // on_hours 0 → ratio 0, not NaN

  const tuesday14 = out.find((c) => c.col === 1 && c.hour === 14);
  assert.equal(tuesday14.ratio, 1);
});

test('busyWeekAggregate returns empty for no cells', () => {
  assert.deepEqual(busyWeekAggregate([]), { cells: [], hours: [] });
});

import { pickEveryNthTicks } from './usage.js';

const seq = (n) => Array.from({ length: n }, (_, i) => i);

test('pickEveryNthTicks: keeps all items when they fit under the target', () => {
  // innerW 720 / 90 = 8 → target 8; 6 items ≤ 8 → every=1 → all kept
  assert.deepEqual(pickEveryNthTicks(seq(6), 720, { maxTicks: 8, pxPerTick: 90 }), seq(6));
});

test('pickEveryNthTicks: thins to every Nth, always keeping the first', () => {
  // 30 items, target 8 → every = ceil(30/8) = 4 → indices 0,4,8,…,28
  const out = pickEveryNthTicks(seq(30), 720, { maxTicks: 8, pxPerTick: 90 });
  assert.deepEqual(out, [0, 4, 8, 12, 16, 20, 24, 28]);
  assert.equal(out[0], 0);
});

test('pickEveryNthTicks: clamps target to maxTicks on wide charts', () => {
  // huge innerW → floor(innerW/90) big, clamped to maxTicks 8; 16 items → every=2
  assert.deepEqual(
    pickEveryNthTicks(seq(16), 100000, { maxTicks: 8, pxPerTick: 90 }),
    [0, 2, 4, 6, 8, 10, 12, 14],
  );
});

test('pickEveryNthTicks: clamps target up to minTicks on narrow charts', () => {
  // innerW 90 / 90 = 1, but minTicks 3 → target 3; 12 items → every=ceil(12/3)=4
  assert.deepEqual(pickEveryNthTicks(seq(12), 90, { maxTicks: 8, pxPerTick: 90 }), [0, 4, 8]);
});

test('pickEveryNthTicks: respects a different maxTicks/pxPerTick (busy-grid settings)', () => {
  // busy chart uses maxTicks 10, pxPerTick 70; 28 cols, innerW 700 → floor=10 → target 10
  // every = ceil(28/10) = 3 → 0,3,…,27
  assert.deepEqual(
    pickEveryNthTicks(seq(28), 700, { maxTicks: 10, pxPerTick: 70 }),
    [0, 3, 6, 9, 12, 15, 18, 21, 24, 27],
  );
});

test('pickEveryNthTicks: interior target (between min and max) is used as-is', () => {
  // innerW 450 / 90 = 5 → target 5 (3 < 5 < 8); 20 items → every=ceil(20/5)=4
  assert.deepEqual(
    pickEveryNthTicks(seq(20), 450, { maxTicks: 8, pxPerTick: 90 }),
    [0, 4, 8, 12, 16],
  );
});

test('pickEveryNthTicks: items.length === target → every=1, all kept', () => {
  // innerW 720 / 90 = 8 = target; 8 items → ceil(8/8)=1 → all kept
  assert.deepEqual(pickEveryNthTicks(seq(8), 720, { maxTicks: 8, pxPerTick: 90 }), seq(8));
});

test('pickEveryNthTicks: empty input → empty output', () => {
  assert.deepEqual(pickEveryNthTicks([], 720, { maxTicks: 8, pxPerTick: 90 }), []);
});

test('pickEveryNthTicks: preserves item identity (not just indices)', () => {
  const days = ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04'];
  assert.deepEqual(pickEveryNthTicks(days, 720, { maxTicks: 8, pxPerTick: 90 }), days);
});

import { buildStackData } from './usage.js';

const STACK_MACHINES = [
  { machine_id: 7, name: 'Tron', color: '#f00', vals: [1, 2, 3] },
  { machine_id: 9, name: 'Tron', color: '#0f0', vals: [4, 5, 6] },  // same display name
];
const keyOf = (m) => 'm' + m.machine_id;
const opts = { keyOf, bucketField: 'day', valueAt: (m, i) => m.vals[i] };

test('buildStackData: keys are the stack order from keyOf', () => {
  const { keys } = buildStackData(STACK_MACHINES, ['a', 'b', 'c'], opts);
  assert.deepEqual(keys, ['m7', 'm9']);
});

test('buildStackData: colorByKey maps each stable key to its colour', () => {
  const { colorByKey } = buildStackData(STACK_MACHINES, ['a'], opts);
  assert.equal(colorByKey.get('m7'), '#f00');
  assert.equal(colorByKey.get('m9'), '#0f0');
});

test('buildStackData: one record per bucket with the bucket under bucketField + each key', () => {
  const { records } = buildStackData(STACK_MACHINES, ['mon', 'tue', 'wed'], opts);
  assert.equal(records.length, 3);
  assert.deepEqual(records[0], { day: 'mon', m7: 1, m9: 4 });
  assert.deepEqual(records[2], { day: 'wed', m7: 3, m9: 6 });
});

test('buildStackData: same-named machines stay distinct (keyed by id, not name)', () => {
  const { records } = buildStackData(STACK_MACHINES, ['mon'], opts);
  // both "Tron"s contribute separately rather than collapsing
  assert.equal(records[0].m7, 1);
  assert.equal(records[0].m9, 4);
});

test('buildStackData: missing/undefined values fall back to 0', () => {
  const machines = [{ machine_id: 1, color: '#000', vals: [10] }];  // vals[1] undefined
  const { records } = buildStackData(machines, ['a', 'b'], {
    keyOf, bucketField: 'ts', valueAt: (m, i) => m.vals[i],
  });
  assert.equal(records[0].m1, 10);
  assert.equal(records[1].m1, 0);  // undefined → 0
});

test('buildStackData: respects a different bucketField (energy chart uses ts)', () => {
  const machines = [{ machine_id: 1, color: '#000', vals: [2] }];
  const { records } = buildStackData(machines, ['2024-01-01T00:00:00Z'], {
    keyOf, bucketField: 'ts', valueAt: (m, i) => m.vals[i],
  });
  assert.equal(records[0].ts, '2024-01-01T00:00:00Z');
  assert.equal(records[0].m1, 2);
});

test('buildStackData: no buckets → no records (empty chart)', () => {
  const { keys, records } = buildStackData(STACK_MACHINES, [], opts);
  assert.deepEqual(keys, ['m7', 'm9']);  // keys still derived from machines
  assert.deepEqual(records, []);
});

test('buildStackData: unassigned sentinel key (energy chart keyOf)', () => {
  const sentinelKeyOf = (m) => 'm' + (m.machine_id == null ? 'unassigned' : m.machine_id);
  const machines = [{ machine_id: null, color: '#ccc', vals: [5] }];
  const { keys, colorByKey, records } = buildStackData(machines, ['h0'], {
    keyOf: sentinelKeyOf, bucketField: 'ts', valueAt: (m, i) => m.vals[i],
  });
  assert.deepEqual(keys, ['munassigned']);
  assert.equal(colorByKey.get('munassigned'), '#ccc');
  assert.equal(records[0].munassigned, 5);
});
