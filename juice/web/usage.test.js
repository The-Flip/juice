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
