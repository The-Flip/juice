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
