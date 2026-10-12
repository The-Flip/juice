import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildInfraBanner } from './infrabanner.js';

test('buildInfraBanner: a healthy floor shows nothing', () => {
  assert.deepEqual(buildInfraBanner([]), { hidden: true });
  assert.deepEqual(buildInfraBanner(null), { hidden: true });
  assert.deepEqual(buildInfraBanner(undefined), { hidden: true });
});

test('buildInfraBanner: anonymous viewers get [] from the server, so no banner', () => {
  // Not a special case in the client — the server already withholds the array.
  assert.deepEqual(buildInfraBanner([]), { hidden: true });
});

test('buildInfraBanner: one unreachable strip is named, with what it costs', () => {
  const v = buildInfraBanner([{
    device_id: 'DEV1', name: 'Row 2', kind: 'unreachable_device',
    since: '2026-09-30T15:00:00+00:00', affects: ['M0005', 'M0006', 'M0011', 'M0012'],
  }]);
  assert.equal(v.hidden, false);
  assert.equal(v.kind, 'unreachable_device');
  assert.match(v.html, /<strong>Row 2<\/strong> is unreachable/);
  assert.match(v.html, /4 machines cannot be seen or switched/);
});

test('buildInfraBanner: several strips are counted rather than listed', () => {
  const v = buildInfraBanner([
    { name: 'Row 1', kind: 'unreachable_device', affects: ['M0001', 'M0002'] },
    { name: 'Row 2', kind: 'unreachable_device', affects: ['M0005'] },
    { name: 'Row 3', kind: 'unreachable_device', affects: [] },
  ]);
  assert.match(v.html, /<strong>3 strips<\/strong> are unreachable/);
  assert.match(v.html, /3 machines cannot be seen/);
});

test('buildInfraBanner: one machine is singular', () => {
  const v = buildInfraBanner([
    { name: 'Row 2', kind: 'unreachable_device', affects: ['M0005'] },
  ]);
  assert.match(v.html, /1 machine cannot be seen/);
  assert.doesNotMatch(v.html, /1 machines/);
});

test('buildInfraBanner: a strip with no name still produces a usable line', () => {
  // A device that was offline at boot has no roster frame yet, so no alias.
  const v = buildInfraBanner([{ kind: 'unreachable_device', affects: ['M0005'] }]);
  assert.match(v.html, /<strong>A strip<\/strong> is unreachable/);
});

test('buildInfraBanner: a strip name is escaped', () => {
  const v = buildInfraBanner([
    { name: '<img src=x onerror=alert(1)>', kind: 'unreachable_device', affects: [] },
  ]);
  assert.doesNotMatch(v.html, /<img/);
  assert.match(v.html, /&lt;img/);
});

test('buildInfraBanner: a lost collector outranks the strips', () => {
  // The server sends a collector entry alone, but if both ever appeared, naming
  // strips would send someone to the wrong floor: the strips are probably fine.
  const v = buildInfraBanner([
    { device_id: null, name: 'tap collector', kind: 'collector_offline',
      since: null, affects: ['M0001', 'M0002'] },
    { name: 'Row 2', kind: 'unreachable_device', affects: ['M0005'] },
  ]);
  assert.equal(v.kind, 'collector_offline');
  assert.match(v.html, /No collector connected/);
  assert.match(v.html, /Nothing can be switched/);
  assert.doesNotMatch(v.html, /Row 2/);
});

test('buildInfraBanner: a silent collector says control still works', () => {
  const v = buildInfraBanner([{
    device_id: null, name: 'tap collector', kind: 'collector_silent',
    since: '2026-09-30T15:00:00+00:00', affects: [],
  }]);
  assert.equal(v.kind, 'collector_silent');
  assert.match(v.html, /catching up/);
  assert.match(v.html, /Power control still works/);
});

test('buildInfraBanner: no "since" is rendered anywhere', () => {
  // The entry's timestamp is when juice gave up on the device, not when it
  // died — tap's own park plus 15s. A duration would be a claim we cannot make.
  for (const entries of [
    [{ name: 'Row 2', kind: 'unreachable_device', since: '2026-09-30T15:00:00+00:00',
       affects: ['M0005'] }],
    [{ name: 'tap collector', kind: 'collector_offline',
       since: '2026-09-30T15:00:00+00:00', affects: [] }],
  ]) {
    assert.doesNotMatch(buildInfraBanner(entries).html, /2026|ago|since/i);
  }
});
