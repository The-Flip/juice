import { test } from 'node:test';
import assert from 'node:assert/strict';
import { escapeHtml, fmtTimeShort } from './format.js';

test('escapeHtml escapes the five HTML-sensitive characters', () => {
  assert.equal(escapeHtml(`<a href="x" id='y'>&`), '&lt;a href=&quot;x&quot; id=&#39;y&#39;&gt;&amp;');
});

test('escapeHtml coerces null/undefined to empty (not "null"/"undefined")', () => {
  assert.equal(escapeHtml(null), '');
  assert.equal(escapeHtml(undefined), '');
  assert.equal(escapeHtml(0), '0'); // falsy-but-valid value is preserved
  assert.equal(escapeHtml('plain'), 'plain');
});

test('fmtTimeShort: today is time-only; older includes a date; prior year includes the year', () => {
  // Assumes an en-US-ish locale (ASCII month abbreviation); CI runs en-US/UTC.
  // Anchor at local noon so day-boundary/TZ rounding can't make "today" look older.
  const base = new Date();
  base.setHours(12, 0, 0, 0);
  const at = (msAgo) => fmtTimeShort(new Date(base.getTime() - msAgo).toISOString());

  const today = at(0);
  assert.ok(!/[A-Za-z]/.test(today), `today should be time-only (no month): "${today}"`);
  assert.match(today, /\d{1,2}:\d{2}:\d{2}/);

  const older = at(100 * 86400000); // ~100 days ago — a date prefix appears
  assert.ok(/[A-Za-z]/.test(older), `older should include a month: "${older}"`);

  const priorYear = at(400 * 86400000); // > a year ago — the year appears
  assert.match(priorYear, /\b\d{4}\b/, `prior-year should include the year: "${priorYear}"`);
});
