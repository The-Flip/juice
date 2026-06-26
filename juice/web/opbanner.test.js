import { test } from 'node:test';
import assert from 'node:assert/strict';
import { JSDOM } from 'jsdom';
import { buildOpBanner } from './opbanner.js';

test('buildOpBanner: no operation → hidden', () => {
  assert.deepEqual(buildOpBanner(null), { hidden: true });
  assert.deepEqual(buildOpBanner(undefined), { hidden: true });
});

test('buildOpBanner: running shows verb, 1-based index/total, machine, live Cancel', () => {
  const v = buildOpBanner({
    state: 'running', kind: 'all_on', index: 2, total: 5,
    current_machine: 'Star Trek', cancel_requested: false,
  });
  assert.equal(v.hidden, false);
  assert.equal(v.text, 'Turning on 3/5 Star Trek…');
  assert.equal(v.html, undefined);
  assert.equal(v.cancelled, false);
  assert.equal(v.complete, false);
  assert.equal(v.retrying, false);
  assert.equal(v.cancelHidden, false);
  assert.equal(v.cancelDisabled, false);
});

test('buildOpBanner: running all_off defaults missing index to 0 (→ 1/N) and omits machine', () => {
  const v = buildOpBanner({ state: 'running', kind: 'all_off', total: 3 });
  assert.equal(v.text, 'Turning off 1/3…');
});

test('buildOpBanner: cancel_requested disables the Cancel button', () => {
  const v = buildOpBanner({ state: 'running', kind: 'all_on', total: 2, cancel_requested: true });
  assert.equal(v.cancelHidden, false);
  assert.equal(v.cancelDisabled, true);
});

test('buildOpBanner: cancelled summarises progress and hides Cancel', () => {
  const v = buildOpBanner({ state: 'cancelled', kind: 'all_on', total: 5, completed: [1, 2] });
  assert.equal(v.cancelled, true);
  assert.equal(v.text, 'All-on cancelled — 2/5 complete');
  assert.equal(v.cancelHidden, true);
  // Cancel hidden → glue must not touch `disabled`; descriptor omits the field.
  assert.equal(v.cancelDisabled, undefined);
});

test('buildOpBanner: complete shows failures only when present', () => {
  const withFail = buildOpBanner({
    state: 'complete', kind: 'all_off', total: 5, completed: [1, 2, 3, 4], failed: [9],
  });
  assert.equal(withFail.complete, true);
  assert.equal(withFail.text, 'All-off complete — 4/5 (1 failed)');
  assert.equal(withFail.cancelHidden, true);
  assert.equal(withFail.cancelDisabled, undefined);

  const noFail = buildOpBanner({
    state: 'complete', kind: 'all_on', total: 3, completed: [1, 2, 3], failed: [],
  });
  assert.equal(noFail.text, 'All-on complete — 3/3');
});

test('buildOpBanner: retrying returns html with spinner, attempt, delay; escapes name + error', () => {
  const v = buildOpBanner({
    state: 'running', kind: 'all_on', total: 5, cancel_requested: false,
    retrying: { machine_name: '<b>M</b>', next_attempt: 2, delay: 1.5, error: '<script>x</script>' },
  });
  assert.equal(v.retrying, true);
  assert.equal(v.text, undefined);
  // html escapes the injected name/error → jsdom finds no live tags from them
  const el = new JSDOM(`<div id="d">${v.html}</div>`).window.document.getElementById('d');
  assert.ok(el.querySelector('.retry-spinner'));      // the one real element
  assert.equal(el.querySelector('b'), null);          // machine_name escaped
  assert.equal(el.querySelector('script'), null);     // error escaped
  assert.match(el.textContent, /Retrying <b>M<\/b> \(attempt 2\): <script>x<\/script>\. Next try in 1\.5s…/);
  assert.equal(v.cancelHidden, false);
  assert.equal(v.cancelDisabled, false);
});

test('buildOpBanner: retrying with null delay and no machine name', () => {
  const v = buildOpBanner({
    state: 'running', kind: 'all_off', total: 2,
    retrying: { next_attempt: 1, delay: null, error: '' },
  });
  assert.match(v.html, /Retrying \(attempt 1\): transient failure\. Next try in …/);
});
