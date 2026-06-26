import { test } from 'node:test';
import assert from 'node:assert/strict';
import { JSDOM } from 'jsdom';
import { buildStripPeaks, buildCircuitPeaks } from './peaks.js';

const parse = (html) => new JSDOM(`<div id="d">${html}</div>`).window.document.getElementById('d');

test('buildStripPeaks: empty / all-null inputs return ""', () => {
  assert.equal(buildStripPeaks([]), '');
  assert.equal(buildStripPeaks(undefined), '');
  assert.equal(buildStripPeaks([{ device_id: 'd', peak_watts_theoretical: null, peak_watts_actual: null, current_watts: null }]), '');
});

test('buildStripPeaks: one row with a strip link, watts, and scaled bars', () => {
  const el = parse(buildStripPeaks([
    { device_id: 'dev1', display_name: 'Strip A', peak_watts_theoretical: 1000, peak_watts_actual: 500, current_watts: 250 },
  ]));
  assert.ok(el.querySelector('a[href="/strip/dev1"]'));
  assert.match(el.querySelector('.peak-name').textContent, /Strip A/);
  // current 250 of max 1000 → 25% bar
  assert.equal(el.querySelector('.peak-bar-current').style.width, '25%');
  assert.equal(el.querySelector('.peak-bar-theoretical').style.width, '100%');
  assert.match(el.querySelector('.peak-num.now').textContent, /250\.0 W/);
});

test('buildStripPeaks: escapes the display name', () => {
  const el = parse(buildStripPeaks([{ device_id: 'd', display_name: '<img src=x>', current_watts: 5 }]));
  assert.equal(el.querySelector('.peak-name img'), null);
  assert.match(el.querySelector('.peak-name a').textContent, /<img src=x>/);
});

test('buildCircuitPeaks: empty returns ""', () => {
  assert.equal(buildCircuitPeaks([]), '');
  assert.equal(buildCircuitPeaks(undefined), '');
});

test('buildCircuitPeaks: capacity cell class reflects the threshold', () => {
  const rows = [
    { circuit_id: 1, label: 'A', current_watts: 10, pct_of_capacity: 85 },
    { circuit_id: 2, label: 'B', current_watts: 10, pct_of_capacity: 65 },
    { circuit_id: 3, label: 'C', current_watts: 10, pct_of_capacity: 20 },
    { circuit_id: 4, label: 'D', current_watts: 10, pct_of_capacity: null },
  ];
  const el = parse(buildCircuitPeaks(rows));
  const caps = [...el.querySelectorAll('tbody tr')].map((tr) => tr.lastElementChild);
  assert.ok(caps[0].classList.contains('over')); // >= 80
  assert.equal(caps[0].textContent, '85%');
  assert.ok(caps[1].classList.contains('warn')); // 60–79
  assert.equal(caps[2].className.trim(), 'peak-num'); // < 60, no extra class
  assert.equal(caps[3].textContent, '—'); // null capacity
});

test('buildCircuitPeaks: links to the circuit and escapes the label', () => {
  const el = parse(buildCircuitPeaks([{ circuit_id: 7, label: '<b>P</b>', current_watts: 1 }]));
  assert.ok(el.querySelector('a[href="/circuit/7"]'));
  assert.equal(el.querySelector('.peak-name b'), null);
});
