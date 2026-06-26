import { test, beforeEach, afterEach, mock } from 'node:test';
import assert from 'node:assert/strict';
import { JSDOM } from 'jsdom';
import { showToast } from './toast.js';

// showToast uses the ambient `document` + `setTimeout`; provide a fresh jsdom
// document and mock timers (so the 4s auto-dismiss doesn't keep node alive).
beforeEach(() => {
  globalThis.document = new JSDOM('<!doctype html><body></body>').window.document;
  mock.timers.enable({ apis: ['setTimeout'] });
});
afterEach(() => {
  mock.timers.reset();
  delete globalThis.document;
});

test('shows a toast with the type class and message', () => {
  showToast('Saved', 'success');
  const t = document.querySelector('.toast');
  assert.ok(t);
  assert.ok(t.classList.contains('toast-success'));
  assert.equal(t.textContent, 'Saved');
});

test('only one toast at a time — a new one replaces the old', () => {
  showToast('first', 'error');
  showToast('second', 'success');
  const toasts = document.querySelectorAll('.toast');
  assert.equal(toasts.length, 1);
  assert.equal(toasts[0].textContent, 'second');
  assert.ok(toasts[0].classList.contains('toast-success'));
});

test('auto-dismisses after the timeout', () => {
  showToast('bye', 'success');
  assert.ok(document.querySelector('.toast'));
  mock.timers.tick(4000); // fade out scheduled
  mock.timers.tick(300); // removal
  assert.equal(document.querySelector('.toast'), null);
});

test('textContent is used (message is not interpreted as HTML)', () => {
  showToast('<img src=x>', 'error');
  const t = document.querySelector('.toast');
  assert.equal(t.querySelectorAll('*').length, 0);
  assert.equal(t.textContent, '<img src=x>');
});
