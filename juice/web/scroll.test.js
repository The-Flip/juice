import { test } from 'node:test';
import assert from 'node:assert/strict';
import { JSDOM } from 'jsdom';
import { honorHashScroll } from './scroll.js';

// jsdom has no ResizeObserver and `scrollIntoView` is unimplemented, so we mock
// them on the injected window — letting us drive "the page grew" and "the user
// scrolled" deterministically.
function harness(html, url) {
  const dom = new JSDOM(html, { url });
  const win = dom.window;
  const ro = { cb: null, observed: null, disconnected: false };
  win.ResizeObserver = class {
    constructor(cb) {
      ro.cb = cb;
    }
    observe(el) {
      ro.observed = el;
    }
    disconnect() {
      ro.disconnected = true;
    }
  };
  const timeouts = [];
  win.setTimeout = (fn, ms) => {
    timeouts.push({ fn, ms });
    return 1;
  };
  return { win, ro, timeouts };
}

test('honorHashScroll: re-aligns to the hash target, then bails on user intent', () => {
  const { win, ro, timeouts } = harness('<h2 id="busy">x</h2>', 'http://t/usage#busy');
  const target = win.document.getElementById('busy');
  let scrolls = 0;
  target.scrollIntoView = () => {
    scrolls += 1;
  };

  honorHashScroll(win);
  assert.equal(scrolls, 1, 'scrolls to the target once on init');
  assert.equal(ro.observed, win.document.body, 'observes <body> for growth');
  assert.deepEqual(
    timeouts.map((t) => t.ms),
    [10000],
    'schedules a safety stop',
  );

  ro.cb(); // a chart drew and grew the page
  assert.equal(scrolls, 2, 're-aligns when the page grows');

  win.dispatchEvent(new win.Event('wheel')); // the user scrolls
  assert.ok(ro.disconnected, 'stops observing on user scroll intent');

  ro.cb(); // any later growth
  assert.equal(scrolls, 2, 'does not re-align after the user scrolled');
});

test('honorHashScroll: aligns to a target that is created later (dynamic panels)', () => {
  // The air page builds its #air-<metric> panels only after its data fetch, so
  // the target does not exist when honorHashScroll runs — it must re-query.
  const { win, ro } = harness('<div id="root"></div>', 'http://t/air#air-co2');
  honorHashScroll(win);
  assert.equal(ro.observed, win.document.body, 'observes even before the target exists');
  assert.doesNotThrow(() => ro.cb(), 'realign is a safe no-op while the target is absent');

  const panel = win.document.createElement('div');
  panel.id = 'air-co2';
  let scrolls = 0;
  panel.scrollIntoView = () => {
    scrolls += 1;
  };
  win.document.body.appendChild(panel);
  ro.cb(); // the panel rendered and grew the page
  assert.equal(scrolls, 1, 'scrolls once the late target appears');
});

test('honorHashScroll: no hash → does nothing', () => {
  const { win, ro } = harness('<h2 id="busy">x</h2>', 'http://t/usage');
  honorHashScroll(win);
  assert.equal(ro.observed, null);
});

test('honorHashScroll: malformed/unknown hash → no scroll, no throw', () => {
  const { win, ro } = harness('<h2 id="busy">x</h2>', 'http://t/usage#%');
  assert.doesNotThrow(() => honorHashScroll(win));
  assert.doesNotThrow(() => ro.cb()); // realign is safe when nothing matches
});
