import { test } from 'node:test';
import assert from 'node:assert/strict';
import { circuitLabel } from './circuit.js';

test('circuitLabel: panel + breaker + description', () => {
  assert.equal(
    circuitLabel({ panel: 'P1', breaker: 'B20', description: 'Backline' }),
    'P1 B20 — Backline',
  );
});

test('circuitLabel: no description is just the location', () => {
  assert.equal(circuitLabel({ panel: 'P1', breaker: 'B20', description: '' }), 'P1 B20');
  assert.equal(circuitLabel({ panel: 'P1', breaker: 'B20' }), 'P1 B20');
});

test('circuitLabel: trims when a field is blank', () => {
  assert.equal(circuitLabel({ panel: '', breaker: 'B20', description: '' }), 'B20');
  assert.equal(circuitLabel({ panel: 'P1', breaker: '', description: '' }), 'P1');
});

test('circuitLabel: returns the raw label (no HTML escaping — callers escape)', () => {
  assert.equal(
    circuitLabel({ panel: 'P1', breaker: 'B20', description: '<b>x</b>' }),
    'P1 B20 — <b>x</b>',
  );
});
