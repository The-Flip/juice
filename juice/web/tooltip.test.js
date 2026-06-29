import { test } from 'node:test';
import assert from 'node:assert/strict';
import { placeTooltipX } from './tooltip.js';

const VW = 1000;

test('placeTooltipX: mid-chart sits to the right of the anchor (anchor + offset)', () => {
  assert.equal(placeTooltipX(400, 140, VW), 414); // 400 + 14
});

test('placeTooltipX: near the right edge it flips to the left of the anchor', () => {
  // anchor 950, width 140: 950+14+140+8=1112 > 1000 → flip to 950-14-140 = 796
  assert.equal(placeTooltipX(950, 140, VW), 796);
});

test('placeTooltipX: right edge is respected even after the flip (never overflows)', () => {
  const left = placeTooltipX(995, 140, VW);
  assert.ok(left + 140 + 8 <= VW, `right edge ${left + 140} should be within ${VW - 8}`);
});

test('placeTooltipX: clamps to left pad when the anchor is near x=0', () => {
  // anchor 2, width 140: 2+14=16 fits on the right, but a flip is not needed;
  // left stays 16 (>= pad 8). A negative anchor would clamp up to pad.
  assert.equal(placeTooltipX(2, 140, VW), 16);
  assert.equal(placeTooltipX(-50, 140, VW), 8); // clamped to pad
});

test('placeTooltipX: a tooltip wider than the viewport pins to the left pad', () => {
  assert.equal(placeTooltipX(500, 1200, VW), 8);
});

test('placeTooltipX: custom offset and pad are honored', () => {
  assert.equal(placeTooltipX(100, 50, VW, 20, 4), 120); // 100 + 20
  // Force a flip with a big offset near the edge, custom pad.
  // anchor 980, width 50, offset 20, pad 4: 980+20+50+4=1054 > 1000 → 980-20-50=910
  assert.equal(placeTooltipX(980, 50, VW, 20, 4), 910);
});
