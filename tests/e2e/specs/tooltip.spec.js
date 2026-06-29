// Regression for PR #67: chart hover tooltips must not vanish off the right edge.
// Hover inside the plot near the right edge and assert the tooltip is visible and
// its right edge stays within the viewport (the bug positioned it off-screen).
import { test, expect } from '@playwright/test';
import { discoverIds, hoverNearRightEdge } from './helpers.js';

let ids;
test.beforeEach(async ({ page }) => {
  ids = await discoverIds(page);
});

async function expectTooltipOnScreen(page, path, chart, tooltip) {
  await page.goto(path);
  // `chart` is the <svg> itself; wait for it to have drawn (axis ticks attached).
  await expect(page.locator(chart)).toBeVisible();
  await expect(page.locator(`${chart} .tick`).first()).toBeAttached();
  await hoverNearRightEdge(page, chart);
  const tip = page.locator(tooltip);
  await expect(tip).toBeVisible();
  const box = await tip.boundingBox();
  const vw = await page.evaluate(() => window.innerWidth);
  expect(box.x + box.width).toBeLessThanOrEqual(vw);
  expect(box.x).toBeGreaterThanOrEqual(0);
}

test('usage energy chart: tooltip stays on screen at the right edge', async ({ page }) => {
  await expectTooltipOnScreen(page, '/usage', '#chart', '#tooltip');
});

test('strip usage chart: tooltip stays on screen at the right edge', async ({ page }) => {
  await expectTooltipOnScreen(page, `/strip/${ids.stripId}`, '#usage-chart', '#usage-tooltip');
});

test('machine detail chart: tooltip stays on screen at the right edge', async ({ page }) => {
  await expectTooltipOnScreen(page, `/machine/${ids.plugId}`, '#chart', '#chart-tooltip');
});
