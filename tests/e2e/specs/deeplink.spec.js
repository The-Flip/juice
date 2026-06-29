// Deep links to a section hash must land on that section even though the charts
// above it render asynchronously (and push it down) after the browser's initial
// scroll. Regression for `/usage#busy` staying stuck at the top.
import { test, expect } from '@playwright/test';

test('deep link to #busy scrolls to the busy section once charts render', async ({ page }) => {
  await page.goto('/usage#busy');

  // The charts above #busy (energy, play-hours) render async and grow, pushing the
  // busy section down — wait for them (and the busy chart) to draw.
  await expect(page.locator('#chart .tick').first()).toBeAttached();
  await expect(page.locator('#play-chart .tick').first()).toBeAttached();
  await expect(page.locator('#busy-chart .tick').first()).toBeAttached();

  // The busy section heading should end up scrolled into view (not stuck at top).
  await expect(page.locator('#busy')).toBeInViewport();
  expect(await page.evaluate(() => window.scrollY)).toBeGreaterThan(0);
});
