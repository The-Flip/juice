// Navigation + auth: public pages render logged-out without errors; operator-only
// routes gate to /login at the HTTP layer; the dev one-click login unlocks them.
import { test, expect } from '@playwright/test';
import { login, trackPageErrors } from './helpers.js';

const PUBLIC_PAGES = ['/', '/usage', '/air'];
// Not in PUBLIC_READABLE_PATTERNS (auth.py) → 302 to /login when logged out.
const GATED_PAGES = ['/events'];

test.describe('public pages (logged out)', () => {
  for (const path of PUBLIC_PAGES) {
    test(`${path} renders with no uncaught errors`, async ({ page }) => {
      const { pageErrors } = trackPageErrors(page);
      const resp = await page.goto(path);
      expect(resp.status()).toBe(200);
      await expect(page.locator('body')).toBeVisible();
      // Don't wait for networkidle — the dashboard holds an SSE stream open, so
      // the network is never idle. A short settle lets inline scripts run.
      await page.waitForTimeout(1500);
      // Assert no uncaught exceptions (real breakage); console.error noise is not
      // asserted (see trackPageErrors) to avoid flaking on transient SSE logs.
      expect(pageErrors, pageErrors.join('\n')).toEqual([]);
    });
  }
});

test.describe('auth gating', () => {
  // The dev /login is a one-click GET that auto-mints a session, so a browser
  // following redirects would log itself in. Assert the gate at the HTTP layer
  // instead: the gated route 302s to /login.
  for (const path of GATED_PAGES) {
    test(`${path} 302s to /login when logged out`, async ({ request }) => {
      const resp = await request.get(path, { maxRedirects: 0 });
      expect(resp.status()).toBe(302);
      expect(resp.headers()['location']).toContain('/login');
    });
  }

  test('one-click dev login then a gated page loads', async ({ page }) => {
    await login(page);
    const resp = await page.goto('/events');
    expect(resp.status()).toBe(200);
    await expect(page).toHaveURL(/\/events/);
  });
});

test('dashboard exposes the seeded machines via the API', async ({ page }) => {
  const data = await (await page.request.get('/api/machines')).json();
  const machines = Array.isArray(data) ? data : data.machines;
  expect(machines.length).toBeGreaterThan(25); // seed has ~32
});
