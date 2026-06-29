// Shared helpers for the juice e2e specs.
import { expect } from '@playwright/test';

// The dev-auth shim: GET /login mints an operator session and redirects to /.
export async function login(page) {
  await page.goto('/login');
  await expect(page).toHaveURL(/\/$/);
}

// Track page health for a "page is clean" assertion. Separates uncaught
// exceptions (`pageErrors` — real breakage, assert these) from console.error
// noise (`consoleErrors` — captured for debugging but NOT asserted, since a
// transient SSE reconnect / slow fetch can log an error without the page being
// broken; asserting on those is the classic e2e flake).
export function trackPageErrors(page) {
  const consoleErrors = [];
  const pageErrors = [];
  page.on('console', (m) => {
    if (m.type() === 'error') consoleErrors.push(m.text());
  });
  page.on('pageerror', (e) => pageErrors.push(String(e)));
  return { consoleErrors, pageErrors };
}

const asList = (d) => (Array.isArray(d) ? d : (d.machines ?? d.circuits ?? d));

// Discover deterministic ids from the seeded fixture (after login) so specs don't
// hard-code them: an emeter machine's plug_id, a strip device_id, a circuit id.
export async function discoverIds(page) {
  await login(page);
  const machines = asList(await (await page.request.get('/api/machines')).json());
  const m = machines.find((x) => x.has_emeter && x.plug?.plug_id);
  // An emeter strip (HS300) — a no-emeter single device has no usage chart data.
  const strip = machines.find((x) => x.has_emeter && x.strip_device_id)?.strip_device_id;
  const circuits = asList(await (await page.request.get('/api/circuits')).json());
  const c = circuits[0];
  return { plugId: m.plug.plug_id, stripId: strip, circuitId: c.circuit_id ?? c.id };
}

// Hover inside a chart's plot area near its right edge and return the tooltip's
// right-edge x and the viewport width — the regression check for PR #67 (the
// tooltip must stay on-screen). `inset` keeps the pointer left of the right
// margin (where the handler hides the tooltip) while still near the viewport edge.
// `inset` (px from the chart's right edge) must land inside the plot area, left
// of the chart's right margin where the hover handler hides the tooltip. Every
// juice chart's right margin is <= 24px, so 50 is comfortably inside while still
// near the viewport's right edge (where the #67 clamp must fire).
export async function hoverNearRightEdge(page, chartSelector, inset = 50) {
  const box = await page.locator(chartSelector).boundingBox();
  await page.mouse.move(box.x + box.width - inset, box.y + box.height / 2);
}
