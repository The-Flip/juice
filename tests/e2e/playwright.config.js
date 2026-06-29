// Playwright config for the juice e2e harness. The webServer launches the
// cloud-free Python server (tests/e2e/serve.py), which seeds a fresh fixture
// DuckDB and serves it with the dev-auth shim — no Kasa cloud, no recorder.
import { defineConfig, devices } from '@playwright/test';

const PORT = 8099;
const baseURL = `http://127.0.0.1:${PORT}`;

export default defineConfig({
  testDir: './specs',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  reporter: process.env.CI ? [['html', { open: 'never' }], ['list']] : 'list',
  use: {
    baseURL,
    trace: 'on-first-retry',
  },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
  webServer: {
    // Run from the repo root (cwd is the config dir) so `tests.e2e.serve` imports.
    command: `uv run python -m tests.e2e.serve --port ${PORT}`,
    cwd: '../..',
    url: baseURL,
    timeout: 120_000, // uv cold start + ~25s fixture seed
    reuseExistingServer: !process.env.CI,
    stdout: 'pipe',
    stderr: 'pipe',
  },
});
