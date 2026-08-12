import { defineConfig, devices } from "@playwright/test";

/**
 * These tests drive the real stack, not mocks: payment-api on 8085, psp-connector on 8086 and
 * realtime-gateway on 8090, all against the running compose cluster. That is deliberate - the
 * thing worth testing here is that an event produced by one service reaches a browser via
 * another, which a mocked EventSource would assert nothing about.
 *
 * M16: the browser's traffic now goes through api-gateway (8000, also required running) rather
 * than straight to payment-api/realtime-gateway - see ui/vite.config.ts and
 * services/api-gateway/README.md. The three services above still need to be running underneath
 * it; api-gateway is what actually routes to them now.
 *
 * Start the backends first (see ui/README.md); Playwright starts the Vite dev server itself.
 */
export default defineConfig({
  testDir: "./e2e",
  // A payment crosses payment-api -> outbox -> Debezium -> psp-connector -> gateway -> SSE, and
  // the simulated provider sleeps up to 5s on top. Generous on purpose: a tight timeout here
  // would fail looking like a broken pipeline.
  timeout: 90_000,
  expect: { timeout: 45_000 },
  fullyParallel: false,
  workers: 1,
  reporter: [["list"]],
  use: {
    baseURL: "http://localhost:5173",
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: {
    command: "pnpm dev --port 5173",
    url: "http://localhost:5173",
    reuseExistingServer: true,
    timeout: 60_000,
  },
});
