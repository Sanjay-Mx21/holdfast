import { defineConfig, devices } from "@playwright/test";

// End-to-end tests against a running stack (make up): see e2e/.
export default defineConfig({
  testDir: "e2e",
  timeout: 180_000,
  use: {
    baseURL: process.env.BASE_URL ?? "http://localhost:8088",
    trace: "retain-on-failure",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
});
