// End to end, in a real browser, through the edge: sign in with a phone code,
// join the waiting room (the proof of work runs in a Web Worker), wait for
// the turn, hold tickets, book, pay at the mock provider, see it confirmed.
// It also checks each page for accessibility problems with axe.
//
// Needs the local stack (make up) and an event that is on sale:
//   EVENT=<id> BASE_URL=http://localhost:8088 npx playwright test
import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page } from "@playwright/test";

const event = process.env.EVENT ?? "";
const phone = `+9199${String(Date.now()).slice(-8)}`;

async function expectAccessible(page: Page, name: string) {
  const results = await new AxeBuilder({ page }).withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"]).analyze();
  const problems = results.violations.map((v) => `${v.id} (${v.impact}): ${v.help} [${v.nodes.length}]`);
  expect(problems, `${name}: accessibility problems`).toEqual([]);
}

test("a buyer signs in, waits their turn and buys two tickets", async ({ page, context }) => {
  test.skip(!event, "set EVENT to an event on sale");

  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Events" })).toBeVisible();
  await expectAccessible(page, "events");

  await page.goto(`/event?id=${event}`);
  await expect(page.getByRole("heading", { level: 1 })).toBeVisible();
  await expectAccessible(page, "event, signed out");
  await page.getByRole("link", { name: "Sign in" }).last().click();

  await expect(page).toHaveURL(/\/signin/);
  await expectAccessible(page, "sign in");
  await page.getByLabel("Phone number").fill(phone);
  await page.getByRole("button", { name: "Send me a code" }).click();
  await page.getByRole("button", { name: "Read the code from the development inbox" }).click();
  await expect(page.getByLabel("6-digit code")).toHaveValue(/^\d{6}$/);
  await page.getByRole("button", { name: "Sign in" }).click();

  await expect(page).toHaveURL(new RegExp(`/event\\?id=${event}`));
  await expectAccessible(page, "event, signed in");
  const started = Date.now();
  await page.getByRole("button", { name: "Join the waiting room" }).click();
  await expect(page).toHaveURL(/\/queue\?id=/, { timeout: 60_000 });
  console.log(`join, with the proof of work solved in the browser: ${Date.now() - started} ms`);

  // The waiting room hands over to checkout by itself when the turn comes.
  await expect(page).toHaveURL(/\/buy\?id=/, { timeout: 60_000 });
  await expectAccessible(page, "buy");
  await page.getByLabel("How many tickets?").selectOption("2");
  await page.getByRole("button", { name: "Hold my tickets" }).click();
  await expect(page.getByRole("heading", { name: "2 tickets held for you" })).toBeVisible();
  await page.getByRole("button", { name: "Continue to payment" }).click();

  await expect(page).toHaveURL(/\/booking\?id=/, { timeout: 30_000 });
  await expectAccessible(page, "booking");
  const [checkout] = await Promise.all([context.waitForEvent("page"), page.getByRole("link", { name: /^Pay/ }).click()]);
  await checkout.getByRole("button", { name: /^Pay/ }).click();
  await checkout.close();

  await expect(page.getByText("Confirmed! Your tickets are booked.")).toBeVisible({ timeout: 90_000 });
});
