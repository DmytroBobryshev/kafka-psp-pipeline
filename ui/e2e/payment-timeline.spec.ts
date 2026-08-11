import { test, expect } from "@playwright/test";

/**
 * End-to-end against the live pipeline. Each run uses a fresh merchantId so the SSE filter only
 * ever matches this test's own events.
 */
function uniqueMerchant() {
  return `e2e-${Date.now()}`;
}

test("creates a payment and streams its events with the causal chain intact", async ({ page }) => {
  const merchantId = uniqueMerchant();

  await page.goto("/");
  await expect(page.getByRole("heading", { name: /create payment/i })).toBeVisible();

  await page.getByLabel("Merchant ID").fill(merchantId);
  await page.getByLabel("Amount").fill("64.50");
  await page.getByRole("button", { name: /create payment/i }).click();

  // First event: payment-api's, emitted through the outbox and relayed by Debezium.
  await expect(page.getByText("payments.payment-requested.v1").first()).toBeVisible();

  // Second event: psp-connector's, after the simulated provider call. Its arrival is the proof
  // that the whole chain ran, not just the HTTP POST.
  await expect(page.getByText("payments.payment-status-changed.v1").first()).toBeVisible();

  // Both services are named, so the timeline shows who acted, not only what happened.
  await expect(page.getByText("payment-api").first()).toBeVisible();
  await expect(page.getByText("psp-connector").first()).toBeVisible();

  // The causal chain: the second event's causationId must equal the first event's eventId. This
  // is ADR-0002's envelope contract, asserted in a browser rather than in a log. Read from data
  // attributes because the visible ids are truncated for readability.
  const cards = await page
    .locator("[data-event-id]")
    .evaluateAll((nodes) =>
      nodes.map((n) => ({
        eventId: n.getAttribute("data-event-id"),
        causationId: n.getAttribute("data-causation-id"),
      })),
    );

  expect(cards.length).toBeGreaterThanOrEqual(2);
  expect(cards[0].causationId).toBeFalsy(); // root event - nothing caused the request
  expect(cards[1].causationId).toBe(cards[0].eventId);

  await page.screenshot({ path: "e2e-output/payment-timeline.png", fullPage: true });
});

test("renders server-side validation errors against the right fields", async ({ page }) => {
  await page.goto("/");

  // A single space, not an empty string: the input carries HTML5 `required`, so an empty value
  // never leaves the browser and the server never gets a say. Whitespace satisfies `required` and
  // fails @NotBlank server-side, which is the path under test - common-web returning RFC 7807
  // with a per-field errors object (fixed after M10) so the message lands on the field itself
  // rather than as a generic banner.
  await page.getByLabel("Merchant ID").fill(" ");
  await page.getByLabel("Amount").fill("10.00");
  await page.getByRole("button", { name: /create payment/i }).click();

  await expect(page.getByText(/must not be blank/i).first()).toBeVisible();
  await page.screenshot({ path: "e2e-output/validation-errors.png", fullPage: true });
});
