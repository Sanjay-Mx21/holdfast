import { test } from "node:test";
import assert from "node:assert/strict";
import { announceAhead, duration, newKey, pollDelay, rankLookupDelay, rupees, turnOf } from "./waiting.ts";

test("rupees use Indian grouping", () => {
  assert.equal(rupees(250000), "₹2,500.00");
  assert.equal(rupees(12345678900), "₹12,34,56,789.00");
});

test("durations read naturally", () => {
  assert.equal(duration(0), "now");
  assert.equal(duration(-5), "now");
  assert.equal(duration(400), "1 s");
  assert.equal(duration(75_000), "1 min 15 s");
  assert.equal(duration(120_000), "2 min");
  assert.equal(duration(3_720_000), "1 h 2 min");
  assert.equal(duration(7_200_000), "2 h");
});

test("polls are jittered between 3 and 4 seconds", () => {
  assert.equal(pollDelay(() => 0), 3000);
  assert.equal(pollDelay(() => 0.999), 3999);
});

test("the rank is asked for within 30 s after T0", () => {
  assert.equal(rankLookupDelay(5000, () => 0), 5000);
  assert.equal(rankLookupDelay(-1000, () => 0.5), 15_000);
});

test("a buyer's turn follows the status document", () => {
  assert.deepEqual(turnOf(undefined, "PRE", 0), { kind: "drawing" });
  assert.deepEqual(turnOf(5, "PRE", 0), { kind: "drawing" });
  assert.deepEqual(turnOf(500, "OPEN", 120), { kind: "waiting", ahead: 380 });
  assert.deepEqual(turnOf(120, "OPEN", 120), { kind: "your-turn" });
  assert.deepEqual(turnOf(100, "OPEN", 120), { kind: "your-turn" });
  assert.deepEqual(turnOf(500, "FROZEN", 120), { kind: "paused", ahead: 380 });
  // Admitted before the freeze: their turn stands (claims are honoured).
  assert.deepEqual(turnOf(100, "FROZEN", 120), { kind: "your-turn" });
  assert.deepEqual(turnOf(1, "SOLD_OUT", 120), { kind: "sold-out" });
  assert.deepEqual(turnOf(1, "CLOSED", 120), { kind: "closed" });
});

test("announcements change only noticeably", () => {
  assert.equal(announceAhead(7), 7);
  assert.equal(announceAhead(380), 400);
  assert.equal(announceAhead(18_204), 20_000);
});

test("idempotency keys are fresh and well-formed", () => {
  const a = newKey("hold");
  assert.match(a, /^hold-[0-9a-f]{32}$/);
  assert.notEqual(a, newKey("hold"));
});
