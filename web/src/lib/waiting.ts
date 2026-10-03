// Pure logic of the waiting room, kept apart from React so it can be tested
// with node --test.

import type { QueueState } from "./api.ts";

/** Rupees from paise, Indian grouping: 250000 -> "₹2,500.00". */
export function rupees(paise: number): string {
  return new Intl.NumberFormat("en-IN", { style: "currency", currency: "INR" }).format(paise / 100);
}

/** A wait as words: 0 -> "now", 75_000 -> "1 min 15 s", 3_720_000 -> "1 h 2 min". */
export function duration(ms: number): string {
  if (ms <= 0) return "now";
  const s = Math.ceil(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  if (h > 0) return m > 0 ? `${h} h ${m} min` : `${h} h`;
  if (m > 0) return sec > 0 ? `${m} min ${sec} s` : `${m} min`;
  return `${sec} s`;
}

/**
 * The delay before the next status poll: 3 s plus up to 1 s of random
 * jitter (design doc, CLIENT_POLL), so a crowd that loaded the page at the
 * same moment does not poll in waves.
 */
export function pollDelay(random: () => number = Math.random): number {
  return 3000 + Math.floor(random() * 1000);
}

/**
 * When to ask for the rank after T0: a random moment in the first 30 s, so
 * 100,000 lottery joiners do not all ask in the same second.
 */
export function rankLookupDelay(msToT0: number, random: () => number = Math.random): number {
  return Math.max(0, msToT0) + Math.floor(random() * 30_000);
}

export type Turn =
  | { kind: "drawing" } // before T0: no rank yet
  | { kind: "waiting"; ahead: number }
  | { kind: "your-turn" }
  | { kind: "paused"; ahead: number }
  | { kind: "sold-out" }
  | { kind: "closed" };

/** Where a buyer with this rank stands, given the shared status document. */
export function turnOf(rank: number | undefined, state: QueueState, admittedUpTo: number): Turn {
  if (state === "SOLD_OUT") return { kind: "sold-out" };
  if (state === "CLOSED") return { kind: "closed" };
  if (rank === undefined || state === "PRE") return { kind: "drawing" };
  const ahead = Math.max(0, rank - admittedUpTo);
  if (ahead === 0) return { kind: "your-turn" };
  if (state === "FROZEN") return { kind: "paused", ahead };
  return { kind: "waiting", ahead };
}

/**
 * The number to announce to screen readers: rounded so the live region only
 * speaks when the position changes noticeably, not on every poll.
 */
export function announceAhead(ahead: number): number {
  if (ahead < 20) return ahead;
  const magnitude = 10 ** Math.floor(Math.log10(ahead));
  return Math.round(ahead / magnitude) * magnitude;
}

/** A fresh random idempotency key: 16 random bytes, hex. */
export function newKey(prefix: string): string {
  const b = new Uint8Array(16);
  crypto.getRandomValues(b);
  return prefix + "-" + Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");
}
