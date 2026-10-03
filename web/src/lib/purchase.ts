// What a purchase in progress keeps in sessionStorage (this tab only), so a
// reload or a retry picks up where it left off: the admission token, the
// hold's idempotency key, and the booking's checkout link.

import type { Admission, Booking, Hold } from "./api.ts";

function get<T>(key: string): T | null {
  try {
    const raw = sessionStorage.getItem(key);
    return raw ? (JSON.parse(raw) as T) : null;
  } catch {
    return null;
  }
}

function set(key: string, value: unknown): void {
  try {
    sessionStorage.setItem(key, JSON.stringify(value));
  } catch {
    // Storage blocked: the purchase still works, it just cannot resume.
  }
}

export function saveAdmission(eventId: string, a: Admission): void {
  set(`hf:admission:${eventId}`, a);
}

/** The admission token for eventId, unless it has expired. */
export function admission(eventId: string): Admission | null {
  const a = get<Admission>(`hf:admission:${eventId}`);
  return a && new Date(a.expiresAt).getTime() > Date.now() ? a : null;
}

/**
 * The idempotency key of this tab's hold attempt for eventId and quantity:
 * the same on every retry, so a retried request returns the same hold
 * instead of a second one.
 */
export function holdKey(eventId: string, quantity: number, fresh: () => string): string {
  const key = `hf:holdkey:${eventId}:${quantity}`;
  const existing = get<string>(key);
  if (existing) return existing;
  const k = fresh();
  set(key, k);
  return k;
}

export function forgetHoldKey(eventId: string, quantity: number): void {
  try {
    sessionStorage.removeItem(`hf:holdkey:${eventId}:${quantity}`);
  } catch {
    // nothing stored
  }
}

export function saveHold(h: Hold): void {
  set(`hf:hold:${h.eventId}`, h);
}

export function savedHold(eventId: string): Hold | null {
  return get<Hold>(`hf:hold:${eventId}`);
}

export function saveBooking(b: Booking): void {
  set(`hf:booking:${b.bookingId}`, b);
}

export function savedBooking(bookingId: string): Booking | null {
  return get<Booking>(`hf:booking:${bookingId}`);
}
