"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { Suspense, useEffect, useState, type FormEvent } from "react";
import { api, ApiError, type Booking, type EventInfo, type Hold } from "@/lib/api.ts";
import { messageFor } from "@/lib/messages.ts";
import { admission, forgetHoldKey, holdKey, saveBooking, saveHold, savedHold } from "@/lib/purchase.ts";
import { authed } from "@/lib/session.ts";
import { duration, newKey, rupees } from "@/lib/waiting.ts";
import { Button, Loading, Notice, useFocusHeading, useNow, useQueryId } from "@/components/ui";

// Checkout, step one: choose how many, hold them, book them. A hold keeps the
// tickets for a few minutes while the buyer pays.
function Buy() {
  const id = useQueryId();
  const router = useRouter();
  const now = useNow(1000);
  const [event, setEvent] = useState<EventInfo | null>(null);
  const [quantity, setQuantity] = useState(1);
  // This page renders only in the browser (inside the Suspense boundary of
  // useSearchParams), so the tab's saved purchase can seed the state.
  const [hold, setHold] = useState<Hold | null>(() => {
    const h = id ? savedHold(id) : null;
    return h && new Date(h.expiresAt).getTime() > Date.now() ? h : null;
  });
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [noTurn, setNoTurn] = useState(() => id !== null && admission(id) === null);
  useFocusHeading("buy-title", event !== null);

  useEffect(() => {
    if (!id) return;
    api<EventInfo>(`/v1/events/${id}`)
      .then(setEvent)
      .catch((err) => setError(messageFor(err)));
  }, [id]);

  async function holdTickets(e: FormEvent) {
    e.preventDefault();
    if (!id) return;
    const a = admission(id);
    if (!a) {
      setNoTurn(true);
      return;
    }
    setBusy(true);
    setError(null);
    try {
      const h = await api<Hold>(`/v1/events/${id}/holds`, {
        method: "POST",
        token: a.token,
        idempotencyKey: holdKey(id, quantity, () => newKey("hold")),
        body: { quantity },
      });
      saveHold(h);
      setHold(h);
    } catch (err) {
      if (err instanceof ApiError && (err.code === "HOLD_EXPIRED" || err.code === "SOLD_OUT")) {
        forgetHoldKey(id, quantity); // a new attempt needs a new key
      }
      if (err instanceof ApiError && err.status === 401) setNoTurn(true);
      else setError(messageFor(err));
    } finally {
      setBusy(false);
    }
  }

  async function book() {
    if (!id || !hold) return;
    setBusy(true);
    setError(null);
    try {
      // One key per hold: retrying returns the same booking.
      const b = await authed<Booking>("/v1/bookings", {
        method: "POST",
        idempotencyKey: `book-${hold.holdId}`,
        body: { eventId: id, holdId: hold.holdId },
      });
      saveBooking(b);
      router.push(`/booking?id=${b.bookingId}`);
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) {
        router.push(`/signin?next=${encodeURIComponent(`/buy?id=${id}`)}`);
        return;
      }
      setError(messageFor(err));
      setBusy(false);
    }
  }

  if (!id) return <Notice kind="error">This link has no valid event in it.</Notice>;
  if (!event) return error ? <Notice kind="error">{error}</Notice> : <Loading what="checkout" />;

  const holdLeft = hold ? new Date(hold.expiresAt).getTime() - now : 0;

  return (
    <>
      <h1 id="buy-title" tabIndex={-1} className="mb-2 text-3xl font-bold">
        {event.name}
      </h1>
      {error && <Notice kind="error">{error}</Notice>}
      {noTurn && !hold && (
        <Notice kind="error">
          Your turn is not active in this tab.{" "}
          <Link href={`/queue?id=${id}`} className="underline">
            Back to the waiting room
          </Link>
        </Notice>
      )}

      {!hold || holdLeft <= 0 ? (
        <form onSubmit={holdTickets} className="space-y-4">
          {hold && holdLeft <= 0 && <Notice kind="error">Your hold expired. Choose your tickets again.</Notice>}
          <div>
            <label htmlFor="quantity" className="block font-medium">
              How many tickets?
            </label>
            <select
              id="quantity"
              value={quantity}
              onChange={(e) => setQuantity(Number(e.target.value))}
              className="mt-1 rounded-md border border-slate-400 bg-background px-3 py-2"
            >
              {Array.from({ length: event.perUserLimit }, (_, i) => i + 1).map((n) => (
                <option key={n} value={n}>
                  {n} · {rupees(n * event.unitPricePaise)}
                </option>
              ))}
            </select>
            <p className="mt-1 text-sm text-muted">Up to {event.perUserLimit} per person, for the whole sale.</p>
          </div>
          <Button type="submit" disabled={busy || noTurn}>
            {busy ? "Holding…" : "Hold my tickets"}
          </Button>
        </form>
      ) : (
        <section aria-labelledby="hold-heading" className="space-y-3">
          <h2 id="hold-heading" className="text-xl font-semibold">
            {hold.quantity} {hold.quantity === 1 ? "ticket" : "tickets"} held for you
          </h2>
          <p>
            Total {rupees(hold.quantity * event.unitPricePaise)}.{" "}
            <span>
              Held for <span aria-live="off">{duration(holdLeft)}</span> more.
            </span>
          </p>
          <Button type="button" onClick={book} disabled={busy}>
            {busy ? "Preparing payment…" : "Continue to payment"}
          </Button>
        </section>
      )}
    </>
  );
}

export default function BuyPage() {
  return (
    <Suspense fallback={<Loading what="checkout" />}>
      <Buy />
    </Suspense>
  );
}
