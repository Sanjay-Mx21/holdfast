"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { Suspense, useEffect, useState } from "react";
import { ApiError, type Booking } from "@/lib/api.ts";
import { messageFor } from "@/lib/messages.ts";
import { savedBooking } from "@/lib/purchase.ts";
import { authed } from "@/lib/session.ts";
import { duration, pollDelay, rupees } from "@/lib/waiting.ts";
import { Loading, Notice, useFocusHeading, useNow, useQueryId } from "@/components/ui";

const finished: Booking["status"][] = ["CONFIRMED", "CANCELLED", "REFUNDED"];

// A booking's status. Payment happens at the provider, in a new tab, while
// this page polls the booking until it is settled.
function BookingView() {
  const id = useQueryId();
  const router = useRouter();
  const now = useNow(1000);
  const [booking, setBooking] = useState<Booking | null>(null);
  // Rendered only in the browser (inside useSearchParams' Suspense boundary).
  const [checkoutUrl] = useState<string | null>(() => (id ? (savedBooking(id)?.checkoutUrl ?? null) : null));
  const [error, setError] = useState<string | null>(null);
  useFocusHeading("booking-title", booking !== null);

  useEffect(() => {
    if (!id) return;
    let timer: ReturnType<typeof setTimeout>;
    let stopped = false;
    const poll = async () => {
      try {
        const b = await authed<Booking>(`/v1/bookings/${id}`);
        setBooking(b);
        if (finished.includes(b.status)) return;
      } catch (err) {
        if (err instanceof ApiError && err.status === 401) {
          router.push(`/signin?next=${encodeURIComponent(`/booking?id=${id}`)}`);
          return;
        }
        setError(messageFor(err));
      }
      if (!stopped) timer = setTimeout(poll, pollDelay());
    };
    poll();
    return () => {
      stopped = true;
      clearTimeout(timer);
    };
  }, [id, router]);

  if (!id) return <Notice kind="error">This link has no valid booking in it.</Notice>;
  if (!booking) return error ? <Notice kind="error">{error}</Notice> : <Loading what="your booking" />;

  const left = new Date(booking.paymentDeadline).getTime() - now;

  return (
    <>
      <h1 id="booking-title" tabIndex={-1} className="mb-4 text-3xl font-bold">
        Your booking
      </h1>
      <p className="mb-4 text-muted">
        {booking.quantity} {booking.quantity === 1 ? "ticket" : "tickets"} · {rupees(booking.amountPaise)} · booking{" "}
        <span className="font-mono text-sm">{booking.bookingId}</span>
      </p>

      <div aria-live="polite">
        {booking.status === "PENDING_PAYMENT" && (
          <section className="space-y-3">
            <Notice>
              Waiting for your payment. Pay within {duration(left)}; this page updates by itself once the payment
              is through.
            </Notice>
            {checkoutUrl ? (
              <a
                href={checkoutUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-block rounded-md bg-accent px-4 py-2 font-medium text-white hover:bg-accent-strong dark:text-slate-950"
              >
                Pay {rupees(booking.amountPaise)} <span className="sr-only">(opens the payment page in a new tab)</span>
              </a>
            ) : (
              <p>The payment link was opened in another tab; finish paying there.</p>
            )}
          </section>
        )}
        {booking.status === "CONFIRMED" && (
          <Notice kind="success">Confirmed! Your tickets are booked. Enjoy the show.</Notice>
        )}
        {booking.status === "CANCELLED" && (
          <Notice kind="error">
            This booking was cancelled: the payment failed or did not arrive in time. Your tickets went back on
            sale, and you were not charged.
          </Notice>
        )}
        {(booking.status === "REFUND_REQUIRED" || booking.status === "REFUNDED") && (
          <Notice kind="error">
            Your payment came through after the tickets were gone, so we could not confirm the booking.{" "}
            {booking.status === "REFUNDED" ? "Your money has been refunded." : "Your refund is on its way."}
          </Notice>
        )}
      </div>
      {error && <Notice kind="error">{error}</Notice>}
      <p className="mt-6">
        <Link href={`/event?id=${booking.eventId}`} className="underline">
          Back to the event
        </Link>
      </p>
    </>
  );
}

export default function BookingPage() {
  return (
    <Suspense fallback={<Loading what="your booking" />}>
      <BookingView />
    </Suspense>
  );
}
