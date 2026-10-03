"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { Suspense, useEffect, useRef, useState } from "react";
import { api, ApiError, type Challenge, type EventInfo, type StatusDoc } from "@/lib/api.ts";
import { messageFor } from "@/lib/messages.ts";
import { solveInWorker } from "@/lib/pow/client.ts";
import { authed } from "@/lib/session.ts";
import { duration, pollDelay, rupees } from "@/lib/waiting.ts";
import { Button, Loading, Notice, useFocusHeading, useNow, useQueryId, useSignedIn } from "@/components/ui";

type Step = { kind: "idle" } | { kind: "checking" } | { kind: "solving"; expected: number; done: number } | { kind: "joining" };

const stateWords: Record<StatusDoc["state"], string> = {
  PRE: "Not open yet: join now for an equal chance in the draw",
  OPEN: "On sale: joining now puts you in line by arrival",
  FROZEN: "Paused for a moment: you can still join",
  SOLD_OUT: "Sold out",
  CLOSED: "Closed",
};

function EventView() {
  const id = useQueryId();
  const router = useRouter();
  const now = useNow(1000);
  const [event, setEvent] = useState<EventInfo | null>(null);
  const [status, setStatus] = useState<StatusDoc | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [joinError, setJoinError] = useState<string | null>(null);
  const [step, setStep] = useState<Step>({ kind: "idle" });
  const signedIn = useSignedIn();
  const cancel = useRef<(() => void) | null>(null);
  useFocusHeading("event-title", event !== null);

  useEffect(() => {
    if (!id) return;
    api<EventInfo>(`/v1/events/${id}`)
      .then(setEvent)
      .catch((err) => setError(messageFor(err)));
  }, [id]);

  // The shared status document, polled with jitter (cached by the edge).
  useEffect(() => {
    if (!id) return;
    let timer: ReturnType<typeof setTimeout>;
    let stopped = false;
    const poll = async () => {
      try {
        setStatus(await api<StatusDoc>(`/v1/events/${id}/status`));
      } catch {
        // Keep the last known state; the next poll tries again.
      }
      if (!stopped) timer = setTimeout(poll, pollDelay());
    };
    poll();
    return () => {
      stopped = true;
      clearTimeout(timer);
      cancel.current?.();
    };
  }, [id]);

  async function join() {
    if (!id) return;
    setJoinError(null);
    setStep({ kind: "checking" });
    try {
      for (let attempt = 0; ; attempt++) {
        const c = await authed<Challenge>(`/v1/queue/${id}/challenge`);
        let body: unknown;
        if (c.required && c.challenge && c.difficulty) {
          const expected = 2 ** c.difficulty;
          setStep({ kind: "solving", expected, done: 0 });
          const solving = solveInWorker(c.challenge, c.difficulty, (done) => setStep({ kind: "solving", expected, done }));
          cancel.current = solving.cancel;
          const { nonce } = await solving.result;
          cancel.current = null;
          body = { pow: { challenge: c.challenge, nonce } };
        }
        setStep({ kind: "joining" });
        try {
          await authed(`/v1/queue/${id}/join`, { method: "POST", body });
          router.push(`/queue?id=${id}`);
          return;
        } catch (err) {
          // A challenge that expired while solving: get a fresh one, once.
          if (err instanceof ApiError && err.code === "POW_EXPIRED" && attempt === 0) continue;
          throw err;
        }
      }
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) {
        router.push(`/signin?next=${encodeURIComponent(`/event?id=${id}`)}`);
        return;
      }
      if (err instanceof Error && err.message === "proof of work cancelled") return;
      setJoinError(messageFor(err));
      setStep({ kind: "idle" });
    }
  }

  if (!id) return <Notice kind="error">This link has no valid event in it.</Notice>;
  if (error) return <Notice kind="error">{error}</Notice>;
  if (!event) return <Loading what="the event" />;

  const opensAt = new Date(event.saleOpensAt);
  const untilOpen = opensAt.getTime() - now;
  const closed = status?.state === "SOLD_OUT" || status?.state === "CLOSED";

  return (
    <article>
      <h1 id="event-title" tabIndex={-1} className="mb-2 text-3xl font-bold">
        {event.name}
      </h1>
      <dl className="mb-6 grid grid-cols-[auto_1fr] gap-x-4 gap-y-1">
        <dt className="text-muted">Price</dt>
        <dd>{rupees(event.unitPricePaise)} per ticket</dd>
        <dt className="text-muted">Limit</dt>
        <dd>Up to {event.perUserLimit} tickets per person</dd>
        <dt className="text-muted">Sale opens</dt>
        <dd>
          <time dateTime={event.saleOpensAt}>{opensAt.toLocaleString()}</time>
          {untilOpen > 0 && <span className="text-muted"> (in {duration(untilOpen)})</span>}
        </dd>
        {status && (
          <>
            <dt className="text-muted">Now</dt>
            <dd>
              {stateWords[status.state]}. {status.queueSize.toLocaleString()} in the waiting room.
            </dd>
          </>
        )}
      </dl>

      {event.verifiedOnlyUntil && new Date(event.verifiedOnlyUntil).getTime() > now && (
        <Notice>
          Until <time dateTime={event.verifiedOnlyUntil}>{new Date(event.verifiedOnlyUntil).toLocaleTimeString()}</time>,
          only buyers who signed in with a phone code may join.
        </Notice>
      )}

      <section aria-labelledby="join-heading" className="rounded-lg border border-slate-200 p-4 dark:border-slate-800">
        <h2 id="join-heading" className="mb-2 text-xl font-semibold">
          Waiting room
        </h2>
        {closed ? (
          <p>This sale has ended.</p>
        ) : signedIn === false ? (
          <p>
            <Link href={`/signin?next=${encodeURIComponent(`/event?id=${id}`)}`} className="font-medium underline">
              Sign in
            </Link>{" "}
            to join the waiting room.
          </p>
        ) : (
          <>
            <p className="mb-3 text-muted">
              Joining takes a short security check that your browser solves on its own, usually in a second or
              two. It makes it expensive for bots to join thousands of times.
            </p>
            <Button type="button" onClick={join} disabled={step.kind !== "idle"} aria-describedby="join-progress">
              {step.kind === "idle" ? "Join the waiting room" : "Joining…"}
            </Button>
            <div id="join-progress" className="mt-3" aria-live="polite">
              {step.kind === "checking" && <p>Getting a security check…</p>}
              {step.kind === "solving" && (
                <>
                  <label htmlFor="pow" className="block">
                    Solving the security check…
                  </label>
                  <progress
                    id="pow"
                    className="w-full"
                    max={100}
                    value={Math.min(95, Math.round((step.done / step.expected) * 100))}
                  />
                </>
              )}
              {step.kind === "joining" && <p>Joining…</p>}
            </div>
          </>
        )}
        {joinError && <Notice kind="error">{joinError}</Notice>}
      </section>
    </article>
  );
}

export default function EventPage() {
  return (
    <Suspense fallback={<Loading what="the event" />}>
      <EventView />
    </Suspense>
  );
}
