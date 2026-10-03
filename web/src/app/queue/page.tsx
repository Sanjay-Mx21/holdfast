"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { Suspense, useEffect, useRef, useState } from "react";
import { api, ApiError, type Admission, type Position, type StatusDoc } from "@/lib/api.ts";
import { messageFor } from "@/lib/messages.ts";
import { authed } from "@/lib/session.ts";
import { saveAdmission } from "@/lib/purchase.ts";
import { announceAhead, duration, pollDelay, rankLookupDelay, turnOf } from "@/lib/waiting.ts";
import { Loading, Notice, useNow, useQueryId } from "@/components/ui";

// The waiting room. The page asks for the buyer's rank once (after T0), then
// polls only the shared status document, which the edge serves from a
// one-second cache, and compares the two locally: the origin's load stays
// flat however many people wait.
function WaitingRoom() {
  const id = useQueryId();
  const router = useRouter();
  const now = useNow(1000);
  const [position, setPosition] = useState<Position | null>(null);
  const [status, setStatus] = useState<StatusDoc | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [claiming, setClaiming] = useState(false);
  const claimed = useRef(false);

  // The rank: once now, and once more after T0 if we joined before it.
  useEffect(() => {
    if (!id) return;
    let timer: ReturnType<typeof setTimeout>;
    const lookUp = async () => {
      try {
        const p = await authed<Position>(`/v1/queue/${id}/me`);
        setPosition(p);
        if (p.rank === undefined && p.randomizingAt) {
          timer = setTimeout(lookUp, rankLookupDelay(new Date(p.randomizingAt).getTime() - Date.now()));
        }
      } catch (err) {
        if (err instanceof ApiError && err.status === 401) {
          router.push(`/signin?next=${encodeURIComponent(`/queue?id=${id}`)}`);
          return;
        }
        if (err instanceof ApiError && err.code === "RATE_LIMITED") {
          timer = setTimeout(lookUp, (err.retryAfter ?? 5) * 1000);
          return;
        }
        setError(messageFor(err));
      }
    };
    lookUp();
    return () => clearTimeout(timer);
  }, [id, router]);

  // The shared status document, every 3 s plus jitter.
  useEffect(() => {
    if (!id) return;
    let timer: ReturnType<typeof setTimeout>;
    let stopped = false;
    const poll = async () => {
      try {
        setStatus(await api<StatusDoc>(`/v1/events/${id}/status`));
      } catch {
        // Keep the last known state; try again next time.
      }
      if (!stopped) timer = setTimeout(poll, pollDelay());
    };
    poll();
    return () => {
      stopped = true;
      clearTimeout(timer);
    };
  }, [id]);

  const turn = status ? turnOf(position?.rank, status.state, status.admittedUpTo) : null;

  // Our turn: claim it for an admission token and go and choose tickets.
  useEffect(() => {
    if (!id || turn?.kind !== "your-turn" || claimed.current) return;
    claimed.current = true;
    setClaiming(true);
    authed<Admission>(`/v1/queue/${id}/admit`, { method: "POST" })
      .then((a) => {
        saveAdmission(id, a);
        router.push(`/buy?id=${id}`);
      })
      .catch((err) => {
        // The cached document can be a second ahead of a replica: try again
        // on the next poll.
        if (err instanceof ApiError && err.code === "NOT_YOUR_TURN") claimed.current = false;
        else setError(messageFor(err));
        setClaiming(false);
      });
  }, [id, turn?.kind, router]);

  if (!id) return <Notice kind="error">This link has no valid event in it.</Notice>;
  if (error)
    return (
      <>
        <Notice kind="error">{error}</Notice>
        <Link href={`/event?id=${id}`} className="underline">
          Back to the event
        </Link>
      </>
    );
  if (!status || !position) return <Loading what="your place in line" />;

  const drawAt = position.randomizingAt ? new Date(position.randomizingAt) : null;
  const stale = status.updatedAt ? now - new Date(status.updatedAt).getTime() > 15_000 : false;

  return (
    <>
      <h1 className="mb-4 text-3xl font-bold">You are in the waiting room</h1>
      <p className="mb-6 text-muted">
        Keep this page open. You do not need to refresh it: it updates itself and takes you to checkout when it
        is your turn.
      </p>

      <section aria-live="polite" aria-atomic="true" className="rounded-lg border border-slate-200 p-6 text-center dark:border-slate-800">
        {turn?.kind === "drawing" && (
          <>
            <p className="text-xl font-semibold">Your place will be drawn when the sale opens.</p>
            {drawAt && (
              <p className="mt-2 text-muted">
                Everyone who joined before then gets an equal chance.{" "}
                <span aria-hidden="true">Opens in {duration(drawAt.getTime() - now)}.</span>
              </p>
            )}
          </>
        )}
        {(turn?.kind === "waiting" || turn?.kind === "paused") && (
          <>
            <p className="text-sm uppercase tracking-wide text-muted">People ahead of you</p>
            <p className="text-5xl font-bold" aria-hidden="true">
              {turn.ahead.toLocaleString()}
            </p>
            <p className="sr-only">About {announceAhead(turn.ahead).toLocaleString()} people are ahead of you.</p>
            <p className="mt-2 text-muted">Your number is {position.rank?.toLocaleString()}.</p>
            {turn.kind === "paused" && (
              <Notice>The sale is paused for a moment. Your place is kept; it will move again shortly.</Notice>
            )}
          </>
        )}
        {turn?.kind === "your-turn" && (
          <p className="text-xl font-semibold">{claiming ? "It is your turn! Taking you to checkout…" : "It is your turn!"}</p>
        )}
        {turn?.kind === "sold-out" && <p className="text-xl font-semibold">Sorry, this sale has sold out.</p>}
        {turn?.kind === "closed" && <p className="text-xl font-semibold">This sale has ended.</p>}
      </section>

      {stale && (
        <Notice>
          The line has not moved for a little while. That can happen at busy moments; there is no need to
          refresh.
        </Notice>
      )}
    </>
  );
}

export default function QueuePage() {
  return (
    <Suspense fallback={<Loading what="your place in line" />}>
      <WaitingRoom />
    </Suspense>
  );
}
