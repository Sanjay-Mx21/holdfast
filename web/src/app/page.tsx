"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { api, type EventInfo } from "@/lib/api.ts";
import { messageFor } from "@/lib/messages.ts";
import { duration, rupees } from "@/lib/waiting.ts";
import { Loading, Notice, useNow } from "@/components/ui";

export default function EventsPage() {
  const [events, setEvents] = useState<EventInfo[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const now = useNow(1000);

  useEffect(() => {
    api<{ events: EventInfo[] }>("/v1/events")
      .then((r) => setEvents(r.events))
      .catch((err) => setError(messageFor(err)));
  }, []);

  return (
    <>
      <h1 className="mb-2 text-3xl font-bold">Events</h1>
      <p className="mb-6 text-muted">
        Join a waiting room before the sale opens: everyone who joins before then gets an equal, random place
        in line. After that, it is first come, first served.
      </p>
      {error && <Notice kind="error">{error}</Notice>}
      {!events && !error && <Loading what="events" />}
      {events && events.length === 0 && <Notice>No sales are coming up right now.</Notice>}
      {events && events.length > 0 && (
        <ul className="space-y-3">
          {events.map((e) => {
            const opens = new Date(e.saleOpensAt);
            const left = opens.getTime() - now;
            return (
              <li key={e.eventId} className="rounded-lg border border-slate-200 p-4 dark:border-slate-800">
                <h2 className="text-xl font-semibold">
                  <Link href={`/event?id=${e.eventId}`} className="underline-offset-4 hover:underline">
                    {e.name}
                  </Link>
                </h2>
                <p className="text-muted">
                  {rupees(e.unitPricePaise)} per ticket · up to {e.perUserLimit} per person
                </p>
                <p>
                  <time dateTime={e.saleOpensAt}>{opens.toLocaleString()}</time>
                  {left > 0 ? ` · opens in ${duration(left)}` : " · on sale"}
                </p>
              </li>
            );
          })}
        </ul>
      )}
    </>
  );
}
