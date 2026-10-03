// Small shared pieces: notices, buttons, and the hooks the pages use.
"use client";

import { useSearchParams } from "next/navigation";
import { useEffect, useState, useSyncExternalStore, type ButtonHTMLAttributes, type ReactNode } from "react";
import { maybeSignedIn } from "@/lib/session.ts";

/**
 * A message for the buyer. Errors are announced at once (role="alert");
 * other notices politely (role="status").
 */
export function Notice({ kind = "info", children }: { kind?: "info" | "error" | "success"; children: ReactNode }) {
  const styles = {
    info: "border-slate-300 bg-slate-50 dark:border-slate-700 dark:bg-slate-900",
    error: "border-red-700 bg-red-50 text-red-900 dark:bg-red-950 dark:text-red-100",
    success: "border-emerald-700 bg-emerald-50 text-emerald-900 dark:bg-emerald-950 dark:text-emerald-100",
  }[kind];
  return (
    <div role={kind === "error" ? "alert" : "status"} className={`my-4 rounded-md border-l-4 p-3 ${styles}`}>
      {children}
    </div>
  );
}

export function Button({ className = "", ...props }: ButtonHTMLAttributes<HTMLButtonElement>) {
  return (
    <button
      {...props}
      className={`rounded-md bg-accent px-4 py-2 font-medium text-white hover:bg-accent-strong disabled:cursor-not-allowed disabled:opacity-60 dark:text-slate-950 ${className}`}
    />
  );
}

const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** The ?id= of the page, or null when missing or not a UUID. */
export function useQueryId(): string | null {
  const id = useSearchParams().get("id");
  return id && uuidPattern.test(id) ? id.toLowerCase() : null;
}

/** The current time, updated every intervalMs (for countdowns). */
export function useNow(intervalMs = 1000): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), intervalMs);
    return () => clearInterval(t);
  }, [intervalMs]);
  return now;
}

/** Moves keyboard and screen-reader focus to the page's heading on load. */
export function useFocusHeading(id: string, ready: boolean): void {
  useEffect(() => {
    if (ready) document.getElementById(id)?.focus();
  }, [id, ready]);
}

function subscribeSession(onChange: () => void): () => void {
  window.addEventListener("storage", onChange); // another tab signed in or out
  window.addEventListener("hf-session", onChange); // this tab did
  return () => {
    window.removeEventListener("storage", onChange);
    window.removeEventListener("hf-session", onChange);
  };
}

/** Whether a sign-in is stored; null while prerendering (unknown). */
export function useSignedIn(): boolean | null {
  return useSyncExternalStore(subscribeSession, maybeSignedIn, () => null);
}

export function Loading({ what }: { what: string }) {
  return (
    <p role="status" aria-live="polite" className="text-muted">
      Loading {what}…
    </p>
  );
}
