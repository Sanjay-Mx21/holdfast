"use client";

import { useRouter, useSearchParams } from "next/navigation";
import { Suspense, useId, useState, type FormEvent } from "react";
import { api } from "@/lib/api.ts";
import { messageFor } from "@/lib/messages.ts";
import { requestCode, verifyCode } from "@/lib/session.ts";
import { Button, Notice } from "@/components/ui";

// The development build reads codes from the mock SMS inbox (auth-svc's
// DEV_SMS_INBOX); a production build leaves the button out.
const devInbox = process.env.NEXT_PUBLIC_DEV_INBOX === "1";

/** Only same-site paths: never send a buyer to another site after sign-in. */
function safeNext(next: string | null): string {
  return next && next.startsWith("/") && !next.startsWith("//") ? next : "/";
}

function SignIn() {
  const router = useRouter();
  const next = safeNext(useSearchParams().get("next"));
  const [phone, setPhone] = useState("");
  const [code, setCode] = useState("");
  const [step, setStep] = useState<"phone" | "code">("phone");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [info, setInfo] = useState<string | null>(null);
  const phoneHint = useId();
  const codeHint = useId();

  async function sendCode(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      await requestCode(phone.replace(/[\s-]/g, ""));
      setStep("code");
      setInfo(`We sent a 6-digit code to ${phone}.`);
    } catch (err) {
      setError(messageFor(err));
    } finally {
      setBusy(false);
    }
  }

  async function signIn(e: FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      await verifyCode(phone.replace(/[\s-]/g, ""), code.trim());
      window.dispatchEvent(new Event("hf-session"));
      router.replace(next);
    } catch (err) {
      setError(messageFor(err));
      setBusy(false);
    }
  }

  async function readInbox() {
    try {
      const m = await api<{ text: string }>(`/v1/auth/dev/inbox?phone=${encodeURIComponent(phone.replace(/[\s-]/g, ""))}`);
      const found = m.text.match(/\b\d{6}\b/);
      if (found) setCode(found[0]);
    } catch (err) {
      setError(messageFor(err));
    }
  }

  return (
    <>
      <h1 className="mb-2 text-3xl font-bold">Sign in</h1>
      <p className="mb-6 text-muted">Sign in with your phone: we send you a one-time code. No password.</p>
      {error && <Notice kind="error">{error}</Notice>}
      {info && !error && <Notice>{info}</Notice>}
      {step === "phone" ? (
        <form onSubmit={sendCode} className="space-y-4">
          <div>
            <label htmlFor="phone" className="block font-medium">
              Phone number
            </label>
            <input
              id="phone"
              type="tel"
              inputMode="tel"
              autoComplete="tel"
              required
              value={phone}
              onChange={(e) => setPhone(e.target.value)}
              aria-describedby={phoneHint}
              className="mt-1 w-full rounded-md border border-slate-400 bg-background px-3 py-2"
            />
            <p id={phoneHint} className="mt-1 text-sm text-muted">
              With the country code, for example +91 98765 43210.
            </p>
          </div>
          <Button type="submit" disabled={busy}>
            {busy ? "Sending…" : "Send me a code"}
          </Button>
        </form>
      ) : (
        <form onSubmit={signIn} className="space-y-4">
          <div>
            <label htmlFor="code" className="block font-medium">
              6-digit code
            </label>
            <input
              id="code"
              type="text"
              inputMode="numeric"
              autoComplete="one-time-code"
              pattern="[0-9]{6}"
              maxLength={6}
              required
              autoFocus
              value={code}
              onChange={(e) => setCode(e.target.value)}
              aria-describedby={codeHint}
              className="mt-1 w-40 rounded-md border border-slate-400 bg-background px-3 py-2 tracking-widest"
            />
            <p id={codeHint} className="mt-1 text-sm text-muted">
              The code works once and expires after a few minutes.
            </p>
          </div>
          <div className="flex flex-wrap gap-3">
            <Button type="submit" disabled={busy}>
              {busy ? "Signing in…" : "Sign in"}
            </Button>
            <button type="button" className="underline" onClick={() => setStep("phone")}>
              Use another number
            </button>
            {devInbox && (
              <button type="button" className="underline" onClick={readInbox}>
                Read the code from the development inbox
              </button>
            )}
          </div>
        </form>
      )}
    </>
  );
}

export default function SignInPage() {
  return (
    <Suspense>
      <SignIn />
    </Suspense>
  );
}
