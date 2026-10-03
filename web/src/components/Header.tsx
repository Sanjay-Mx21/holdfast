"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { signOut } from "@/lib/session.ts";
import { useSignedIn } from "@/components/ui";

export function Header() {
  const router = useRouter();
  // Unknown while prerendering: the static HTML is the same for everyone.
  const signedIn = useSignedIn();

  return (
    <header className="border-b border-slate-200 dark:border-slate-800">
      <nav aria-label="Main" className="mx-auto flex max-w-2xl items-center justify-between px-4 py-3">
        <Link href="/" className="text-lg font-semibold text-accent-strong">
          HoldFast
        </Link>
        <ul className="flex items-center gap-4 text-sm">
          <li>
            <Link href="/" className="underline-offset-4 hover:underline">
              Events
            </Link>
          </li>
          <li>
            {signedIn === false && (
              <Link href="/signin" className="underline-offset-4 hover:underline">
                Sign in
              </Link>
            )}
            {signedIn && (
              <button
                type="button"
                className="underline-offset-4 hover:underline"
                onClick={async () => {
                  await signOut();
                  window.dispatchEvent(new Event("hf-session"));
                  router.push("/");
                }}
              >
                Sign out
              </button>
            )}
          </li>
        </ul>
      </nav>
    </header>
  );
}
