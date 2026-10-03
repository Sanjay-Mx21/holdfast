// Sign-in state. The access token (15 minutes) lives in localStorage so
// every tab shares it; the refresh token is an httpOnly cookie scripts never
// see (Path=/v1/auth). Refreshing rotates the refresh token, and presenting a
// rotated one revokes the whole login (reuse detection), so refreshes are
// serialized across tabs with the Web Locks API: a tab that waited for the
// lock first checks whether another tab already refreshed.

import { api, ApiError } from "./api.ts";

const KEY = "hf:access";

interface Stored {
  token: string;
  exp: number; // ms since epoch
}

/** exp of a JWT, in ms, without verifying it (the server verifies). */
export function tokenExpiry(token: string): number {
  const part = token.split(".")[1];
  if (!part) return 0;
  try {
    const json = atob(part.replace(/-/g, "+").replace(/_/g, "/").padEnd(Math.ceil(part.length / 4) * 4, "="));
    const exp = (JSON.parse(json) as { exp?: number }).exp;
    return typeof exp === "number" ? exp * 1000 : 0;
  } catch {
    return 0;
  }
}

function read(): Stored | null {
  try {
    const raw = localStorage.getItem(KEY);
    return raw ? (JSON.parse(raw) as Stored) : null;
  } catch {
    return null;
  }
}

function save(token: string): string {
  try {
    localStorage.setItem(KEY, JSON.stringify({ token, exp: tokenExpiry(token) } satisfies Stored));
  } catch {
    // Storage blocked: the token still works for this page.
  }
  return token;
}

function fresh(s: Stored | null): string | null {
  return s && s.exp - Date.now() > 30_000 ? s.token : null;
}

export function forget(): void {
  try {
    localStorage.removeItem(KEY);
  } catch {
    // nothing stored
  }
}

let inflight: Promise<string | null> | null = null;

async function refresh(): Promise<string | null> {
  const run = async () => {
    const again = fresh(read()); // another tab may have refreshed meanwhile
    if (again) return again;
    try {
      const r = await api<{ accessToken: string }>("/v1/auth/refresh", { method: "POST" });
      return save(r.accessToken);
    } catch (err) {
      if (err instanceof ApiError && (err.status === 401 || err.status === 403)) {
        forget();
        return null; // signed out, or the login was revoked
      }
      throw err;
    }
  };
  if (typeof navigator !== "undefined" && navigator.locks) {
    return navigator.locks.request("hf-refresh", run);
  }
  return run();
}

/** A valid access token, refreshing if needed; null when signed out. */
export async function accessToken(): Promise<string | null> {
  const t = fresh(read());
  if (t) return t;
  inflight ??= refresh().finally(() => {
    inflight = null;
  });
  return inflight;
}

/** True when a token is stored (it may still need a refresh). */
export function maybeSignedIn(): boolean {
  return read() !== null;
}

export async function requestCode(phone: string): Promise<void> {
  await api("/v1/auth/otp/request", { method: "POST", body: { phone } });
}

export async function verifyCode(phone: string, code: string): Promise<void> {
  const r = await api<{ accessToken: string }>("/v1/auth/otp/verify", { method: "POST", body: { phone, code } });
  save(r.accessToken);
}

export async function signOut(): Promise<void> {
  forget();
  try {
    await api("/v1/auth/logout", { method: "POST" });
  } catch {
    // The cookie expires on its own; the local token is already gone.
  }
}

/**
 * Calls the API as the signed-in buyer. On 401 it refreshes once and
 * retries; if that fails too, it throws ApiError 401 (send them to sign in).
 */
export async function authed<T>(path: string, opts: Parameters<typeof api>[1] = {}): Promise<T> {
  let token = await accessToken();
  if (!token) throw new ApiError(401, "UNAUTHENTICATED", "Please sign in.");
  try {
    return await api<T>(path, { ...opts, token });
  } catch (err) {
    if (!(err instanceof ApiError) || err.status !== 401) throw err;
    forget();
    token = await accessToken();
    if (!token) throw err;
    return api<T>(path, { ...opts, token });
  }
}
