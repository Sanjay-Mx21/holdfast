// The HoldFast API, as the web app uses it. Everything is on this origin,
// through the edge: /v1/events (catalog), /v1/queue (waiting room),
// /v1/events/{id}/holds (inventory), /v1/bookings, /v1/auth.

/** An RFC 9457 problem document from the API, or a network failure. */
export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    detail: string,
    /** Seconds from Retry-After, when the server sent one. */
    readonly retryAfter?: number,
  ) {
    super(detail);
    this.name = "ApiError";
  }
}

export interface RequestOptions {
  method?: "GET" | "POST" | "DELETE";
  body?: unknown;
  /** A bearer token: an access token, or an admission token for holds. */
  token?: string;
  idempotencyKey?: string;
  signal?: AbortSignal;
}

/** Sends a request and parses the JSON answer, or throws ApiError. */
export async function api<T>(path: string, opts: RequestOptions = {}): Promise<T> {
  const headers: Record<string, string> = {};
  if (opts.body !== undefined) headers["Content-Type"] = "application/json";
  if (opts.token) headers["Authorization"] = `Bearer ${opts.token}`;
  if (opts.idempotencyKey) headers["Idempotency-Key"] = opts.idempotencyKey;
  let res: Response;
  try {
    res = await fetch(path, {
      method: opts.method ?? "GET",
      headers,
      body: opts.body === undefined ? undefined : JSON.stringify(opts.body),
      credentials: "same-origin",
      signal: opts.signal,
    });
  } catch (err) {
    if (err instanceof DOMException && err.name === "AbortError") throw err;
    throw new ApiError(0, "NETWORK", "The connection failed. Check your network and try again.");
  }
  if (res.status === 204) return undefined as T;
  const text = await res.text();
  const data = text ? safeJSON(text) : undefined;
  if (!res.ok) {
    const p = (data ?? {}) as { code?: string; detail?: string };
    const retry = Number(res.headers.get("Retry-After"));
    throw new ApiError(
      res.status,
      p.code ?? `HTTP_${res.status}`,
      p.detail ?? res.statusText,
      Number.isFinite(retry) && retry > 0 ? retry : undefined,
    );
  }
  return data as T;
}

function safeJSON(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    return undefined;
  }
}

// ---- Shapes of the API's answers (see docs/services/*.md) ----

export interface EventInfo {
  eventId: string;
  name: string;
  saleOpensAt: string;
  verifiedOnlyUntil?: string;
  agentLockoutUntil?: string;
  perUserLimit: number;
  unitPricePaise: number;
  capacity: number;
}

export type QueueState = "PRE" | "OPEN" | "FROZEN" | "SOLD_OUT" | "CLOSED";

export interface StatusDoc {
  eventId: string;
  state: QueueState;
  opensAt: string;
  admittedUpTo: number;
  queueSize: number;
  updatedAt: string | null;
}

export interface Position {
  eventId: string;
  state: QueueState;
  rank?: number;
  randomizingAt?: string;
}

export interface Challenge {
  required: boolean;
  challenge?: string;
  difficulty?: number;
  expiresAt?: string;
}

export interface Admission {
  eventId: string;
  token: string;
  expiresAt: string;
  rank: number;
}

export interface Hold {
  holdId: string;
  eventId: string;
  quantity: number;
  state: string;
  expiresAt: string;
  remaining?: number;
}

export type BookingStatus = "PENDING_PAYMENT" | "CONFIRMED" | "CANCELLED" | "REFUND_REQUIRED" | "REFUNDED";

export interface Booking {
  bookingId: string;
  eventId: string;
  holdId: string;
  quantity: number;
  amountPaise: number;
  status: BookingStatus;
  paymentDeadline: string;
  checkoutUrl?: string;
}
