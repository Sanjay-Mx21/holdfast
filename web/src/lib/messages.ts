// What to tell a buyer when the API refuses: plain words, and what to do.

import { ApiError } from "./api.ts";
import { duration } from "./waiting.ts";

export function messageFor(err: unknown): string {
  if (!(err instanceof ApiError)) return "Something went wrong. Please try again.";
  const wait = err.retryAfter ? ` Try again in ${duration(err.retryAfter * 1000)}.` : "";
  switch (err.code) {
    case "NETWORK":
      return err.message;
    case "UNAUTHENTICATED":
      return "Please sign in again.";
    case "VERIFIED_ONLY":
      return `For now only verified buyers may join this sale.${wait}`;
    case "AGENT_LOCKOUT":
      return `Agents may not join this sale yet.${wait}`;
    case "QUEUE_CLOSED":
      return "This sale's waiting room is closed: it has sold out or ended.";
    case "EVENT_NOT_FOUND":
      return "We could not find this event.";
    case "RATE_LIMITED":
      return `Too many attempts.${wait || " Wait a moment and try again."}`;
    case "POW_EXPIRED":
      return "The security check took too long. Please try again.";
    case "NOT_IN_QUEUE":
      return "You have not joined this waiting room.";
    case "TURN_EXPIRED":
      return "Your turn came and went: its time ran out.";
    case "SOLD_OUT":
      return "Not enough tickets are left for that many. Try fewer.";
    case "USER_LIMIT":
      return "You already hold or bought the most tickets allowed for this event.";
    case "SALE_PAUSED":
      return `The sale is paused for a moment. Your place is kept.${wait}`;
    case "HOLD_EXPIRED":
      return "Your hold expired. Choose your tickets again.";
    case "INVALID_CODE":
      return "That code is not right, or it has expired. Check it, or ask for a new one.";
    case "INVALID_PHONE":
      return "Enter your number with its country code, for example +91 98765 43210.";
    case "UNAVAILABLE":
      return `The service is busy.${wait || " Try again shortly."}`;
    default:
      return err.message || "Something went wrong. Please try again.";
  }
}
