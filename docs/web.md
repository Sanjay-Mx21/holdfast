# The web app

The buyer's side of HoldFast: browse sales, sign in with a phone code, join a
waiting room, wait for a turn, hold tickets, book and pay. Code: `web/`
(Next.js 16.3, React 19, TypeScript, Tailwind CSS 4). Built in task 4.5 (the
proof-of-work solver in 4.4).

Locally: `make up`, then open http://localhost:8088. Create a sale with
`make event NAME="Coldplay Mumbai" CAPACITY=100`. The sign-in page has a
button that reads the code from the mock SMS inbox (development builds only).

## Pages

| Page | What it does | API |
|---|---|---|
| `/` | Sales still to open, soonest first, then sales already open | `GET /v1/events` |
| `/signin?next=` | Phone, then the 6-digit code; returns to `next` (same-site paths only) | `/v1/auth/otp/request`, `/v1/auth/otp/verify` |
| `/event?id=` | Price, limit, opening time with a countdown, the live state; **join**: a proof-of-work challenge solved in a Web Worker, then the join | `GET /v1/events/{id}`, `/status`, `/v1/queue/{id}/challenge`, `/join` |
| `/queue?id=` | The waiting room: the draw before T0, then people ahead; claims the turn when it comes | `/v1/queue/{id}/me` once, `/status` polled, `/admit` |
| `/buy?id=` | Quantity (1 to the limit), the hold with its countdown, then the booking | `POST /v1/events/{id}/holds`, `POST /v1/bookings` |
| `/booking?id=` | The booking's state; the payment link opens the provider in a new tab | `GET /v1/bookings/{id}` |

## How it works, and why

**Static export, served from cache.** `next build` with `output: 'export'`
writes plain HTML, JS and CSS (`web/out`), served by nginx in the `web`
container. The edge caches the pages for a minute and the hashed assets for
a year. Pages take their IDs from the query string (`/event?id=...`), not the
path, so the same cached HTML serves every event and every buyer: a surge of
page loads never reaches anything but the edge. (Static export cannot have
dynamic path segments without listing them at build time, and sales are
created after the build.)

**The waiting room polls one shared document.** After T0 the page asks for
the buyer's rank once (`/me`), at a random moment in the first 30 seconds so
a lottery crowd does not ask in the same second. From then on it polls only
the status document, every 3 seconds plus up to 1 second of random jitter;
the edge serves that from a one-second cache. The page compares the rank
with `admittedUpTo` itself. The origin's load stays flat however many people
wait (ADR 0006, experiment E2). When the turn comes, the page claims it
(`/admit`), keeps the admission token in `sessionStorage` and moves to
checkout.

**Proof of work in a Web Worker.** Joining needs a solved challenge
(`docs/services/queue.md`). The solver (`web/src/lib/pow/`) is a synchronous
SHA-256 in TypeScript with a midstate for the challenge's constant first
block, run in a worker so the page stays responsive; a progress bar shows
the work against its expected size.

**Sign-in state.** The access token (15 minutes) is kept in `localStorage`,
shared by the tabs; the refresh token is an httpOnly cookie scripts never
see. A refresh rotates the refresh token, and presenting an old one revokes
the login (reuse detection), so two tabs refreshing at once would sign the
buyer out. Refreshes are therefore serialized across tabs with the Web Locks
API, and a tab that waited for the lock first checks whether another tab has
refreshed already.

**Retries are safe.** The hold's idempotency key is kept per tab, event and
quantity in `sessionStorage`, so a retried or reloaded request returns the
same hold; the booking's key is derived from the hold (`book-<holdId>`).

**Payment in a new tab.** The mock provider's checkout page has no link back
to the merchant, so the booking page opens it in a new tab and keeps polling
the booking until it is confirmed, cancelled or refunded.

## Accessibility basics

- Landmarks (`header`, `nav`, `main`, `footer`), a skip link, `lang="en"`,
  one `h1` per page, and focus moved to the heading when a page's content
  loads.
- Every input has a label; hints are tied with `aria-describedby`; the phone
  and code inputs use `autocomplete="tel"` and `"one-time-code"`.
- Errors are announced at once (`role="alert"`), other notices politely
  (`role="status"`). The waiting room's live region announces a rounded
  number of people ahead, so a screen reader speaks when the position moves
  noticeably, not every 3 seconds; countdowns are not announced.
- Native controls only (buttons, links, `select`, `progress`), a visible
  focus ring, text and accent colours at WCAG AA contrast in light and dark
  schemes, and no animation for `prefers-reduced-motion`.
- The end-to-end test runs axe (WCAG 2.1 A and AA rules) on every page of
  the purchase.

## Security

- Content Security Policy: this origin only, plus inline scripts and styles,
  which Next.js's static bootstrap needs; `frame-ancestors 'none'`,
  `form-action 'self'`. Also `X-Content-Type-Options`, `Referrer-Policy:
  no-referrer` and `X-Frame-Options`.
- The access token in `localStorage` is readable by scripts on this origin,
  so the policy above (no third-party scripts) matters; it lives 15 minutes.
- After sign-in the page only returns to same-site paths (`next` must start
  with a single `/`).

## Tests

| What | How |
|---|---|
| The solver against the server's hash vectors; the waiting-room logic (turns, jitter, rounding, durations, keys) | `npm test` (`node --test`), in CI |
| Types, lint (Next.js core web vitals rules), the static export, the image | `npm run typecheck`, `npm run lint`, `npm run build`; CI job `web` |
| A whole purchase in Chromium through the edge, with axe on each page | `EVENT=<id> npm run e2e` against `make up` (`web/e2e/`) |

## Configuration

| Build argument | Default | Meaning |
|---|---|---|
| `NEXT_PUBLIC_DEV_INBOX` | `0` (`1` in Compose) | Show the button that reads sign-in codes from auth-svc's development inbox |
