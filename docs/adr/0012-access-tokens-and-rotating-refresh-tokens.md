# 0012. Short-lived access tokens, and refresh tokens that rotate with reuse detection

- Status: accepted
- Date: 2026-10-04

## Context

From Phase 4, buyers sign in (auth-svc, task 4.1), and queue-svc and
booking-svc must know who is calling (task 4.2). The forces:

- **Verification on the hot path.** A join at T0 is one of thousands a
  second; it cannot wait on a call to auth-svc or a session lookup.
- **Revocation.** A stolen credential must stop working, and signing out
  must mean something.
- **Theft.** Tokens kept by a browser can be read by injected script, and a
  cookie can be sent by other sites (CSRF).
- **Retries and tabs.** Clients retry, and buyers open several tabs during a
  sale.

The options:

- **Server-side sessions** (an ID in a cookie, looked up per request): easy
  revocation, but every request in every service needs the session store,
  and the store becomes part of the hot path's failure domain.
- **Long-lived JWTs:** verified locally, but a stolen token works until it
  expires, and nothing can revoke it.
- **Opaque tokens with introspection:** revocable, but every request calls
  auth-svc.
- **Short-lived JWTs plus refresh tokens** that the issuer tracks.

## Decision

Short-lived access tokens, verified locally; long-lived refresh tokens,
tracked by auth-svc, that rotate on every use and betray their own theft.

- **Access tokens:** EdDSA (Ed25519) JWTs, 15 minutes, with `iss`
  `holdfast-auth`, `aud` `holdfast-api`, `sub` the user, `role`, `vrf`
  (verified identity) and a `jti`. Services verify them with auth-svc's
  public keys, fetched from its JWKS and cached (`authn.NewAccessIdentity`).
  Only EdDSA is accepted, so no `alg` confusion. They cannot be revoked; the
  short life bounds the damage.
- **Refresh tokens:** 32 random bytes, stored only as a SHA-256 hash, valid
  30 days, in an `HttpOnly; Secure; SameSite=Strict` cookie with
  `Path=/v1/auth`. Scripts cannot read it, other sites cannot send it, and it
  travels only to the refresh and logout endpoints.
- **Rotation:** each refresh replaces the presented token with a new one,
  once, by a compare-and-set on `replaced_by IS NULL`. Every token from one
  sign-in shares a **family**.
- **Reuse detection:** presenting a token that was already replaced means
  two parties hold it, the buyer and a thief. The whole family is revoked,
  so both are signed out, and the buyer signs in again with a phone code.
  Logout revokes the family too.
- **Clients refresh one at a time.** Two refreshes racing with one token
  look exactly like reuse. The web app keeps the access token in
  `localStorage`, shared by its tabs, and serialises refreshes across tabs
  with the Web Locks API; a tab that waited for the lock first checks
  whether another tab has refreshed.

## Consequences

- Queue and booking verify a token with one signature check and no network
  call; auth-svc is off the hot path and can be down for up to 15 minutes
  without stopping signed-in buyers.
- A stolen access token works for up to 15 minutes. A stolen refresh token
  works until the buyer or the thief refreshes next; then reuse is detected
  and both are signed out. Damage is bounded, at the cost of an occasional
  forced sign-in for a buyer whose token was copied.
- Signing out cannot cut short an access token already issued; it ends the
  refresh family. Revoking a user's access at once would need a deny list
  checked on every request, which is not built.
- The access token in `localStorage` is readable by script on the page, so
  the web app serves a Content Security Policy that allows only its own
  origin. An XSS hole would expose 15 minutes of access, never the refresh
  token.
- Clients must not refresh concurrently. The web app handles it; other
  clients (scripts, mobile apps) must serialise refreshes themselves, or
  their buyers get signed out.
- Signing keys rotate without restarts: services follow the JWKS (a new
  `kid` triggers a fetch, rate-limited), the same machinery inventory-svc
  uses for admission tokens (`docs/services/queue.md`).
