# auth-svc

Signs people in with a one-time code sent to their phone, and issues the
tokens the other services trust. Binary: `cmd/auth`. Code: `internal/auth`
(access tokens: `internal/platform/authn/access.go`). Schema: `auth`
(migrations in `db/migrations/auth`, queries in `internal/auth/queries`).

**Status:** built in Phase 4 (task 4.1). queue-svc and booking-svc accept its
access tokens from task 4.2; until then they still use the development
header.

## Responsibilities

- Send a six-digit code to a phone (through a mock SMS gateway), and sign
  in whoever enters it in time, creating the user the first time.
- Issue **access tokens**: 15-minute EdDSA JWTs that cannot be revoked, so
  they are short-lived.
- Issue **refresh tokens** that rotate on every use, and revoke a whole login
  when an old one is presented again (**reuse detection**).
- Publish the public signing keys (JWKS) for the services that verify access
  tokens.
- Never store or log a phone number, code or refresh token in the clear.

## API

All on the public port; the edge routes `/v1/auth/` here.

### `POST /v1/auth/otp/request`

```http
POST /v1/auth/otp/request
Content-Type: application/json

{"phone": "+919876543210"}
```

202 `{"expiresInSeconds": 300}`. The phone may be E.164 (spaces and dashes
allowed) or a bare 10-digit Indian mobile number, which gets +91. The answer
is the same whether or not the phone has an account.

- 400 `INVALID_PHONE`.
- 429 `RATE_LIMITED` with `Retry-After`. Each phone gets at most one code per
  `OTP_RESEND_AFTER` (30 s) and `OTP_MAX_PER_HOUR` (5) codes per hour. The
  edge also allows each client address 5 requests a minute (burst 5).
  Concurrent requests for one phone are serialised by a PostgreSQL advisory
  lock, so the limits cannot be raced.

### `POST /v1/auth/otp/verify`

```http
POST /v1/auth/otp/verify
Content-Type: application/json

{"phone": "+919876543210", "code": "482913"}
```

The code is checked against the phone's latest open code.

- **Attempts.** Every guess, right or wrong, takes one of the code's
  `OTP_MAX_ATTEMPTS` (5) attempts, in one conditional `UPDATE`. Concurrent
  guesses cannot exceed the limit.
- **Single use.** A correct code is used once (compare-and-set), so 10
  concurrent correct guesses sign in exactly once.
- **Expiry.** Codes expire after `OTP_TTL` (5 minutes).

200:

```json
{"accessToken": "eyJ...", "tokenType": "Bearer", "expiresIn": 900,
 "userId": "01a1...", "role": "BUYER"}
```

The refresh token is **not** in the body. It is set as a cookie:
`hf_refresh=...; Path=/v1/auth; HttpOnly; Secure; SameSite=Strict`, valid
for `REFRESH_TTL` (30 days). It is always Secure: browsers accept Secure
cookies from `http://localhost`, so development needs no exception.

- **401 `INVALID_CODE`.** One answer for a wrong, expired, used or exhausted
  code, so nothing is revealed.
- **400 `INVALID_PHONE`.**

### `POST /v1/auth/refresh`

Sends the cookie and receives a fresh access token and a **new** refresh
cookie.

- **Rotation.** The presented token is replaced, once, by a compare-and-set
  on `replaced_by IS NULL`.
- **Reuse detection.** A token that was already replaced means someone else
  has it: it was stolen, or replayed. Its whole **family** (every token
  descending from that sign-in) is revoked, and the answer is 401
  `REFRESH_REUSED` with the cookie cleared. Two refreshes racing with the
  same token count as reuse too, so a client must refresh one request at a
  time.
- **401 `INVALID_REFRESH`.** An unknown, expired or revoked token; the
  cookie is cleared.

### `POST /v1/auth/logout`

Revokes the cookie's family and clears it. 204, also when already signed
out.

### `GET /.well-known/jwks.json`

The public keys access tokens are signed with (`kid` = `authn.KeyID`),
cacheable for 5 minutes. Other services fetch it from auth-svc directly
(`http://auth:8080`); the edge's `/.well-known/jwks.json` is queue-svc's
admission-token key set.

### `GET /v1/auth/dev/inbox?phone=...` (development only)

The mock SMS gateway's last message to a number, code included, so a
developer, a test or the demo can sign in. It exists only with
`DEV_SMS_INBOX=true`, which the config refuses in production. URL-encode the
phone (`%2B91...`): a raw `+` in a query string means a space.

## Access tokens

EdDSA (Ed25519) JWTs with `kid` in the header and these claims:

| Claim | Value |
|---|---|
| `iss` | `holdfast-auth` |
| `aud` | `holdfast-api` |
| `sub` | the user's ID (UUIDv7) |
| `role` | `BUYER`, `AGENT` or `ADMIN` |
| `iat`, `nbf`, `exp`, `jti` | 15 minutes' life |

`authn.AccessVerifier` checks the signature (EdDSA only: no `none`, no HMAC
confusion), issuer, audience, expiry, a UUID subject and a known role. It is
used with a `JWKSClient` following auth-svc's key set.

## What is stored

| Table | Holds | Never holds |
|---|---|---|
| `auth.users` | `phone_hmac` (HMAC-SHA256 of the number under `PHONE_PEPPER`), the last 4 digits, role, verification time | the phone number |
| `auth.otp_challenges` | HMAC of challenge ID and code under the pepper, attempts, expiry, use | the code |
| `auth.refresh_tokens` | SHA-256 of the token, user, family, `replaced_by`, `revoked_at`, expiry | the token |

A six-digit code alone is easy to brute-force from a stolen table, so it is
keyed with the pepper and bound to its challenge. Refresh tokens are 256
random bits, so a plain SHA-256 suffices. Logs carry user and family IDs,
never phones, codes or tokens.

## Configuration

Shared settings (`ENVIRONMENT`, `LOG_*`, `HTTP_*`, `POSTGRES_*`, `OTEL_*`) are
defined in `internal/platform/config` and `internal/platform/otel`.

| Variable | Default | Meaning |
|---|---|---|
| `SIGNING_KEY_FILE` | required | Ed25519 private key (PEM) for access tokens; `make keys` creates `.local/keys/auth.key` |
| `PHONE_PEPPER` | required | At least 32 characters; set once per environment (changing it orphans every user) |
| `ACCESS_TTL` | `15m` | Access-token life (1m to 1h) |
| `REFRESH_TTL` | `720h` | Refresh-token life; each rotation starts a new one |
| `OTP_TTL` | `5m` | Code life (1m to 15m) |
| `OTP_MAX_ATTEMPTS` | `5` | Guesses per code |
| `OTP_RESEND_AFTER` | `30s` | Minimum time between codes for a phone |
| `OTP_MAX_PER_HOUR` | `5` | Codes per phone per hour |
| `DEV_SMS_INBOX` | `false` | Serve the mock SMS inbox (refused in production) |

Locally, Compose maps the public port to 8086 and the admin port to 9096.
Compose's `PHONE_PEPPER` is a development placeholder that `.env` can
override.

## Metrics

| Metric | Labels | Use |
|---|---|---|
| `holdfast_auth_otp_requests_total` | `result` | sent, rate_limited, invalid_phone, error |
| `holdfast_auth_otp_verifications_total` | `result` | ok, invalid |
| `holdfast_auth_refresh_total` | `result` | rotated, invalid, reused (a family was revoked: watch for spikes) |
| `holdfast_auth_users_created_total` | | First sign-ins |
| `holdfast_http_*` | `route`, `code` | RED metrics per route pattern |
