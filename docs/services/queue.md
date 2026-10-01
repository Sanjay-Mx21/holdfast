# queue-svc

The waiting room: decides who may enter the purchase path, in what order and
how fast. Binary: `cmd/queue`. Code: `internal/queue`.

**Status:** Phase 2 in progress. Built: provisioning (task 2.1). Joining, the
T0 transition, ranks, the admission controller, the status document and
admission tokens follow in tasks 2.2 to 2.8.

## Responsibilities

- Store each event's queue settings and open its waiting room in state PRE (built).
- Accept joins: a random lottery position before T0, first come, first served after (task 2.2).
- Admit buyers at a controlled rate and issue admission tokens (tasks 2.5 to 2.7).
- Publish the status document every client polls (task 2.6).

## API

### `PUT /internal/v1/events/{eventID}/queue` (admin port)

Operator token required. Stores the event's queue settings and puts the
queue in state `PRE`.

```http
PUT /internal/v1/events/0196f0c1-.../queue
Authorization: Bearer <operator token>
Content-Type: application/json

{"opensAt":"2026-10-05T12:00:00Z","admissionRatePerSecond":83,"maxSessions":10000,"sessionTtlSeconds":600}
```

```http
HTTP/1.1 201 Created

{"eventId":"0196f0c1-...","created":true}
```

| Field | Meaning | Bounds |
|---|---|---|
| `opensAt` | T0, RFC 3339. Joins before it get a lottery position, joins after it are first come, first served | After 2020-01-01; stored with millisecond precision |
| `admissionRatePerSecond` | Most buyers admitted per second | 1 to 100,000 |
| `maxSessions` | Most concurrent checkout sessions (L in Little's Law) | 1 to 10,000,000 |
| `sessionTtlSeconds` | Lifetime of an admitted buyer's session | 60 to 3,600 |

Returns 201 when created and 200 when already provisioned with identical
settings, so retries are safe. Different settings return 409
`PROVISION_CONFLICT` and change nothing. Provisioning never moves a queue that
is already past `PRE` back to `PRE`.

`holdfastctl event create` provisions the queue together with inventory:
`--admission-rate` (default 83), `--max-sessions` (default 10,000) and
`--session-ttl` (default 10m); the opening time is the event's `--opens-at`.
It validates these flags before writing anything to PostgreSQL.

## Error codes

| Code | HTTP | Meaning |
|---|---|---|
| `UNAUTHENTICATED` | 401 | Missing or wrong operator token |
| `INVALID_REQUEST` | 400 | Malformed event ID, or a setting outside its bounds (`detail` says which) |
| `INVALID_BODY`, `MALFORMED_JSON`, `EMPTY_BODY`, `TRAILING_DATA`, `INVALID_FIELD_TYPE` | 400 | Body problems, including unknown fields |
| `UNSUPPORTED_MEDIA_TYPE` | 415 | Body is not JSON |
| `PROVISION_CONFLICT` | 409 | Queue already provisioned with different settings |
| `ROUTE_NOT_FOUND`, `METHOD_NOT_ALLOWED` | 404, 405 | No such endpoint or method |
| `UNAVAILABLE` | 503 | Valkey unreachable or timed out; retry after `Retry-After` |
| `INTERNAL` | 500 | Unexpected error, logged with the request ID |

## Queue states

```
PRE --T0--> OPEN --> SOLD_OUT --> CLOSED
             |  ^
             v  |
            FROZEN
```

Only `PRE` is reachable today (provisioning). The T0 transition is task 2.3;
the freeze switch is task 4.3.

## Keyspace

| Key | Type | Content |
|---|---|---|
| `q:{E}:config` | hash | `opens_at_ms`, `admission_rate`, `max_sessions`, `session_ttl_ms`; written once by `provision.lua` |
| `q:{E}:state` | string | `PRE`, `OPEN`, `FROZEN`, `SOLD_OUT` or `CLOSED` |

The remaining `q:*`, `adm:*`, `rl:*` and `jti:*` keys in the design doc
(section 8.2) arrive with tasks 2.2 to 2.8.

## Scripts

| Script | Replies |
|---|---|
| `provision.lua` | 1 created, 0 identical settings, -1 conflict |

## Configuration

Shared settings (`ENVIRONMENT`, `LOG_*`, `HTTP_*`, `SHUTDOWN_*`, `VALKEY_*`)
are defined in `internal/platform/config`. Service settings:

| Variable | Default | Meaning |
|---|---|---|
| `ADMIN_TOKEN` | required | Operator token, at least 32 characters; removed from the environment after loading |

Locally, Compose maps the public port to 8082 and the admin port to 9092.

## Metrics

Only the shared HTTP metrics so far: `holdfast_http_requests_total{route,code}`
and `holdfast_http_request_duration_seconds{route}`, labelled by route pattern.
Queue metrics (size, `admittedUpTo`, sessions, admission rate, leader epoch)
arrive with task 2.10.
