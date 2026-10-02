// Experiment E2: the waiting room under a T0 stampede (make load-e2).
//
// 50,000 users arrive as the doors open: joins ramp from 0 to JOIN_PEAK per
// second in 10 s and hold there until everyone has joined. Every waiting user
// polls the status document through the edge every 3 s, so the polls ramp up
// to USERS / 3 per second (about 16,700). Once admission reaches them, users
// claim their turn and get an admission token.
//
// k6 cannot run 50,000 virtual users on one laptop, so the users are modelled
// as arrival rates: one iteration is one request by some user, which is the
// load the edge and the origin actually see. The question E2 answers is
// whether the origin's status traffic stays flat while the edge's grows, so a
// sampler reads queue-svc's own request counter every second, and both
// counts are kept per 10-second window.
//
// Run through make load-e2, which provisions the event and lifts the per-IP
// limits (one load generator is one IP); the per-user limits stay on.
import http from 'k6/http';
import exec from 'k6/execution';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const BASE = __ENV.BASE || 'http://localhost:8088';
const ADMIN = __ENV.ADMIN || 'http://localhost:9092';
const EVENT = __ENV.EVENT;
const USERS = parseInt(__ENV.USERS || '50000', 10);
const JOIN_PEAK = parseInt(__ENV.JOIN_PEAK || '2000', 10);
const ADMIT_RATE = parseInt(__ENV.ADMIT_RATE || '1000', 10);
const POLL_EVERY_S = parseInt(__ENV.POLL_EVERY || '3', 10);
const WINDOW_S = 10;

// Joins: a 10 s ramp (JOIN_PEAK * 10 / 2 joins), then JOIN_PEAK per second until
// USERS have joined.
const rampJoins = (JOIN_PEAK * 10) / 2;
// One extra second: the arrival schedule can fall a few iterations short of
// USERS, and every user must join (iterations past USERS do nothing).
const holdS = Math.max(1, Math.ceil((USERS - rampJoins) / JOIN_PEAK)) + 1;
const pollPeak = Math.ceil(USERS / POLL_EVERY_S);
const claimStartS = 10 + holdS + 5;
// Long enough for every claim, at least 100 s, in whole windows.
const DURATION_S = Math.max(100, Math.ceil((claimStartS + Math.ceil(USERS / ADMIT_RATE) + 10) / WINDOW_S) * WINDOW_S);
const WINDOWS = DURATION_S / WINDOW_S;

export const options = {
  discardResponseBodies: true, // only the sampler reads a body
  scenarios: {
    joins: {
      executor: 'ramping-arrival-rate',
      exec: 'join',
      startRate: 0,
      timeUnit: '1s',
      preAllocatedVUs: 200,
      maxVUs: 2000,
      stages: [
        { target: JOIN_PEAK, duration: '10s' },
        { target: JOIN_PEAK, duration: `${holdS}s` },
      ],
    },
    polls: {
      executor: 'ramping-arrival-rate',
      exec: 'poll',
      startRate: 0,
      timeUnit: '1s',
      preAllocatedVUs: 300,
      maxVUs: 3000,
      // Waiting users poll every 3 s, so polling grows with the queue.
      stages: [
        { target: pollPeak, duration: `${10 + holdS}s` },
        { target: pollPeak, duration: `${DURATION_S - 10 - holdS}s` },
      ],
    },
    claims: {
      executor: 'constant-arrival-rate',
      exec: 'claim',
      startTime: `${claimStartS}s`,
      rate: ADMIT_RATE,
      timeUnit: '1s',
      duration: `${Math.ceil(USERS / ADMIT_RATE)}s`,
      preAllocatedVUs: 300,
      maxVUs: 3000,
    },
    origin: {
      executor: 'constant-vus',
      exec: 'sample',
      vus: 1,
      duration: `${DURATION_S}s`,
    },
  },
  thresholds: {
    // The SLO for joining at T0 (design doc 13.4).
    'http_req_duration{name:join}': ['p(99)<150'],
    'http_req_failed{name:join}': ['rate<0.001'],
    'http_req_failed{name:status}': ['rate<0.001'],
    tokens_issued: [`count>=${USERS}`],
  },
};

// Per-window counters, made visible in the summary by trivial thresholds.
for (let w = 0; w < WINDOWS; w++) {
  options.thresholds[`edge_status_requests{win:${w}}`] = ['count>=0'];
  options.thresholds[`origin_status_requests{win:${w}}`] = ['count>=0'];
}

const edgeStatus = new Counter('edge_status_requests');
const originStatus = new Counter('origin_status_requests');
const tokensIssued = new Counter('tokens_issued');
const claimsNotYet = new Counter('claims_not_your_turn');
const claimsExpired = new Counter('claims_turn_expired');
let warned = 0; // unexpected claim answers reported by this VU

// The test's start, the same for every VU (k6 runs setup once).
export function setup() {
  if (!EVENT) {
    throw new Error('EVENT is required');
  }
  return { start: Date.now() };
}

function windowOf(start) {
  return String(Math.min(WINDOWS - 1, Math.floor((Date.now() - start) / 1000 / WINDOW_S)));
}

// User n's ID: a valid UUID that differs per user.
function userID(n) {
  return `00000000-0000-4000-8000-${n.toString(16).padStart(12, '0')}`;
}

export function join() {
  const n = exec.scenario.iterationInTest;
  if (n >= USERS) {
    return;
  }
  const res = http.post(`${BASE}/v1/queue/${EVENT}/join`, null, {
    headers: { 'X-Dev-User-Id': userID(n) },
    tags: { name: 'join' },
  });
  check(res, { 'join accepted': (r) => r.status === 202 });
}

export function poll(data) {
  const res = http.get(`${BASE}/v1/events/${EVENT}/status`, { tags: { name: 'status' } });
  edgeStatus.add(1, { win: windowOf(data.start) });
  check(res, { 'status ok': (r) => r.status === 200 });
}

// User n claims their turn, retrying once a second while it is not yet theirs.
export function claim() {
  const n = exec.scenario.iterationInTest;
  if (n >= USERS) {
    return;
  }
  for (let attempt = 0; attempt < 30; attempt++) {
    const res = http.post(`${BASE}/v1/queue/${EVENT}/admit`, null, {
      headers: { 'X-Dev-User-Id': userID(n) },
      tags: { name: 'admit' },
      responseType: 'text', // to report an unexpected answer
      responseCallback: http.expectedStatuses(200, 409),
    });
    if (res.status === 200) {
      tokensIssued.add(1);
      return;
    }
    const body = String(res.body);
    if (res.status === 409 && body.includes('"NOT_YOUR_TURN"')) {
      claimsNotYet.add(1); // admission has not reached this user yet: wait and ask again
      sleep(1);
      continue;
    }
    // Anything else is final: TURN_EXPIRED (the session slot ran out before the
    // claim), or an error.
    if (body.includes('"TURN_EXPIRED"')) {
      claimsExpired.add(1);
    }
    check(res, { 'claim answered': () => false });
    if (warned++ < 3) console.warn(`claim by user ${n}: ${res.status} ${body.slice(0, 200)}`);
    return;
  }
}

// Reads queue-svc's own count of status requests once a second: what reached
// the origin, as opposed to what the edge answered from its cache.
const statusRoute = /^holdfast_http_requests_total\{[^}]*route="GET \/v1\/events\/\{eventID\}\/status"[^}]*\} ([0-9.e+]+)/;
let last = -1;

export function sample(data) {
  const res = http.get(`${ADMIN}/metrics`, { tags: { name: 'sampler' }, responseType: 'text' });
  let total = 0;
  for (const line of String(res.body).split('\n')) {
    const m = statusRoute.exec(line);
    if (m) {
      total += parseFloat(m[1]);
    }
  }
  if (last >= 0 && total >= last) {
    originStatus.add(total - last, { win: windowOf(data.start) });
  }
  last = total;
  sleep(1);
}

