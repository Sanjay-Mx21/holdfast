-- advance.lua (fenced): one admission tick. Admits the next people in line
-- and rewrites the status document every client polls.
-- Only the current leader may run it. Every leadership change increments the
-- epoch, and a caller whose epoch is not the current one is refused: a leader
-- that paused (a long GC, a frozen VM) and woke up after a new leader was
-- elected cannot admit anyone, nor overwrite the status document. This is
-- the fencing-token pattern.
--
-- Concurrency is capped with Little's Law: every admitted rank holds a
-- session slot until its session TTL passes, and active slots never exceed
-- max_sessions, nor the units left times the oversubscription factor, so
-- thousands are not admitted to fight over the last few units. Expired
-- slots are removed first, in the same atomic step.
-- Admission only happens while the queue is OPEN; FROZEN pauses it. The
-- status document is written in every state, so it always says how fresh it is.
-- KEYS[1] adm:{E}:epoch    KEYS[2] q:{E}:state    KEYS[3] q:{E}:config
-- KEYS[4] q:{E}:members    KEYS[5] q:{E}:admitted KEYS[6] adm:{E}:sessions
-- KEYS[7] q:{E}:status
-- ARGV[1] the caller's epoch   ARGV[2] most people to admit this tick (from the rate)
-- ARGV[3] most sessions to have open at once for the units left (units left
--         times the oversubscription factor; P17), or -1 for no such cap
-- Returns {code, admitted_up_to, admitted_now, active_sessions, queue_size, state}
-- (state as the status document shows it)
--    1 admitted some       0 nothing to admit (not OPEN, no budget, or nobody waiting)
--   -1 fenced: the caller is not the current leader
--   -2 not provisioned
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return {-1, 0, 0, 0, 0, ''} end

local cfg = redis.call('HMGET', KEYS[3], 'max_sessions', 'session_ttl_ms', 'opens_at_ms')
local max_sessions = tonumber(cfg[1])
local ttl = tonumber(cfg[2])
local opens = tonumber(cfg[3])
local state = redis.call('GET', KEYS[2])
if not max_sessions or not ttl or not opens or not state then return {-2, 0, 0, 0, 0, ''} end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[6], '-inf', now)
local active = redis.call('ZCARD', KEYS[6])
local total = redis.call('ZCARD', KEYS[4])
local cur = tonumber(redis.call('GET', KEYS[5]) or '0')

local code, admitted_now = 0, 0
if state == 'OPEN' then
  local want = math.min(tonumber(ARGV[2]), max_sessions - active, total - cur)
  local units_cap = tonumber(ARGV[3])
  if units_cap and units_cap >= 0 then want = math.min(want, units_cap - active) end
  if want > 0 then
    local expires = now + ttl
    for rank = cur + 1, cur + want do
      redis.call('ZADD', KEYS[6], expires, rank)
    end
    cur = cur + want
    redis.call('SET', KEYS[5], cur)
    active = active + want
    code, admitted_now = 1, want
  end
end

-- The status document. A queue still marked PRE after T0 reads as OPEN: T0
-- is the clock's call (the opener flips the stored state within a tick).
local shown = state
if shown == 'PRE' and now >= opens then shown = 'OPEN' end
redis.call('SET', KEYS[7], cjson.encode({
  state = shown,
  opensAtMs = opens,
  admittedUpTo = cur,
  queueSize = total,
  updatedAtMs = now,
}))
return {code, cur, admitted_now, active, total, shown}
