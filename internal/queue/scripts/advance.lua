-- advance.lua (fenced): admit the next people in line, one admission tick.
-- Only the current leader may admit. Every leadership change increments the
-- epoch, and a caller whose epoch is not the current one is refused: a leader
-- that paused (a long GC, a frozen VM) and woke up after a new leader was
-- elected cannot admit anyone. This is the fencing-token pattern.
--
-- Concurrency is capped with Little's Law: every admitted rank holds a
-- session slot until its session TTL passes, and active slots never exceed
-- max_sessions. Expired slots are removed first, in the same atomic step.
-- Admission only happens while the queue is OPEN; FROZEN pauses it.
-- KEYS[1] adm:{E}:epoch    KEYS[2] q:{E}:state    KEYS[3] q:{E}:config
-- KEYS[4] q:{E}:members    KEYS[5] q:{E}:admitted KEYS[6] adm:{E}:sessions
-- ARGV[1] the caller's epoch   ARGV[2] most people to admit this tick (from the rate)
-- Returns {code, admitted_up_to, admitted_now, active_sessions}
--    1 admitted some       0 nothing to admit (not OPEN, no budget, or nobody waiting)
--   -1 fenced: the caller is not the current leader
--   -2 not provisioned
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return {-1, 0, 0, 0} end

local cfg = redis.call('HMGET', KEYS[3], 'max_sessions', 'session_ttl_ms')
local max_sessions = tonumber(cfg[1])
local ttl = tonumber(cfg[2])
if not max_sessions or not ttl then return {-2, 0, 0, 0} end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[6], '-inf', now)
local active = redis.call('ZCARD', KEYS[6])
local cur = tonumber(redis.call('GET', KEYS[5]) or '0')

if redis.call('GET', KEYS[2]) ~= 'OPEN' then return {0, cur, 0, active} end

local want = math.min(tonumber(ARGV[2]), max_sessions - active, redis.call('ZCARD', KEYS[4]) - cur)
if want <= 0 then return {0, cur, 0, active} end

local nxt = cur + want
local expires = now + ttl
for rank = cur + 1, nxt do
  redis.call('ZADD', KEYS[6], expires, rank)
end
redis.call('SET', KEYS[5], nxt)
return {1, nxt, want, active + want}
