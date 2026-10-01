-- status.lua: read an event's status document. Read-only.
-- The admission leader rewrites q:{E}:status every tick. If no leader has
-- written it yet (just provisioned, or no leader elected), a fallback is built
-- from the raw keys, with no update time, so clients can tell it apart.
-- KEYS[1] q:{E}:status   KEYS[2] q:{E}:state   KEYS[3] q:{E}:config
-- KEYS[4] q:{E}:admitted KEYS[5] q:{E}:members
-- Returns {code, document}
--    1 the leader's document (JSON)
--    0 fallback document (JSON, without updatedAtMs)
--   -2 not provisioned
local doc = redis.call('GET', KEYS[1])
if doc then return {1, doc} end

local state = redis.call('GET', KEYS[2])
local opens = tonumber(redis.call('HGET', KEYS[3], 'opens_at_ms'))
if not state or not opens then return {-2, ''} end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
if state == 'PRE' and now >= opens then state = 'OPEN' end
return {0, cjson.encode({
  state = state,
  opensAtMs = opens,
  admittedUpTo = tonumber(redis.call('GET', KEYS[4]) or '0'),
  queueSize = redis.call('ZCARD', KEYS[5]),
})}
