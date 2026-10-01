-- position.lua: where a user stands in an event's queue. Read-only.
-- Before T0 (by Valkey's clock) there is no meaningful rank: lottery
-- positions keep arriving until T0, so the caller is told when the draw
-- closes instead. From T0 on, no lottery position can be added, so a rank
-- only improves as people ahead are admitted.
-- KEYS[1] q:{E}:state   KEYS[2] q:{E}:config   KEYS[3] q:{E}:members
-- ARGV[1] user_id
-- Returns {code, value, state}
--    1 ranked          value = rank, 1-based
--    0 before T0       value = opens_at_ms
--   -1 not in queue    value = 0
--   -2 not provisioned value = 0
local state = redis.call('GET', KEYS[1])
local opens = tonumber(redis.call('HGET', KEYS[2], 'opens_at_ms'))
if not state or not opens then return {-2, 0, ''} end

if not redis.call('ZSCORE', KEYS[3], ARGV[1]) then return {-1, 0, state} end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
if state == 'PRE' and now < opens then return {0, opens, state} end
-- PRE past T0 (nobody has flipped it yet) reads as OPEN: T0 is the clock's call.
if state == 'PRE' then state = 'OPEN' end

return {1, redis.call('ZRANK', KEYS[3], ARGV[1]) + 1, state}
