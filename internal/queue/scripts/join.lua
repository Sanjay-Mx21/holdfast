-- join.lua: put a user in an event's queue exactly once.
-- Before T0 (state PRE) a joiner gets the random lottery score from ARGV[2],
-- in [0, 1). From T0 on (OPEN, and FROZEN, which is only reachable after T0)
-- a joiner gets 1 plus an arrival counter, so every post-T0 joiner ranks
-- behind every lottery joiner, in arrival order. ZADD NX plus the existence
-- check make joining idempotent and stop anyone re-rolling their position.
-- KEYS[1] q:{E}:state   KEYS[2] q:{E}:members   KEYS[3] q:{E}:seq
-- ARGV[1] user_id       ARGV[2] lottery score, generated server-side (crypto/rand)
-- Returns {code, score}; scores are strings, because Lua numbers are
-- truncated to integers in replies.
--    1 joined
--    0 already joined (score is the original one)
--   -1 queue closed (SOLD_OUT or CLOSED)
--   -2 event not provisioned
local state = redis.call('GET', KEYS[1])
if not state then return {-2, ''} end
if state == 'SOLD_OUT' or state == 'CLOSED' then return {-1, ''} end

local existing = redis.call('ZSCORE', KEYS[2], ARGV[1])
if existing then return {0, existing} end

local score
if state == 'PRE' then
  score = ARGV[2]
else
  score = tostring(1 + redis.call('INCR', KEYS[3]))
end
redis.call('ZADD', KEYS[2], 'NX', score, ARGV[1])
return {1, score}
