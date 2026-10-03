-- state.lua: move a queue from one state to another, atomically (the freeze
-- switch: OPEN to FROZEN and back, runbook RB-1).
-- KEYS[1] q:{E}:state   ARGV[1] from   ARGV[2] to
-- Returns {code, state}: the state after the call
--    1 moved
--    0 already in the target state (a retry)
--   -1 in another state: not moved
--   -2 not provisioned
local state = redis.call('GET', KEYS[1])
if not state then return {-2, ''} end
if state == ARGV[2] then return {0, state} end
if state ~= ARGV[1] then return {-1, state} end
redis.call('SET', KEYS[1], ARGV[2])
return {1, ARGV[2]}
