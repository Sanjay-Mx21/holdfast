-- provision.lua: initialise an event's inventory exactly once (idempotent).
-- KEYS[1] inv:{E}:avail   KEYS[2] inv:{E}:config
-- ARGV[1] capacity        ARGV[2] per_user_limit
-- ARGV[3] initial available units (capacity for a new sale; capacity minus
--         units already sold when rebuilding after data loss)
-- Returns  1 created
--          0 already provisioned with identical settings (safe retry)
--         -1 already provisioned with different settings (refuse; see runbook)
local cap = redis.call('HGET', KEYS[2], 'capacity')
if cap then
  if cap == ARGV[1] and redis.call('HGET', KEYS[2], 'per_user_limit') == ARGV[2] then
    return 0
  end
  return -1
end
redis.call('HSET', KEYS[2], 'capacity', ARGV[1], 'per_user_limit', ARGV[2])
redis.call('SET', KEYS[1], ARGV[3])
return 1
