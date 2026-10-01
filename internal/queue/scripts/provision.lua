-- provision.lua: store an event's queue settings exactly once (idempotent)
-- and open the waiting room in state PRE.
-- KEYS[1] q:{E}:config   KEYS[2] q:{E}:state
-- ARGV[1] opens_at_ms    ARGV[2] admission_rate (per second)
-- ARGV[3] max_sessions   ARGV[4] session_ttl_ms
-- Returns  1 created
--          0 already provisioned with identical settings (safe retry)
--         -1 already provisioned with different settings (refuse; see runbook)
local cfg = redis.call('HMGET', KEYS[1], 'opens_at_ms', 'admission_rate', 'max_sessions', 'session_ttl_ms')
if cfg[1] then
  if cfg[1] == ARGV[1] and cfg[2] == ARGV[2] and cfg[3] == ARGV[3] and cfg[4] == ARGV[4] then
    return 0
  end
  return -1
end
redis.call('HSET', KEYS[1], 'opens_at_ms', ARGV[1], 'admission_rate', ARGV[2],
           'max_sessions', ARGV[3], 'session_ttl_ms', ARGV[4])
-- NX: never move a queue that is already past PRE back to PRE.
redis.call('SET', KEYS[2], 'PRE', 'NX')
return 1
