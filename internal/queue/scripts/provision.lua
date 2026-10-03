-- provision.lua: store an event's queue settings exactly once (idempotent),
-- open the waiting room in state PRE, and set its policy windows.
-- KEYS[1] q:{E}:config   KEYS[2] q:{E}:state   KEYS[3] q:{E}:policy
-- ARGV[1] opens_at_ms    ARGV[2] admission_rate (per second)
-- ARGV[3] max_sessions   ARGV[4] session_ttl_ms
-- ARGV[5] verified_only_until_ms   ARGV[6] agent_lockout_until_ms (0: no window)
-- The settings are fixed once stored. The policy windows are not: every
-- accepted call (a create, or a retry with identical settings) sets them.
-- Returns  1 created
--          0 already provisioned with identical settings (safe retry)
--         -1 already provisioned with different settings (refuse; see runbook)
local cfg = redis.call('HMGET', KEYS[1], 'opens_at_ms', 'admission_rate', 'max_sessions', 'session_ttl_ms')
local code = 1
if cfg[1] then
  if not (cfg[1] == ARGV[1] and cfg[2] == ARGV[2] and cfg[3] == ARGV[3] and cfg[4] == ARGV[4]) then
    return -1
  end
  code = 0
else
  redis.call('HSET', KEYS[1], 'opens_at_ms', ARGV[1], 'admission_rate', ARGV[2],
             'max_sessions', ARGV[3], 'session_ttl_ms', ARGV[4])
  -- NX: never move a queue that is already past PRE back to PRE.
  redis.call('SET', KEYS[2], 'PRE', 'NX')
end
redis.call('HSET', KEYS[3], 'verified_only_until_ms', ARGV[5], 'agent_lockout_until_ms', ARGV[6])
return code
