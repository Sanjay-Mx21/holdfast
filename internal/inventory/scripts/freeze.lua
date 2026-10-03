-- freeze.lua: set or clear an event's freeze flag (runbook RB-1). While it
-- is set, hold.lua takes no new holds; existing holds, checkouts,
-- confirmations and releases carry on as usual.
-- KEYS[1] inv:{E}:config   ARGV[1] '1' to freeze, '0' to unfreeze
-- Returns  1 changed
--          0 already so (a retry)
--         -1 event not provisioned
if not redis.call('HGET', KEYS[1], 'capacity') then return -1 end
local cur = redis.call('HGET', KEYS[1], 'frozen') or '0'
if cur == ARGV[1] then return 0 end
redis.call('HSET', KEYS[1], 'frozen', ARGV[1])
return 1
