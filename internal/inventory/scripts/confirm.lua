-- confirm.lua: mark a hold SOLD once PostgreSQL has committed the booking.
-- KEYS[1] inv:{E}:hold:H   KEYS[2] inv:{E}:expiry   KEYS[3] inv:{E}:avail   KEYS[4] inv:{E}:user:U
-- ARGV[1] expiry member "H|U"   ARGV[2] qty   ARGV[3] user_id
-- Returns  1 confirmed
--          2 already confirmed (idempotent retry)
--          3 late confirm: the hold had been released, so its units were
--            re-taken from the pool (avail may go negative; the edge treats
--            anything <= 0 as sold out and reconciliation reports it)
--         -1 the hold belongs to a different user
local h = redis.call('HMGET', KEYS[1], 'user', 'state')
if h[1] and h[1] ~= ARGV[3] then return -1 end
local state = h[2]
if state == 'SOLD' then return 2 end
if state == 'HELD' or state == 'PAYING' then
  redis.call('HSET', KEYS[1], 'state', 'SOLD')
  redis.call('PERSIST', KEYS[1])
  redis.call('ZREM', KEYS[2], ARGV[1])
  return 1
end
local qty = tonumber(ARGV[2])
redis.call('DECRBY', KEYS[3], qty)
redis.call('INCRBY', KEYS[4], qty)
redis.call('HSET', KEYS[1], 'user', ARGV[3], 'qty', qty, 'state', 'SOLD')
redis.call('PERSIST', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return 3
