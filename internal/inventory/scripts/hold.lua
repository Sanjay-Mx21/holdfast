-- hold.lua: atomically hold units for one user (quantity mode).
-- Runs as a single atomic step on the server: no locks, no check-then-act race.
-- KEYS[1] inv:{E}:avail     KEYS[2] inv:{E}:config    KEYS[3] inv:{E}:user:U
-- KEYS[4] inv:{E}:hold:H    KEYS[5] inv:{E}:expiry
-- ARGV[1] qty   ARGV[2] user_id   ARGV[3] expiry member "H|U"   ARGV[4] ttl_ms
-- Returns {code, n, expires_at_ms}
--    1 held           n = units left
--    2 replay         n = units left (a hold already exists for this idempotency key)
--    0 sold out       n = units left
--   -1 user limit     n = units this user already holds or bought
--   -2 bad quantity   n = per-user limit
--   -3 event not provisioned
--   -4 sale frozen (runbook RB-1): no new holds until it is unfrozen
local limit = tonumber(redis.call('HGET', KEYS[2], 'per_user_limit'))
if not limit then return {-3, 0, 0} end

local qty = tonumber(ARGV[1])
if (not qty) or qty < 1 or qty > limit then return {-2, limit, 0} end

if redis.call('EXISTS', KEYS[4]) == 1 then
  local exp = tonumber(redis.call('HGET', KEYS[4], 'expires_at')) or 0
  return {2, tonumber(redis.call('GET', KEYS[1]) or '0'), exp}
end

-- A frozen sale takes no new holds; a retry of a hold made before the freeze
-- still gets its answer above.
if redis.call('HGET', KEYS[2], 'frozen') == '1' then return {-4, 0, 0} end

local used = tonumber(redis.call('GET', KEYS[3]) or '0')
if used + qty > limit then return {-1, used, 0} end

local avail = tonumber(redis.call('GET', KEYS[1]) or '0')
if avail < qty then return {0, avail, 0} end

-- Server time, not client time: every replica agrees on when a hold expires.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local expires = now + tonumber(ARGV[4])

redis.call('DECRBY', KEYS[1], qty)
redis.call('INCRBY', KEYS[3], qty)
redis.call('HSET', KEYS[4], 'user', ARGV[2], 'qty', qty, 'state', 'HELD',
           'expires_at', expires, 'created_at', now)
redis.call('ZADD', KEYS[5], expires, ARGV[3])
return {1, avail - qty, expires}
