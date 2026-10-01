-- release.lua: return a hold's units to the pool.
-- KEYS[1] inv:{E}:avail   KEYS[2] inv:{E}:user:U   KEYS[3] inv:{E}:hold:H   KEYS[4] inv:{E}:expiry
-- ARGV[1] expiry member "H|U"   ARGV[2] mode: EXPIRE | USER_CANCEL | PAYMENT_FAILED
-- ARGV[3] user_id (must own the hold; KEYS[2] is derived from it)
-- Returns  1 released
--          0 nothing to do (already released or sold; unknown holds are cleaned up)
--         -1 not expired yet (EXPIRE mode only; the sweeper will retry later)
--         -2 a PAYING hold cannot be cancelled by the user
--         -3 the hold belongs to a different user
local h = redis.call('HMGET', KEYS[3], 'user', 'qty', 'state', 'expires_at')
if not h[1] then
  redis.call('ZREM', KEYS[4], ARGV[1])
  return 0
end
local state = h[3]
if state == 'RELEASED' or state == 'SOLD' then
  redis.call('ZREM', KEYS[4], ARGV[1])
  return 0
end
if h[1] ~= ARGV[3] then return -3 end

local mode = ARGV[2]
if mode == 'EXPIRE' then
  local t = redis.call('TIME')
  local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
  if tonumber(h[4]) > now then return -1 end
elseif mode == 'USER_CANCEL' then
  if state == 'PAYING' then return -2 end
end

local qty = tonumber(h[2])
redis.call('INCRBY', KEYS[1], qty)
local used = redis.call('DECRBY', KEYS[2], qty)
if used <= 0 then redis.call('DEL', KEYS[2]) end
redis.call('HSET', KEYS[3], 'state', 'RELEASED', 'released_by', mode)
redis.call('PEXPIRE', KEYS[3], 3600000) -- keep a 1-hour tombstone for replays and audits
redis.call('ZREM', KEYS[4], ARGV[1])
return 1
