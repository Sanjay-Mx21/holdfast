-- mark_paying.lua: HELD -> PAYING when checkout starts. A PAYING hold is
-- protected from the expiry sweeper until the payment deadline (plus grace).
-- KEYS[1] inv:{E}:hold:H   KEYS[2] inv:{E}:expiry
-- ARGV[1] user_id   ARGV[2] expiry member "H|U"   ARGV[3] payment_window_ms
-- Returns {code, expires_at_ms}
--    1 marked PAYING   2 already PAYING (idempotent retry)
--    0 hold missing, released or expired   -1 hold belongs to another user
local h = redis.call('HMGET', KEYS[1], 'user', 'state', 'expires_at')
if not h[1] then return {0, 0} end
if h[1] ~= ARGV[1] then return {-1, 0} end
if h[2] == 'PAYING' then return {2, tonumber(h[3])} end
if h[2] ~= 'HELD' then return {0, 0} end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
if tonumber(h[3]) <= now then return {0, 0} end

local expires = now + tonumber(ARGV[3])
redis.call('HSET', KEYS[1], 'state', 'PAYING', 'expires_at', expires)
redis.call('ZADD', KEYS[2], expires, ARGV[2])
return {1, expires}
