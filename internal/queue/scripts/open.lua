-- open.lua: the T0 transition. Flip an event's queue from PRE to OPEN once
-- Valkey's clock reaches opens_at_ms. Conditional and idempotent, so every
-- replica may run it on every tick: there is no leader to get wrong, and a
-- second caller simply finds the queue already open.
-- KEYS[1] q:{E}:state   KEYS[2] q:{E}:config
-- Returns {code, ms}
--    1 opened now          ms = how late, after opens_at_ms
--    0 nothing to do       ms = time left until T0 (0 if already past PRE)
--   -1 not provisioned     ms = 0
local state = redis.call('GET', KEYS[1])
local opens = tonumber(redis.call('HGET', KEYS[2], 'opens_at_ms'))
if not state or not opens then return {-1, 0} end
if state ~= 'PRE' then return {0, 0} end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
if now < opens then return {0, opens - now} end

redis.call('SET', KEYS[1], 'OPEN')
return {1, now - opens}
