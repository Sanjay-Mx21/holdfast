-- admit.lua: may this user claim their turn? Read-only.
-- The admission leader already gave every admitted rank a session slot in
-- adm:{E}:sessions (advance.lua). A user may claim while their rank is within
-- admittedUpTo and their slot is alive; the slot's expiry bounds the token.
-- A slot is judged by Valkey's clock, not by whether the leader has swept it
-- yet: the leader removes expired slots only on its next tick, and not at all
-- while no leader runs.
-- Claiming again returns the same answer, so it is safe to retry.
-- KEYS[1] q:{E}:state   KEYS[2] q:{E}:members   KEYS[3] q:{E}:admitted
-- KEYS[4] adm:{E}:sessions
-- ARGV[1] user_id
-- Returns {code, rank, value}
--    1 admitted           value = session slot expiry (ms)
--    0 not your turn yet  value = admittedUpTo
--   -1 not in queue
--   -2 not provisioned
--   -3 turn expired: admitted, but the session slot has run out
--   -4 queue closed (SOLD_OUT or CLOSED)
local state = redis.call('GET', KEYS[1])
if not state then return {-2, 0, 0} end
if state == 'SOLD_OUT' or state == 'CLOSED' then return {-4, 0, 0} end

local r = redis.call('ZRANK', KEYS[2], ARGV[1])
if not r then return {-1, 0, 0} end
local rank = r + 1

local up = tonumber(redis.call('GET', KEYS[3]) or '0')
if rank > up then return {0, rank, up} end

local expires = tonumber(redis.call('ZSCORE', KEYS[4], rank))
if not expires then return {-3, rank, 0} end
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
if expires <= now then return {-3, rank, 0} end
return {1, rank, expires}
