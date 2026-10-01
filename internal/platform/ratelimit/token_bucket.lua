-- token_bucket.lua: take one token from a bucket, refilling it first.
-- One atomic step, so every replica sharing this Valkey sees the same bucket
-- and concurrent requests cannot both spend the last token.
-- KEYS[1] rl:SCOPE:ID (hash: tokens, ts_ms)
-- ARGV[1] capacity (burst size)   ARGV[2] refill rate in tokens per second
-- Returns {allowed, remaining_milli_tokens, retry_after_ms}
--   allowed 1: a token was taken; retry_after_ms is 0
--   allowed 0: bucket empty; retry_after_ms is when one token will be available
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])

-- Server time, not client time: every replica agrees on the refill.
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local b = redis.call('HMGET', KEYS[1], 'tokens', 'ts_ms')
local tokens = tonumber(b[1])
local ts = tonumber(b[2])
if not tokens or not ts then
  tokens = capacity
  ts = now
end
if now > ts then
  tokens = math.min(capacity, tokens + (now - ts) * rate / 1000)
end

local allowed = 0
local retry = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry = math.ceil((1 - tokens) * 1000 / rate)
end

redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts_ms', now)
-- A bucket left alone refills completely; after that it carries no state
-- worth keeping, so let it expire.
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity * 1000 / rate) + 1000)
return {allowed, math.floor(tokens * 1000), retry}
