-- rebuild.lua: set an event's inventory to what PostgreSQL's records say
-- (runbook RB-2), after Valkey lost writes (a failover) or everything.
-- PostgreSQL is the source of truth; this makes Valkey agree with it, in one
-- atomic step. The sale must be frozen first, so that nothing new is held
-- between reading PostgreSQL and running this; it stays frozen afterwards.
--
-- KEYS[1] inv:{E}:avail   KEYS[2] inv:{E}:config   KEYS[3] inv:{E}:expiry
-- then, in order:
--   nu  inv:{E}:user:U   the users who hold or bought units (set)
--   ns  inv:{E}:user:U   other existing per-user counters (deleted)
--   nk  inv:{E}:hold:H   the holds of PENDING_PAYMENT bookings (kept or recreated)
--   nd  inv:{E}:hold:H   every other open hold in the expiry index (dropped)
-- ARGV[1] dry run (1: count what would change, write nothing)
-- ARGV[2] capacity   ARGV[3] per_user_limit   ARGV[4] available
-- ARGV[5] nu   ARGV[6] ns   ARGV[7] nk   ARGV[8] nd
-- then nu quantities; nk times (user, qty, expires_at_ms, expiry member);
-- nd expiry members.
-- Returns {avail_before (or the string "none"), was_frozen, users_changed,
--          users_deleted, holds_recreated, holds_kept, holds_sold, holds_dropped}
local dry = ARGV[1] == '1'
local nu, ns, nk, nd = tonumber(ARGV[5]), tonumber(ARGV[6]), tonumber(ARGV[7]), tonumber(ARGV[8])

local before = redis.call('GET', KEYS[1]) or 'none'
local was_frozen = redis.call('HGET', KEYS[2], 'frozen') == '1' and 1 or 0

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local key, arg = 3, 8
local users_changed, users_deleted = 0, 0
for _ = 1, nu do
  key, arg = key + 1, arg + 1
  if redis.call('GET', KEYS[key]) ~= ARGV[arg] then
    users_changed = users_changed + 1
    if not dry then redis.call('SET', KEYS[key], ARGV[arg]) end
  end
end
for _ = 1, ns do
  key = key + 1
  if redis.call('EXISTS', KEYS[key]) == 1 then
    users_deleted = users_deleted + 1
    if not dry then redis.call('DEL', KEYS[key]) end
  end
end

-- A pending booking's hold must exist and be PAYING until the booking
-- resolves: its confirmation then finds it (no "late" re-take of units the
-- rebuild already counted), and its cancellation returns the units once.
-- A hold already SOLD was confirmed after PostgreSQL was read: the units are
-- counted either way (pending then, sold now), so it is left alone.
local recreated, kept, sold = 0, 0, 0
for _ = 1, nk do
  key = key + 1
  local user, qty, expires, member = ARGV[arg + 1], ARGV[arg + 2], ARGV[arg + 3], ARGV[arg + 4]
  arg = arg + 4
  local h = redis.call('HMGET', KEYS[key], 'user', 'state')
  if h[2] == 'SOLD' then
    sold = sold + 1
  else
    if h[1] == user and (h[2] == 'HELD' or h[2] == 'PAYING') then
      kept = kept + 1
    else
      recreated = recreated + 1
    end
    if not dry then
      redis.call('HSET', KEYS[key], 'user', user, 'qty', qty, 'state', 'PAYING', 'expires_at', expires)
      redis.call('HSETNX', KEYS[key], 'created_at', now)
      redis.call('HDEL', KEYS[key], 'released_by')
      redis.call('PERSIST', KEYS[key])
      redis.call('ZADD', KEYS[3], expires, member)
    end
  end
end

-- Every other open hold has no booking: its units are back in "available".
local dropped = 0
for _ = 1, nd do
  key, arg = key + 1, arg + 1
  local state = redis.call('HGET', KEYS[key], 'state')
  if state == 'HELD' or state == 'PAYING' then
    dropped = dropped + 1
    if not dry then
      redis.call('HSET', KEYS[key], 'state', 'RELEASED', 'released_by', 'REBUILD')
      redis.call('PEXPIRE', KEYS[key], 3600000) -- the same 1-hour tombstone as release.lua
    end
  end
  if not dry then redis.call('ZREM', KEYS[3], ARGV[arg]) end
end

if not dry then
  redis.call('HSET', KEYS[2], 'capacity', ARGV[2], 'per_user_limit', ARGV[3], 'frozen', '1')
  redis.call('SET', KEYS[1], ARGV[4])
end
return {before, was_frozen, users_changed, users_deleted, recreated, kept, sold, dropped}
