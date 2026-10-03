-- soldout.lua (fenced): the leader marks an OPEN queue SOLD_OUT once
-- inventory has no units left and no hold that could return any. Joins are
-- refused from then on, and the status document says so on the next tick. A
-- frozen queue stays FROZEN: freezing is an operator's decision.
-- KEYS[1] adm:{E}:epoch   KEYS[2] q:{E}:state   ARGV[1] the caller's epoch
-- Returns  1 marked   0 not OPEN (already sold out, frozen, ...)   -1 fenced
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
if redis.call('GET', KEYS[2]) ~= 'OPEN' then return 0 end
redis.call('SET', KEYS[2], 'SOLD_OUT')
return 1
