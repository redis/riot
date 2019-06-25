local sorted_set_key = KEYS[1]
local lease_expiration = ARGV[1]
local primary_key = ARGV[2]
 
local current_expiration = redis.call('ZSCORE', sorted_set_key, primary_key)
 
if not current_expiration or current_expiration < lease_expiration then
    -- Add PK key name to SSK with a SCORE of PK TTL
    redis.call('ZADD', sorted_set_key, lease_expiration, primary_key)
    -- Set SSK TTL to max SSK SCORE
    redis.call('EXPIREAT', sorted_set_key, redis.call('ZRANGE', sorted_set_key, -1, -1, 'WITHSCORES')[2])
end
 
-- Remove all expired SSK members
redis.call('ZREMRANGEBYSCORE', sorted_set_key, 0, redis.call('TIME')[1])
 