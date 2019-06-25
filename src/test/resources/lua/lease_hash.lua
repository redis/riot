local primary_key = KEYS[1]
local lease_blob = ARGV[1]
local transaction_time = ARGV[2]
local lease_expiration = ARGV[3]
 
-- Check timestamps to determine what should be updated, and what is already up to date.
local previous_expiration_time = redis.call('HGET', primary_key, 'transaction_time')
local oldest_known_transaction_time = redis.call('HGET', primary_key, 'oldest_known_transaction_time')
 
if not previous_expiration_time or previous_expiration_time < transaction_time then
    -- Save lease data
    redis.call('HSET', primary_key, 'lease', lease_blob)
    redis.call('HSET', primary_key, 'transaction_time', transaction_time)
    redis.call('EXPIREAT', primary_key, lease_expiration)
end
 
-- The creation time is used by IPv4 leases, since the lease data does not contain a creation time.
-- IPv6 Leases use the creation time in the lease data blob, and ignore this.
-- We assume that the oldest transaction ever processed  since the lease was stored
-- is the most accurate creation time.
if not oldest_known_transaction_time or oldest_known_transaction_time > transaction_time then
    redis.call('HSET', primary_key, 'oldest_known_transaction_time', transaction_time)
end