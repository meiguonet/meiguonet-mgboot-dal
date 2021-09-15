local key1 = "RateLimiter@" .. KEYS[1]
local key2 = "RateLimiter@ExpiredAt@" .. KEYS[1]
local ts1 = ARGV[1]
local expiredAt = ARGV[2]
local keyExists = redis.call('exists', key1)

if keyExists == true then
    local ts2 = redis.call('get', key2)

    if tonumber(ts1) >= tonumber(ts2) then
        redis.call('set', key1, '0')
        redis.call('set', key2, expiredAt)
    end
else
    redis.call('set', key1, '0')
    redis.call('set', key2, expiredAt)
end

return redis.call(key1, 'incr')