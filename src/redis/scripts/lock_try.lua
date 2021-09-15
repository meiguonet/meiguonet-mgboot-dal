local key = KEYS[1]
local content = ARGV[1]
local ttl = tonumber(ARGV[2])
local result = redis.call('setex', key, content)

if result == 1 then
    redis.call('pexpire', key, ttl)
else
    local value = redis.call('get', key)

    if value == content then
        result = 1
        redis.call('pexpire', key, ttl)
    end
end

return result