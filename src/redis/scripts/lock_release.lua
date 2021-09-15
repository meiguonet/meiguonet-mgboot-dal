local key = KEYS[1]
local content = ARGV[1]
local value = redis.call('get', key)

if value == content then
    return redis.call('del', key)
else
    return 0
end