local res = {}
local limit = redis.call('hmget', KEYS[1], 'ct', 'lt', 'dn', 'rt')

if limit[1] then
  res[1] = tonumber(limit[1]) - 1
  res[2] = tonumber(limit[2])
  res[3] = tonumber(limit[3]) or ARGV[2]
  res[4] = tonumber(limit[4])

  if res[1] >= -1 then
    redis.call('hincrby', KEYS[1], 'ct', -1)
  else
    res[1] = -1
  end
else
  local total = tonumber(ARGV[1])
  res[1] = total - 1
  res[2] = total
  res[3] = tonumber(ARGV[2])
  res[4] = tonumber(ARGV[3])
  redis.call('hmset', KEYS[1], 'ct', res[1], 'lt', res[2], 'dn', res[3], 'rt', res[4])
  redis.call('pexpire', KEYS[1], res[3])
end

return res
