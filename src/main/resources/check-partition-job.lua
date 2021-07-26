--lua
local old_time=redis.call('get', KEYS[1])
local now=ARGV[1]
local new_time=ARGV[2]

if old_time <= now then
    redis.call('set', KEYS[1], new_time)
    return true
else
    return false
end
