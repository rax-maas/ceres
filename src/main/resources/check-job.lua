local log_list = {}

local function logit(msg)
  log_list[#log_list+1] = msg
end

local partition = ARGV[1]
local pod_name = ARGV[2]

local is_free = redis.call('get', 'job|' .. partition)

if is_free == "free" then
    redis.call('set', 'job|'.. partition, pod_name)
    return "free"
else
    return "processing"
end
