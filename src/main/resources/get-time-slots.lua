local ts_list = {}

local function save_ts(ts)
  ts_list[#ts_list+1] = ts
end

local partition = ARGV[3]

local function is_ingesting(timeslot)
    local now = ARGV[1]
    local lastTouchDelay = ARGV[2]
    local ts_plus_delay = tonumber(timeslot) + tonumber(lastTouchDelay)

    if ts_plus_delay < tonumber(now) then
        return "false"
    else
        return "true"
    end
end

local timeslots = redis.call("smembers", "pending|" .. partition)

for i=1, #timeslots do
    local ts = timeslots[i]
    if is_ingesting(ts) == "false" then
        redis.call("srem", "pending|" .. partition, ts)
        save_ts(ts)
    end
end
return ts_list