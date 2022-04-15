local ts_list = {}

local function save_ts(ts)
  ts_list[#ts_list+1] = ts
end

local partition = ARGV[3]

local function is_ingesting(timeslot)
    local now = ARGV[1]
    local time_slot_width = ARGV[2]
    local ts_plus_width = tonumber(timeslot) + tonumber(time_slot_width)

    if ts_plus_width < tonumber(now) then
        if redis.call("get", "ingesting|" .. partition .. "|" .. timeslot) == "" then
                return "true"
            else
                return "false"
        end
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