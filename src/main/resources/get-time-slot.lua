local now = ARGV[1]
local time_slot_width = ARGV[2]
local partition = ARGV[3]
local group = ARGV[4]
local pending_key = "pending|" .. partition .. "|" .. group
local ingesting_prefix = "ingesting|" .. partition .. "|" .. group

local ts_list = {}

local function save_ts(ts)
  ts_list[#ts_list+1] = ts
end

local function is_ingesting(timeslot)
    local ts_plus_width = tonumber(timeslot) + tonumber(time_slot_width)

    if ts_plus_width < tonumber(now) then
        if redis.call("get", ingesting_prefix .. "|" .. timeslot) == "" then
                return "true"
            else
                return "false"
        end
    else
        return "true"
    end
end

local timeslots = redis.call("smembers", pending_key)
local timeslot = ""

for i=1, #timeslots do
    local ts = timeslots[i]
    if is_ingesting(ts) == "false" then
        redis.call("srem", pending_key, ts)
        timeslot = ts
         -- We just handle one time slot at a time to give someone else a chance to work
        break
    end
end
return timeslot
