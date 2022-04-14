-- local logtable = {}
local ts_list = {}

-- local function logit(msg)
--    logtable[#logtable+1] = msg
-- end

local function save_ts(ts)
  ts_list[#ts_list+1] = ts
end

local function is_ingesting(timeslot)
    local now = ARGV[1]
    local lastTouchDelay = ARGV[2]
--     logit('timeslot: ' .. timeslot)
--     logit('lastTouchDelay: ' .. lastTouchDelay)
    local ts_plus_delay = tonumber(timeslot) + tonumber(lastTouchDelay)
--     logit('ts_plus_delay: ' .. ts_plus_delay)
--     logit('now: ' .. now)

    if ts_plus_delay < tonumber(now) then
        return "false"
    else
        return "true"
    end
end

local timeslots = redis.call("smembers", "pending")

for i=1, #timeslots do
    local ts = timeslots[i]
    if is_ingesting(ts) == "false" then
        redis.call("srem", "pending", ts)
        save_ts(ts)
    end
end
return ts_list