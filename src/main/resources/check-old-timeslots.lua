local now = ARGV[1]
local groups = ARGV[2]
local num_partitions = ARGV[3]

local time_delta = 21600 -- 6 hours

local log_list = {}

local function logit(msg)
  log_list[#log_list+1] = msg
end

local function split(s, delimiter)
    local result = {};
    for match in (s..delimiter):gmatch("(.-)"..delimiter) do
        table.insert(result, match);
    end
    return result;
end

local group_list = split(groups, "|")

for partition=0, tonumber(num_partitions) do -- partitions
    for g=1, #group_list do
        local timeslots = redis.call("keys", "downsampling|" .. partition .. "|" .. group_list[g] .. "|*")
        if #timeslots ~= 0 then
            for i=1, #timeslots do
                local ts_tokens = split(timeslots[i], "|")
                local epoch = ts_tokens[#ts_tokens]
                local epoch_plus_time_delta = tonumber(epoch) + time_delta

                if epoch_plus_time_delta < tonumber(now) then
                    logit("partition: " .. partition .. " group: " .. group_list[g] .. " epoch: " .. epoch)
                    redis.call("sadd", "pending|" .. partition .. "|" .. group_list[g], epoch)
                end
            end
        end
    end
end

local ret_val = "ok"

if #log_list ~= 0 then
    for i=1, #log_list do
        ret_val = ret_val .. " # " .. log_list[i]
    end
end
return ret_val
