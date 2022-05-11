local timeslot = ARGV[1]
local partition = ARGV[2]
local group = ARGV[3]
local value = ARGV[4]
local downsampling_key = "downsampling|" .. partition .. "|" .. group .. "|" .. timeslot
local pending_key = "pending|" .. partition .. "|" .. group

local log_list = {}

local function logit(msg)
  log_list[#log_list+1] = msg
end

local num_sets_remaining = redis.call("scard", downsampling_key)
logit("num_sets_remaining before : " .. num_sets_remaining)

redis.call("srem", downsampling_key, value)

num_sets_remaining = redis.call("scard", downsampling_key)
logit("num_sets_remaining after : " .. num_sets_remaining)

if num_sets_remaining == 0 then
    logit("removing timeslot from pending: " .. timeslot)
    redis.call("srem", pending_key, timeslot)
end

local ret_val = "ok"

if #log_list ~= 0 then
    for i=1, #log_list do
        ret_val = ret_val .. " # " .. log_list[i]
    end
end
return log_list