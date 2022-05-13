local timeslot = ARGV[1]
local partition = ARGV[2]
local group = ARGV[3]
local value = ARGV[4]
local downsampling_key = "downsampling|" .. partition .. "|" .. group .. "|" .. timeslot
local pending_key = "pending|" .. partition .. "|" .. group

local num_sets_remaining = redis.call("scard", downsampling_key)

redis.call("srem", downsampling_key, value)
num_sets_remaining = redis.call("scard", downsampling_key)

if num_sets_remaining == 0 then
    redis.call("srem", pending_key, timeslot)
end

return "ok"