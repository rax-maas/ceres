local partition = ARGV[1]
local group = ARGV[2]
local timeslot = ARGV[3]
local limit = tonumber(ARGV[4]) + 1
local downsampling_key = "downsampling|" .. partition .. "|" .. group .. "|" .. timeslot

local set_list = {}

local function save_set_hash(set)
  set_list[#set_list+1] = set
end

local set_hashes = redis.call("smembers", downsampling_key)

for i=1, #set_hashes do
    if i < limit then
        save_set_hash(set_hashes[i])
    else
        break
    end
end

return set_list
