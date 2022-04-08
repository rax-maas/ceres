local logtable = {}

local function logit(msg)
  logtable[#logtable+1] = msg
end

local timeslots = redis.call("smembers", "pending")
logit(#timeslots)

for i=1, #timeslots do
    local ts = timeslots[i]
    local ingest_key = "ingesting|" .. ts
    redis.call("del", ingest_key)
end

local res = redis.call("keys", "*")
logit(res)

return logtable