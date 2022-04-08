local logtable = {}
local ts_list = {}

local function logit(msg)
  logtable[#logtable+1] = msg
end

local function save_ts(ts)
  ts_list[#ts_list+1] = ts
end

local timeslots = redis.call("smembers", "pending")
logit(#timeslots)

for i=1, #timeslots do
    local ts = timeslots[i]
    logit("ts: " .. ts)
    local ingest_key = "ingesting|" .. ts
    local val = redis.call("get", ingest_key)
    if val == "" then
        logit("val exists")
    else
        logit("val is nil")
        local remove_result = redis.call("srem", "pending", ts)
        logit(remove_result)
        logit("saving ts: " .. ts)
        save_ts(ts)
    end
end
return ts_list