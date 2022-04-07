local logtable = {}
local ts_list = {}

local function logit(msg)
  logtable[#logtable+1] = msg
end

local function save_ts(ts)
  ts_list[#ts_list+1] = ts
end

logit("hej")

local timeslots = redis.call("smembers", "pending")
for i=1, #timeslots do
    local ts = timeslots[i]
    logit(ts)
    local ingest_key = "ingesting|" .. ts
    logit(ingest_key)
    local val = redis.call("get", ingest_key)
    logit(val)
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

logit("hopp")

return ts_list