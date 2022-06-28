local partition = ARGV[1]
local pod_name = ARGV[2]
local now = ARGV[3]
local max_duration = ARGV[4]

local job_key = 'job|'.. partition

local function split(s, delimiter)
    local result = {};
    for match in (s..delimiter):gmatch("(.-)"..delimiter) do
        table.insert(result, match);
    end
    return result;
end

local function is_max_lock_time_exceeded(status)
    local token_list = split(status, '|')
    local timestamp = token_list[#token_list]
    local timestamp_plus_max_lock_time = tonumber(timestamp) + tonumber(max_duration)

    if timestamp_plus_max_lock_time < tonumber(now) then
        return "true"
    else
        return "false"
    end
end

local function claim_job()
    return redis.call('set', job_key, pod_name .. '|' .. now)
end

local job_status = redis.call('get', job_key)

if job_status == "free" then
    claim_job()
    return "free"
else
    if is_max_lock_time_exceeded(job_status) == "true" then
        claim_job()
        return "free"
    else
        return "processing"
    end
end
