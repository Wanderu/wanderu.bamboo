--[[

Copyright 2015 Wanderu, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

--]]

--[[

ack <ns>, <jobid>

ns: Namespace under which queue data exists.
jobid: Job identifier.

Error return strings:

    UNKNOWN_JOB_ID

--]]

local fname = "ack"
local sep = ":"
local result

local log_warn = function (message)
    redis.log(redis.LOG_WARNING, "<" .. fname .. ">" .. " " .. message)
end

local log_verbose = function (message)
    redis.log(redis.LOG_VERBOSE, "<" .. fname .. ">" .. " " .. message)
end

local is_error = function(result)
    return type(result) == 'table' and result.err
end

local ns = KEYS[1]
local jobid = ARGV[1]

local kworking = ns .. sep .. "WORKING"  -- Jobs that have been consumed
local kworkers = ns .. sep .. "WORKERS"  -- Worker IDs

result = redis.pcall("ZSCORE", kworking, jobid)
if result == nil or is_error(result) then
    log_warn("Provided job not found. Job ID: " .. jobid .. "Queue: " .. kworking)
    return redis.error_reply("UNKNOWN_JOB_ID" .. " Job not found in queue." .. kworking)
end

-- {MY:NS}:JOBS:jobid
local kjob = ns .. sep .. "JOBS" .. sep .. jobid

-- ######################
-- Remove job from working queue
-- ######################
redis.call("ZREM", kworking, jobid)

-- ######################
-- Remove job from worker's set
-- ######################
local client_name = redis.call("HGET", kjob, "owner")
if client_name == "" or client_name == false then
    log_warn("No worker registered/found for job.")
else
    -- TODO: Cluster support with hash tag {}
    local kworker = kworkers .. sep .. client_name
    result = redis.pcall("SREM", kworker, jobid)
    -- If there are no more outstanding jobs, remove the worker from the set of
    -- workers.
    local njobs = redis.call("SCARD", kworker)
    if njobs <= 0 then
        redis.call("SREM", ns, client_name)
    end
end

-- ######################
-- Remove Job Data
-- ######################
result = redis.pcall("DEL", kjob);

return 1
