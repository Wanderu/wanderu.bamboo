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

--[[ recover <ns> , <datetime> <requeue_seconds>

Find any Jobs that have been abandoned.
Clean extraneous worker entries.

Keys:
    ns: Namespace under which queue data exists.

Args:
    datetime: Current datetime (Unix seconds since epoch)
    requeue_seconds: Seconds until requeue.

for each worker in <ns>:WORKERS
    if the worker is no longer active
        for each job in <ns>:WORKERS:<worker_name>
            if job key exists
                fail job appropriately (schedule or fail)
        delete key <ns>:WORKERS:<worker_name>
        remove <worker_name> from <ns>:WORKERS
--]]

-- ########### PRE
local fname = "recover"
local sep = ":"

local log_warn = function (message)
    redis.log(redis.LOG_WARNING, "<" .. fname .. ">" .. " " .. message)
end

local log_notice = function (message)
    redis.log(redis.LOG_NOTICE, "<" .. fname .. ">" .. " " .. message)
end

local log_verbose = function (message)
    redis.log(redis.LOG_VERBOSE, "<" .. fname .. ">" .. " " .. message)
end

local is_error = function(result)
    return type(result) == 'table' and result.err
end

local add_key = function(tab, key)
    tab[key] = true
end

local tab_as_array = function(tab)
    local keys = {}
    local n = 0
    for k, v in pairs(tab) do
        n = n+1
        keys[n] = k
    end
    return keys
end
-- ########### END PRE

local ns = KEYS[1]
local dtutcnow = tonumber(ARGV[1])
if dtutcnow == nil then
    return redis.error_reply("INVALID_PARAMETER: datetime")
end
local requeue_seconds = tonumber(ARGV[2])
if requeue_seconds == nil then
    return redis.error_reply("INVALID_PARAMETER: requeue_seconds")
end

local dtreschedule = dtutcnow + requeue_seconds

local kworking   = ns .. sep .. "WORKING"   -- Jobs that have been consumed
local kworkers   = ns .. sep .. "WORKERS"   -- Worker IDs
local kfailed    = ns .. sep .. "FAILED"    -- Failed Queue
local kscheduled = ns .. sep .. "SCHEDULED" -- Scheduled Queue
local kmaxfailed = ns .. sep .. "MAXFAILED" -- Max number of failures allowed

local abandoned = {}
local kworker, kactive, worker, workers, jobid
workers = redis.call("SMEMBERS", kworkers)
for i = 1, #workers do
    worker = workers[i]
    kworker = kworkers .. sep .. worker
    kactive = kworker .. sep .. "ACTIVE"
    if redis.call("EXISTS", kactive) == 0 then
        log_notice("Found inactive worker: " .. worker .. " Reaping jobs.")
        -- gather the job ids that have been abandoned
        local jobstrs = ""
        for _, jobid in ipairs(redis.call("SMEMBERS", kworker)) do
            if jobstrs == "" then
                jobstrs = jobstrs .. jobid
            else
                jobstrs = jobstrs .. ", " .. jobid
            end
            add_key(abandoned, jobid)
        end
        log_notice("Found abandoned jobs: " .. jobstrs)
        -- remove the worker entry from the set
        redis.pcall("SREM", kworkers, worker)
        -- remove the worker set
        redis.pcall("DEL", kworker)
    end
end

abandoned = tab_as_array(abandoned) -- convert to array (int indices)
for i, jobid in ipairs(abandoned) do
    log_verbose("Recovering: " .. jobid)
    local kjob = ns .. sep .. "JOBS" .. sep .. jobid

    -- # NOTE: Same block of code as fail.lua #

    -- ######################
    -- Remove job from WORKING queue
    -- ######################
    redis.call("ZREM", kworking, jobid)

    -- ######################
    -- Increment failure count
    -- Set failure date
    -- ######################
    local failures = tonumber(redis.pcall("HGET", kjob, "failures"))
    if failures == nil then failures = 0 end
    redis.call("HMSET", kjob,
            "failures", tostring(failures+1),
            "failed", tostring(dtutcnow))

    -- ######################
    -- Either add to SCHEDULED or FAILED queues depending on MAX_FAILED
    -- ######################
    local maxfailed = tonumber(redis.pcall("GET", kmaxfailed))
    if maxfailed == nil or failures >= maxfailed then
        -- ######################
        -- Move to FAILED queue
        -- ######################
        redis.call("ZADD", kfailed, dtutcnow, jobid);
        redis.call("PUBLISH", kfailed, jobid)
        redis.call("HMSET", kjob, "state", "failed")
    else
        -- ######################
        -- Move to SCHEDULED queue, keep Job data
        -- ######################
        redis.call("ZADD", kscheduled, dtreschedule, jobid);
        redis.call("PUBLISH", kscheduled, jobid)
        redis.call("HMSET", kjob, "state", "scheduled")
    end

    -- # END: fail.lua

end

return abandoned
