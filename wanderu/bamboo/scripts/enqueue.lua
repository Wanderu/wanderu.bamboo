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

--[[ enqueue

Enqueue the Job onto the specified queue; either QUEUED or SCHEDULED.
By default, this script only allows requeuing the job in a way that
makes it available to run sooner. IE. SCHEDULED -> QUEUED,
SCHEDULED -> SCHEDULED with earlier date, or QUEUED -> QUEUED with
lower score (higher priority). Use the 'force' argument to have
full requeue behavior.

Keys: <ns>
ns: Namespace under which queue data exists.
Args: <queue> <priority> <jobid> <force> <key> <val> [<key> <val> ...]
        key, val pairs are values representing a Job object.
        queue: String. The queue name. One of "QUEUED" or "SCHEDULED"
        priority: Int. Lower means higher priority. Should be a unix utc
                timestamp if scheduling onto the SCHEDULED queue.
        jobid: String. Unique ID of the job.
        force: If "1", and the job already exists then acts as a requeue
            function.

Returns: 1 if added.
Errors: INVALID_PARAMETER, JOB_IN_WORK, JOB_EXISTS

-- ]]

local fname = "enqueue"
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

local ns = KEYS[1]

local queue = ARGV[1]
local priority = tonumber(ARGV[2])
local jobid = ARGV[3]
local force = ARGV[4] == "1"

local kqueue     = ns .. sep .. queue
local kqueued    = ns .. sep .. "QUEUED"
local kscheduled = ns .. sep .. "SCHEDULED"
local kworking   = ns .. sep .. "WORKING"
local kfailed    = ns .. sep .. "FAILED"
local kjob       = ns .. sep .. "JOBS" .. sep .. jobid

local msg, result;

-- ######################
-- Validation. Only support enqueuing to QUEUED or SCHEDULED.
if (kqueue ~= kqueued and kqueue ~= kscheduled) then
    return redis.error_reply("INVALID_PARAMETER: Cannot enqueue to queue: " .. kqueue)
end
-- ######################

-- ######################
-- Make a table of all Job object parameters
-- ######################
local job_data = {}
for i = 5, tonumber(table.getn(ARGV)) do
    table.insert(job_data, ARGV[i])
end

local n = table.getn(ARGV) - 2

if (n % 2 == 1) then
    msg = "Invalid number of job object parameters: " .. tostring(n)
    log_warn(msg)
    return redis.error_reply("INVALID_PARAMETER")
end

local exists = tonumber(redis.call("EXISTS", kjob))

-- ######################
-- Make/Update the Job object (hash map).
-- ######################
redis.call("HMSET", kjob, unpack(job_data))

-- ######################
-- If this job already existed, then we are updating it's queue status
-- ######################
if exists == 1 then
    -- ######################
    -- Check to see if the job is in the WORKING queue.
    -- If it is, quit with error.
    -- ######################
    if tonumber(redis.call("ZSCORE", kworking, jobid)) ~= nil then
        log_warn("Job exists in WORKING queue: " .. kworking .. ". Cannot remove it. Job ID: " .. jobid)
        return redis.error_reply("JOB_IN_WORK")
    end

    -- ######################
    -- Check to see if the job is in the FAILED queue.
    -- If it is, move it to the QUEUED queue.
    -- ######################
    if tonumber(redis.call("ZSCORE", kfailed, jobid)) ~= nil then
        log_notice("Item already exists in FAILED queue: " .. kfailed .. ". " .. (force and "Replacing" or "Removing") .. " it. Job ID: " .. jobid)
        redis.call("ZREM", kfailed, jobid)
    end

    -- ######################
    -- Check to see if the job is in the SCHEDULED queue.
    -- Only requeue if the reschedule date is earlier (lesser score).
    -- ######################
    local current_score = tonumber(redis.call("ZSCORE", kscheduled, jobid))
    if current_score ~= nil then
        if (kqueue == kscheduled) and (priority >= current_score) and (force == false) then
            log_warn("Not enqueing item. An existing item has the same or lesser SCHEDULED score.")
            return redis.error_reply("JOB_EXISTS: Already a member of " .. kscheduled)
        end
        log_notice("Item already exists in SCHEDULED queue: " .. kscheduled .. ". " .. (force and "Replacing" or "Removing") .. " it. Job ID: " .. jobid)
        redis.call("ZREM", kscheduled, jobid)
    end

    -- ######################
    -- Check to see if the job is in the QUEUED queue.
    -- Don't allow requeuing from the QUEUED queue onto other queues.
    -- Only requeue if the priority is higher (lesser score).
    -- ######################
    current_score = tonumber(redis.call("ZSCORE", kqueued, jobid))
    if current_score ~= nil then
        if kqueue ~= kqueued and force == false then
            log_warn("Not enqueing item. Cannot requeue onto a lower priority queue.")
            return redis.error_reply("JOB_EXISTS: Already a member of " .. kqueued)
        end
        if kqueue == kqueued and priority >= current_score and force == false then
            log_warn("Not enqueing item. An existing item has the same or lesser QUEUED score.")
            return redis.error_reply("JOB_EXISTS: Already a member of " .. kqueued)
        end
        log_notice("Item already exists in QUEUED queue: " .. kqueue .. ". " .. (force and "Replacing" or "Removing") .. " it. Job ID: " .. jobid)
        redis.call("ZREM", kqueued, jobid)
    end
end

-- ######################
-- Update the priority and state with the current applicable values.
-- ######################
if kqueue == kqueued then
    redis.call("HMSET", kjob, "priority", priority, "state", "enqueued")
end

if kqueue == kscheduled then
    redis.call("HMSET", kjob, "state", "scheduled")
end

-- ######################
-- Add Job ID to queue
-- ######################
redis.call("ZADD", kqueue, priority, jobid)
-- Publish on a channel who's name is the same as the queue key.
redis.call("PUBLISH", kqueue, jobid)

return 1
