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

consume <ns> , <client_name> <jobid> <datetime> <expires>

Keys:
    ns: Namespace under which queue data exists.
Args:
    client_name: The name/id of the worker.
    jobid: The Job ID to consume. Optional. Defaults to "". If not specified
            the next highest priority job is consumed.
    datetime: Current datetime (Unix seconds since epoch)
    expires: Seconds until Job expires and is candidate to go back on the queue.
            If "", defaults to 60 seconds.

Returns: Job data
Errors: MAXJOBS_REACHED, INVALID_CLIENT_NAME, NO_ITEMS, UNKNOWN_JOB_ID

-- ]]

local fname = "consume"
local sep = ":"

local log_notice = function (message)
    redis.log(redis.LOG_NOTICE, "<" .. fname .. ">" .. " " .. message)
end

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
local kqueued    = ns .. sep .. "QUEUED"    -- Primary Job queue
local kscheduled = ns .. sep .. "SCHEDULED" -- Scheduled Job queue
local kworking   = ns .. sep .. "WORKING"   -- Jobs that have been consumed
local kmaxjobs   = ns .. sep .. "MAXJOBS"   -- Max number of jobs allowed
local kworkers   = ns .. sep .. "WORKERS"   -- Worker IDs

local client_name = ARGV[1]
local jobid       = ARGV[2]
local score

local dtutcnow = tonumber(ARGV[3])
if dtutcnow == nil then
    return redis.error_reply("INVALID_PARAMETER: datetime")
end

local expires = tonumber(ARGV[4])
if expires == "" or expires == nil then expires = 60 end

local max_jobs = tonumber(redis.pcall("GET", kmaxjobs))
local njobs = tonumber(redis.pcall("ZCARD", kworking))
local result

-- ######################
-- Don't consume more than the max number of jobs
-- ######################
if njobs and max_jobs and (njobs >= max_jobs) then
    return redis.error_reply("MAXJOBS_REACHED")
end

-- ######################
-- The client name must be non-blank.
-- Blank indicates initial job state with no owner.
-- ######################
if client_name == "" then
    log_warn("Client name not specified.")
    return redis.error_reply("INVALID_CLIENT_NAME")
end

-- ######################
-- If there is an item ready to be moved to QUEUED from SCHEDULED, 
-- then move it. (Cascade built into consume).
-- ######################
local qitem = redis.pcall("ZRANGE", kscheduled, 0, 0, 'withscores')
-- ZRANGE always returns a table. empty {} if queue does not exist.
if #qitem > 0 then
    local _jobid = qitem[1]
    local _score = tonumber(qitem[2])
    if _score <= dtutcnow then
        local _kjob = ns .. sep .. "JOBS" .. sep .. _jobid
        local _priority = tonumber(redis.call("HGET", _kjob, "priority"))
        if _priority ~= nil then
            redis.call("ZREM", kscheduled, _jobid)
            redis.call("ZADD", kqueued, _priority, _jobid)
            redis.call("PUBLISH", kqueued, _jobid)
            redis.call("HMSET", _kjob, "state", "enqueued")
            log_verbose("Moved " .. _jobid .. " from " .. kscheduled .. " to " .. kqueued)
        else
            log_warn("Could not retrieve priority from job object: " .. _kjob)
        end
    end
end

-- ######################
-- If no jobid specified, then take the first item in the queue.
-- If a jobid was specified, find that job in the queue.
-- ######################
if jobid == "" then
    -- peek at 1 item from the queue
    result = redis.call("ZRANGE", kqueued, 0, 0, 'withscores')

    if table.getn(result) == 0 then
        log_verbose("No items on the queue: " .. kqueued)
        return redis.error_reply("NO_ITEMS")
    end

    -- res is a table of length 2 with the item in the queue and the
    -- score/priority
    jobid = result[1]
    score = tonumber(result[2])
else
    result = redis.pcall("ZSCORE", kqueued, jobid)
    if is_error(result) then
        log_error(fname, "Cannot obtain score for job: " .. jobid ..
                "from zset: " .. kqueued);
        return redis.error_reply("UNKNOWN_JOB_ID")
    end
    score = tonumber(result)
end

-- We have a jobid by now. Make it's redis key.
local kjob = ns .. sep .. "JOBS" .. sep .. jobid

-- ######################
-- Pop Job from Main Queue
-- ######################
local nremoved = redis.call("ZREM", kqueued, jobid)

-- ######################
-- Add Job to Working Set.
-- NOTE: The score of the WORKING ZSET is the date it was consumed.
-- ######################
redis.call("ZADD", kworking, dtutcnow, jobid)
-- Publish on a channel who's name is the same as the queue key.
redis.call("PUBLISH", kworking, jobid)

-- ######################
-- Update Job state
-- ######################
redis.call("HMSET", kjob,
           "owner", client_name,
           "state", "working",
           "consumed", dtutcnow);

-- ######################
-- Register this worker as a current worker and register the jobid with this
-- worker.  Expire the worker after a certain amount of time.
-- ######################
redis.call("SADD", kworkers, client_name) -- Set of worker names

local kworker = kworkers .. sep .. client_name
redis.call("SADD", kworker, jobid) -- Set of jobs that a worker has

-- Make a key for the worker that expires.
local kworker_active = kworkers .. sep .. client_name .. sep .. "ACTIVE"
redis.call("SET", kworker_active, 1) -- Key to signify that the worker is active.
redis.call("EXPIRE", kworker_active, expires)

-- NOTE: Expired jobs are those jobs in the working queue (and/or whose `state`
-- is set to "working") and has an owner (worker) that has expired.  A worker
-- whose kworker_active key has expired is considered no longer active. Any
-- outstanding Jobs belonging to that worker should be reaped and re-queued.

-- ######################
-- Return the Job data
-- ######################
return redis.call('HGETALL', kjob)
