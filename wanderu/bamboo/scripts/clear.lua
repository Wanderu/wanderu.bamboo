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

--[[ clear <ns> , [QUEUED] [WORKING] [SCHEDULED] [FAILED]

Clear a queue

Keys:
    ns: Namespace under which queue data exists.

Args:
    At least 1 of: QUEUED, WORKING, SCHEDULED, or FAILED

--]]

local fname = "cancel"
local sep = ":"
local ns = KEYS[1]

local log_verbose = function (message)
    redis.log(redis.LOG_VERBOSE, "<" .. fname .. ">" .. " " .. message)
end

local log_warn = function (message)
    redis.log(redis.LOG_WARNING, "<" .. fname .. ">" .. " " .. message)
end

local log_notice = function (message)
    redis.log(redis.LOG_NOTICE, "<" .. fname .. ">" .. " " .. message)
end

local qidx
local jobidx

-- log_verbose(cjson.encode(ARGV))

local CLEARABLE_QUEUES = {}
CLEARABLE_QUEUES["QUEUED"] = true
CLEARABLE_QUEUES["SCHEDULED"] = true
CLEARABLE_QUEUES["WORKING"] = true  -- can't pull jobs out from under the active workers
CLEARABLE_QUEUES["FAILED"] = true

-- validation
for qidx = 1, #ARGV do
    if CLEARABLE_QUEUES[ARGV[qidx]] ~= true then
        return redis.error_reply("INVALID_PARAMETER: " .. ARGV[qidx])
    end
end

-- for each queue
local number_of_jobs_removed = 0
for qidx = 1, #ARGV do
    local queue = ns .. sep .. ARGV[qidx]

    local job_ids = redis.call("ZRANGE", queue, 0, -1)

    for jobidx = 1, #job_ids do
        local job_id = job_ids[jobidx]
        -- exists returns 0 if not, 1 if so
        local job_key = ns .. sep .. "JOBS" .. sep .. job_id
        -- Remove job data
        log_verbose("deleting job key: " .. job_key)
        redis.call("DEL", job_key)
        number_of_jobs_removed = number_of_jobs_removed + 1
    end  -- for each job id

    redis.call("DEL", queue)
    log_notice("deleted " .. queue)
end  -- for each queue

return number_of_jobs_removed
