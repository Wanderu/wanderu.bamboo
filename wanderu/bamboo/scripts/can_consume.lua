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

can_consume <ns> , <datetime>

Keys:
    ns: Namespace under which queue data exists.
Args:
    datetime: Current datetime (Unix seconds since epoch)

Returns: 0 if not 1 if so.

Errors: INVALID_PARAMETER
-- ]]

local fname = "can_consume"
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

local dtutcnow = tonumber(ARGV[1])
log_verbose("dtutcnow: " .. tostring(dtutcnow))
if dtutcnow == nil then
    log_warn("INVALID_PARAMETER: datetime -> " .. ARGV[1])
    return redis.error_reply("INVALID_PARAMETER: datetime")
end

local njobs = tonumber(redis.pcall("ZCARD", kqueued))
log_verbose("queue " .. kqueued .. " has njobs " .. tostring(njobs))

if njobs ~= nil and njobs > 0 then
    return 1
end

local qitem = redis.pcall("ZRANGE", kscheduled, 0, 0, 'withscores')

if #qitem > 0 then
    local _jobid = qitem[1]
    local _score = tonumber(qitem[2])
    if _score <= dtutcnow then
        return 1
    end
end

return 0
