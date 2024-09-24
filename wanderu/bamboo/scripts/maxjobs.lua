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

--[[ maxjobs <ns> , [<maxjobs>]

Set or Get the max number of simultaneous jobs.

Errors: INVALID_PARAMETER

--]]

local fname = "maxjobs"
local sep = ":"

local log_warn = function (message)
    redis.log(redis.LOG_WARNING, "<" .. fname .. ">" .. " " .. message)
end

local ns = KEYS[1]
local maxjobs = nil

local kmaxjobs = ns .. sep .. "MAXJOBS" -- Max number of failures allowed

-- ######################
-- If the maxjobs parameter was provided, set it.
-- ######################
if table.getn(ARGV) > 0 then
    maxjobs = ARGV[1]
    -- If the maxjobs paramter was provided, check that it is a number.
    if tonumber(maxjobs) == nil then
        log_warn("INVALID_PARAMETER - " .. tostring(maxjobs) ..
                 " is not an integer.")
        return redis.error_reply("INVALID_PARAMETER")
    end
    redis.call("SET", kmaxjobs, maxjobs)
end

-- ######################
-- Return the current number
-- ######################
maxjobs = tonumber(redis.call("GET", kmaxjobs))
if maxjobs == nil then
    return 0
end
return maxjobs
