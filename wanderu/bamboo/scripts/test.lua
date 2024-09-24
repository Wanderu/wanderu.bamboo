-- return redis.error_reply("MAXJOBS")
-- return redis.status_reply("<test> status")
-- return {0}
-- return {"error", "custom-error-object"}
-- redis.call("ZADD", "T:QUEUE", 1, "item")
-- return redis.call("ZSCORE", "T:QUEUE", "item2")
-- local time = redis.call("TIME")
-- return tonumber(time[1])
return redis.call("PUBLISH", "TEST:QUEUED", "dude")

-- local tab = {"a","A"}
-- return tab

-- redis.call("HMSET", "TEST:HMAP", "a", "A", "b", "B")
-- local res = redis.call("HGETALL", "TEST:HMAP")
-- redis.log(redis.LOG_NOTICE, type(res))
-- return type(res)

