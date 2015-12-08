import re

REDIS_CONN   = {'host': 'localhost', 'port': 6379, 'db': 0}

LUA_SCR_PKG  = 'wanderu.bamboo'
LUA_SCR_DIR  = 'scripts'
LUA_EXT      = '.lua'

NS_QUEUED    = "QUEUED"
NS_WORKING   = "WORKING"
NS_FAILED    = "FAILED"
NS_SCHEDULED = "SCHEDULED"
NS_JOB       = "JOB"
NS_MAXJOBS   = "MAXJOBS"
NS_MAXFAILED = "MAXFAILED"
NS_WORKERS   = "WORKERS"
NS_ACTIVE    = "ACTIVE"
NS_SEP       = ":"

DEFAULT_PRIORITY = 5  # [0, 10) (our choice of priority range)
REQUEUE_TIMEOUT = 60*60  # in seconds
JOB_TIMEOUT = 60*60  # in seconds

RE_HASHSLOT = re.compile(r".*?{.*?}.*?")
