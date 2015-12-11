# Copyright 2015 Wanderu, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

DEFAULT_PRIORITY = 5      # [0, 10) (our choice)
REQUEUE_TIMEOUT = 60*60   # seconds
JOB_TIMEOUT = 60*60       # seconds
WORKER_EXPIRATION = 5*60  # seconds

RE_HASHSLOT = re.compile(r".*?{.*?}.*?")
