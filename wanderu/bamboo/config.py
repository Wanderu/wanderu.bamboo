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

# Py 3 Compatibility
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import re

JOB_STATE_ENQUEUED  = "enqueued"    # available for work on a queue
                                    # (even if already failed)
JOB_STATE_WORKING   = "working"     # consumed/reserved by a worker
JOB_STATE_SCHEDULED = "scheduled"   # on the scheduled queue
JOB_STATE_FAILED    = "failed"      # not enqueued any more (failed out)
JOB_STATE_PROCESSED = "processed"   # acked successfully, still hanging
                                    # around somewhere

JOB_STATES = [JOB_STATE_ENQUEUED, JOB_STATE_WORKING,
              JOB_STATE_SCHEDULED, JOB_STATE_PROCESSED,
              JOB_STATE_FAILED]

REDIS_CONN   = {'host': 'localhost', 'port': 6379, 'db': 0}

LUA_SCR_PKG  = 'wanderu.bamboo'
LUA_SCR_DIR  = 'scripts'
LUA_EXT      = '.lua'

NS_QUEUED    = "QUEUED"
NS_WORKING   = "WORKING"
NS_FAILED    = "FAILED"
NS_SCHEDULED = "SCHEDULED"
NS_JOB       = "JOBS"
NS_MAXJOBS   = "MAXJOBS"
NS_MAXFAILED = "MAXFAILED"
NS_WORKERS   = "WORKERS"
NS_ACTIVE    = "ACTIVE"
NS_SEP       = ":"

QUEUE_NAMES  = {NS_QUEUED, NS_WORKING, NS_FAILED, NS_SCHEDULED}

DEFAULT_EXPIRY = None

DEFAULT_PRIORITY = 5      # [0, 10) (our choice)
REQUEUE_TIMEOUT = 60*60   # seconds
JOB_TIMEOUT = 60*60       # seconds
WORKER_EXPIRATION = 5*60  # seconds

# Is there a balanced pair of brackets {} in the string?
# If so, then that's is what is hashed to determine the slot in redis cluster.
RE_HASHSLOT = re.compile(r".*?{.*?}.*?")
