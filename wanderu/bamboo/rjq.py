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

import logging
import calendar
import redis
import uuid
import socket
import os

from types import StringTypes
from itertools import chain, imap

from redis import StrictRedis, Redis
from redis.exceptions import RedisError, ResponseError, ConnectionError

from wanderu.bamboo.job import Job, utcunixts
from wanderu.bamboo.util import make_key
from wanderu.bamboo.io import read_lua_scripts
from wanderu.bamboo.config import (RE_HASHSLOT, REDIS_CONN,
                        NS_JOB, NS_QUEUED, NS_WORKING, NS_FAILED,
                        NS_MAXJOBS, NS_MAXFAILED, NS_SCHEDULED,
                        NS_WORKERS, NS_ACTIVE, NS_SEP,
                        REQUEUE_TIMEOUT, JOB_TIMEOUT,
                        WORKER_EXPIRATION)

logger = logging.getLogger(__name__)
error  = logger.error
warn   = logger.warn
info   = logger.info
debug  = logger.debug

class NotFoundError(KeyError):
    """Used when a specified Redis object is not found. IE. Job ID
    """
    pass


class OperationError(Exception):
    """Used when an operation fails or cannot be executed.
    """
    pass


def gen_random_name():
    return str(uuid.uuid1())  # or use uuid4


def gen_worker_name():
    try:
        return socket.gethostname() + "-" + str(os.getpid())
    except:
        return gen_random_name()


class RedisJobQueueBase(object):
    scripts = set()  # redis.Script objects
    # initialize as many classes as we want, but only load scripts once
    scripts_loaded = False

    @classmethod
    def _load_lua_scripts(cls, conn):
        if not cls.scripts_loaded:

            cls.scripts.clear()

            script_names = [
                'ack.lua'
                'cancel.lua'
                'close.lua'
                'consume.lua'
                'enqueue.lua'
                'fail.lua'
                'maxfailed.lua'
                'maxjobs.lua'
                'recover.lua'
                'test.lua'
            ]

            script_map = read_lua_scripts(script_names)

            # load scripts and/or templates and assign them as attribute names
            # to the class, prefixed by an underscore
            for name, contents in script_map.items():
                script = conn.register_script(contents)
                setattr(cls, "_{}".format(name), script)
                cls.scripts.add(script)

                # Additional info for runtime profiling using redis' SLOWLOG
                # Match slowlog entries to these log messages.
                script_sha = sha.sha(contents).hexdigest()
                info("script name: %s sha: %s",
                      name, script_sha,
                      extra={'sha': script_sha, 'script': name})

            # ^above^ is a generic way of saying:
            # cls._ack     = add_script('ack.lua')
            # cls._consume = add_script('consume.lua')

            cls.scripts_loaded = True


def get_redis_connection(conn):
    if isinstance(conn, (StrictRedis, Redis)):
        return conn
    if isinstance(conn, StringTypes):
        return redis.from_url(conn)
    if isinstance(conn, tuple):
        return StrictRedis(*conn)
    if isinstance(conn, dict):
        return StrictRedis(**conn)
    raise TypeError("Invalid conn parameter type.")


class RedisJobQueue(RedisJobQueueBase):
    """
    RedisJobQueue(name="worker1", namespace="MY:NS",
                  conn="localhost/0")
    """

    def __init__(self, namespace, name=None, conn=None,
                 worker_expiration=WORKER_EXPIRATION,
                 requeue_timeout=REQUEUE_TIMEOUT,
                 ):

        self.namespace = RE_HASHSLOT.match(namespace) and namespace \
                            or ("{%s}" % namespace)
        self.name = name or gen_worker_name()
        self.worker_expiration = worker_expiration
        self.requeue_timeout = requeue_timeout

        self.conn = get_redis_connection(REDIS_CONN if conn is None else conn)
        self.conn.client_setname(self.name)  # unique name for this client
        self._load_lua_scripts(self.conn)

    def key(self, *args):
        "Helper to build redis keys given the namespace."
        return make_key(NS_SEP, self.namespace, *args)

    def op(self, name, keys, args):
        try:
            res = getattr(self, "_"+name)(keys, args)
        except RedisError, err:
            error("Error in %s: %s", (name, err))
            raise OperationError("%s" % err)
        return res

    def queue_iter(self, Q, withscores=False):
        """Use this function to retrieve Jobs via a queue iterator that
        retrieves one job at a time from the database. The iterator works with
        a queue that changes over time.
        """
        for jid, score in self.conn.zscan_iter(self.key(Q)):
            try:
                job = self.get(jid)
                yield (job, score) if withscores else job
            except NotFoundError:
                continue

    def subscribe(self):
        pass

    def add(self, job):
        keys = (self.namespace,)
        # <queue> <priority> <jobid> <force> <key> <val> [<key> <val> ...]
        args = (NS_QUEUED, job.priority, job.id, "0") + job.as_string_tup()
        return self.op("enqueue", keys, args)

    def schedule(self, job, time):
        keys = (self.namespace,)
        # <queue> <priority> <jobid> <force> <key> <val> [<key> <val> ...]
        args = (NS_SCHEDULED, job.priority, job.id, "0") + job.as_string_tup()
        return self.op("enqueue", keys, args)

    def reschedule(self, job, time):
        keys = (self.namespace,)
        # <queue> <priority> <jobid> <force> <key> <val> [<key> <val> ...]
        args = (NS_SCHEDULED, job.priority, job.id, "1") + job.as_string_tup()
        return self.op("enqueue", keys, args)

    def get(self, job_id):
        """
        Return a Job instance for a given job_id by introspecting redis.
        This does *NOT* consume/reserve a job.
        Use this for introspecting job contents.

        Raises a NotFoundError when the job_id is not found in the database.
        """
        job_dict = self.conn.hgetall(self.key(NS_JOB, job_id))
        # hgetall returns an empty dict {} if the item was not found
        if len(job_dict) == 0:
            raise NotFoundError("No job with job ID {jid} found."
                                .format(jid=job_id))

        return Job.from_dict(job_dict)

    def consume(self, job_id=None):
        # <ns>
        keys = (self.namespace,)
        # <client_name> <datetime> <job_id> <expires>
        args = (self.name,
                utcunixts(),
                job_id or "",
                self.worker_expiration)
        res = self.op("consume", keys, args)
        job = Job.from_string_list(res)
        return job

    def ack(self, job):
        # <ns>
        keys = (self.namespace,)
        # <jobid>
        args = (job.id,)
        res = self.op("ack", keys, args)
        return res

    def fail(self, job, requeue_seconds=None):
        # <ns>
        keys = (self.namespace,)
        # <jobid> <datetime> <requeue_seconds>
        args = (job.id,
                utcunixts(),
                (requeue_seconds is None)
                    # 0, 1, 4, 9, 16, 25 ... hours
                    and (3600 * (job.failures**2))
                    or requeue_seconds
                )
        res = self.op("fail", keys, args)
        return res

    def recover(self, requeue_seconds=None):
        """
        This function fails each abandoned job individually and sets their
        scheduled requeue time to requeue_seconds. It is useful for the caller
        to reschedule jobs individually after recovering.
        """
        # <ns>
        keys = (self.namespace,)
        # <datetime> <requeue_seconds>
        args = (utcunixts(),
                requeue_seconds is None and 3600 or requeue_seconds)

        # list of job IDs that have been recovered
        recovered_jobs = self.op("recover", keys, args)
        return recovered_jobs

    def maxfailed(self, val=None):
        """Get or set maxfailed.
        val: int.
        Returns the value of MAXFAILED for this namespace.
        """
        # <ns>
        keys = (self.namespace,)
        # <val>
        args = (val or "",)
        res = self.op("maxfailed", keys, args)
        return res

    def maxjobs(self, val=None):
        """Get or set maxjobs.
        val: int.
        Returns the value of MAXJOBS for this namespace.
        """
        # <ns>
        keys = (self.namespace,)
        # <val>
        args = (val or "",)
        res = self.op("maxjobs", keys, args)
        return res
