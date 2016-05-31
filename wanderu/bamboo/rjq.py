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

import logging
import redis
import redis.client
import sha
from types import StringTypes

from redis import StrictRedis, Redis
from redis.exceptions import RedisError
# ConnectionError happens when the database is unavailable.

from wanderu.bamboo.job import Job
from wanderu.bamboo.util import make_key, gen_worker_name, utcunixts
from wanderu.bamboo.io import read_lua_scripts
from wanderu.bamboo.config import (
                        RE_HASHSLOT, REDIS_CONN, QUEUE_NAMES,
                        NS_JOB, NS_QUEUED, NS_SCHEDULED, NS_SEP,
                        NS_FAILED, NS_WORKING, NS_WORKERS,
                        NS_ACTIVE, REQUEUE_TIMEOUT, WORKER_EXPIRATION)
from wanderu.bamboo.errors import (message_to_error,
                                   OperationError,
                                   AbnormalOperationError,
                                   NormalOperationError,
                                   UnknownJobId, InvalidQueue)
from wanderu.bamboo.python import getNamespaceViewForQueue

logger = logging.getLogger(__name__)

SCRIPT_NAMES = [
    'ack.lua',
    'cancel.lua',
    'close.lua',
    'clear.lua',
    'can_consume.lua',
    'consume.lua',
    'enqueue.lua',
    'fail.lua',
    'maxfailed.lua',
    'maxjobs.lua',
    'recover.lua',
    'test.lua',
]


class RedisJobQueueBase(object):

    def __init__(self, namespace, name=None, conn=None,
                 worker_expiration=WORKER_EXPIRATION,
                 requeue_timeout=REQUEUE_TIMEOUT):

        self.namespace = RE_HASHSLOT.match(namespace) and namespace \
                            or ("{%s}" % namespace)
        self.name = name or gen_worker_name()
        self.worker_expiration = worker_expiration
        self.requeue_timeout = requeue_timeout
        self.conn = None

        # Subclasses should implement these methods
        self._init_connection(conn)
        self._load_lua_scripts()

    def key(self, *args):
        "Helper to build redis keys given the instance's namespace."
        return make_key(NS_SEP, self.namespace, *args)


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
    RedisJobQueue(namespace, name="worker1", conn="localhost/0")
    """
    RedisScript = redis.client.Script

    def _init_connection(self, conn):
        self.conn = get_redis_connection(REDIS_CONN if conn is None else conn)
        self.conn.client_setname(self.name)  # unique name for this client

    def _load_lua_scripts(self):

        self.scripts = {
            name: self.RedisScript(self.conn, contents)
            for name, contents in read_lua_scripts(SCRIPT_NAMES).items()
        }

        if logger.isEnabledFor(logging.DEBUG):
            for name, script in self.scripts.items():
                script_sha = sha.sha(script.script).hexdigest()
                logger.debug("script loaded", extra={'sha': script_sha, 'scriptName': name})

    def call_script(self, name, keys, args):
        try:
            res = self.scripts[name](keys, args)
            return res
        except RedisError as err:
            converted_error = message_to_error("%s" % err)
            if isinstance(converted_error, AbnormalOperationError):
                logger.error("Error in %s: %s" % (name, err))
            raise converted_error

    def clear(self, queues=(NS_QUEUED, NS_SCHEDULED, NS_FAILED)):
        """
        """
        # NS_WORKING?
        keys = (self.namespace,)
        args = queues
        res = self.call_script("clear", keys, args)
        return res

    # def delete(self,
    #            queues=(NS_QUEUED, NS_SCHEDULED, NS_FAILED, NS_WORKING),
    #            keys=(NS_MAXJOBS, NS_MAXFAILED),
    #            ns_sets=(NS_WORKERS, NS_ACTIVE)):
    #     """Delete all known queue entries under this namespace.
    #     Warning: This removes this entire namespace including all
    #     jobs in it.

    #     TODO: Tests
    #     """
    #     logger.warn("Deleting entire namespace: %s", self.namespace)
    #     _k = self._key
    #     pipe = self.conn.pipeline()

    #     for queue in queues:
    #         job_ids = self.conn.zscan_iter(_k(queue))
    #         for jid, score in job_ids:
    #             pipe.delete(_k(NS_JOB, jid))
    #         pipe.delete(_k(queue))

    #     for key in keys:
    #         pipe.delete(_k(key))

    #     for namespace in ns_sets:
    #         for cid in self.conn.smembers(_k(namespace)):
    #             pipe.delete(_k(namespace, cid))
    #         pipe.delete(_k(namespace))

    #     pipe.execute()


    def peek(self, Q, count=None, withscores=False):
        """Use this function to retrieve Jobs via a queue iterator that
        retrieves one job at a time from the database. The iterator works with
        a queue that changes over time.
        """
        if Q not in QUEUE_NAMES:
            raise InvalidQueue("Invalid queue name: %s" % Q)

        for jid, score in self.conn.zscan_iter(self.key(Q), count=count):
            try:
                job = self.get(jid)
                yield (job, score) if withscores else job
            except UnknownJobId:
                continue

    queue_iter = peek  # backwards-compatibility

    def can_consume(self):
        """Returns True if there are jobs available to consume. False
        otherwise.
        """
        keys = (self.namespace,)
        args = (utcunixts(),)
        res = self.call_script("can_consume", keys, args)
        return res > 0

    def cancel(self, job):
        """
        job: Job object or Job ID string.
        """
        jobid = job if isinstance(job, StringTypes) else job.id
        keys = (self.namespace,)
        args = (jobid,)
        return self.call_script("cancel", keys, args)

    def count(self, queue):
        """Return the number of items in a given queue."""
        if queue not in QUEUE_NAMES:
            raise OperationError("Invalid queue name: %s" % queue)
        return self.conn.zcard(self.key(queue))

    # def subscribe_callback(self, callback):
    #     """Threaded queue event subscribe. `callback` will be called
    #     in the worker thread each time a message is received. The
    #     callback function should take 2 arguments, the job id and
    #     the name of the queue.

    #     Returns an object that should be closed (ob.close()) in order
    #     to unsubscribe and stop receiving events. It can be used with
    #     `contextlib.closing`.
    #     """

    #     keys_rev = {self.key(q): q for q in QUEUE_NAMES}

    #     def proxy_callback(msg):
    #         #         job id           name of queue
    #         callback(msg['data'], keys_rev[msg['channel']])

    #     ps = self.conn.pubsub()
    #     ps.subscribe(**{self.key(q): proxy_callback for q in QUEUE_NAMES})

    #     # run_in_thread already ignores subscribe messages
    #     psthread = ps.run_in_thread(callback)

    #     # with closing(rjq.subscribe_callback(cb)):
    #     #     pass

    #     class Closer(object):
    #         def close(self):
    #             psthread.stop()

    #     return Closer()

    def subscribe(self, timeout=0.1):
        """Returns a generator yielding messages for all queue events.

        The generator yields tuples of the form (job-id, queue).

        Note: The generator should be closed (gen.close()) in order to
        free the connection resouces used and unsubscribe from messages.
        """
        ps = self.conn.pubsub()
        # Subscribe to all queue messages
        ps.subscribe(*(self.key(q) for q in QUEUE_NAMES))

        keys_rev = {self.key(q): q for q in QUEUE_NAMES}

        def message_gen():
            try:
                while ps.subscribed:
                    msg = ps.get_message(ignore_subscribe_messages=False, timeout=timeout)
                    # msg = ps.handle_message(
                    #         ps.parse_response(block=block, timeout=timeout),
                    #         ignore_subscribe_messages=True)

                    if msg is None:
                        yield None
                        continue

                    if 'message' not in msg['type']:
                        continue

                    #       job id           name of queue
                    yield msg['data'], keys_rev[msg['channel']]

            # except GeneratorExit:
            finally:
                ps.unsubscribe()
                ps.close()

        return message_gen()

    def add(self, job):
        keys = (self.namespace,)
        # <queue> <priority> <jobid> <force> <key> <val> [<key> <val> ...]
        args = (NS_QUEUED, job.priority, job.id, "0") + job.as_string_tup()
        return self.call_script("enqueue", keys, args)

    enqueue = add

    def requeue(self, job):
        keys = (self.namespace,)
        # <queue> <priority> <jobid> <force> <key> <val> [<key> <val> ...]
        args = (NS_QUEUED, job.priority, job.id, "1") + job.as_string_tup()
        return self.call_script("enqueue", keys, args)

    def schedule(self, job, dt):
        """Enqueue a Job directly onto the SCHEDULED queue.

        dt: Int. Unix UTC timestamp. Schedule date.
        """
        keys = (self.namespace,)
        # <queue> <priority> <jobid> <force> <key> <val> [<key> <val> ...]
        args = (NS_SCHEDULED, dt, job.id, "0") + job.as_string_tup()
        return self.call_script("enqueue", keys, args)

    def reschedule(self, job, dt):
        """Reschedule an existing job no matter the parameters.

        dt: Int. Unix UTC timestamp. Schedule date.

        Raises UnknownJobId if the job does not exist or InWork if
        it has already been consumed and is in the working state.
        """
        keys = (self.namespace,)
        # <queue> <priority> <jobid> <force> <key> <val> [<key> <val> ...]
        args = (NS_SCHEDULED, dt, job.id, "1") + job.as_string_tup()
        return self.call_script("enqueue", keys, args)

    def get(self, job_id):
        """
        Return a Job instance for a given job_id by introspecting redis.
        This does *NOT* consume/reserve a job.
        Use this for introspecting job contents.

        Raises a UnknownJobId when the job_id is not found in the database.

        TODO: Return the scheduled date as well.
        """
        job_dict = self.conn.hgetall(self.key(NS_JOB, job_id))
        # hgetall returns an empty dict {} if the item was not found
        if len(job_dict) == 0:
            raise UnknownJobId("No job with job ID {jid} found."
                                .format(jid=job_id))

        return Job.from_dict(job_dict)

    def consume(self, job_id=None):
        # <ns>
        keys = (self.namespace,)
        # <client_name> <job_id> <datetime> <expires>
        args = (self.name,
                job_id or "",
                utcunixts(),
                self.worker_expiration)
        res = self.call_script("consume", keys, args)
        job = Job.from_string_list(res)
        return job

    def ack(self, job):
        # <ns>
        keys = (self.namespace,)
        # <jobid>
        args = (job.id,)
        res = self.call_script("ack", keys, args)
        return res

    def fail(self, job, requeue_seconds=None):
        # <ns>
        keys = (self.namespace,)
        # <jobid> <datetime> <requeue_seconds>
        if requeue_seconds is None:
            requeue_seconds = (REQUEUE_TIMEOUT * (job.failures**2))
        args = (job.id,
                utcunixts(),
                requeue_seconds)
        res = self.call_script("fail", keys, args)
        return res

    def recover(self, requeue_seconds=None):
        """
        This function fails each abandoned job individually and sets their
        scheduled requeue time to requeue_seconds. It is useful for the caller
        to reschedule jobs individually after recovering.
        """
        if requeue_seconds is None:
            # TODO: This could be set to 0 to immediately requeue items
            #       What's the strategy when we don't know why a worker
            #       failed? Requeue right away? or requeue later?
            requeue_seconds = REQUEUE_TIMEOUT
        # <ns>
        keys = (self.namespace,)
        # <datetime> <requeue_seconds>
        args = (utcunixts(),
                requeue_seconds)

        # list of job IDs that have been recovered
        recovered_jobs = self.call_script("recover", keys, args)
        return recovered_jobs

    def maxfailed(self, val=None):
        """Get or set maxfailed.
        val: int.
        Returns the value of MAXFAILED for this namespace.
        """
        # <ns>
        keys = (self.namespace,)
        # <val>
        if val is None:
            args = tuple()
        else:
            args = (val,)

        res = self.call_script("maxfailed", keys, args)
        return res

    def maxjobs(self, val=None):
        """Get or set maxjobs.
        val: int.
        Returns the value of MAXJOBS for this namespace.
        """
        # <ns>
        keys = (self.namespace,)
        # <val>
        if val is None:
            args = tuple()
        else:
            args = (val,)

        res = self.call_script("maxjobs", keys, args)
        return res

    def workers(self):
        """Returns the names of worker processes for this queue."""
        return self.conn.smembers(self.key(NS_WORKERS))

    def jobs_for_worker(self, worker_name):
        """Returns the Job IDs of the active jobs for a given worker.
        IE. Jobs a worker has `consume()`ed.
        """
        return self.conn.smembers(self.key(NS_WORKERS, worker_name))

    def active(self):
        """Returns the naes of workers that are currently processing jobs.
        IE. The worker has `consume()`ed at least 1 job.
        """
        workers = set()
        for worker in self.workers():
            if self.conn.exists(self.key(NS_WORKERS, worker, NS_ACTIVE)):
                workers.add(worker)
        return workers

def RedisJobQueueView(rjq, namespace):
    return getNamespaceViewForQueue(rjq, namespace)

# class RedisJobQueueView(RedisJobQueue):
#     __slots__ = ['rjq', 'namespace']

#     def __init__(self, rjq, namespace):
#         self.rjq = rjq
#         self.namespace = namespace

#     def __getattr__(self, attr):
#         """
#         __getattr__ handles attributes that are not found (member lookup fail)
#         """
#         return getattr(self.rjq, attr)
