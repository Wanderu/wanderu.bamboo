# Py 3 Compatibility
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import logging

import txredisapi as redis
from twisted.internet import defer
from twisted.python.failure import Failure

from wanderu.bamboo.job import Job
from wanderu.bamboo.txscript import Script
from wanderu.bamboo.rjq import SCRIPT_NAMES, RedisJobQueue
from wanderu.bamboo.io import read_lua_scripts
from wanderu.bamboo.config import NS_JOB, QUEUE_NAMES, NS_QUEUED
from wanderu.bamboo.errors import (message_to_error,
                                   OperationError,
                                   AbnormalOperationError,
                                   NormalOperationError,
                                   UnknownJobId)
from wanderu.bamboo.util import utcunixts
from wanderu.bamboo.txred import (makeConnection, makeSubscriber,
                                  zscan_items, JobScanner)

logger = logging.getLogger(__name__)
error  = logger.error
warn   = logger.warn
info   = logger.info
debug  = logger.debug

class TxRedisJobQueue(RedisJobQueue):

    def _init_connection(self, url):
        """conn: String. Redis connection URL string."""
        self.url = url
        self.conn = makeConnection(url or "", self.name)

    def _load_lua_scripts(self):
        self.scripts = {
            name: Script(self.conn, script)
            for (name, script) in read_lua_scripts(SCRIPT_NAMES).items()
        }
        # backwards compatibility
        # for name, script in self.scripts.items():
        #     setattr(self, "_{}".format(name), script.eval)

    def _op_error(self, failure, name):
        if failure.check(redis.ResponseError):
            # error translation
            converted_error = message_to_error(failure.getErrorMessage())
            if isinstance(converted_error, AbnormalOperationError):
                error("Error in %s: %s" % (name, failure))
            return Failure(converted_error)
        return failure

    def op(self, name, keys, args):
        # call the script, returns a deferred
        d = self.scripts[name].eval(keys, args)
        d.addErrback(self._op_error, name)
        return d

    def can_consume(self):
        """Returns a deferred that is called back with True if there are jobs
        available to consume and False otherwise.
        """
        keys = (self.namespace,)
        args = (utcunixts(),)
        d = self.op("can_consume", keys, args)
        d.addCallback(lambda res: res > 0)
        return d

    def count(self, queue):
        # Wrap the existing method in a deferred due to how it raises
        # exceptions.
        d = defer.succeed(queue)
        d.addCallback(super(TxRedisJobQueue, self).count)
        return d

    def peek(self, cb, Q=NS_QUEUED, count=None):
        """Returns a deferred that is called back (finishes) after all items
        have been exhausted.

        cb: Function. Takes 1 parameter, a job object. It is called
        for each returned job object in the queue, in priority order.
        Q: String. Optional. The base name of the queue. Default: "QUEUED".
        count: Int. Optional. The maximum number of jobs to return.
        """
        if Q not in QUEUE_NAMES:
            return defer.fail(AbnormalOperationError("Invalid queue name: %s" % Q))

        scanner = JobScanner(self)
        scanner.receivedJob = cb
        d = scanner.scan(Q, count=count)
        return d

    @defer.inlineCallbacks
    def queue_iter(self, Q, count=None):
        """Returns a deferred that is called back with an iterator. The
        iterator yields deferreds that result in Job objects.

        IE.
        >> q_d = rjq.queue_iter(QUEUED, 5)
        >> def printJob(job):
              print job
        >> def processJobs(jobs):
              for job_d in jobs:
                job_d.addCallback(printJob)
        >> q_d.addCallback(processJobs)

        Even better:
        >> @defer.inlineCallbacks
           def printJobs(rjq):
              it = yield rjq.queue_iter(QUEUED, 5)
              for d in it:
                  job = yield d
                  print job
        """
        items = yield zscan_items(self.conn, self.key(Q),
                                 match=None, count=count)
        defer.returnValue((self.get(jobid) for jobid, score in items))

    def subscribe(self, callback):
        """
        Returns the connection instance after subscribing to queue events.

        Use conn.unsubscribe("") to no longer received messsages.
        """
        conn = makeSubscriber(self.url, self.name, callback)
        d = conn.subscribe([self.key(q) for q in QUEUE_NAMES])
        d.addCallback(lambda *a: conn)
        return d

    # add(self, job)  # Alias: enqueue
    # requeue(self, job, priority)
    # schedule(self, job, dt)
    # reschedule(self, job, dt)

    @staticmethod
    def _get_cb(job_dict, job_id):
        if len(job_dict) == 0:
            raise UnknownJobId("No job with job ID {jid} found."
                                .format(jid=job_id))
        return Job.from_dict(job_dict)

    def get(self, job_id):
        """Returns a deferred that is called with a Job object representing the
        given `job_id`.
        """
        return self.conn.hgetall(self.key(NS_JOB, job_id)) \
                   .addCallback(self._get_cb, job_id)

    def consume(self, job_id=None):
        # <ns>
        keys = (self.namespace,)
        # <client_name> <job_id> <datetime> <expires>
        args = (self.name,
                job_id or "",
                utcunixts(),
                self.worker_expiration)
        return self.op("consume", keys, args) \
                   .addCallback(lambda res: Job.from_string_list(res))

    # ack(self, job)
    # fail(self, job)
    # recover(self, requeue_seconds=None)
    # maxfailed(self, val=None):
    # maxjobs(self, val=None):

class TxRedisJobQueueView(TxRedisJobQueue):
    __slots__ = ['rjq', 'namespace']

    def __init__(self, rjq, namespace):
        self.rjq = rjq
        self.namespace = namespace

    def __getattr__(self, attr):
        """
        __getattr__ handles attributes that are not found (member lookup fail)
        """
        return getattr(self.rjq, attr)
