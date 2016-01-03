import logging

import txredisapi as redis
from twisted.internet import defer

from wanderu.bamboo.job import Job
from wanderu.bamboo.txscript import Script
from wanderu.bamboo.rjq import SCRIPT_NAMES, RedisJobQueue
from wanderu.bamboo.io import read_lua_scripts
from wanderu.bamboo.config import NS_JOB, QUEUE_NAMES
from wanderu.bamboo.errors import (message_to_error,
                                   OperationError, UnknownJobId)
from wanderu.bamboo.util import utcunixts
from wanderu.bamboo.txred import makeConnection, makeSubscriber, zscan_cb

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
        self.client_setname(self.name)

    def _load_lua_scripts(self):
        self.scripts = {
            name: Script(self.conn, script)
            for (name, script) in read_lua_scripts(SCRIPT_NAMES).items()
        }

    def client_setname(self, name):
        return self.conn.execute_command("CLIENT", "SETNAME", name)

    def _op_error(self, failure, name):
        if failure.check(redis.ResponseError):
            # error translation
            error("Error in %s: %s" % (name, failure))
            raise message_to_error(failure.getErrorMessage())
        return failure

    def op(self, name, keys, args):
        # call the script, returns a deferred
        d = self.scripts[name].eval(keys, args)
        d.addErrback(self._op_error, name)
        return d

    def can_consume(self):
        """Returns True if there are jobs available to consume. False
        otherwise.
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
        d.addCallback(super(RedisJobQueue, self).count)
        return d

    def jobs(self, cb, Q, withscores=False, count=None):
        def _cb(jobid, score):
            job = self.get(jobid)
            if withscores:
                cb(job, score)
            else:
                cb(job)

        return zscan_cb(_cb, self.conn, self.key(Q), match=None, count=count)

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
