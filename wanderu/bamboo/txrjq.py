import logging
from urlparse import urlparse
from urllib import unquote

import txredisapi as redis
from twisted.internet import defer, reactor

from wanderu.bamboo.job import Job
from wanderu.bamboo.txscript import Script
from wanderu.bamboo.rjq import SCRIPT_NAMES, RedisJobQueue
from wanderu.bamboo.io import read_lua_scripts
from wanderu.bamboo.config import NS_JOB, QUEUE_NAMES
from wanderu.bamboo.errors import (message_to_error,
                            OperationError, UnknownJobId)
from wanderu.bamboo.util import utcunixts

logger = logging.getLogger(__name__)
error  = logger.error
warn   = logger.warn
info   = logger.info
debug  = logger.debug

def parse_url(url):
    """url: redis://localhost:6379/0"""
    pr = urlparse(url)
    host, path, password = map(unquote,
            (pr.hostname or "", pr.path or "", pr.password or ""))
    dbid = path[1:].split('/')[0]
    return (host or "localhost", pr.port or 6379, dbid or None,
            password or None)

def make_conn(url):
    host, port, db, password = parse_url(url)
    return redis.lazyConnection(host, port, db, password=password)

class TxRedisJobQueue(RedisJobQueue):

    def _init_connection(self, url):
        """conn: String. Redis connection URL string."""
        self.url = url
        self.conn = make_conn(url or "")
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

    def queue_iter(self, Q, withscores=False):
        pass

    def subscribe(self, callback):
        """
        Returns the connection instance after subscribing to queue events.
        """
        conn = makeSubscriber(self.url, callback)
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

class TxRedisSubscriberProtocol(redis.SubscriberProtocol):

    def messageReceived(self, pattern, channel, message):
        self.factory.callback(message)

    # def connectionMade(self):
    # def connectionLost(self, reason):

class TxRedisSubscriberFactory(redis.RedisFactory):
    protocol = TxRedisSubscriberProtocol

    def __init__(self, callback, password=None):
        redis.RedisFactory.__init__(self, None, None, 1, False,
                                    password=password)
        self.callback = callback

def makeSubscriber(url, callback):
    """
    Remember to disconnect the connection as well.

    >> conn = makeSubscriber(url, cb)
    ...
    >> conn.disconnect()
    """
    host, port, db, password = parse_url(url)
    factory = TxRedisSubscriberFactory(callback)
    reactor.connectTCP(host, port, factory)
    return factory.handler
