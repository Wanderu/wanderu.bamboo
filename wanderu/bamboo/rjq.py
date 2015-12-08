import logging
import calendar
import redis
import uuid

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
                        REQUEUE_TIMEOUT, JOB_TIMEOUT)

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


'''
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

TODO:
cascade_enqueue
abandoned, untracked, unknown, clean_set_ns?
'''

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
    if isinstance(conn, (StringTypes)):
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

    def __init__(self, namespace, name=None, conn=None):
        self.jobs = dict()  # mapping from job id -> Job object
        self.conn = get_redis_connection(REDIS_CONN if conn is None else conn)

        if name is None:
            name = gen_random_name()

        self.name = name
        self.namespace = RE_HASHSLOT.match(namespace) and namespace \
                            or ("{%s}" % namespace)

        self.conn.client_setname(name)  # unique name for this client
        self._load_lua_scripts(self.conn)

    def _key(self, *args):
        "Helper to build redis keys given the namespace."
        return make_key(NS_SEP, self.namespace, *args)

    def queue_iter(self, Q, withscores=False):
        """Use this function to retrieve Jobs via a queue iterator that
        retrieves one job at a time from the database. The iterator works with
        a queue that changes over time.
        """
        for jid, score in self.conn.zscan_iter(self._key(Q)):
            try:
                job = self.get(jid)
                yield (job, score) if withscores else job
            except NotFoundError:
                continue

    def subscribe(self):
        pass

    def add(self, job):
        keys = (self.nameapce,)
        # <queue> <priority> <jobid> <force> <key> <val> [<key> <val> ...]
        args = (NS_QUEUED, job.priority, job.id, "0") \
                    + tuple(chain(*job.as_dict(filter=True).items()))
        try:
            self._enqueue(keys, args)
        except RedisError, err:
            error("Error enqueing: %s", err)
            raise OperationError("%s" % err)
        return 0

    def schedule(self, job, time):
        pass

    def get(self, job_id):
        """
        Return a Job instance for a given job_id by introspecting redis.
        This does *NOT* consume/reserve a job.
        Use this for introspecting job contents.

        Raises a NotFoundError when the job_id is not found in the database.
        """
        job_dict = self.conn.hgetall(self._key(NS_JOB, job_id))
        # hgetall returns an empty dict {} if the item was not found
        if len(job_dict) == 0:
            raise NotFoundError("No job with job ID {jid} found."
                                .format(jid=job_id))

        return Job.from_dict(job_dict)

    def consume(self, jobid=None):
        pass

    def ack(self, job):
        pass

    def fail(self, job):
        pass

    def maxfailed(self, val=None):
        """Get or set maxfailed.
        """
        pass

    def maxjobs(self, val=None):
        """Get or set maxjobs.
        """
        pass

