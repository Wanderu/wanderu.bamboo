import string
import random
import json
import os

from random import choice, randrange
from datetime import datetime, timedelta

from wanderu.bamboo.rjq import RedisJobQueue
from wanderu.bamboo.job import Job
# from wanderu.bamboo.config import QUEUE_NAMES

# def printQueues(rjq):
#     for queue in QUEUE_NAMES:
#         print (queue)
#         for job in rjq.peek(queue):
#             print (job)
#         print ("")

def job_cmp(ja, jb):
    """Tests a subset of job parameters to make sure the job is the same
    pre/post operation.
    """
    return all((
                (ja.id       == jb.id),
                (ja.payload  == jb.payload),
                (ja.created  == jb.created),
                (ja.priority == jb.priority)))

def random_job_dict():

    c = "".join((choice(string.ascii_uppercase) for _ in xrange(3)))
    f = "".join((choice(string.ascii_uppercase) for _ in xrange(6)))
    t = "".join((choice(string.ascii_uppercase) for _ in xrange(6)))
    s = "something"
    date = (datetime.utcnow() +
            timedelta(seconds=randrange(1, 24*60*60*60)))\
                .strftime("%m/%d/%Y")
    return {
        "id": "{c}_{f}_{t}_{date}".format(c=c, f=f, t=t, date=date.replace("/", "")),
        "payload": json.dumps({
                        "C": c, "F": f,
                        "T": t, "S": s,
                        "D": date}),
        "priority": randrange(1, 10)
    }

def generate_jobs(seed=None):
    if seed is not None:
        random.seed(seed)

    while True:
        d = random_job_dict()
        yield Job.from_dict(d)


def clear_ns(rjq):
    nskeys = rjq.conn.keys(rjq.namespace + "*")
    if len(nskeys) > 0:
        rjq.conn.delete(*nskeys)


class TCBase(object):
    def setUp(self):
        if getattr(self, 'ns', None) is None:
            self.ns = "".join((choice(string.ascii_uppercase)
                              for _ in xrange(3)))

        conn  = os.environ.get('REDIS_CONN', 'redis://localhost')
        self.rjq = RedisJobQueue(namespace="TEST:RJQ:%s" % self.ns, conn=conn)
        clear_ns(self.rjq)

    def tearDown(self):
        clear_ns(self.rjq)
