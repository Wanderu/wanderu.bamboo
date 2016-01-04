import string
from random import choice
from functools import partial

from twisted.trial import unittest
from twisted.internet import defer

from wanderu.bamboo.txrjq import TxRedisJobQueue
from wanderu.bamboo.test.util import (generate_jobs, job_cmp)
from wanderu.bamboo.config import (NS_QUEUED, NS_WORKING, NS_SCHEDULED,
                                   NS_FAILED)
from wanderu.bamboo.txred import JobScanner

def remove_keys(keys, rjq):
    if len(keys) > 0:
        return rjq.conn.delete(*keys)
    return None

def clear_ns(rjq):
    return rjq.conn.keys(rjq.namespace + "*") \
                .addCallback(remove_keys, rjq)

class TXTCBase(object):
    def setUp(self):
        if getattr(self, 'ns', None) is None:
            self.ns = "".join((choice(string.ascii_uppercase)
                              for _ in xrange(3)))

        self.rjq = TxRedisJobQueue(namespace="TEST:RJQ:%s" % self.ns)
        return clear_ns(self.rjq)

    def _disconnect(self, *args):
        return self.rjq.conn.disconnect()

    def tearDown(self):
        return clear_ns(self.rjq).addCallback(self._disconnect)

class TestEnqueue(TXTCBase, unittest.TestCase):

    @defer.inlineCallbacks
    def test_add_consume_ack(self):
        rjq = self.rjq
        jobgen = generate_jobs()
        job1 = next(jobgen)
        res = yield rjq.enqueue(job1)
        self.assertEqual(res, 1)
        job1b = yield rjq.consume()
        self.assertTrue(job_cmp(job1, job1b))
        res = yield rjq.ack(job1b)
        self.assertEqual(res, 1)

    @defer.inlineCallbacks
    def test_client_name(self):
        rjq = self.rjq
        for i in range(10):
            client = yield rjq.conn.execute_command("CLIENT", "GETNAME")
            self.assertEqual(client, rjq.name)

    @defer.inlineCallbacks
    def test_peek(self):
        rjq = self.rjq
        jobgen = generate_jobs()
        job0 = next(jobgen)
        job0.priority = 1
        job1 = next(jobgen)
        job1.priority = 2
        job2 = next(jobgen)
        job2.priority = 3
        yield rjq.add(job0)
        yield rjq.add(job1)
        yield rjq.add(job2)

        def aggregateCb(jobs, job):
            jobs.append(job)

        # This method calls aggregateCb on retrieving each job.
        jobs = []
        yield rjq.peek(partial(aggregateCb, jobs), NS_QUEUED)

        self.assertTrue(job_cmp(job0, jobs[0]))
        self.assertTrue(job_cmp(job1, jobs[1]))
        self.assertTrue(job_cmp(job2, jobs[2]))

        jobs = []
        yield rjq.peek(partial(aggregateCb, jobs), NS_QUEUED, count=2)
        self.assertTrue(job_cmp(job0, jobs[0]))
        self.assertTrue(job_cmp(job1, jobs[1]))
        self.assertEqual(len(jobs), 2)

        # This method works like an iterator
        jobit = yield rjq.queue_iter(NS_QUEUED)
        job0b = yield next(jobit)
        job1b = yield next(jobit)
        job2b = yield next(jobit)
        self.assertTrue(job_cmp(job0, job0b))
        self.assertTrue(job_cmp(job1, job1b))
        self.assertTrue(job_cmp(job2, job2b))

    @defer.inlineCallbacks
    def test_scanner(self):
        jobgen = generate_jobs()
        for _ in range(10):
            job = next(jobgen)
            yield self.rjq.add(job)

        # count < available
        scanner = JobCollectScanner(self.rjq)
        yield scanner.scan(NS_QUEUED, count=2)
        self.assertEqual(len(scanner.jobs), 2)

        # count == available
        scanner = JobCollectScanner(self.rjq)
        yield scanner.scan(NS_QUEUED)
        self.assertEqual(len(scanner.jobs), 10)

        # count > available
        scanner = JobCollectScanner(self.rjq)
        yield scanner.scan(NS_QUEUED, count=20)
        self.assertEqual(len(scanner.jobs), 10)

    @defer.inlineCallbacks
    def test_consume(self):
        no = yield self.rjq.can_consume()
        self.assertFalse(no)

        jobgen = generate_jobs()
        jobs = []
        for i in range(10):
            job = next(jobgen)
            job.priority = i
            jobs.append(job)
            yield self.rjq.add(job)

        yes = yield self.rjq.can_consume()
        self.assertTrue(yes)

        job0 = yield self.rjq.consume()
        self.assertTrue(job_cmp(job0, jobs[0]))

        @defer.inlineCallbacks
        def get_jobs(queue):
            d = yield self.rjq.queue_iter(queue)
            jobs = []
            for d_job in d:
                job = yield d_job
                jobs.append(job)
            defer.returnValue(jobs)

        working = yield get_jobs(NS_WORKING)
        self.assertEqual(len(working), 1)
        wct = yield self.rjq.count(NS_WORKING)
        self.assertEqual(wct, 1)

        yield self.rjq.ack(working[0])

        working = yield get_jobs(NS_WORKING)
        self.assertEqual(len(working), 0)
        wct = yield self.rjq.count(NS_WORKING)
        self.assertEqual(wct, 0)

    # def test_subscribe


class JobCollectScanner(JobScanner):
    """An example custom scanner that collects the returned Job objects into
    a list.
    """
    def __init__(self, rjq):
        JobScanner.__init__(self, rjq)
        self.jobs = []

    def receivedJob(self, job):
        self.jobs.append(job)
