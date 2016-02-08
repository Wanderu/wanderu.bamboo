import string
from random import choice
from functools import partial

from twisted.trial import unittest
from twisted.internet import defer, task, reactor

from wanderu.bamboo.txrjq import TxRedisJobQueue, TxRedisJobQueueView
from wanderu.bamboo.test.util import (generate_jobs, job_cmp)
from wanderu.bamboo.config import (NS_QUEUED, NS_WORKING, NS_SCHEDULED,
                                   NS_FAILED)
from wanderu.bamboo.txred import JobScanner
from wanderu.bamboo.errors import (NoItems, NormalOperationError,
                                   JobExists, AbnormalOperationError,
                                   UnknownJobId, InvalidQueue)
from wanderu.bamboo.util import utcunixts

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
        for rjq in (self.rjq,
                TxRedisJobQueueView(self.rjq, self.rjq.namespace + ":MORE")):
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

        jobs = []
        yield rjq.peek(jobs.append, NS_QUEUED)

        self.assertTrue(job_cmp(job0, jobs[0]))
        self.assertTrue(job_cmp(job1, jobs[1]))
        self.assertTrue(job_cmp(job2, jobs[2]))

        jobs = []
        yield rjq.peek(jobs.append, NS_QUEUED, count=2)
        self.assertTrue(job_cmp(job0, jobs[0]))
        self.assertTrue(job_cmp(job1, jobs[1]))
        self.assertEqual(len(jobs), 2)

        try:
            yield rjq.peek(jobs.append, "INVALID_QUEUE_NAME")
        except AbnormalOperationError as err:
            assert isinstance(err, InvalidQueue)

        # This method works like an iterator
        jobit = yield rjq.queue_iter(NS_QUEUED)
        job0b = yield next(jobit)
        job1b = yield next(jobit)
        job2b = yield next(jobit)
        self.assertTrue(job_cmp(job0, job0b))
        self.assertTrue(job_cmp(job1, job1b))
        self.assertTrue(job_cmp(job2, job2b))

    @defer.inlineCallbacks
    def test_add_twice(self):
        jobgen = generate_jobs()
        job = next(jobgen)
        # First time is good
        yield self.rjq.add(job)
        try:
            # Second time raises `JobExists`
            yield self.rjq.add(job)
        except JobExists as err:
            pass

    @defer.inlineCallbacks
    def test_get_job_by_id(self):
        jobgen = generate_jobs()
        job = next(jobgen)
        yield self.rjq.add(job)
        job2 = yield self.rjq.get(job.id)
        self.assertEqual(job.id, job2.id)
        try:
            yield self.rjq.get(job.id + "INVALIDJOBID")
        except UnknownJobId as err:
            pass

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
    def test_consume_fail(self):
        try:
            nothing = yield self.rjq.consume()
        except NoItems as err:
            pass

    def test_consume_fail2(self):
        def checkErr(failure):
            # Only succeed if failure is a NoItems exception
            failure.trap(NoItems)

        def checkRes(*args):
            self.fail("Consume should have triggered the errback.")

        return self.rjq.consume().addCallbacks(checkRes, checkErr)

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

    @defer.inlineCallbacks
    def consume_fail(self, job1a):
        yield self.rjq.enqueue(job1a)
        t = utcunixts()+1
        scheduled_jobs = yield self.rjq.count(NS_SCHEDULED)
        self.assertEqual(scheduled_jobs, 0)
        queued_jobs = yield self.rjq.count(NS_QUEUED)
        self.assertEqual(queued_jobs, 1)
        job1b = yield self.rjq.consume()
        self.assertTrue(job_cmp(job1a, job1b))
        yield self.rjq.fail(job1b, requeue_seconds=1)
        scheduled_jobs = yield self.rjq.count(NS_SCHEDULED)
        self.assertEqual(scheduled_jobs, 1)
        can_consume = yield self.rjq.can_consume()
        self.assertFalse(can_consume)
        yield task.deferLater(reactor, 1.2, lambda *args: None)
        can_consume = yield self.rjq.can_consume()
        self.assertTrue(can_consume)
        job1c = yield self.rjq.consume()
        self.assertTrue(job_cmp(job1a, job1c))
        working_jobs = yield self.rjq.count(NS_WORKING)
        self.assertEqual(working_jobs, 1)
        defer.returnValue(job1c)

    @defer.inlineCallbacks
    def test_tx_consume_fail_ack(self):
        yield self.rjq.maxfailed(1)
        jobgen = generate_jobs()
        job1a = next(jobgen)
        job1c = yield self.consume_fail(job1a)
        yield self.rjq.ack(job1c)
        working_jobs = yield self.rjq.count(NS_WORKING)
        self.assertEqual(working_jobs, 0)

    @defer.inlineCallbacks
    def test_tx_consume_fail_fail(self):
        yield self.rjq.maxfailed(1)
        jobgen = generate_jobs()
        job1a = next(jobgen)
        job1c = yield self.consume_fail(job1a)
        yield self.rjq.fail(job1c)
        working_jobs = yield self.rjq.count(NS_WORKING)
        self.assertEqual(working_jobs, 0)
        scheduled_jobs = yield self.rjq.count(NS_SCHEDULED)
        self.assertEqual(scheduled_jobs, 0)
        failed_jobs = yield self.rjq.count(NS_FAILED)
        self.assertEqual(failed_jobs, 1)

    @defer.inlineCallbacks
    def test_tx_schedule_1(self):
        can_consume = yield self.rjq.can_consume()
        self.assertFalse(can_consume)
        jobgen = generate_jobs()
        job1a = next(jobgen)
        t = utcunixts()+1
        yield self.rjq.schedule(job1a, t)
        scheduled_jobs = yield self.rjq.count(NS_SCHEDULED)
        self.assertEqual(scheduled_jobs, 1)
        can_consume = yield self.rjq.can_consume()
        self.assertFalse(can_consume)
        yield task.deferLater(reactor, 1.2, lambda *args: None)
        can_consume = yield self.rjq.can_consume()
        self.assertTrue(can_consume)
        job1b = yield self.rjq.consume()
        self.assertTrue(job_cmp(job1a, job1b))

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
