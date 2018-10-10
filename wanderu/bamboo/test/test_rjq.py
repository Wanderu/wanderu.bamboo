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

import unittest
import time
import logging
import os

from wanderu.bamboo.rjq import RedisJobQueue, RedisJobQueueView
from wanderu.bamboo.config import (
    NS_QUEUED,
    NS_SCHEDULED,
    NS_FAILED,
    NS_WORKING,
    NS_MAXJOBS,
    NS_JOB,
    JOB_STATE_FAILED,
    JOB_STATE_WORKING,
    JOB_STATE_ENQUEUED,
    JOB_STATE_SCHEDULED
)

from wanderu.bamboo.test.util import (generate_jobs, TCBase, job_cmp, clear_ns)
from wanderu.bamboo.util import utcunixts
from wanderu.bamboo.errors import (OperationError, NoItems, UnknownJobId)

logger = logging.getLogger(__name__)


class TestEnqueue(TCBase, unittest.TestCase):

    def test_list_workers(self):
        rjq = self.rjq

        jobgen = generate_jobs()
        job1 = next(jobgen)
        rjq.enqueue(job1)

        nworkers = len(rjq.workers())
        njobs = len(rjq.jobs_for_worker(rjq.name))
        nactive = len(rjq.active())

        self.assertEqual(nworkers, 0)
        self.assertEqual(njobs, 0)
        self.assertEqual(nactive, 0)

        job1consumed = rjq.consume()

        nworkers = len(rjq.workers())
        njobs = len(rjq.jobs_for_worker(rjq.name))
        nactive = len(rjq.active())

        self.assertEqual(nworkers, 1)
        self.assertEqual(njobs, 1)
        self.assertEqual(nactive, 1)

    def test_subscribe_post_consume(self):
        rjq = self.rjq

        jobgen = generate_jobs()
        job1 = next(jobgen)

        sub = rjq.subscribe()

        msg = next(sub)
        self.assertTrue(msg is None)

        rjq.enqueue(job1)

        msg = next(sub)
        self.assertEqual(msg[1], NS_QUEUED)
        self.assertEqual(msg[0], job1.id)

    def test_enqueue_consume_ack_1(self):
        # """Enqueue, consume, ack. Test for state changes."""
        rjq = self.rjq

        jobgen = generate_jobs()
        job1 = next(jobgen)
        rjq.enqueue(job1)

        kjob = rjq.key(NS_JOB, job1.id)
        logger.info("Job Key: %s", kjob)

        state = rjq.conn.hget(kjob, "state")
        logger.info("Job state: %s", state)
        self.assertEqual(state, JOB_STATE_ENQUEUED)

        job2 = rjq.consume()

        state = rjq.conn.hget(kjob, "state")
        logger.info("Job state: %s", state)
        self.assertEqual(state, JOB_STATE_WORKING)

        # We should get the same job object as we enqueued.
        self.assertTrue(job_cmp(job1, job2))

        state = rjq.conn.hget(rjq.key(NS_JOB, job1.id), "state")
        # the database should have the state attribute updated
        self.assertEqual(state, JOB_STATE_WORKING)
        # the object should have received the updated state attribute
        self.assertEqual(job2.state, JOB_STATE_WORKING)

        rjq.ack(job2)
        # acking should remove the job entry from the queue and remove the hash
        self.assertIsNone(rjq.conn.zscore(rjq.key(NS_QUEUED), job2.id))
        self.assertDictEqual(rjq.conn.hgetall(rjq.key(NS_JOB, job2.id)), {})

    def test_enqueue_consume_ack_2(self):
        # """Enqueue multiple, checking queue counts, consume all."""
        rjq = self.rjq
        jobgen = generate_jobs()
        self.assertEqual(rjq.count(NS_QUEUED), 0)
        for i in range(1, 5):
            job = next(jobgen)
            rjq.add(job)
            self.assertEqual(rjq.count(NS_QUEUED), i)

        while rjq.can_consume():
            job = rjq.consume()

        with self.assertRaises(NoItems):
            rjq.consume()

    def test_enqueue_consume_fail_1(self):
        # """One reschedule attempt and then fail. maxfailed=1"""
        rjq = self.rjq

        # Consume, Fail->Reschedule, Consume, Fail->Failed
        rjq.maxfailed(1)

        jobgen = generate_jobs()
        job1a = next(jobgen)

        rjq.enqueue(job1a)

        self.assertEqual(rjq.get(job1a.id).state, JOB_STATE_ENQUEUED)

        # You can't fail a job that has not been consumed (that is not
        # in the WORKING queue)
        with self.assertRaises(UnknownJobId):
            rjq.fail(job1a)

        job1b = rjq.consume()

        self.assertEqual(rjq.count(NS_WORKING), 1)
        self.assertTrue(job_cmp(job1a, job1b))
        self.assertEqual(job1b.state, JOB_STATE_WORKING)
        self.assertEqual(rjq.get(job1a.id).state, JOB_STATE_WORKING)

        # 1st time failure goes to SCHEDULED queue
        self.assertEqual(rjq.count(NS_SCHEDULED), 0)
        rjq.fail(job1b)
        self.assertEqual(rjq.count(NS_SCHEDULED), 1)

        job1b = rjq.get(job1b.id)
        self.assertEqual(job1b.state, JOB_STATE_SCHEDULED)
        self.assertEqual(job1b.failures, 1)

        job1c = rjq.consume()
        self.assertEqual(rjq.count(NS_SCHEDULED), 0)
        self.assertEqual(rjq.count(NS_WORKING), 1)
        self.assertTrue(job_cmp(job1b, job1c))
        self.assertEqual(job1c.state, JOB_STATE_WORKING)

        # 2nd time failure goes to FAILED queue
        rjq.fail(job1c)
        self.assertEqual(rjq.count(NS_WORKING), 0)
        self.assertEqual(rjq.count(NS_QUEUED), 0)
        self.assertEqual(rjq.count(NS_SCHEDULED), 0)
        self.assertEqual(rjq.count(NS_FAILED), 1)

        job1c = rjq.get(job1c.id)
        self.assertEqual(job1c.state, JOB_STATE_FAILED)
        self.assertEqual(job1c.failures, 2)

        self.assertTrue(job_cmp(job1b, job1c))

    def test_enqueue_consume_fail_2(self):
        # """Enqueue, consume, fail, fail to consume. maxfailed=0"""
        rjq = self.rjq
        rjq.maxfailed(0)
        jobgen = generate_jobs()
        job1a = next(jobgen)

        self.assertFalse(rjq.can_consume())

        # No jobs in any queue
        with self.assertRaises(NoItems):
            rjq.consume()

        rjq.enqueue(job1a)
        self.assertTrue(rjq.can_consume())
        job1b = rjq.consume()
        rjq.fail(job1b)

        job1b = rjq.get(job1b.id)
        self.assertEqual(job1b.state, JOB_STATE_FAILED)
        self.assertEqual(job1b.failures, 1)

        self.assertFalse(rjq.can_consume())

        # The only Job is in the FAILED queue
        with self.assertRaises(NoItems):
            rjq.consume()

    def test_schedule_1(self):
        rjq = self.rjq
        rjq.maxfailed(0)
        jobgen = generate_jobs()
        job1a = next(jobgen)

        self.assertFalse(rjq.can_consume())
        t = utcunixts()+1
        rjq.schedule(job1a, t)
        self.assertEqual(rjq.count(NS_SCHEDULED), 1)
        self.assertFalse(rjq.can_consume())

        job1b = rjq.get(job1a.id)
        self.assertTrue(job1b.priority != t)
        self.assertEqual(job1b.state, JOB_STATE_SCHEDULED)

        time.sleep(1.2)
        self.assertTrue(rjq.can_consume())

        job1c = rjq.consume()
        self.assertEqual(rjq.count(NS_SCHEDULED), 0)
        self.assertEqual(rjq.count(NS_WORKING), 1)
        self.assertEqual(job1c.state, JOB_STATE_WORKING)
        self.assertFalse(rjq.can_consume())
        rjq.ack(job1c)

    def test_reschedule(self):
        rjq = self.rjq
        rjq.maxfailed(0)
        jobgen = generate_jobs()
        job1a = next(jobgen)
        t = utcunixts()
        rjq.schedule(job1a, t)

        job1b, job1b_t = next(rjq.queue_iter(NS_SCHEDULED, withscores=True))
        self.assertEqual(t, job1b_t)
        self.assertTrue(rjq.can_consume())

        t = utcunixts() + 1
        rjq.reschedule(job1a, t)

        self.assertFalse(rjq.can_consume())
        job1b, job1b_t = next(rjq.queue_iter(NS_SCHEDULED, withscores=True))
        self.assertEqual(t, job1b_t)

    def test_requeue(self):
        rjq = self.rjq
        rjq.maxfailed(0)
        jobgen = generate_jobs()
        job1a = next(jobgen)
        job1a.priority = 5
        jobid = job1a.id

        rjq.add(job1a)

        job1a, prio = next(rjq.queue_iter(NS_QUEUED, withscores=True))
        self.assertEqual(rjq.get(jobid).priority, 5)
        self.assertEqual(prio, 5)

        job1a.priority = 6
        rjq.requeue(job1a)

        job1a, prio = next(rjq.queue_iter(NS_QUEUED, withscores=True))
        self.assertEqual(rjq.get(jobid).priority, 6)
        self.assertEqual(prio, 6)

    def test_maxn(self):
        rjq = self.rjq

        # no value means 0
        self.assertEqual(rjq.maxfailed(), 0)

        rjq.maxfailed(0)
        self.assertEqual(rjq.maxfailed(), 0)
        rjq.maxfailed(5)
        self.assertEqual(rjq.maxfailed(), 5)

        rjq.maxjobs(0)
        self.assertEqual(rjq.maxjobs(), 0)
        rjq.maxjobs(5)
        self.assertEqual(rjq.maxjobs(), 5)

    def test_recover(self):
        # rjq = self.rjq

        jobgen = generate_jobs()
        job1a = next(jobgen)

        namespace = "RECOVER"

        conn  = os.environ.get('REDIS_CONN', 'redis://localhost')
        rjq1 = RedisJobQueue(namespace="TEST:RJQ:%s" % namespace,
                worker_expiration=1, name="testrjq1", conn=conn)

        rjq2 = RedisJobQueue(namespace="TEST:RJQ:%s" % namespace,
                worker_expiration=1, name="testrjq2", conn=conn)

        rjq1.add(job1a)
        job1a = rjq1.consume()
        clients = {e['name'] for e in rjq1.conn.client_list()}
        self.assertIn(rjq1.name, clients)
        self.assertIn(rjq2.name, clients)
        del rjq1

        time.sleep(1.2)

        try:
            job_ids = rjq2.recover()
            self.assertEqual(len(job_ids), 1)
            self.assertEqual(job_ids[0], job1a.id)
        finally:
            clear_ns(rjq2)

    def test_misc(self):
        rjq = self.rjq

        with self.assertRaises(OperationError):
            next(rjq.queue_iter('notaqueuename'))

        with self.assertRaises(OperationError):
            rjq.count('notaqueuename')

    def test_queue_iter(self):
        rjq = self.rjq
        jobgen = generate_jobs()
        rjq.add(next(jobgen))
        rjq.add(next(jobgen))

        it = rjq.queue_iter(NS_QUEUED)
        next(it)

        job1 = rjq.consume()
        rjq.ack(job1)
        job2 = rjq.consume()
        rjq.ack(job2)

        with self.assertRaises(StopIteration):
            next(it)

    def test_rjq_view(self):
        rjq = self.rjq

        ns2 = rjq.key("NEWNAMESPACE")
        rjqv1 = RedisJobQueueView(rjq, ns2)

        self.assertEqual(rjqv1.namespace, ns2)
        self.assertEqual(rjqv1.key("A"), ns2 + ":A")

        self.assertEqual(rjqv1.count(NS_QUEUED), 0)
        # TODO: Add, consume, ack, fail, etc with the view.
