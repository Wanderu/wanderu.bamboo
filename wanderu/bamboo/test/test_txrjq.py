import string
from random import choice

from twisted.trial import unittest
from twisted.internet import defer

from wanderu.bamboo.txrjq import TxRedisJobQueue
from wanderu.bamboo.test.util import (generate_jobs, job_cmp)

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
        raise unittest.SkipTest("Doesn't work with pooled connections yet.")
        rjq = self.rjq
        client = yield rjq.conn.execute_command("CLIENT", "GETNAME")
        self.assertEqual(client, rjq.name)

    # @defer
