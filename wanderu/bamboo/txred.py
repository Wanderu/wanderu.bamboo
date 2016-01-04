from twisted.internet import reactor
from twisted.internet import defer

import txredisapi as redis

from wanderu.bamboo.util import parse_url, twos

class TxRedisFactory(redis.RedisFactory):
    def __init__(self, name, *args, **kwargs):
        redis.RedisFactory.__init__(self, *args, **kwargs)
        self.name = name

    def addConnection(self, conn):
        conn.execute_command("CLIENT", "SETNAME", self.name)
        return redis.RedisFactory.addConnection(self, conn)

def makeConnection(url, name,
                    reconnect = True, charset = "utf-8",
                    isLazy = True, poolsize = 5,
                    replyTimeout = None,
                    convertNumbers = True,
                    connectTimeout = None):
    """A version of txredisapi.makeConnection that uses TxRedisFactory
    instead of txredisapi.RedisFactory.
    """
    host, port, dbid, password = parse_url(url)
    uuid = "%s:%s" % (host, port)
    factory = TxRedisFactory(name, uuid, dbid, poolsize, isLazy,
                            redis.ConnectionHandler, charset, password,
                            replyTimeout, convertNumbers)
    factory.continueTrying = reconnect
    for x in xrange(poolsize):
        reactor.connectTCP(host, port, factory, connectTimeout)

    if isLazy:
        return factory.handler
    else:
        return factory.deferred

class TxRedisSubscriberProtocol(redis.SubscriberProtocol):

    def messageReceived(self, pattern, channel, message):
        # return a tuple of (job_id, queue)
        self.factory.callback((message, channel))

    # def connectionMade(self):
    # def connectionLost(self, reason):

class TxRedisSubscriberFactory(TxRedisFactory):
    protocol = TxRedisSubscriberProtocol

    def __init__(self, name, callback, password=None):
        TxRedisFactory.__init__(self, name, None, None, 1, False,
                                password=password)
        self.callback = callback

def makeSubscriber(url, name, callback):
    """
    Remember to disconnect the connection as well.

    >> conn = makeSubscriber(url, cb)
    ...
    >> d = conn.unsubscribe("")
    >> d.addCallbacks(lambda *a: conn.disconnect)
    >> return d
    """
    host, port, db, password = parse_url(url)
    factory = TxRedisSubscriberFactory(name, callback, password)
    reactor.connectTCP(host, port, factory)
    return factory.handler

class Scanner(object):
    """An abstract class that iterates over zset items.

    Subclasses should override the `receivedItem` method.
    """
    def __init__(self, conn):
        """
        conn: The redis connection object.
        """
        self._stop = False
        self.deferred = defer.Deferred()
        self.conn = conn

    def receivedItem(self, item, score):
        """
        item: String. Item from the queue.
        score: Number. Queue Score (priority) of the item.
        """
        raise NotImplementedError("Subclasses must override this method.")

    def scan(self, key, match=None, count=None):
        self.deferred = self._scan(key, match, count)
        return self.deferred

    @defer.inlineCallbacks
    def _scan(self, key, match=None, count=None):
        """
        Returns a deferred that executes it's callback after all items
        from the queue have been returned.
        """
        # Redis will return `count` or greater items from the zset.
        # We manually limit it by keeping track of the number of items.
        ct = 0
        cursor = 0
        while not self._stop:
            cursor, items = yield self.conn.zscan(key, cursor, match, count)
            for item, score in twos(items):
                if self._stop:
                    break
                # item is a tuple of (data, score)
                yield self.receivedItem(item, score)
                ct += 1
                if count is not None and ct == count:
                    self.stop()
            if cursor == 0:
                self.stop()

    def stop(self):
        self._stop = True

class JobScanner(Scanner):
    """An abstract class that iterates over Job items.

    Subclasses should override the `receivedJob` method.
    """
    def __init__(self, rjq):
        self.rjq = rjq
        Scanner.__init__(self, rjq.conn)

    def scan(self, Q, match=None, count=None):
        return Scanner.scan(self, self.rjq.key(Q), match, count)

    @defer.inlineCallbacks
    def receivedItem(self, item, score):
        job = yield self.rjq.get(item)
        yield self.receivedJob(job)

    def receivedJob(self, job):
        raise NotImplementedError("Subclass must override this method.")

@defer.inlineCallbacks
def zscan_items(conn, key, match=None, count=None):
    cursor = 0
    ct = 0

    items = []
    while True:
        cursor, _items = yield conn.zscan(key, cursor, match, count)
        for item, score in twos(_items):
            if ct == count:
                break
            items.append((item, score))
        if ct == count or cursor == 0:
            break

    defer.returnValue(items)
