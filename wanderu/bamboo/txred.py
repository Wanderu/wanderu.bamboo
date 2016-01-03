from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue

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
    def __init__(self, conn, receivedItem=None):
        """
        conn: The redis connection object.
        receivedItem: Callback function that is executed for each item
                      in the queue.
        """
        self.conn = conn
        if receivedItem is not None:
            self.receivedItem = receivedItem

    def receivedItem(self, item, score):
        pass

    def scan(self, key, match=None, count=None):
        """
        Returns a deferred that executes it's callback after all items
        from the queue have been returned.

        >> def cb(item, data):
              print ((item, data))
        >> d = Scanner(conn, cb).scan(key)
        >> d.addCallback(...)
        """
        return zscan_cb(self.receivedItem, self.conn, key, match, count)

@inlineCallbacks
def zscan_iter(conn, key, match=None, count=None):
    items = []

    def cb(data):
        item, score = data
        items.append(item)

    yield zscan_cb(cb, conn, key, match, count)
    returnValue(items)

@inlineCallbacks
def zscan_cb(cb, conn, key, match=None, count=None):
    cursor = 0
    while True:
        cursor, items = yield conn.zscan(key, cursor, match, count)
        for item in twos(items):
            # item is a tuple of (data, score)
            cb(item)
        if cursor == 0:
            break
