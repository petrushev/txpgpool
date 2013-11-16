from collections import deque

from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.python import log

from psycopg2 import connect as pgConnect
from psycopg2.extras import DictConnection as _DictConnection
from txpostgres.txpostgres import Connection

class DictConnection(Connection):
    @staticmethod
    def connectionFactory(*args, **kwargs):
        kwargs['connection_factory'] = _DictConnection
        return pgConnect(*args, **kwargs)

def connect(**params):
    return DictConnection().connect(**params)

class BasePgPool(object):

    _params = None

    def runQuery(self, *args, **kwargs):
        """Returns a defered that will fire the callback with the query result when
        the latter is available. Internally it uses the `_connRunQuery` as an
        intermediate callbackand and `_queryFinished` as a final one."""
        d = self.fetch()
        d.addCallback(self._connRunQuery, *args, **kwargs)
        return d

    def _connRunQuery(self, conn, *args, **kwargs):
        """Called imediatelly after a connection is ready
        for the deferred from `runQuery`"""
        d = conn.runQuery(*args, **kwargs)
        # adds a callback _after_ the user callback is added
        reactor.callLater(0, d.addCallback, self._queryFinished, d, conn)
        return d

    def _queryFinished(self, callbackResult, d, conn):
        """This callback is fired after the user-set callback on the deferred from
        `runQuery` is complete, and will be called with its result"""
        self.putback(conn)

    def addNotifyObserver(self, observerCallback, channels):
        """Sets async listening on `channels`. The payloads from all of them
        will be passed to `observerCallback`.
        Will hold one connection indefinetely"""
        self.fetch()\
            .addCallback(self._addConnObserver, observerCallback, channels)\
            .addErrback(self._notifyError)

    def _addConnObserver(self, conn, observerCallback, channels):
        conn.addNotifyObserver(observerCallback)

        for channel in channels:
            log.msg('Listening on channel: ' + channel)

            conn.runOperation('LISTEN ' + channel)\
                .addErrback(self._notifyError)

    def _notifyError(self, failure):
        pass
        #failure.printTraceback()

    def fetch(self):
        """Returns a deferred that will fire its callback with a connection when
        the latter is ready. Can be used if more detailed control of connection
        fetching is needed. Otherwise, use the `runQuery` method."""
        raise NotImplementedError

    def putback(self, conn):
        """Call to put a connection back to the pool explicitly."""
        raise NotImplementedError

    def destroyed(self):
        """Call to notify the pool that a connection is closed outside its control"""
        raise NotImplementedError


class NullPgPool(BasePgPool):
    """Does not do any pooling, provides direct connection to a single connection"""

    def __init__(self, **kwargs):
        self._params = kwargs
        self.fetched = False
        self._waitingForConn = deque()

    def fetch(self):
        d = Deferred()
        self._waitingForConn.append(d)

        if self.fetched == False:
            self.fetched = True
            connect(**self._params).addCallback(self._connReady)

        return d

    def _connReady(self, conn):
        if self._waitingForConn:
            d = self._waitingForConn.popleft()
            d.callback(conn)
        else:
            self.putback(conn)

    def putback(self, conn):
        conn.close()
        if self._waitingForConn:
            connect(**self._params).addCallback(self._connReady)
        else:
            self.fetched = False

    def destroyed(self):
        self.fetched = False

class StaticPgPool(BasePgPool):

    def __init__(self, **kwargs):
        self._params = kwargs
        self.conn = None
        self.fetched = False
        self._waitingForConn = deque()

    def fetch(self):
        d = Deferred()

        if self.fetched or self.conn is None or self._waitingForConn:
            self._waitingForConn.append(d)
            if self.conn is None:
                # connecting for the first time
                connect(**self._params).addCallback(self._connReady)
        else:
            # available now
            self.fetched = True
            reactor.callLater(0, d.callback, self.conn)

        return d

    def _connReady(self, conn):
        self.conn = conn
        if not self.fetched and self._waitingForConn:
            self.fetched = True
            d = self._waitingForConn.popleft()
            d.callback(conn)

    def putback(self, conn):
        self.fetched = False
        self._connReady(conn)

    def destroyed(self):
        self.conn = None
        self.fetched = False
        if self._waitingForConn:
            connect(**self._params).addCallback(self._connReady)

class QueuePgPool(set, BasePgPool):
    """MinMax connection pool. Tries to maintain `min` number of ready connections
    - if there are more clients will connect on demand but won't exceed `max` """

    def __init__(self, minConn, maxConn, **kwargs):
        self._params = kwargs
        self._min = minConn
        self._max = maxConn
        self._currentTotal = 0
        self._waitingForConn = deque()
        set.__init__(self, [])

        reactor.callLater(0, self._poolReady)

    def _poolReady(self):
        #log.msg('ready:%d, total:%d, waiters:%d' % \
        #    (len(self), self._currentTotal, len(self._waitingForConn)))
        while len(self) > 0 and self._waitingForConn:
            conn = self.pop()
            reactor.callLater(0, self._connReady, conn)

        if self._currentTotal < self._max and len(self) < self._min:
            self._currentTotal = self._currentTotal + 1
            connect(**self._params).addCallback(self._connReady)

    def _connReady(self, conn):
        """Called when a connection is ready, either by connecting
        or when a client puts it back"""
        if self._waitingForConn:
            d = self._waitingForConn.popleft()
            d.callback(conn)

        elif len(self) == self._min:
            # pool has enough ready connections, this one is closed
            conn.close()
            self._currentTotal = self._currentTotal - 1

        else:
            self.add(conn)

        self._poolReady()

    def fetch(self):
        d = Deferred()
        self._waitingForConn.append(d)

        if len(self) > 0:
            conn = self.pop()
            reactor.callLater(0, self._connReady, conn)

        elif self._currentTotal < self._max:
            self._currentTotal = self._currentTotal + 1
            connect(**self._params).addCallback(self._connReady)

        return d

    def putback(self, conn):
        if len(self) == self._min:
            conn.close()
            self._currentTotal = self._currentTotal - 1
        else:
            self.add(conn)

        self._poolReady()

    def destroyed(self):
        self._currentTotal = self._currentTotal - 1
        self._poolReady()
