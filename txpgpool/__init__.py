from collections import deque

from twisted.internet.defer import Deferred, succeed
from twisted.python import log
from twisted.python.failure import Failure

from psycopg2 import connect as pgConnect
from psycopg2.extras import DictConnection
from txpostgres.txpostgres import Connection


class TxDictConnection(Connection):

    @staticmethod
    def connectionFactory(*args, **kwargs):
        kwargs['connection_factory'] = DictConnection
        return pgConnect(*args, **kwargs)


def connect(**params):
    return TxDictConnection().connect(**params)

def echo(failure):
    log.err(failure.getErrorMessage())


def _connRunQuery(conn, pool, *args, **kwargs):
    """Called when a connection is acquired for the deferred from`runQuery`.
    The result will proceed to the callback, and the connection will be
    returned to the pool"""

    d = Deferred()
    internal = conn.runQuery(*args, **kwargs)

    @internal.addCallback
    def onQueryResult(result):
        pool.putback(conn)
        d.callback(result)

    @internal.addErrback
    def onQueryFailure(failure):
        pool.putback(conn)
        d.errback(failure)

    return d

def _connAddObserver(self, conn, observerCallback, channels):
    conn.addNotifyObserver(observerCallback)

    for channel in channels:
        log.msg('Listening on channel: ' + channel)

        conn.runOperation('LISTEN ' + channel)\
            .addErrback(self._notifyError)


class BasePgPool(object):

    _params = None

    def runQuery(self, *args, **kwargs):
        """Returns a defered that will fire the callback with the query result when
        the latter is available. Internally it uses the `_connRunQuery` as an
        intermediate callback."""
        d = self.fetch()
        d.addCallback(_connRunQuery, self, *args, **kwargs)
        return d

    def addNotifyObserver(self, observerCallback, channels):
        """Sets async listening on `channels`. The payloads from all of them
        will be passed to `observerCallback`.
        Will hold one connection indefinetely"""
        d = self.fetch()
        d.addCallback(_connAddObserver, observerCallback, channels)

    def fetch(self):
        """Returns a deferred that will fire its callback with a connection when
        the latter is ready. Can be used if more detailed control of connection
        fetching is needed. Otherwise, use the `runQuery` method."""
        raise NotImplementedError

    def putback(self, conn):
        """Call to put a connection back to the pool explicitly."""
        raise NotImplementedError

    def closeAll(self):
        """Close all connections in the pool. If there is a client waiting its
        errback will fire"""
        raise NotImplementedError


class NullPgPool(BasePgPool):
    """Does not do any pooling, provides direct connection to one single client at a time"""

    def __init__(self, **kwargs):
        self._params = kwargs
        self.ready = True
        self._waitingForConn = deque()

    def fetch(self):
        d = Deferred()

        if not self.ready:
            self._waitingForConn.append(d)
            return d

        # available for connecting
        self.ready = False

        internal = connect(**self._params)
        internal.addCallback(d.callback)

        @internal.addErrback
        def onConnError(failure):
            self.ready = True
            d.errback(failure)

        return d

    def putback(self, conn):
        conn.close()
        self._next()

    def _next(self):
        if self._waitingForConn:
            d = self._waitingForConn.popleft()

            internal = connect(**self._params)
            internal.addCallback(d.callback)

            @internal.addErrback
            def onConnError(failure):
                d.errback(failure)
                self._next()

        else:
            self.ready = True

    def closeAll(self):
        failure = Failure('Connection pool closed', ValueError)
        while self._waitingForConn:
            d = self._waitingForConn.popleft()
            d.errback(failure)

class StaticPgPool(BasePgPool):
    """Maintains only one connection, the clients must wait for it"""

    def __init__(self, **kwargs):
        self._params = kwargs
        self.conn = None
        self.ready = True
        self._waitingForConn = deque()

    def fetch(self):
        if self.conn is not None:
            if self.ready and self.conn is not False:
                # connected and available
                self.ready = False
                return succeed(self.conn)

        # either connected or connecting
        d = Deferred()
        self._waitingForConn.append(d)

        if self.conn is None:
            # set as connecting
            self.conn = False
            internal = connect(**self._params)
            internal.addCallback(self.onConnReady)
            internal.addErrback(d.errback)

        return d

    def onConnReady(self, conn):
        self.conn = conn
        self.ready = False
        d = self._waitingForConn.popleft()
        d.callback(conn)

    def putback(self, conn):
        if self._waitingForConn:
            d = self._waitingForConn.popleft()
            d.callback(conn)

        else:
            self.ready = True

    def closeAll(self):
        if self.ready:
            self.conn.close()
            self.conn = None

        failure = Failure('Connection pool closed', ValueError)
        while self._waitingForConn:
            d = self._waitingForConn.popleft()
            d.errback(failure)


class QueuePgPool(BasePgPool):
    """MinMax connection pool. Tries to maintain `min` number of ready connections.
    If there are more clients will connect on demand but won't exceed `max` """

    def __init__(self, minConn, maxConn, **kwargs):
        self._params = kwargs
        self._min = minConn
        self._max = maxConn
        self._available = set()
        self._numBusy = 0
        self._waitingForConn = deque()

    def fetch(self):
        if len(self._available) > 0:
            conn = self._available.pop()
            self._numBusy = self._numBusy + 1
            return succeed(conn)

        # no available connections

        if self._numBusy == self._max:
            # overloaded
            d = Deferred()
            self._waitingForConn.append(d)
            return d

        # connections busy but can make new one
        self._numBusy = self._numBusy + 1

        internal = connect(**self._params)

        @internal.addErrback
        def onConnError(failure):
            self._numBusy = self._numBusy - 1
            internal.errback()

        return internal

    def putback(self, conn):
        if self._waitingForConn:
            d = self._waitingForConn.popleft()
            d.callback(conn)

        else:
            self._numBusy = self._numBusy - 1

            if len(self._available) == self._min:
                # min ready connections satisfied
                # close this one as excessive
                conn.close()

            else:
                self._available.add(conn)

    def closeAll(self):
        while self._available:
            conn = self._available.pop()
            conn.close()

        failure = Failure('Connection pool closed', ValueError)
        while self._waitingForConn:
            d = self._waitingForConn.popleft()
            d.errback(failure)
