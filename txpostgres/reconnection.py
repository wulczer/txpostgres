"""
Reconnection support for txpostgres.
"""
from __future__ import absolute_import

from twisted.internet import defer
from twisted.python import log

from txpostgres import retrying
from txpostgres.psycopg2_impl import psycopg2
from txpostgres.txpostgres import RollbackFailed


class ConnectionDead(Exception):
    """
    The connection is dead.
    """


def defaultDeathChecker(f):
    """
    Checker function suitable for use with
    :class:`.DeadConnectionDetector`.
    """
    return f.check(psycopg2.InterfaceError, psycopg2.OperationalError,
                   RollbackFailed)


def defaultReconnectionIterator():
    """
    A function returning sane defaults for a reconnection iterator, for use
    with :class:`.DeadConnectionDetector`.

    The defaults have maximum reconnection delay capped at 10 seconds and no
    limit on the number of retries.
    """
    return retrying.simpleBackoffIterator(
        initialDelay=1.0, maxDelay=10.0, factor=1.7, maxRetries=0, now=True)


class DeadConnectionDetector(object):
    """
    A class implementing reconnection strategy. When the connection is
    discovered to be dead, it will start the reconnection process.

    The object being reconnected should proxy all operations through the
    detector's :meth:`.callChecking` which will automatically fail them if the
    connection is currently dead. This is done to prevent sending requests to a
    resource that's not currently available.

    When an instance of :class:`~txpostgres.txpostgres.Connection` is passed a
    :class:`.DeadConnectionDetector` it automatically starts using it to
    provide reconnection.

    Another way of using this class is manually calling
    :meth:`.checkForDeadConnection` passing it a :tm:`Failure
    <python.failure.Failure>` instance to trigger reconnection. This is useful
    to handle initial connection errors, for example::

        conn = txpostgres.Connection(detector=DeadConnectionDetector())
        d = conn.connect('dbname=test')
        d.addErrback(conn.detector.checkForDeadConnection)

    :var reconnectable: An object to be reconnected. It should provide a
        `connect` and a `close` method.
    :vartype reconnectable: :class:`object`

    :var connectionIsDead: If the connection is currently believed to be dead.
    :vartype connectionIsDead: :class:`bool`
    """
    reconnectable = None
    connectionIsDead = None

    def __init__(self, deathChecker=None,
                 reconnectionIterator=None, reactor=None):
        """
        Create a new detector.

        :param deathChecker: A one-argument callable that will be called with a
            failure instance and should return True if reconnection should be
            triggered. If :class:`None` then :func:`.defaultDeathChecker` will
            be used.
        :type deathChecker: callable

        :param reconnectionIterator: A zero-argument callable that should
            return a iterator yielding reconnection delay periods. If
            :class:`None` then :func:`.defaultReconnectionIterator` will be
            used.
        :type reconnectionIterator: callable

        :param reactor: A Twisted reactor or :class:`None`, which means
            the current reactor.
        """
        self.deathChecker = deathChecker or defaultDeathChecker
        self.reconnectionIterator = (reconnectionIterator or
                                     defaultReconnectionIterator)

        if not reactor:
            from twisted.internet import reactor

        self.reactor = reactor
        self.connectionIsDead = False
        self._recoveryHandlers = set()

    def setReconnectable(self, reconnectable, *connargs, **connkw):
        """
        Register a reconnectable with the detector. Needs to be called before
        the detector will be used. The remaining arguments will be passed to
        the reconnectable's `connect` method on each reconnection.

        :param reconnectable: An object to be reconnected. It should provide a
            `connect` and a `close` method.
        :type reconnectable: :class:`object`
        """
        self.reconnectable = reconnectable
        self._connargs = connargs
        self._connkw = connkw

    def callChecking(self, method, *args, **kwargs):
        """
        Call a method if the connection is still alive.
        """
        # the connection is already dead and a reconnect is underway
        if self.connectionIsDead:
            return defer.fail(ConnectionDead())

        # call the method and check if the connection died
        d = defer.maybeDeferred(method, *args, **kwargs)
        return d.addErrback(self.checkForDeadConnection)

    def checkForDeadConnection(self, f):
        """
        Get passed a :tm:`Failure <python.failure.Failure>` instance and
        determine if it means that the connection is dead. If so, start
        reconnecting.
        """
        # if the error does not indicate that the connection is dead, just
        # return the failure
        if not self.deathChecker(f):
            return f

        # if we already know that the connection is dead, we just need to wait
        if self.connectionIsDead:
            return f

        # we detected that the connection died, start the reconnection process
        self.connectionIsDead = True
        self.startReconnecting(f)

        # return the original failure, we never want to swallow errors
        return f

    def startReconnecting(self, f):
        """
        Called when the connection is detected to be dead.
        """
        # set up a retrying reconnecting call and start it
        rc = retrying.RetryingCall(self.reconnect)
        rc.reactor = self.reactor

        d = rc.start(self.reconnectionIterator())
        d.addCallback(lambda _: self.connectionRecovered())
        # the reconnection should never fail (it doesn't with the default
        # iterator), but buggy recovery handlers and custom iterators might
        # cause that, so just log the error and swallow it
        d.addErrback(log.err)

    def reconnect(self):
        """
        Called on each attempt of reconnection.
        """
        # if the connection is down even closing it might cause error, but
        # then they should be safe to ignore (probably it's already closed)
        try:
            self.reconnectable.close()
        except:
            pass
        # reuse the stored connection arguments
        return self.reconnectable.connect(
            *self._connargs, **self._connkw)

    def connectionRecovered(self):
        """
        Called when the connection has recovered.
        """
        self.connectionIsDead = False

        dl = []
        for handler in self.getRecoveryHandlers():
            d = defer.maybeDeferred(handler)
            d.addErrback(log.err)
            dl.append(d)

        return defer.gatherResults(dl)

    def addRecoveryHandler(self, handler):
        """
        Add a handler function that will get called whenever the connection is
        recovered. Any number of handlers can be added. Adding a handler that's
        already been added is ignored.

        Recovery handlers are ran in parallel. If any of them return a
        :d:`Deferred`, recovery will wait until it fires.

        There are no guarantees as to the order in which handler functions are
        called. Exceptions in handlers are logged and discarded.

        :param handler: A zero-argument callable.
        """
        self._recoveryHandlers.add(handler)

    def removeRecoveryHandler(self, handler):
        """
        Remove a previously added recovery handler. Removing a handler that's
        never been added will be ignored.

        :param handler: A callable that should no longer be called when the
            connection recovers.
        """
        self._recoveryHandlers.discard(handler)

    def getRecoveryHandlers(self):
        """
        Get the currently registered recovery handlers.

        :return: A set of callables that will get called on recovery.
        :rtype: :class:`set`
        """
        return set(self._recoveryHandlers)
