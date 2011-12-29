# -*- test-case-name: test.test_txpostgres -*-
# Copyright (c) 2010 Jan Urbanski.
# See LICENSE for details.

"""
A Twisted wrapper for the asynchronous features of the PostgreSQL psycopg2
driver.
"""

try:
    import psycopg2
except ImportError:
    # try psycopg2-ctypes
    import psycopg2ct as psycopg2

from zope.interface import implements

from twisted.internet import interfaces, main, defer
from twisted.python import failure, log


try:
    psycopg2.extensions.POLL_OK
except AttributeError:
    import warnings
    warnings.warn(RuntimeWarning(
            "psycopg2 does not have async support. "
            "You need at least version 2.2.0 of psycopg2 "
            "to use txpostgres."))


class UnexpectedPollResult(Exception):
    """
    Polling returned an unexpected result.
    """


class AlreadyPolling(Exception):
    """
    The previous poll cycle has not been finished yet.

    This probably indicates an issue in txpostgres, rather than in user code.
    """


class _CancelInProgress(Exception):
    """
    A query cancellation is in progress.
    """


class _PollingMixin(object):
    """
    An object that wraps something pollable. It can take care of waiting for
    the wrapped pollable to reach the OK state and adapts the pollable's
    interface to U{interfaces.IReadWriteDescriptor}. It will forward all
    attribute access that is has not been wrapped to the underlying
    pollable. Useful as a mixin for classes that wrap a psycopg2 pollable
    object.

    @type reactor: A U{interfaces.IReactorFDSet} provider.
    @ivar reactor: The reactor that the class will use to wait for the wrapped
        pollable to reach the OK state.

    @type prefix: C{str}
    @ivar prefix: Prefix used during log formatting to indicate context.
    """

    implements(interfaces.IReadWriteDescriptor)

    reactor = None
    prefix = "pollable"
    _pollingD = None

    def pollable(self):
        """
        Return the pollable object. Subclasses should override this.

        @return: A psycopg2 pollable.
        """
        raise NotImplementedError()

    def poll(self):
        """
        Start polling the wrapped pollable.

        @rtype: C{Deferred}
        @return: A Deferred that will fire with an instance of this class when
            the pollable reaches the OK state.
        """
        # this should never be called while the previous Deferred is still
        # active, as it would clobber its reference
        if self._pollingD:
            return defer.fail(AlreadyPolling())

        ret = self._pollingD = defer.Deferred(self._cancel)
        # transform a psycopg2 QueryCanceledError into CancelledError
        self._pollingD.addErrback(self._handleCancellation)

        self.continuePolling()

        return ret

    def continuePolling(self, swallowErrors=False):
        """
        Move forward in the poll cycle. This will call poll() on the wrapped
        pollable and either wait for more I/O or callback or errback the
        C{Deferred} returned earlier if the polling cycle has been
        completed.

        @type swallowErrors: C{bool}
        @param swallowErrors: Should errors with no one to report them to be
            ignored.
        """
        # This method often gets called from the reactor's doRead/doWrite
        # handlers. Don't callback or errback the polling Deferred here, as
        # arbitrary user code can be run by that and we don't want to deal with
        # reentrancy issues if this user code tries running queries. The
        # polling Deferred might also be simply not present, if we got called
        # from a doRead after receiving a NOTIFY event.

        try:
            state = self.pollable().poll()
        except:
            if self._pollingD:
                d, self._pollingD = self._pollingD, None
                self.reactor.callLater(0, d.errback, failure.Failure())
            elif not swallowErrors:
                # no one to report the error to
                raise
        else:
            if state == psycopg2.extensions.POLL_OK:
                if self._pollingD:
                    d, self._pollingD = self._pollingD, None
                    self.reactor.callLater(0, d.callback, self)
            elif state == psycopg2.extensions.POLL_WRITE:
                self.reactor.addWriter(self)
            elif state == psycopg2.extensions.POLL_READ:
                self.reactor.addReader(self)
            else:
                if self._pollingD:
                    d, self._pollingD = self._pollingD, None
                    self.reactor.callLater(0, d.errback, UnexpectedPollResult())
                elif not swallowErrors:
                    # no one to report the error to
                    raise UnexpectedPollResult()

    def doRead(self):
        self.reactor.removeReader(self)
        if not self.pollable().closed:
            self.continuePolling()

    def doWrite(self):
        self.reactor.removeWriter(self)
        if not self.pollable().closed:
            self.continuePolling()

    def logPrefix(self):
        return self.prefix

    def fileno(self):
        # this should never get called after the pollable has been
        # disconnected, but Twisted versions affected by bug #4539 might cause
        # it to happen, in which case we should return -1
        if self.pollable().closed:
            return -1

        return self.pollable().fileno()

    def connectionLost(self, reason):
        # Do not errback self._pollingD here if the connection is still open!
        # We need to keep on polling until it reports an error, which will
        # errback self._pollingD with the correct failure. If we errback here,
        # we won't finish the polling cycle, which would leave psycopg2 in a
        # state where it thinks there's still an async query underway.
        #
        # If the connection got lost right after the first poll(), the Deferred
        # returned from it will never fire, leaving the caller hanging forever,
        # unless we push the connection state forward here. OTOH, if the
        # connection is already closed, there's no pollable to poll, so if
        # self._pollingD is still present, the only option is to errback it to
        # prevent its waiters from hanging (you can't poll() a closed psycopg2
        # connection)
        if not self.pollable().closed:
            # we're pushing the polling cycle to report pending failures, so if
            # there's no one to report them to, swallow them
            self.continuePolling(swallowErrors=True)
        elif self._pollingD:
            d, self._pollingD = self._pollingD, None
            d.errback(reason)

    def _cancel(self, d):
        try:
            self.pollable().cancel()
        except AttributeError:
            # the pollable has no cancellation support, ignore
            pass
        # prevent Twisted from errbacking the deferred being cancelled, because
        # the PostgreSQL protocol requires finishing the entire polling process
        # before reusing the connection
        raise _CancelInProgress()

    def _handleCancellation(self, f):
        f.trap(psycopg2.extensions.QueryCanceledError)
        return failure.Failure(defer.CancelledError())

    # Hack required to work with the Gtk2 reactor in Twisted <=11.0, which
    # tries to access the "disconnected" property on the IReadWriteDescriptor
    # it polls. To avoid attribute errors, forward that access to the "closed"
    # property of the underlying connection.
    def disconnected(self):
        return self.pollable().closed
    disconnected = property(disconnected)

    # forward all other access to the underlying connection
    def __getattr__(self, name):
        return getattr(self.pollable(), name)


class Cursor(_PollingMixin):
    """
    A wrapper for a psycopg2 asynchronous cursor.

    The wrapper will forward almost everything to the wrapped cursor, so the
    usual DB-API interface can be used, but will take care of preventing
    concurrent execution of asynchronous queries, which the PostgreSQL C
    library does not support and will return Deferreds for some operations.
    """

    def __init__(self, cursor, connection):
        self.reactor = connection.reactor
        self.prefix = "cursor"

        self._connection = connection
        self._cursor = cursor

    def pollable(self):
        return self._connection.pollable()

    def execute(self, query, params=None):
        """
        A regular DB-API execute, but returns a Deferred.

        The caller must be careful not to call this method twice on cursors
        from the same connection without waiting for the previous execution to
        complete.

        @rtype: C{Deferred}
        @return: A C{Deferred} that will fire with the results of the
            execute().
        """
        return self._doit('execute', query, params)

    def callproc(self, procname, params=None):
        """
        A regular DB-API callproc, but returns a Deferred.

        The caller must be careful not to call this method twice on cursors
        from the same connection without waiting for the previous execution to
        complete.

        @rtype: C{Deferred}
        @return: A C{Deferred} that will fire with the results of the
            callproc().
        """
        return self._doit('callproc', procname, params)

    def _doit(self, name, *args, **kwargs):
        try:
            getattr(self._cursor, name)(*args, **kwargs)
        except:
            return defer.fail()

        # tell the connection that a cursor is starting its poll cycle
        self._connection.cursorRunning(self)

        def finishedAndPassthrough(ret):
            # tell the connection that the poll cycle has finished
            self._connection.cursorFinished(self)
            return ret

        d = self.poll()
        return d.addBoth(finishedAndPassthrough)

    def close(self):
        return self._cursor.close()

    def __getattr__(self, name):
        # the pollable is the connection, but the wrapped object is the cursor
        return getattr(self._cursor, name)


class AlreadyConnected(Exception):
    """
    The database connection is already open.
    """


class RollbackFailed(Exception):
    """
    Rolling back the transaction failed, the connection might be in an unusable
    state.

    @type connection: L{Connection}
    @ivar connection: The connection that failed to roll back its transaction.

    @type originalFailure: L{failure.Failure}
    @ivar originalFailure: The failure that caused the connection to try to
        roll back the transaction.
    """

    def __init__(self, connection, originalFailure):
        self.connection = connection
        self.originalFailure = originalFailure

    def __str__(self):
        return "<RollbackFailed, original error: %s>" % self.originalFailure


class Connection(_PollingMixin):
    """
    A wrapper for a psycopg2 asynchronous connection.

    The wrapper forwards almost everything to the wrapped connection, but
    provides additional methods for compatibility with C{adbapi.Connection}.

    @type connectionFactory: Any callable.
    @ivar connectionFactory: The factory used to produce connections.

    @type cursorFactory: Any callable.
    @ivar cursorFactory: The factory used to produce cursors.
    """

    connectionFactory = staticmethod(psycopg2.connect)
    cursorFactory = Cursor

    def __init__(self, reactor=None):
        if not reactor:
            from twisted.internet import reactor
        self.reactor = reactor
        self.prefix = "connection"

        # this lock will be used to prevent concurrent query execution
        self.lock = defer.DeferredLock()
        self._connection = None
        # a set of cursors that should be notified about a disconnection
        self._cursors = set()
        # observers for NOTIFY events
        self._notifyObservers = set()

    def pollable(self):
        return self._connection

    def connect(self, *args, **kwargs):
        """
        Connect to the database.

        Positional arguments will be passed to the psycop2.connect()
        method. Use them to pass database names, usernames, passwords, etc.

        @rtype: C{Deferred}
        @returns: A Deferred that will fire when the connection is open.
        """
        if self._connection and not self._connection.closed:
            return defer.fail(AlreadyConnected())

        kwargs['async'] = True
        try:
            self._connection = self.connectionFactory(*args, **kwargs)
        except:
            return defer.fail()

        def startReadingAndPassthrough(ret):
            self.reactor.addReader(self)
            return ret

        # The connection is always a reader in the reactor, to receive NOTIFY
        # events immediately when they're available.
        d = self.poll()
        return d.addCallback(startReadingAndPassthrough)

    def close(self):
        """
        Close the connection and disconnect from the database.
        """
        # We'll be closing the underlying socket so stop watching it.
        self.reactor.removeReader(self)
        self.reactor.removeWriter(self)

        # make it safe to call Connection.close() multiple times, psycopg2
        # treats this as an error but we don't
        if not self._connection.closed:
            self._connection.close()

        # The above closed the connection socket from C code. Normally we would
        # get connectionLost called on all readers and writers of that socket,
        # but not if we're using the epoll reactor. According to the epoll(2)
        # man page, closing a file descriptor causes it to be removed from all
        # epoll sets automatically. In that case, the reactor might not have
        # the chance to notice that the connection has been closed. To cover
        # that, call connectionLost explicitly on the Connection and all
        # outstanding Cursors. It's OK if connectionLost ends up being called
        # twice, as the second call will not have any effects.

        for cursor in set(self._cursors):
            cursor.connectionLost(main.CONNECTION_DONE)

        self.connectionLost(main.CONNECTION_DONE)

    def cursor(self):
        """
        Create an asynchronous cursor.
        """
        return self.cursorFactory(self._connection.cursor(), self)

    def runQuery(self, *args, **kwargs):
        """
        Execute an SQL query and return the result.

        An asynchronous cursor will be created and its execute() method will
        be invoked with the provided *args and **kwargs. After the query
        completes the cursor's fetchall() method will be called and the
        returned Deferred will fire with the result.

        The connection is always in autocommit mode, so the query will be run
        in a one-off transaction. In case of errors a Failure will be returned.

        It is safe to call this method multiple times without waiting for the
        first query to complete.

        @rtype: C{Deferred}
        @return: A Deferred that will fire with the return value of the
            cursor's fetchall() method.
        """
        return self.lock.run(self._runQuery, *args, **kwargs)

    def _runQuery(self, *args, **kwargs):
        c = self.cursor()
        d = c.execute(*args, **kwargs)
        d.addCallback(lambda c: c.fetchall())
        return d.addCallback(lambda ret: (c.close(), ret)[1])

    def runOperation(self, *args, **kwargs):
        """
        Execute an SQL query and return the result.

        Identical to runQuery, but the cursor's fetchall() method will not be
        called and instead None will be returned. It is intended for statements
        that do not normally return values, like INSERT or DELETE.

        It is safe to call this method multiple times without waiting for the
        first query to complete.

        @rtype: C{Deferred}
        @return: A Deferred that will fire None.
        """
        return self.lock.run(self._runOperation, *args, **kwargs)

    def _runOperation(self, *args, **kwargs):
        c = self.cursor()
        d = c.execute(*args, **kwargs)
        d.addCallback(lambda _: None)
        return d.addCallback(lambda ret: (c.close(), ret)[1])

    def runInteraction(self, interaction, *args, **kwargs):
        """
        Run commands in a transaction and return the result.

        The 'interaction' is a callable that will be passed a
        C{txpostgres.Cursor} object. Before calling 'interaction' a new
        transaction will be started, so the callable can assume to be running
        all its commands in a transaction. If 'interaction' returns a
        C{Deferred} processing will wait for it to fire before proceeding.

        After 'interaction' finishes work the transaction will be automatically
        committed. If it raises an exception or returns a C{Failure} the
        connection will be rolled back instead.

        If committing the transaction fails it will be rolled back instead and
        the C{Failure} obtained trying to commit will be returned.

        If rolling back the transaction fails the C{Failure} obtained from the
        rollback attempt will be logged and a C{RollbackFailed} failure will be
        returned. The returned failure will contain references to the original
        C{Failure} that caused the transaction to be rolled back and to the
        C{Connection} in which that happend, so the user can take a decision
        whether she still wants to be using it or just close it, because an
        open transaction might have been left open in the database.

        It is safe to call this method multiple times without waiting for the
        first query to complete.

        @type interaction: Any callable
        @param interaction: A callable whose first argument is a
            L{txpostgres.Cursor}.

        @rtype: C{Deferred}
        @return: A Deferred that will fire with the return value of
            'interaction'.
        """
        return self.lock.run(
            self._runInteraction, interaction, *args, **kwargs)

    def _runInteraction(self, interaction, *args, **kwargs):
        c = self.cursor()
        d = c.execute("begin")
        d.addCallback(interaction, *args, **kwargs)

        def commitAndPassthrough(ret, cursor):
            e = cursor.execute("commit")
            return e.addCallback(lambda _: ret)

        def rollbackAndPassthrough(f, cursor):
            # maybeDeferred in case cursor.execute raises a synchronous
            # exception
            e = defer.maybeDeferred(cursor.execute, "rollback")

            def justPanic(rf):
                log.err(rf)
                return defer.fail(RollbackFailed(self, f))

            # if rollback failed panic
            e.addErrback(justPanic)
            # reraise the original failure afterwards
            return e.addCallback(lambda _: f)

        d.addCallback(commitAndPassthrough, c)
        d.addErrback(rollbackAndPassthrough, c)
        d.addCallback(lambda ret: (c.close(), ret)[1])

        return d

    def cancel(self, d):
        """
        Cancel the current operation. The cancellation does not happen
        immediately, because the PostgreSQL protocol requires that the
        application waits for confirmation after the query has been cancelled.
        Cancelling an interaction is tricky, because if the interaction
        includes sending multiple queries to the database server, you can't
        really be sure which one are you cancelling.

        @type d: C{Deferred}
        @param d: a Deferred returned by one of L{txpostgres.Connection}
            methods.
        """
        try:
            d.cancel()
        except _CancelInProgress:
            pass

    def cursorRunning(self, cursor):
        """
        Called automatically when a L{txpostgres.Cursor} created by this
        L{txpostgres.Connection} starts polling after executing a query. User
        code should never have to call this method.
        """
        # The cursor will now proceed to poll the psycopg2 connection, so stop
        # polling it ourselves until it's done. Failure to do so would result
        # in the connection "stealing" the POLL_OK result that appears after
        # the query is completed and the Deferred returned from the cursor's
        # poll() will never fire.
        self.reactor.removeReader(self)
        self._cursors.add(cursor)

    def cursorFinished(self, cursor):
        """
        Called automatically when a L{txpostgres.Cursor} created by this
        L{txpostgres.Connection} is done with polling after executing a
        query. User code should never have to call this method.
        """
        self._cursors.remove(cursor)
        # The cursor is done polling, resume watching the connection for NOTIFY
        # events. Be careful to check the connection state, because it might
        # have been closed while the cursor was polling and adding ourselves as
        # a reader to a closed connection would be an error.
        if not self._connection.closed:
            self.reactor.addReader(self)
            # While cursor was running, some notifies could have been
            # delivered, so check for them.
            self.checkForNotifies()

    def doRead(self):
        # call superclass to handle the pending read event on the socket
        _PollingMixin.doRead(self)

        # check for NOTIFY events
        self.checkForNotifies()

        # continue watching for NOTIFY events, but be careful to check the
        # connection state in case one of the notify handler function caused a
        # disconnection
        if not self._connection.closed:
            self.reactor.addReader(self)

    def checkForNotifies(self):
        """
        Check if NOTIFY events have been received and if so, dispatch them to
        the registered observers. This is done automatically, user code should
        never need to call this method.
        """
        while self._connection.notifies:
            notify = self._connection.notifies.pop()
            # don't iterate over self._notifyObservers directly because the
            # observer function might call removeNotifyObserver, thus modifying
            # the set while it's being iterated
            for observer in self.getNotifyObservers():
                # don't allow exceptions from observers to propagate, as this
                # method is called from doRead -- exceptions here would lead to
                # a disconnect
                try:
                    observer(notify)
                except:
                    log.err()

    def addNotifyObserver(self, observer):
        """
        Add an observer function that will get called whenever a NOTIFY event
        is delivered to this connection. Any number of observers can be added
        to a connection. Adding an observer that's already been added is
        ignored.

        @type observer: Any callable
        @param observer: A callable whose first argument is a
            L{psycopg2.extensions.Notify}.
        """
        self._notifyObservers.add(observer)

    def removeNotifyObserver(self, observer):
        """
        Remove a previously added observer function. Removing an observer
        that's never been added will be ignored.

        @type observer: Any callable
        @param observer: A callable that should no longer be called on NOTIFY
            events.
        """
        self._notifyObservers.discard(observer)

    def getNotifyObservers(self):
        """
        Get the currently registered notify observers.

        @rtype: C{set}
        @return: An set of callables that will get called on NOTIFY events.
        """
        return set(self._notifyObservers)


class ConnectionPool(object):
    """
    A poor man's pool of L{txpostgres.Connection} instances.

    @type min: C{int}
    @ivar min: The amount of connections that will be open at start. The pool
        never opens or closes connections on its own.

    @type connectionFactory: Any callable.
    @ivar connectionFactory: The factory used to produce connections.
    """

    min = 3
    connectionFactory = Connection
    reactor = None

    def __init__(self, _ignored, *connargs, **connkw):
        """
        Create a new connection pool.

        Any positional or keyword arguments other than the first one and a
        'min' keyword argument are passed to the L{Connection} when
        connecting. Use these arguments to pass database names, usernames,
        passwords, etc.

        @type _ignored: Any object.
        @param _ignored: Ignored, for L{adbapi.ConnectionPool} compatibility.
        """
        if not self.reactor:
            from twisted.internet import reactor
            self.reactor = reactor
        # for adbapi compatibility, min can be passed in kwargs
        if 'min' in connkw:
            self.min = connkw.pop('min')
        self.connargs = connargs
        self.connkw = connkw
        self.connections = set(
            [self.connectionFactory(self.reactor) for _ in range(self.min)])

        # to avoid checking out more connections than there are pooled in total
        self._semaphore = defer.DeferredSemaphore(self.min)

    def start(self):
        """
        Start the connection pool.

        This will create as many connections as the pool's 'min' variable says.

        @rtype: C{Deferred}
        @return: A C{Deferred} that fires when all connection have succeeded.
        """
        # use DeferredList here, as gatherResults only got a consumeErrors
        # keyword argument in Twisted 11.1.0
        d = defer.DeferredList([c.connect(*self.connargs, **self.connkw)
                                 for c in self.connections],
                               fireOnOneErrback=True, consumeErrors=True)
        return d.addCallback(lambda _: self)

    def close(self):
        """
        Stop the pool.

        Disconnect all connections.
        """
        for c in self.connections:
            c.close()

    def remove(self, connection):
        """
        Remove a connection from the pool.

        Provided to be able to remove broken connections from the pool. The
        caller should make sure the removed connection does not have queries
        pending.

        @type connection: An object produced by the pool's connection factory.
        @param connection: The connection to be removed.
        """
        if not self.connections:
            raise ValueError("Connection still in use")
        self.connections.remove(connection)
        self._semaphore.limit -= 1
        self._semaphore.acquire()  # bleargh...

    def add(self, connection):
        """
        Add a connection to the pool.

        Provided to be able to extend the pool with new connections.

        @type connection: An object compatible with those produce by the pool's
            connection factory.
        @param connection: The connection to be added.
        """
        self.connections.add(connection)
        self._semaphore.limit += 1
        self._semaphore.release()  # uuuugh...

    def _putBackAndPassthrough(self, result, connection):
        self.connections.add(connection)
        return result

    def runQuery(self, *args, **kwargs):
        """
        Execute an SQL query and return the result.

        An asynchronous cursor will be created from a randomly chosen pooled
        connection and its execute() method will be invoked with the provided
        *args and **kwargs. After the query completes the cursor's fetchall()
        method will be called and the returned Deferred will fire with the
        result.

        The connection is always in autocommit mode, so the query will be run
        in a one-off transaction. In case of errors a Failure will be returned.

        @rtype: C{Deferred}
        @return: A Deferred that will fire with the return value of the
            cursor's fetchall() method.
        """
        return self._semaphore.run(self._runQuery, *args, **kwargs)

    def _runQuery(self, *args, **kwargs):
        c = self.connections.pop()
        d = c.runQuery(*args, **kwargs)
        return d.addBoth(self._putBackAndPassthrough, c)

    def runOperation(self, *args, **kwargs):
        """
        Execute an SQL query and return the result.

        Identical to runQuery, but the cursor's fetchall() method will not be
        called and instead None will be returned. It is intended for statements
        that do not normally return values, like INSERT or DELETE.

        @rtype: C{Deferred}
        @return: A Deferred that will fire None.
        """
        return self._semaphore.run(self._runOperation, *args, **kwargs)

    def _runOperation(self, *args, **kwargs):
        c = self.connections.pop()
        d = c.runOperation(*args, **kwargs)
        return d.addBoth(self._putBackAndPassthrough, c)

    def runInteraction(self, interaction, *args, **kwargs):
        """
        Run commands in a transaction and return the result.

        The 'interaction' is a callable that will be passed a
        C{txpostgres.Cursor} object. Before calling 'interaction' a new
        transaction will be started, so the callable can assume to be running
        all its commands in a transaction. If 'interaction' returns a
        C{Deferred} processing will wait for it to fire before proceeding.

        After 'interaction' finishes work the transaction will be automatically
        committed. If it raises an exception or returns a C{Failure} the
        connection will be rolled back instead.

        If committing the transaction fails it will be rolled back instead and
        the C{Failure} obtained trying to commit will be returned.

        If rolling back the transaction fails the C{Failure} obtained from the
        rollback attempt will be logged and a C{RollbackFailed} failure will be
        returned. The returned failure will contain references to the original
        C{Failure} that caused the transaction to be rolled back and to the
        C{Connection} in which that happend, so the user can take a decision
        whether she still wants to be using it or just close it, because an
        open transaction might have been left open in the database.

        @type interaction: Any callable
        @param interaction: A callable whose first argument is a
            L{txpostgres.Cursor}.

        @rtype: C{Deferred}
        @return: A Deferred that will file with the return value of
            'interaction'.
        """
        return self._semaphore.run(
            self._runInteraction, interaction, *args, **kwargs)

    def _runInteraction(self, interaction, *args, **kwargs):
        c = self.connections.pop()
        d = c.runInteraction(interaction, *args, **kwargs)
        return d.addBoth(self._putBackAndPassthrough, c)
