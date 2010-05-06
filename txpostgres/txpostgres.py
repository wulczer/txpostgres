# -*- test-case-name: test.test_txpostgres -*-
# Copyright (c) 2010 Jan Urbanski.
# See LICENSE for details.

"""
A Twisted wrapper for the asynchronous features of the PostgreSQL psycopg2
driver.
"""

import psycopg2
from psycopg2 import extensions
from zope.interface import implements

from twisted.internet import interfaces, reactor, defer
from twisted.python import log


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
        if not self._pollingD:
            self._pollingD = defer.Deferred()
        ret = self._pollingD

        try:
            self._pollingState = self.pollable().poll()
        except:
            d, self._pollingD = self._pollingD, None
            d.errback()
            return ret

        if self._pollingState == psycopg2.extensions.POLL_OK:
            d, self._pollingD = self._pollingD, None
            d.callback(self)
        elif self._pollingState == psycopg2.extensions.POLL_WRITE:
            self.reactor.addWriter(self)
        elif self._pollingState == psycopg2.extensions.POLL_READ:
            self.reactor.addReader(self)
        else:
            d, self._pollingD = self._pollingD, None
            d.errback(UnexpectedPollResult())

        return ret

    def doRead(self):
        self.reactor.removeReader(self)
        self.poll()

    def doWrite(self):
        self.reactor.removeWriter(self)
        self.poll()

    def logPrefix(self):
        return self.prefix

    def fileno(self):
        if self.pollable():
            return self.pollable().fileno()
        else:
            return -1

    def connectionLost(self, reason):
        if self._pollingD:
            d, self._pollingD = self._pollingD, None
            d.errback(reason)

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
        return self._cursor.connection

    def execute(self, query, params=None):
        """
        A regular DB-API execute, but returns a Deferred.

        @rtype: C{Deferred}
        @return: A C{Deferred} that will fire with the results of the
            execute().
        """
        return self._connection.lock.run(
            self._doit, 'execute', query, params)

    def callproc(self, procname, params=None):
        """
        A regular DB-API callproc, but returns a Deferred.

        @rtype: C{Deferred}
        @return: A C{Deferred} that will fire with the results of the
            callproc().
        """
        return self._connection.lock.run(
            self._doit, 'callproc', procname, params)

    def _doit(self, name, *args, **kwargs):
        try:
            getattr(self._cursor, name)(*args, **kwargs)
        except:
            return defer.fail()

        return self.poll()

    def close(self):
        _cursor, self._cursor = self._cursor, None
        return _cursor.close()

    def fileno(self):
        if self._cursor and self._connection._connection:
            return self._cursor.connection.fileno()
        else:
            return -1

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

    connectionFactory = psycopg2.connect
    cursorFactory = Cursor

    def __init__(self, reactor=None):
        if not reactor:
            from twisted.internet import reactor
        self.reactor = reactor
        self.prefix = "connection"

        # this lock will be used to prevent concurrent query execution
        self.lock = defer.DeferredLock()
        self._connection = None

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
        if self._connection:
            return defer.fail(AlreadyConnected())

        kwargs['async'] = True
        try:
            self._connection = self.connectionFactory(*args, **kwargs)
        except:
            return defer.fail()

        return self.poll()

    def close(self):
        """
        Close the connection and disconnect from the database.
        """
        _connection, self._connection = self._connection, None
        _connection.close()

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

        @rtype: C{Deferred}
        @return: A Deferred that will fire with the return value of the
            cursor's fetchall() method.
        """
        c = self.cursor()
        d = c.execute(*args, **kwargs)
        return d.addCallback(lambda c: c.fetchall())

    def runOperation(self, *args, **kwargs):
        """
        Execute an SQL query and return the result.

        Identical to runQuery, but the cursor's fetchall() method will not be
        called and instead None will be returned. It is intended for statements
        that do not normally return values, like INSERT or DELETE.

        @rtype: C{Deferred}
        @return: A Deferred that will fire None.
        """
        c = self.cursor()
        d = c.execute(*args, **kwargs)
        return d.addCallback(lambda _: None)

    def runInteraction(self, interaction, *args, **kwargs):
        """
        Run commands in a transaction and return the result.

        The 'interaction' is a callable that will be passed a
        C{pgadbapi.Cursor} object. Before calling 'interaction' a new
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
            L{pgadbapi.Cursor}.

        @rtype: C{Deferred}
        @return: A Deferred that will file with the return value of
            'interaction'.
        """
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
            def just_panic(rf):
                log.err(rf)
                return defer.fail(RollbackFailed(self, f))
            # if rollback failed panic
            e.addErrback(just_panic)
            # reraise the original failure afterwards
            return e.addCallback(lambda _: f)
        d.addCallback(commitAndPassthrough, c)
        d.addErrback(rollbackAndPassthrough, c)

        return d


class ConnectionPool(object):
    """
    A poor man's pool of L{pgadbapi.Connection} instances.

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
        d = defer.gatherResults([c.connect(*self.connargs, **self.connkw)
                                 for c in self.connections])
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
        self._semaphore.release() # uuuugh...

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
        C{pgadbapi.Cursor} object. Before calling 'interaction' a new
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
            L{pgadbapi.Cursor}.

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
