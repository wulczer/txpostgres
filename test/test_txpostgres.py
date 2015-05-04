from __future__ import absolute_import

import os

from txpostgres.psycopg2_impl import psycopg2
from txpostgres import txpostgres, reconnection

from twisted.trial import unittest
from twisted.internet import defer, error, main, posixbase, reactor, task
from twisted.python import failure

simple_table_schema = "CREATE TABLE simple (x integer)"

DB_NAME = os.getenv("TXPOSTGRES_TEST_DATABASE", "twisted_test")
DB_HOST = os.getenv("TXPOSTGRES_TEST_HOST", "127.0.0.1")
DB_USER = os.getenv("TXPOSTGRES_TEST_USER", "twisted_test")
DB_PASS = os.getenv("TXPOSTGRES_TEST_PASSWORD", "twisted_test")


def getSkipForPsycopg2():
    try:
        psycopg2.extensions.POLL_OK
    except AttributeError:
        return ("psycopg2 does not have async support. "
                "You need at least version 2.2.0 of psycopg2 "
                "to use txpostgres.")
    try:
        psycopg2.connect(user=DB_USER, password=DB_PASS,
                         host=DB_HOST, database=DB_NAME).close()
    except psycopg2.Error, e:
        return ("cannot connect to test database %r "
                "using host %r, user %r and password %r: %s" %
                (DB_NAME, DB_HOST, DB_USER, DB_PASS, e))
    return None


_skip = getSkipForPsycopg2()


class Psycopg2TestCase(unittest.TestCase):

    skip = _skip


class PollableThing(object):
    """
    A fake thing that provides a psycopg2 pollable interface.
    """
    closed = 0
    notifies = []

    def __init__(self):
        self.NEXT_STATE = psycopg2.extensions.POLL_READ

    def poll(self):
        if isinstance(self.NEXT_STATE, Exception):
            raise self.NEXT_STATE
        return self.NEXT_STATE

    def fileno(self):
        return 42


class CancellablePollableThing(PollableThing):

    cancelled = False

    def cancel(self):
        assert not self.cancelled
        self.cancelled = True


class FakeReactor(object):
    """
    A reactor that just counts how many things were added and removed.
    """
    readersAdded = 0
    writersAdded = 0
    readersRemoved = 0
    writersRemoved = 0

    def reset(self):
        self.readersAdded = self.writersAdded = 0
        self.readersRemoved = self.writersRemoved = 0

    def addReader(self, _):
        self.readersAdded += 1

    def addWriter(self, _):
        self.writersAdded += 1

    def removeReader(self, _):
        self.readersRemoved += 1

    def removeWriter(self, _):
        self.writersRemoved += 1

    def callLater(self, delay, callable, *args, **kwargs):
        callable(*args, **kwargs)


class FakeWrapper(txpostgres._PollingMixin):
    """
    A mock subclass of L{txpostgres._PollingMixin}.
    """
    reactor = FakeReactor()
    prefix = "fake-wrapper"

    def pollable(self):
        return self._pollable


class TxPostgresPollingMixinTestCase(Psycopg2TestCase):

    def test_empty(self):
        """
        The default L{txpostgres._PollingMixin} implementation raises an
        exception on pollable().
        """
        p = txpostgres._PollingMixin()
        self.assertRaises(NotImplementedError, p.pollable)

    def check(self, r, *args):
        self.assertEquals(args, (r.readersAdded, r.writersAdded,
                                 r.readersRemoved, r.writersRemoved))

    def test_polling(self):
        """
        L{txpostgres._PollingMixin} adds and removes itself from the reactor
        according to poll() results from the wrapped pollable.
        """
        p = FakeWrapper()
        p._pollable = PollableThing()
        p.reactor.reset()

        # start off with reading
        p._pollable.NEXT_STATE = psycopg2.extensions.POLL_READ
        d = p.poll()
        # after the initial poll we should get a Deferred and one reader added
        self.check(p.reactor, 1, 0, 0, 0)

        p._pollable.NEXT_STATE = psycopg2.extensions.POLL_WRITE
        p.doRead()
        # the reader should get removed and a writer should get added, because
        # we made the next poll() return POLL_WRITE
        self.check(p.reactor, 1, 1, 1, 0)

        p._pollable.NEXT_STATE = psycopg2.extensions.POLL_READ
        p.doWrite()
        # the writer is removed, a reader is added
        self.check(p.reactor, 2, 1, 1, 1)

        p._pollable.NEXT_STATE = psycopg2.extensions.POLL_READ
        p.doRead()
        # the reader is removed, but then readded because we returned POLL_READ
        self.check(p.reactor, 3, 1, 2, 1)

        p._pollable.NEXT_STATE = psycopg2.extensions.POLL_OK
        p.doRead()
        # we're done, the reader should just get removed
        self.check(p.reactor, 3, 1, 3, 1)

        # and the Deferred should succeed
        return d

    def test_interface(self):
        """
        L{txpostgres._PollingMixin} correctly provides the
        L{interfaces.IReadWriteDescriptor} interface.
        """
        p = FakeWrapper()
        p._pollable = PollableThing()

        self.assertEquals(p.fileno(), 42)
        self.assertEquals(p.logPrefix(), "fake-wrapper")

        # check if it will correctly return -1 after the connection got lost,
        # to work with Twisted affected by bug #4539
        p._pollable.closed = 1
        self.assertEquals(p.fileno(), -1)

    def test_connectionLost(self):
        """
        Calls to connectionLost() get swallowed.
        """
        p = FakeWrapper()
        p._pollable = PollableThing()
        p._pollable.NEXT_STATE = psycopg2.extensions.POLL_OK

        d = p.poll()
        p.connectionLost(failure.Failure(RuntimeError("boom")))
        p.connectionLost(failure.Failure(RuntimeError("bam")))
        return d.addCallback(self.assertEquals, p)

    def test_connectionLostWhileWaiting(self):
        """
        If the connection is lost while waiting for the socket to become
        writable, the C{Deferred} returned from poll() still errbacks.
        """
        p = FakeWrapper()
        p._pollable = PollableThing()
        p._pollable.NEXT_STATE = psycopg2.extensions.POLL_WRITE

        d = p.poll()
        p._pollable.NEXT_STATE = RuntimeError("forced poll error")
        p.connectionLost(failure.Failure(main.CONNECTION_LOST))
        return self.assertFailure(d, RuntimeError)
    test_connectionLostWhileWaiting.timeout = 5

    def test_connectionLostWhileReading(self):
        """
        If the connection is closed after the polling succeeded and after the
        socket became readable again, but not all of the data has been read
        from the socket, the closed flag is set and no errors are reported.

        This might seem elaborated, but a hypothetical error scenario is:
         * the connection is estabilished and starts watching for socket
           readability to get NOTIFY events
         * a NOTIFY comes through and doRead is triggered
         * not all data is read from the socket and it remains readable
         * the connection is closed and the Deferred returned from poll()
           called inside doRead is errbacked without being ever available to
           user code

        It has been reported that under heavy load this can happen.
        """
        p = FakeWrapper()
        p._pollable = PollableThing()

        # the connection is estabilished
        p._pollable.NEXT_STATE = psycopg2.extensions.POLL_OK
        d = p.poll()
        # doRead is called, but the socket is still readable
        p._pollable.NEXT_STATE = psycopg2.extensions.POLL_READ
        p.doRead()
        # the connection is closed
        p._pollable.closed = True
        p.connectionLost(failure.Failure(main.CONNECTION_LOST))
        # return the connection Deferred
        return d

    def test_errors(self):
        """
        Unexpected results from poll() make L{txpostgres._PollingMixin} raise
        an exception.
        """
        p = FakeWrapper()
        p._pollable = PollableThing()

        p._pollable.NEXT_STATE = "foo"
        d = p.poll()
        return self.assertFailure(d, txpostgres.UnexpectedPollResult)

    def test_cancel(self):
        """
        Cancelling a C{Deferred} returned from poll() proxies the cancellation
        to the pollable and raises C{_CancelInProgress}.
        """
        p = FakeWrapper()
        p._pollable = CancellablePollableThing()

        d = p.poll()
        self.assertRaises(txpostgres._CancelInProgress, d.cancel)
        self.assertEquals(p._pollable.cancelled, True)

    def test_noCancelSupport(self):
        """
        Cancelling a C{Deferred} returned from poll() with a pollable that does
        not support cancelling just raises C{_CancelInProgress}.
        """
        p = FakeWrapper()
        p._pollable = PollableThing()

        d = p.poll()
        self.assertRaises(txpostgres._CancelInProgress, d.cancel)
        self.assertRaises(AttributeError, getattr, p._pollable, 'cancel')
        self.assertRaises(AttributeError, getattr, p._pollable, 'cancelled')


class TxPostgresConnectionTestCase(Psycopg2TestCase):

    def test_simpleConnection(self):
        """
        Just connecting and disconnecting works.
        """
        conn = txpostgres.Connection()
        d = conn.connect(user=DB_USER, password=DB_PASS,
                         host=DB_HOST, database=DB_NAME)
        d.addCallback(lambda c: c.close())
        return d

    def test_connectionSetup(self):
        """
        The created connection should be asynchronous and in autocommit mode
        and the C{Deferred} returned from connect() should fire with the
        connection itself.
        """
        conn = txpostgres.Connection()
        d = conn.connect(user=DB_USER, password=DB_PASS,
                         host=DB_HOST, database=DB_NAME)

        def doChecks(c):
            self.assertIdentical(c, conn)
            self.assertTrue(c.async)
            self.assertEquals(c.isolation_level, 0)
            return c
        d.addCallback(doChecks)
        return d.addCallback(lambda c: c.close())

    def test_multipleConnections(self):
        """
        Trying to connect twice raises an exception, but after closing you can
        connect again.
        """
        conn = txpostgres.Connection()
        d = conn.connect(user=DB_USER, password=DB_PASS,
                         host=DB_HOST, database=DB_NAME)

        d.addCallback(lambda c: conn.connect(
            user=DB_USER, password=DB_PASS,
            host=DB_HOST, database=DB_NAME))
        d = self.failUnlessFailure(d, txpostgres.AlreadyConnected)

        d.addCallback(lambda _: conn.close())
        d.addCallback(lambda _: conn.connect(
            user=DB_USER, password=DB_PASS,
            host=DB_HOST, database=DB_NAME))
        return d.addCallback(lambda c: c.close())

    def test_errors(self):
        """
        Errors from psycopg2's poll() make connect() return failures. Errors on
        creating the psycopg2 connection too. Unexpected results from poll()
        also make connect() return a failure.
        """
        conn = txpostgres.Connection()

        def setFactory(conn, factory):
            conn.connectionFactory = factory

        class BadPollable(object):
            closed = 1

            def __init__(*args, **kwars):
                pass

            def poll(self):
                raise RuntimeError("booga")

            def close(self):
                pass

        setFactory(conn, BadPollable)

        d = conn.connect()
        d = self.assertFailure(d, RuntimeError)
        d.addCallback(lambda _: conn.close())

        class BadThing(object):
            closed = 1

            def __init__(*args, **kwargs):
                raise RuntimeError("wooga")

            def close(self):
                pass

        d.addCallback(lambda _: setFactory(conn, BadThing))

        d.addCallback(lambda _: conn.connect())
        d = self.assertFailure(d, RuntimeError)

        class BrokenPollable(object):
            closed = 1

            def __init__(*args, **kwars):
                pass

            def poll(self):
                return "tee hee hee"

            def close(self):
                pass

        d.addCallback(lambda _: setFactory(conn, BrokenPollable))

        d.addCallback(lambda _: conn.connect())
        return self.assertFailure(d, txpostgres.UnexpectedPollResult)

    def test_openRunCloseOpen(self):
        conn = txpostgres.Connection()
        connargs = dict(user=DB_USER, password=DB_PASS,
                        host=DB_HOST, database=DB_NAME)
        d = conn.connect(**connargs)
        d.addCallback(lambda _: conn.runQuery("select 1"))
        # make sure the txpostgres.Cursor created by runQuery got closed,
        # otherwise it will still be polled and will result in an error
        d.addCallback(lambda _: conn.close())
        d.addCallback(lambda _: conn.connect(**connargs))
        return d.addCallback(lambda _: conn.close())

    def test_closeTwice(self):
        """
        Calling close() on the connection twice does not result in an error.
        """
        conn = txpostgres.Connection()
        d = conn.connect(user=DB_USER, password=DB_PASS,
                         host=DB_HOST, database=DB_NAME)

        def closeTwice(_):
            conn.close()
            conn.close()

        d.addCallback(closeTwice)
        return d.addCallback(lambda _: self.assertTrue(conn.closed))

    def test_connectionRemovingReader(self):
        """
        The connection is not reading from the socket while a cursor is
        running.
        """
        class ExclusiveCursor(txpostgres.Cursor):
            testcase = self

            def doRead(self):
                for reader in self.reactor.getReaders():
                    if isinstance(reader, txpostgres.Connection):
                        self.testcase.fail(
                            "doRead called on Cursor "
                            "while a Connection is among readers: %r" %
                            self.reactor.getReaders())

                return txpostgres.Cursor.doRead(self)

        conn = txpostgres.Connection()
        conn.cursorFactory = ExclusiveCursor
        d = conn.connect(user=DB_USER, password=DB_PASS,
                         host=DB_HOST, database=DB_NAME)
        # use something more complex than select 1 or otherwise the query might
        # complete in a single reactor cycle
        d.addCallback(lambda _: conn.cursor().execute("select pg_sleep(0.1)"))
        return d.addCallback(lambda _: conn.close())


class _SimpleDBSetupMixin(object):

    connectionClass = txpostgres.Connection

    def setUp(self):
        d = self.restoreConnection(None)
        d.addCallback(lambda _: self.conn.cursor())
        return d.addCallback(self.createInitialSchema)

    def tearDown(self):
        c = self.conn.cursor()
        d = c.execute("drop table simple")
        return d.addCallback(lambda _: self.conn.close())

    def restoreConnection(self, res):
        """
        Restore the connection to the database and return whatever argument has
        been passed through. Useful as an addBoth handler for tests that
        disconnect from the database.
        """
        self.conn = self.connectionClass()
        d = self.conn.connect(user=DB_USER, password=DB_PASS,
                              host=DB_HOST, database=DB_NAME)
        return d.addCallback(lambda _: res)

    def createInitialSchema(self, c):
        d = defer.succeed(None)
        if self.conn.server_version > 80200:
            d.addCallback(lambda _: c.execute("drop table if exists simple"))
        return d.addCallback(lambda _: c.execute(simple_table_schema))


class TxPostgresManualQueryTestCase(_SimpleDBSetupMixin, Psycopg2TestCase):

    def test_simpleQuery(self):
        """
        A simple select works.
        """
        c = self.conn.cursor()
        return c.execute("select * from simple")

    def test_simpleCallproc(self):
        """
        A simple procedure call works.
        """
        c = self.conn.cursor()
        return c.callproc("now")

    def test_closeCursor(self):
        """
        Closing the cursor works.
        """
        c = self.conn.cursor()
        d = c.execute("select 1")
        return d.addCallback(lambda c: c.close())

    def test_queryResults(self):
        """
        Query results are obtainable from the asynchronous cursor.
        """
        c = self.conn.cursor()
        d = defer.Deferred()
        d.addCallback(
            lambda c: c.execute("insert into simple values (%s)", (1, )))
        d.addCallback(
            lambda c: c.execute("insert into simple values (%s)", (2, )))
        d.addCallback(
            lambda c: c.execute("insert into simple values (%s)", (3, )))
        d.addCallback(
            lambda c: c.execute("select * from simple"))
        d.addCallback(
            lambda c: self.assertEquals(c.fetchall(), [(1, ), (2, ), (3, )]))
        d.callback(c)
        return d

    def test_cursorKwargs(self):
        """
        Cursor methods can be called using keyword arguments.
        """
        c = self.conn.cursor()
        d = c.execute(params=(1, ), query="select %s")
        return d.addCallback(
            lambda c: self.assertEquals(c.fetchall(), [(1, )]))

    def test_errors(self):
        """
        Errors from the database are reported as failures.
        """
        c = self.conn.cursor()
        d = c.execute("select * from nonexistent")
        return self.assertFailure(d, psycopg2.ProgrammingError)

    def test_wrongCall(self):
        """
        Errors raised inside psycopg2 are reported as failures.
        """
        c = self.conn.cursor()
        d = c.execute("select %s", "whoops")
        return self.assertFailure(d, TypeError)

    def test_manualTransactions(self):
        """
        Transactions can be constructed manually by issuing BEGIN and ROLLBACK
        as appropriate, and should work.
        """
        c = self.conn.cursor()
        d = defer.Deferred()
        d.addCallback(
            lambda c: c.execute("begin"))
        d.addCallback(
            lambda c: c.execute("insert into simple values (%s)", (1, )))
        d.addCallback(
            lambda c: c.execute("insert into simple values (%s)", (2, )))
        d.addCallback(
            lambda c: c.execute("select * from simple order by x"))
        d.addCallback(
            lambda c: self.assertEquals(c.fetchall(), [(1, ), (2, )]))
        d.addCallback(
            lambda _: c.execute("rollback"))
        d.addCallback(
            lambda c: c.execute("select * from simple"))
        d.addCallback(
            lambda c: self.assertEquals(c.fetchall(), []))
        d.callback(c)
        return d


class NotRollingBackCursor(txpostgres.Cursor):
    """
    A cursor that does not like rolling back.
    """
    def _doit(self, name, *args, **kwargs):
        if name == "execute" and args == ("rollback", None):
            raise RuntimeError("boom")
        return txpostgres.Cursor._doit(self, name, *args, **kwargs)


class TxPostgresQueryTestCase(_SimpleDBSetupMixin, Psycopg2TestCase):

    def test_runQuery(self):
        """
        runQuery() works and returns the result.
        """
        d = self.conn.runQuery("select 1")
        return d.addCallback(self.assertEquals, [(1, )])

    def test_runQueryMultiple(self):
        """
        Multiple calls to runQuery() without waiting for the previous one work
        and return correct results.
        """
        d = defer.gatherResults([self.conn.runQuery("select %s", (i, ))
                                 for i in range(5)])
        d.addCallback(
            lambda results: self.assertEquals(
                sorted(map(lambda res: res[0][0], results)),
                [0, 1, 2, 3, 4]))
        return d

    def test_runOperation(self):
        """
        runOperation() works and executes the operation while returning None.
        """
        d = self.conn.runQuery("select count(*) from simple")
        d.addCallback(self.assertEquals, [(0, )])

        d.addCallback(lambda _: self.conn.runOperation(
            "insert into simple values (%s)", (1, )))
        d.addCallback(self.assertIdentical, None)

        d.addCallback(lambda _: self.conn.runQuery(
            "select count(*) from simple"))
        return d.addCallback(self.assertEquals, [(1, )])

    def test_runSimpleInteraction(self):
        """
        Interactions are being run in a transaction, the parameters from
        runInteraction are being passed to them and they are being committed
        after they return. Their return value becomes the return value of the
        Deferred from runInteraction.
        """
        def interaction(c, arg1, kwarg1):
            self.assertEquals(arg1, "foo")
            self.assertEquals(kwarg1, "bar")
            d = c.execute("insert into simple values (1)")
            d.addCallback(lambda c: c.execute("insert into simple values (2)"))
            return d.addCallback(lambda _: "interaction done")

        d = self.conn.runInteraction(interaction, "foo", kwarg1="bar")

        d.addCallback(self.assertEquals, "interaction done")

        d.addCallback(lambda _: self.conn.runQuery(
            "select * from simple order by x"))
        return d.addCallback(self.assertEquals, [(1, ), (2, )])

    def test_runErrorInteraction(self):
        """
        Interactions that produce errors are rolled back and the correct error
        is reported.
        """
        keepCursor = []

        def interaction(c):
            keepCursor.append(c)
            d = c.execute("insert into simple values (1)")
            return d.addCallback(
                lambda c: c.execute("select * from nope_not_here"))

        d = self.conn.runInteraction(interaction)
        d = self.assertFailure(d, psycopg2.ProgrammingError)

        d.addCallback(lambda _: self.assertTrue(keepCursor[0].closed))
        d.addCallback(lambda _: self.conn.runQuery(
            "select count(*) from simple"))
        return d.addCallback(self.assertEquals, [(0, )])

    def test_errorOnRollback(self):
        """
        Interactions that produce errors and are unable to roll back return a
        L{txpostgres.RollbackFailed} failure that has references to the faulty
        connection and the original failure that cause all that trouble.
        """
        keepCursor = []

        def interaction(c):
            keepCursor.append(c)
            d = c.execute("insert into simple values (1)")
            return d.addCallback(
                lambda c: c.execute("select * from nope_not_here"))

        mp = self.patch(self.conn, 'cursorFactory', NotRollingBackCursor)

        d = self.conn.runInteraction(interaction)
        d.addCallback(lambda _: self.fail("No exception"))

        def checkError(f):
            f.trap(txpostgres.RollbackFailed)
            original = f.value.originalFailure
            # original should reference the error that started all the mess
            self.assertIsInstance(original.value,
                                  psycopg2.ProgrammingError)
            self.assertEquals(
                str(f.value),
                "<RollbackFailed, original error: %s>" % original)
            # the error from the failed rollback should get logged
            errors = self.flushLoggedErrors()
            self.assertEquals(len(errors), 1)
            self.assertEquals(errors[0].value.args[0], "boom")
            self.assertTrue(keepCursor[0].closed)
            # restore or we won't be able to clean up the mess
            mp.restore()
        d.addErrback(checkError)

        # rollback for real, or tearDown won't be able to drop the table
        return d.addCallback(lambda _: self.conn.runOperation("rollback"))

    def test_terminatedConnection(self):
        """
        If the connection gets terminated (because of a segmentation fault,
        administrative backend termination or other circumstances), a failure
        wrapping the original psycopg2 error is returned and subsequent queries
        fail with an error indicating that the connection is already closed.
        """
        # this tests uses pg_terminate_backend, so it only works on PostgreSQL
        # 8.4+ and if the user running the tests is a superuser.
        if self.conn.server_version < 80400:
            raise unittest.SkipTest(
                "PostgreSQL < 8.4.0 does not have pg_terminate_backend")

        # check if this Twisted has a patch for #4539, otherwise the test will
        # fail because the terminated cursor will have fileno() called on it
        if not getattr(posixbase, '_PollLikeMixin', None):
            raise unittest.SkipTest("This test fails on versions of Twisted "
                                    "affected by Twisted bug #4539")

        # Check if we're running under psycopg2ct from before the patch that
        # added correct terminated connection handling. Then only way to know
        # that seems to be looking at the __version__ string.
        if (getattr(psycopg2, '_impl', None) and
                'ctypes' not in psycopg2.__version__):
            raise unittest.SkipTest(
                "This test fails on versions of psycopg2ct 0.3 and older, "
                "which have a bug in terminated connection handling")

        def checkSuperuser(ret):
            if ret[0][0] != 'on':
                raise unittest.SkipTest(
                    "This test uses pg_terminate_backend, "
                    "which can only be called by a database superuser")

        d = self.conn.runQuery("show is_superuser")
        d.addCallback(checkSuperuser)

        def terminateAndRunQuery():
            d = self.conn.runQuery("select pg_terminate_backend(%s)",
                                   (self.conn.get_backend_pid(), ))

            def fail(ignore):
                self.fail("did not catch an error, instead got %r" % (ignore,))

            def checkDatabaseError(f):
                if f.check(psycopg2.DatabaseError):
                    return f.value

                if f.check(SystemError):
                    raise unittest.SkipTest(
                        "This test fails on versions of psycopg2 before 2.4.1 "
                        "which have a bug in libpq error handling")

                self.fail(("\nExpected: %r\nGot:\n%s"
                           % (psycopg2.DatabaseError, str(f))))

            def runSimpleQuery(_):
                d = self.conn.runQuery("select 1")
                return self.assertFailure(d, psycopg2.InterfaceError)

            d.addCallbacks(fail, checkDatabaseError)
            d.addCallback(runSimpleQuery)
            # restore the connection, otherwise all the other tests will fail
            return d.addBoth(self.restoreConnection)

        return d.addCallback(lambda _: terminateAndRunQuery())

    def test_connectionLostWhileRunning(self):
        """
        If the connection is lost while a query is still underway, the polling
        cycle is continued until psycopg2 either reports success or an error.
        """
        cursors = []

        class RetainingCursor(txpostgres.Cursor):

            def __init__(self, cursor, connection):
                cursors.append(self)
                txpostgres.Cursor.__init__(self, cursor, connection)

        mp = self.patch(self.conn, 'cursorFactory', RetainingCursor)

        d1 = self.conn.runQuery("select 1")
        d2 = self.conn.runQuery("select 1")

        self.assertEquals(len(cursors), 1)
        # even if the cursor gets connectionLost called on it, it will continue
        # to poll the connection, which is mandated by the API (the client
        # can't stop polling the connection until either POLL_OK is returned or
        # an exception is raised.
        cursors[0].connectionLost(failure.Failure(RuntimeError("boom")))

        # since no error was reported from psycopg2, both Deferreds callback
        d = defer.gatherResults([d1, d2])
        d.addCallback(self.assertEquals, [[(1, )], [(1, )]])
        return d.addCallback(lambda _: mp.restore())

    def test_disconnectWhileRunning(self):
        """
        Disconnecting from the server when there is a query underway causes the
        query to fail with ConnectionDone.
        """
        # check if this Twisted has a patch for #4539, otherwise the cursor
        # will have fileno() called on it after the psycopg2 closes the
        # connection socket, resulting in an error
        if not getattr(posixbase, '_PollLikeMixin', None):
            raise unittest.SkipTest("This test fails on versions of Twisted "
                                    "affected by Twisted bug #4539")

        def _checkConnectionLost(f, testcase):
            if not isinstance(f, failure.Failure):
                testcase.fail("connectionLost got non-Failure: %r" % f)
            if not f.check(error.ConnectionDone):
                testcase.fail("connectionLost got wrong Failure: %r" % f.value)

        producedCursors = []

        class StrictLosingConnection(txpostgres.Connection):
            testcase = self
            seenConnectionLost = False

            def connectionLost(self, reason):
                txpostgres.Connection.connectionLost(self, reason)
                self.seenConnectionLost = True
                _checkConnectionLost(reason, self.testcase)

            def cursor(self, *args, **kwargs):
                cursor = txpostgres.Connection.cursor(self, *args, **kwargs)
                producedCursors.append(cursor)
                return cursor

        class StrictLosingCursor(txpostgres.Cursor):
            testcase = self
            seenConnectionLost = False

            def connectionLost(self, reason):
                txpostgres.Cursor.connectionLost(self, reason)
                self.seenConnectionLost = True
                _checkConnectionLost(reason, self.testcase)

        def _runAndClose():
            # start executing the query and close it in the next reactor cycle
            d = conn.runQuery("select pg_sleep(5)")
            reactor.callLater(0, conn.close)
            return d

        def _waitReactorCycle():
            # wait one reactor cycle to allow callback to propagate
            d = defer.Deferred()
            reactor.callLater(0, d.callback, None)
            return d

        def _checkSeenConnectionLost():
            self.assertTrue(conn.seenConnectionLost)
            for cursor in producedCursors:
                self.assertTrue(cursor.seenConnectionLost)

        conn = StrictLosingConnection()
        conn.cursorFactory = StrictLosingCursor
        d = conn.connect(user=DB_USER, password=DB_PASS,
                         host=DB_HOST, database=DB_NAME)

        d.addCallback(lambda _: _runAndClose())
        # the query fails with a disconnected error
        d = self.assertFailure(d, error.ConnectionDone)
        d.addCallback(lambda _: _waitReactorCycle())
        return d.addCallback(lambda _: _checkSeenConnectionLost())


class TxPostgresConnectionPoolTestCase(Psycopg2TestCase):

    def setUp(self):
        self.pool = txpostgres.ConnectionPool(
            None, user=DB_USER, password=DB_PASS,
            host=DB_HOST, database=DB_NAME)
        return self.pool.start()

    def tearDown(self):
        return self.pool.close()

    def test_basics(self):
        """
        Exactly 'min' connections are always created.
        """
        self.assertEquals(len(self.pool.connections), self.pool.min)

    def test_simpleQuery(self):
        """
        The pool can run 'min' queries in parallel without making any of them
        wait. The queries return correct values.
        """
        ds = [self.pool.runQuery("select 1") for _ in range(self.pool.min)]
        self.assertEquals(len(self.pool._semaphore.waiting), 0)

        d = defer.gatherResults(ds)
        return d.addCallback(self.assertEquals, [[(1, )]] * self.pool.min)

    def test_moreQueries(self):
        """
        The pool can handle more parallel queries than its size.
        """
        d = defer.gatherResults(
            [self.pool.runQuery("select 1") for _ in range(self.pool.min * 5)])
        return d.addCallback(self.assertEquals, [[(1, )]] * self.pool.min * 5)

    def test_operation(self):
        """
        The pool's runOperation works.
        """
        d = self.pool.runOperation("create table x (i int)")
        # give is a workout, 20 x the number of connections
        d.addCallback(lambda _: defer.gatherResults(
            [self.pool.runOperation("insert into x values (%s)", (i, ))
             for i in range(self.pool.min * 20)]))
        d.addCallback(lambda _: self.pool.runQuery(
            "select * from x order by i"))
        d.addCallback(self.assertEquals, [(i, ) for i in
                                          range(self.pool.min * 20)])
        return d.addCallback(lambda _: self.pool.runOperation(
            "drop table x"))

    def test_interaction(self):
        """
        The pool's runInteraction works.
        """
        def interaction(c):
            # cursors can only be declared in a transaction, so that's a good
            # indication that we're in one
            d = c.execute("declare x cursor for values (1), (2)")
            d.addCallback(lambda c: c.execute("fetch 1 from x"))
            d.addCallback(lambda c: self.assertEquals(c.fetchone()[0], 1))
            d.addCallback(lambda _: c.execute("fetch 1 from x"))
            d.addCallback(lambda c: self.assertEquals(c.fetchone()[0], 2))
            return d

        return defer.gatherResults([self.pool.runInteraction(interaction)
                                    for _ in range(self.pool.min * 20)])


class TxPostgresConnectionPoolErrorsTestCase(Psycopg2TestCase):

    def test_errorsInConnections(self):
        """
        A failure in any connection makes the Deferred returned from start()
        fail.
        """
        class ErrorConnection(object):
            connid = 0

            def __init__(self, *args):
                pass

            def connect(self, *args, **kwargs):
                # the third connection fails
                ErrorConnection.connid += 1
                if ErrorConnection.connid == 3:
                    return defer.fail(RuntimeError("boom"))
                return defer.succeed(None)

        class ErrorConnectionPool(txpostgres.ConnectionPool):
            connectionFactory = ErrorConnection

        pool = ErrorConnectionPool(None, min=3)
        d = pool.start()

        d = self.assertFailure(d, defer.FirstError)
        return d.addCallback(lambda exc: exc.subFailure.trap(RuntimeError))


class TxPostgresConnectionPoolHotswappingTestCase(Psycopg2TestCase):

    def test_errorsInInteractionHotswappingConnections(self):
        """
        After getting a RollbackFailed failure it is possible to remove the
        offending connection from the pool, open a new one and put it in the
        pool to replace the removed one.
        """
        pool = txpostgres.ConnectionPool(
            None, user=DB_USER, password=DB_PASS,
            host=DB_HOST, database=DB_NAME, min=1)
        self.assertEquals(pool.min, 1)
        d = pool.start()

        # poison the connection
        c, = pool.connections
        c.cursorFactory = NotRollingBackCursor

        # run stuff that breaks
        def brokenInteraction(c):
            return c.execute("boom")
        d.addCallback(lambda _: pool.runInteraction(brokenInteraction))
        d.addCallback(lambda _: self.fail("No exception"))

        def checkErrorAndHotswap(f):
            f.trap(txpostgres.RollbackFailed)
            e = f.value
            self.assertIdentical(e.connection.cursorFactory,
                                 NotRollingBackCursor)
            errors = self.flushLoggedErrors()
            self.assertEquals(len(errors), 1)
            self.assertEquals(errors[0].value.args[0], "boom")
            pool.remove(e.connection)
            e.connection.close()
            c = txpostgres.Connection()
            self.assertNotIdentical(c.cursorFactory,
                                    NotRollingBackCursor)
            d = c.connect(user=DB_USER, password=DB_PASS,
                          host=DB_HOST, database=DB_NAME)
            return d.addCallback(lambda c: pool.add(c))

        d.addErrback(checkErrorAndHotswap)

        d.addCallback(lambda _: defer.gatherResults([
            pool.runQuery("select 1") for __ in range(3)]))
        d.addCallback(self.assertEquals, [[(1, )]] * 3)
        return d.addCallback(lambda _: pool.close())

    def test_removeWhileBusy(self):
        """
        Removing a connection from the pool while it's running a query raises
        an exception.
        """
        pool = txpostgres.ConnectionPool(
            None, user=DB_USER, password=DB_PASS,
            host=DB_HOST, database=DB_NAME, min=1)

        d = pool.start()

        def simple(c):
            self.assertRaises(ValueError, pool.remove, c._connection)
        d.addCallback(lambda pool: pool.runInteraction(simple))
        return d.addCallback(lambda _: pool.close())


class TxPostgresCancellationTestCase(_SimpleDBSetupMixin, Psycopg2TestCase):

    def setUp(self):
        def checkCancellationSupport():
            # check for cancellation support in psycopg2, skip if not present
            if not getattr(self.conn.pollable(), "cancel", None):
                raise unittest.SkipTest(
                    "psycopg2 does not have query cancellation support. "
                    "You need at least version 2.3.0 of psycopg2 "
                    "to use query cancellation.")

        def createAdditionalConnection():
            # Create an additional connection that will be used to check if the
            # main one is already waiting on pg_sleep.
            self.extra = txpostgres.Connection()
            return self.extra.connect(user=DB_USER, password=DB_PASS,
                                      host=DB_HOST, database=DB_NAME)

        d = _SimpleDBSetupMixin.setUp(self)
        d.addCallback(lambda _: checkCancellationSupport())
        return d.addCallback(lambda _: createAdditionalConnection())

    def tearDown(self):
        d = _SimpleDBSetupMixin.tearDown(self)
        return d.addCallback(lambda _: self.extra.close())

    def waitForSleep(self):
        """
        Return a Deferred that fires when the main connection starts executing
        pg_sleep. This is done by polling pg_stat_statements from the extra
        connection.
        """
        # XXX is this really the only way to make these tests not racy?
        interval = 0.5
        initial_remaining = 10

        def gotResult(res, remaining):
            if res and res[0][0]:
                # the result of the query was True, we're done
                return

            if not remaining:
                self.fail('main connection did not execute pg_sleep')

            # main connection still not executing pg_sleep, wait
            return task.deferLater(reactor, interval, checkActivity, remaining)

        def checkActivity(remaining):
            cols = {'query_col': 'current_query', 'pid_col': 'procpid'}
            if self.conn.server_version >= 90200:
                cols = {'query_col': 'query', 'pid_col': 'pid'}

            sql = ("select %(query_col)s like '%%%%pg_sleep%%%%' "
                   "from pg_stat_activity where %(pid_col)s = %%s") % cols
            d = self.extra.runQuery(sql, (self.conn.get_backend_pid(), ))
            return d.addCallback(gotResult, remaining - 1)

        return checkActivity(initial_remaining)

    def test_simpleCancellation(self):
        d = self.conn.runQuery("select pg_sleep(5)")

        def cancelQuery(_):
            self.conn.cancel(d)
            return self.failUnlessFailure(d, defer.CancelledError)

        waiting = self.waitForSleep()
        return waiting.addCallback(cancelQuery)

    def test_directCancellation(self):
        d = self.conn.runQuery("select pg_sleep(5)")

        def tryDirectCancel(_):
            self.assertRaises(txpostgres._CancelInProgress, d.cancel)
            return self.failUnlessFailure(d, defer.CancelledError)

        waiting = self.waitForSleep()
        return waiting.addCallback(tryDirectCancel)

    def test_cancelInteraction(self):
        def interaction(c):
            def cancelQuery(_):
                self.conn.cancel(d)
                return d

            d = c.execute("insert into simple values (1)")
            d.addCallback(lambda c: c.execute("insert into simple values (2)"))
            d.addCallback(lambda c: c.execute("select pg_sleep(5)"))
            d.addCallback(lambda _: "interaction done")

            waiting = self.waitForSleep()
            return waiting.addCallback(cancelQuery)

        d = self.conn.runInteraction(interaction)
        d = self.failUnlessFailure(d, defer.CancelledError)
        d.addCallback(lambda _: self.conn.runQuery(
            "select * from simple"))
        return d.addCallback(self.assertEquals, [])

    def test_cancelMultipleQueries(self):
        d1 = self.conn.runQuery("select pg_sleep(5)")
        d2 = self.conn.runQuery("select pg_sleep(5)")

        def cancelQueries(_):
            self.conn.cancel(d1)
            self.conn.cancel(d2)

            self.failUnlessFailure(d1, defer.CancelledError)
            self.failUnlessFailure(d2, defer.CancelledError)

            return defer.gatherResults([d1, d2])

        waiting = self.waitForSleep()
        return waiting.addCallback(cancelQueries)


class TxPostgresNotifyObserversTestCase(Psycopg2TestCase):

    def test_sameObserverAddedTwice(self):
        """
        Adding the same observer twice results in just one registration.
        """
        c = txpostgres.Connection()

        def observer(notify):
            pass

        self.assertEquals(len(c.getNotifyObservers()), 0)

        c.addNotifyObserver(observer)
        c.addNotifyObserver(observer)

        self.assertEquals(len(c.getNotifyObservers()), 1)

    def test_removeNonexistentObserver(self):
        """
        Removing an observer twice is valid and results in the observer being
        removed. Removing one that does not exist at all is valid as well.
        """
        c = txpostgres.Connection()

        def observer1(notify):
            pass

        def observer2(notify):
            pass

        c.addNotifyObserver(observer1)
        c.addNotifyObserver(observer2)

        self.assertEquals(len(c.getNotifyObservers()), 2)

        c.removeNotifyObserver(observer1)
        c.removeNotifyObserver(observer1)
        c.removeNotifyObserver(lambda _: _)

        self.assertEquals(len(c.getNotifyObservers()), 1)
        self.assertIn(observer2, c.getNotifyObservers())


class TxPostgresNotifyTestCase(_SimpleDBSetupMixin, Psycopg2TestCase):

    def setUp(self):
        self.notifyconn = txpostgres.Connection()
        self.notifies = []

        d = self.notifyconn.connect(user=DB_USER, password=DB_PASS,
                                    host=DB_HOST, database=DB_NAME)
        return d.addCallback(lambda _: _SimpleDBSetupMixin.setUp(self))

    def tearDown(self):
        self.notifyconn.close()
        return _SimpleDBSetupMixin.tearDown(self)

    def sendNotify(self):
        return self.notifyconn.runOperation('notify txpostgres_test')

    def test_simpleNotify(self):
        """
        Notifications sent form another session are delivered to the listening
        session.
        """
        notifyD = defer.Deferred()

        def observer(notify):
            self.notifies.append(notify)
            notifyD.callback(None)

        self.conn.addNotifyObserver(observer)

        d = self.conn.runOperation("listen txpostgres_test")
        d.addCallback(lambda _: self.sendNotify())
        # wait for the notification to be processed
        d.addCallback(lambda _: notifyD)
        d.addCallback(lambda _: self.assertEquals(len(self.notifies), 1))
        return d.addCallback(lambda _: self.assertEquals(
            self.notifies[0][1], "txpostgres_test"))

    def test_simpleNotifySameConnection(self):
        """
        Notifications sent from the listening session are delivered to the
        session.
        """
        notifyD = defer.Deferred()

        def observer(notify):
            self.notifies.append(notify)
            notifyD.callback(None)

        self.notifyconn.addNotifyObserver(observer)

        d = self.notifyconn.runOperation("listen txpostgres_test")
        d.addCallback(lambda _: self.sendNotify())
        # wait for the notification to be processed
        d.addCallback(lambda _: notifyD)
        d.addCallback(lambda _: self.assertEquals(len(self.notifies), 1))
        return d.addCallback(lambda _: self.assertEquals(
            self.notifies[0][1], "txpostgres_test"))

    def test_listenUnlisten(self):
        """
        Unlistening causes notifications not to be delivered anymore.
        """
        notifyD = defer.Deferred()

        def observer(notify):
            self.notifies.append(notify)
            notifyD.callback(None)

        self.conn.addNotifyObserver(observer)

        d = self.conn.runOperation("listen txpostgres_test")
        d.addCallback(lambda _: self.sendNotify())
        d.addCallback(lambda _: notifyD)
        d.addCallback(lambda _: self.assertEquals(len(self.notifies), 1))
        d.addCallback(lambda _: self.conn.runOperation(
            "unlisten txpostgres_test"))
        d.addCallback(lambda _: self.sendNotify())
        # run a query to force the reactor to spin and flush eventual pending
        # notifications, which there should be none since we did unlisten
        d.addCallback(lambda _: self.conn.runOperation("select 1"))
        return d.addCallback(lambda _: self.assertEquals(
            len(self.notifies), 1))

    def test_multipleNotifies(self):
        """
        Multiple notifications sent in a row are gradually delivered.
        """
        dl = [defer.Deferred(), defer.Deferred(), defer.Deferred()]
        notifyD = defer.DeferredList(dl)

        def observer(notify):
            self.notifies.append(notify)
            dl.pop().callback(None)

        self.conn.addNotifyObserver(observer)

        d = self.conn.runOperation("listen txpostgres_test")
        d.addCallback(lambda _: self.sendNotify())
        d.addCallback(lambda _: self.sendNotify())
        d.addCallback(lambda _: self.sendNotify())
        d.addCallback(lambda _: notifyD)
        return d.addCallback(lambda _: self.assertEquals(
            len(self.notifies), 3))

    def test_notifyDeliveryOrder(self):
        """
        Notifications are delivered to observers in the same order that they
        were sent.
        """
        # this tests accesses NOTIFY payloads, so it requires PostgreSQL 9.0+
        # and Psycopg 2.3+
        if self.conn.server_version < 90000:
            raise unittest.SkipTest(
                "PostgreSQL < 9.0.0 does not support NOTIFY payloads")

        if getattr(psycopg2.extensions.Notify, 'payload', None) is None:
            raise unittest.SkipTest(
                "psycopg2 does not have NOTIFY payload support. You need at "
                "least version 2.3.0 of psycopg2 to process NOTIFY payloads.")

        dl = [defer.Deferred() for _ in range(10)]
        payloads = map(str, range(10))
        notifyD = defer.DeferredList(dl)

        def observer(notify):
            self.notifies.append(notify)
            dl.pop().callback(None)

        self.conn.addNotifyObserver(observer)

        d = self.conn.runOperation("listen txpostgres_test")
        # send all notification together to ensure that they are processed
        # inside a single checkForNotifies call
        d.addCallback(lambda _: self.notifyconn.runOperation(
            'notify txpostgres_test, %s; ' * 10, payloads))
        # wait for all notification to be processed
        d.addCallback(lambda _: notifyD)
        # check that they were processed in the right order
        return d.addCallback(lambda _: self.assertEquals(
            [n.payload for n in self.notifies], payloads))

    def test_multipleObservers(self):
        """
        Multiple registered notify observers each get notified.
        """
        dl1 = [defer.Deferred(), defer.Deferred()]
        dl2 = [defer.Deferred()]

        firstNotifyD = defer.DeferredList([dl1[1], dl2[0]])
        secondNotifyD = dl1[0]

        def observer1(notify):
            self.notifies.append(1)
            dl1.pop().callback(None)

        def observer2(notify):
            self.notifies.append(2)
            dl2.pop().callback(None)

        self.conn.addNotifyObserver(observer1)
        self.conn.addNotifyObserver(observer2)

        d = self.conn.runOperation("listen txpostgres_test")
        d.addCallback(lambda _: self.sendNotify())
        # two observers mean two notifications received
        d.addCallback(lambda _: firstNotifyD)
        # the order is not determined though
        d.addCallback(lambda _: self.assertEquals(
            set(self.notifies), set([1, 2])))
        d.addCallback(lambda _: self.conn.removeNotifyObserver(observer2))
        d.addCallback(lambda _: self.sendNotify())
        d.addCallback(lambda _: secondNotifyD)
        # the second observer has been removed, so there should be three
        # notifies and the last one should come from the first observer
        d.addCallback(lambda _: self.assertEquals(len(self.notifies), 3))
        return d.addCallback(lambda _: self.assertEquals(self.notifies[-1], 1))

    def test_errorInObserver(self):
        """
        An exception in an observer function gets logged and ignored.
        """
        dl = [defer.Deferred(), defer.Deferred()]
        notifyD = defer.DeferredList(dl)
        errorD = defer.Deferred()

        def observer1(notify):
            self.notifies.append(1)
            dl.pop().callback(None)

        def observer2(notify):
            errorD.callback(None)
            raise RuntimeError("boom")

        self.conn.addNotifyObserver(observer1)
        self.conn.addNotifyObserver(observer2)

        d = self.conn.runOperation("listen txpostgres_test")
        d.addCallback(lambda _: self.sendNotify())
        # at some point both observer functions will get called, one of them
        # raising an exception, to make sure they still are registered and
        # executing, send another notify
        d.addCallback(lambda _: errorD)
        d.addCallback(lambda _: self.conn.removeNotifyObserver(observer2))
        d.addCallback(lambda _: self.sendNotify())
        d.addCallback(lambda _: notifyD)
        d.addCallback(lambda _: self.assertEquals(
            len(self.flushLoggedErrors(RuntimeError)), 1))
        return d.addCallback(lambda _: self.assertEquals(
            self.notifies, [1, 1]))

    def test_observerReturnsDeferred(self):
        """
        If the observer function returns a Deferred, the next notify won't be
        processed until it fires.
        """
        observerDs = [defer.Deferred(), defer.Deferred()]
        notifyDs = [defer.Deferred(), defer.Deferred()]

        oneObserverDone = defer.DeferredList(notifyDs, fireOnOneCallback=True)
        allObserversDone = defer.DeferredList(notifyDs)

        def fireAllObserverDs():
            for observerD in observerDs:
                observerD.callback(None)
            return allObserversDone

        def observer(notify, observerD, notifyD):
            self.notifies.append(notify)
            # don't callback notifyD synchronously, or oneObserverDone might
            # start processing immediately
            reactor.callLater(0, notifyD.callback, None)
            return observerD

        def makeObserver(observerD, notifyD):
            return lambda notify: observer(notify, observerD, notifyD)

        for observerD, notifyD in zip(observerDs, notifyDs):
            self.conn.addNotifyObserver(makeObserver(observerD, notifyD))

        d = self.conn.runOperation("listen txpostgres_test")
        d.addCallback(lambda _: self.sendNotify())
        # wait for one of the observers to finish -- there's no guarantee of
        # order, so can't say which
        d.addCallback(lambda _: oneObserverDone)
        # only one observer should have been called
        d.addCallback(lambda _: self.assertEquals(len(self.notifies), 1))
        # fire both observer Deferreds to make sure the processing finishes
        d.addCallback(lambda _: fireAllObserverDs())
        # both observers should now be called
        return d.addCallback(lambda _: self.assertEquals(
            len(self.notifies), 2))

    def test_observerReturnsFailingDeferred(self):
        """
        If the observer function returns a failing Deferred, the failure is
        logged and processing continues.
        """
        dl = [defer.Deferred(), defer.Deferred()]
        notifyD = defer.DeferredList(dl)

        # use a factory function to ensure that id(observer1) != id(observer2)
        def makeObserver():
            def observer(notify):
                self.notifies.append(1)
                # callback asynchronously to make sure the function runs to
                # completion before continuing notifyD's callback chain
                reactor.callLater(0, dl.pop().callback, None)
                return defer.fail(RuntimeError("boom"))

            return observer

        self.conn.addNotifyObserver(makeObserver())
        self.conn.addNotifyObserver(makeObserver())

        d = self.conn.runOperation("listen txpostgres_test")
        d.addCallback(lambda _: self.sendNotify())
        # even though the observer functions return a failing Deferred, both of
        # them will be called
        d.addCallback(lambda _: notifyD)
        # both errors get logged
        d.addCallback(lambda _: self.assertEquals(
            len(self.flushLoggedErrors(RuntimeError)), 2))
        return d.addCallback(lambda _: self.assertEquals(
            self.notifies, [1, 1]))

    def test_customNotifyCooperator(self):
        """
        Using a custom cooperator for notifies is possible.
        """
        cooperateD = defer.Deferred()

        class FakeCooperator(object):

            def cooperate(self, iterator):
                self.result = []

                for ret in iterator:
                    defer.maybeDeferred(lambda: ret).addCallback(
                        self.result.append)

                cooperateD.callback(None)

        cooperator = FakeCooperator()

        def observer(notify):
            return True

        c = txpostgres.Connection(cooperator=cooperator)
        c.addNotifyObserver(observer)

        d = c.connect(user=DB_USER, password=DB_PASS,
                      host=DB_HOST, database=DB_NAME)
        d.addCallback(lambda _: c.runOperation("listen txpostgres_test"))
        d.addCallback(lambda _: self.sendNotify())
        d.addCallback(lambda _: cooperateD)
        d.addCallback(lambda _: self.assertEquals(
            cooperator.result, [True]))
        return d.addCallback(lambda _: c.close())


class SignallingDetector(reconnection.DeadConnectionDetector):

    died = False

    def startReconnecting(self, f):
        self.died = True
        return reconnection.DeadConnectionDetector.startReconnecting(self, f)


class ReconnectingConnection(txpostgres.Connection):

    def __init__(self, *args, **kwargs):
        self.clock = task.Clock()

        detector = SignallingDetector(
            deathChecker=self.checkDataError,
            reconnectionIterator=self.sillyIterator,
            reactor=self.clock)

        kwargs['detector'] = detector
        txpostgres.Connection.__init__(self, *args, **kwargs)

    def checkDataError(self, f):
        return f.check(psycopg2.DataError)

    def sillyIterator(self):
        return [1, 1, 1]


class TxPostgresReconnectionTestCase(_SimpleDBSetupMixin, Psycopg2TestCase):

    connectionClass = ReconnectingConnection

    def test_reconnection(self):
        """
        An error causing the connection to die starts the reconnection process.
        """
        recovery_d = defer.Deferred()
        self.conn.detector.addRecoveryHandler(
            lambda: recovery_d.callback(None))

        d = self.conn.runOperation('insert into simple values (1)')
        d.addCallback(lambda _: self.assertFalse(self.conn.detector.died))

        # try inserting text, which will cause a DataError and cause a
        # reconnection
        d.addCallback(lambda _: self.conn.runOperation(
            "insert into simple values ('sometext')"))
        self.assertFailure(d, psycopg2.DataError)
        d.addCallback(lambda _: self.assertTrue(self.conn.detector.died))

        # the next query will fail immediately
        d.addCallback(lambda _: self.conn.runQuery('select 1'))
        self.assertFailure(d, reconnection.ConnectionDead)

        # after the delay expires, a reconnection attempt will be triggered and
        # the connection will recover eventually
        d.addCallback(lambda _: self.conn.clock.advance(1))
        d.addCallback(lambda _: recovery_d)

        # now the query will no loger fail
        d.addCallback(lambda _: self.conn.runQuery('select 1'))
        return d.addCallback(self.assertEquals, [(1, )])
