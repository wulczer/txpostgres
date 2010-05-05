# Copyright (c) 2010 Twisted Matrix Laboratories.
# See LICENSE for details.

"""
Tests for twisted.enterprise.pgadbapi.
"""

try:
    import psycopg2
    import psycopg2.extensions
except ImportError:
    psycopg2 = None

from txpostgres import txpostgres

from twisted.trial import unittest
from twisted.internet import defer

simple_table_schema = """
CREATE TABLE simple (
  x integer
)
"""

DB_NAME = "twisted_test"
DB_HOST = "localhost"
DB_USER = "wulczer"
DB_PASS = ""


def getSkipForPsycopg2():
    if not psycopg2:
        return "psycopg2 not installed"
    try:
        psycopg2.extensions.POLL_OK
    except AttributeError:
        return ("psycopg2 does not have async support")
    try:
        psycopg2.connect(user=DB_USER, password=DB_PASS,
                         host=DB_HOST, database=DB_NAME).close()
    except psycopg2.Error:
        return "cannot connect to test database"
    return None


_skip = getSkipForPsycopg2()


class Psycopg2TestCase(unittest.TestCase):

    skip = _skip


class PollableThing(object):
    """
    A fake thing that provides a psycopg2 pollable interface.
    """
    def __init__(self):
        self.NEXT_STATE = psycopg2.extensions.POLL_READ

    def poll(self):
        if self.NEXT_STATE is None:
            raise Exception("no next state")
        return self.NEXT_STATE

    def fileno(self):
        return 42


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


class FakeWrapper(txpostgres._PollingMixin):
    """
    A mock subclass of L{txpostgres._PollingMixin}.
    """
    reactor = FakeReactor()
    prefix = "fake-wrapper"

    def pollable(self):
        return self._pollable


class TxPostgresPollingMixingTestCase(Psycopg2TestCase):

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

        p._pollable = None
        self.assertEquals(p.fileno(), -1)

    def test_connectionLost(self):
        """
        Calling connectionLost() errbacks the C{Deferred} returned from poll()
        but another connectionLost() is harmless.
        """
        p = FakeWrapper()
        p._pollable = PollableThing()

        d = p.poll()
        p.connectionLost(RuntimeError("boom"))
        d = self.assertFailure(d, RuntimeError)
        p.connectionLost(RuntimeError("bam"))
        return d

    def test_errors(self):
        """
        Unexpected results from poll() make L{txpostgres._PollingMixin} raise an
        exception.
        """
        p = FakeWrapper()
        p._pollable = PollableThing()

        p._pollable.NEXT_STATE = "foo"
        d = p.poll()
        return self.assertFailure(d, txpostgres.UnexpectedPollResult)


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
        class BadPollable(object):
            def __init__(*args, **kwars):
                pass
            def poll(self):
                raise RuntimeError("booga")
            def close(self):
                pass
        conn.connectionFactory = BadPollable

        d = conn.connect()
        d = self.assertFailure(d, RuntimeError)
        d.addCallback(lambda _: conn.close())

        class BadThing(object):
            def __init__(*args, **kwargs):
                raise RuntimeError("wooga")
            def close(self):
                pass
        conn.connectionFactory = BadThing

        d.addCallback(lambda _: conn.connect())
        d = self.assertFailure(d, RuntimeError)

        class BrokenPollable(object):
            def __init__(*args, **kwars):
                pass
            def poll(self):
                return "tee hee hee"
            def close(self):
                pass
        conn.connectionFactory = BrokenPollable

        d.addCallback(lambda _: conn.connect())
        return self.assertFailure(d, txpostgres.UnexpectedPollResult)


class _SimpleDBSetupMixin(object):

    def setUp(self):
        self.conn = txpostgres.Connection()
        d = self.conn.connect(user=DB_USER, password=DB_PASS,
                              host=DB_HOST, database=DB_NAME)
        d.addCallback(lambda c: c.cursor())
        return d.addCallback(lambda c: c.execute(simple_table_schema))

    def tearDown(self):
        c = self.conn.cursor()
        d = c.execute("drop table simple")
        return d.addCallback(lambda _: self.conn.close())



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

    def test_multipleQueries(self):
        """
        Multiple calls to execute() without waiting for the previous one to
        finish work and return correct results.
        """
        cursors = [self.conn.cursor() for _ in range(5)]
        d = defer.gatherResults([c.execute("select %s", (i, ))
                                 for i, c in enumerate(cursors)])
        d.addCallback(
            lambda cursors: self.assertEquals(
                sorted(map(lambda c: c.fetchone()[0], cursors)),
                [0, 1, 2, 3, 4]))
        return d

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
        def interaction(c):
            d = c.execute("insert into simple values (1)")
            return d.addCallback(
                lambda c: c.execute("select * from nope_not_here"))

        d = self.conn.runInteraction(interaction)
        d = self.assertFailure(d, psycopg2.ProgrammingError)

        d.addCallback(lambda _: self.conn.runQuery(
                "select count(*) from simple"))
        return d.addCallback(self.assertEquals, [(0, )])

    def test_errorOnRollback(self):
        """
        Interactions that produce errors and are unable to roll back return a
        L{txpostgres.RollbackFailed} failure that has references to the faulty
        connection and the original failure that cause all that trouble.
        """
        def interaction(c):
            d = c.execute("insert into simple values (1)")
            return d.addCallback(
                lambda c: c.execute("select * from nope_not_here"))

        mp = self.patch(self.conn, 'cursorFactory', NotRollingBackCursor)

        d = self.conn.runInteraction(interaction)
        d.addCallback(lambda _: self.fail("No exception"))

        def check_error(f):
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
            # restore or we won't be able to clean up the mess
            mp.restore()
        d.addErrback(check_error)

        # rollback for real, or tearDown won't be able to drop the table
        return d.addCallback(lambda _: self.conn.runOperation("rollback"))


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
                    pool.runQuery("select 1") for _ in range(3)]))
        return d.addCallback(self.assertEquals, [[(1, )]] * 3)

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
        return d.addCallback(lambda pool: pool.runInteraction(simple))
