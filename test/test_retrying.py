from twisted.trial import unittest
from twisted.internet import defer, task
from twisted.python import failure

from txpostgres import retrying


class ArbitraryException(Exception):
    pass


class TestSimpleBackoffIterator(unittest.TestCase):

    def test_simple(self):
        """
        Consecutive delays are increasing.
        """
        it = retrying.simpleBackoffIterator()

        # by default "now" is set
        self.assertEquals(it.next(), 0)

        r1, r2, r3 = it.next(), it.next(), it.next()
        self.assertTrue(r1 < r2 < r3)

    def test_maxDelay(self):
        """
        The delay is capped at some point.
        """
        it = retrying.simpleBackoffIterator(maxDelay=2.0)

        res = [it.next() for _ in range(5)]
        self.assertEquals(res[-1], 2.0)

    def test_notNow(self):
        """
        If now is not set, the first delay is not zero.
        """
        it = retrying.simpleBackoffIterator(now=False)
        self.assertNotEquals(it.next(), 0)

    def test_maxRetries(self):
        """
        The iterator gets exhausted if retries are capped.
        """
        it = retrying.simpleBackoffIterator(maxRetries=3)

        it.next(), it.next(), it.next()
        self.assertRaises(StopIteration, it.next)

    def test_noMaxRetries(self):
        """
        The iterator does not get exhausted if retries are not capped.
        """
        it = retrying.simpleBackoffIterator(maxRetries=0, maxDelay=1)

        alot = [it.next() for _ in range(1000)]
        self.assertEquals(alot[-1], 1)

    def test_noMaxDelay(self):
        """
        Without maximum delay, the consecutive delays grow unboundedly.
        """
        it = retrying.simpleBackoffIterator(maxRetries=0, maxDelay=0)

        # putting range(1000) here makes Python return a NaN, so be moderate
        alot = [it.next() for _ in range(100)]
        self.assertTrue(alot[-2] < alot[-1])

    def test_precise(self):
        """
        Knowing starting values and disabling jitter, it is possible to predict
        consecutive values.
        """
        it = retrying.simpleBackoffIterator(initialDelay=10, maxDelay=90,
                                            factor=2, jitter=0)

        self.assertEquals(it.next(), 0)
        self.assertEquals(it.next(), 20)
        self.assertEquals(it.next(), 40)
        self.assertEquals(it.next(), 80)
        self.assertEquals(it.next(), 90)
        self.assertEquals(it.next(), 90)


class TestRetryingCall(unittest.TestCase):

    def setUp(self):
        self.calls = 0
        self.raiseError = None

        self.clock = task.Clock()
        self.call = retrying.RetryingCall(self.func)
        self.call.reactor = self.clock
        self.sillyIterator = retrying.simpleBackoffIterator(
            initialDelay=1, factor=1, jitter=0, maxDelay=0, maxRetries=0)

    def func(self):
        self.calls += 1

        if self.raiseError:
            raise self.raiseError()

    def test_basic(self):
        """
        First retrying call is synchronous.
        """
        d = self.call.start()

        # the first call is synchronous, no need to advance the clock
        return d.addCallback(lambda _: self.assertEquals(self.calls, 1))

    def test_someFailures(self):
        """
        If the function fails a few times and then succeeds, the whole Deferred
        succeeds.
        """
        self.raiseError = ArbitraryException
        tests = []

        def tester(f):
            tests.append(f)

        d = self.call.start(self.sillyIterator, tester)

        self.assertEquals(len(tests), 1)
        self.assertIsInstance(tests[0], failure.Failure)
        self.assertIsInstance(tests[0].value, ArbitraryException)

        self.clock.advance(0.5)
        self.assertEquals(len(tests), 1)
        self.assertEquals(self.calls, 1)

        self.clock.advance(0.5)
        self.assertEquals(len(tests), 2)
        self.assertEquals(self.calls, 2)

        self.raiseError = None

        self.clock.advance(0.8)
        self.assertEquals(len(tests), 2)
        self.assertEquals(self.calls, 2)

        self.clock.advance(0.2)
        self.assertTrue(d.called)
        self.assertEquals(len(tests), 2)
        self.assertEquals(self.calls, 3)

        return d

    def test_ranOutOfRetries(self):
        """
        If the function fails too many times, the whole Deferred fails.
        """
        self.raiseError = ArbitraryException

        d = self.call.start([1, 1])

        self.clock.advance(1)
        self.assertEquals(self.calls, 1)

        self.clock.advance(1)
        self.assertEquals(self.calls, 2)

        self.clock.advance(1)
        # already failed, not called again
        self.assertEquals(self.calls, 2)
        return self.assertFailure(d, ArbitraryException)

    def test_customTester(self):
        """
        A failure tester function that only recognizes TypeErrors will cause
        the call to fail with another exception.
        """
        self.raiseError = TypeError

        # swallow type errors
        d = self.call.start(self.sillyIterator,
                            lambda f: isinstance(f.value, TypeError) or f)

        self.clock.advance(0)
        self.assertEquals(self.calls, 1)
        self.clock.advance(1)
        self.assertEquals(self.calls, 2)
        self.clock.advance(1)
        self.assertEquals(self.calls, 3)

        self.raiseError = ArbitraryException

        self.clock.advance(1)
        self.assertEquals(self.calls, 4)

        self.clock.advance(1)
        # already failed, not called again
        self.assertEquals(self.calls, 4)
        return self.assertFailure(d, ArbitraryException)

    def test_failureInFailureTester(self):
        """
        Failures in the failure tester will cause the entire call to fail and
        the error will be propagated to the call's Deferred.
        """
        self.raiseError = RuntimeError

        def failingTester(f):
            raise ArbitraryException()

        d = self.call.start(self.sillyIterator, failingTester)

        self.assertEquals(self.calls, 1)
        self.clock.advance(1)
        # already failed, not called again
        self.assertEquals(self.calls, 1)
        return self.assertFailure(d, ArbitraryException)

    def test_resetBackoff(self):
        """
        Changing the backoff iterator causes the next delay to be taken from
        the new iterator.
        """
        self.raiseError = ArbitraryException

        everysecond = [1, 1, 1, 1]
        everytwo = [2, 2, 2, 2]

        d = self.call.start(everysecond)

        self.assertEquals(self.calls, 0)
        self.clock.advance(1)
        self.assertEquals(self.calls, 1)
        self.clock.advance(1)
        self.assertEquals(self.calls, 2)

        self.call.resetBackoff(everytwo)

        self.clock.advance(1)
        # the call gets executed, because the previous delay is still pending
        self.assertEquals(self.calls, 3)
        self.clock.advance(1)
        # but another advance of one second does not fire the call
        self.assertEquals(self.calls, 3)
        self.clock.advance(1)
        self.assertEquals(self.calls, 4)
        self.clock.pump([2, 2, 2])

        return self.assertFailure(d, ArbitraryException)

    def test_cancel(self):
        """
        Cancelling the call causes it to fail with a CancelledError.
        """
        self.raiseError = ArbitraryException
        everysecond = [1, 1, 1, 1]

        d = self.call.start(everysecond)
        self.assertEquals(self.calls, 0)
        self.clock.advance(1)
        self.assertEquals(self.calls, 1)
        self.clock.advance(1)
        self.assertEquals(self.calls, 2)
        d.cancel()
        self.clock.advance(1)
        self.assertEquals(self.calls, 2)

        return self.assertFailure(d, defer.CancelledError)
