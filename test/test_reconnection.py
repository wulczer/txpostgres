from twisted.internet import defer
from twisted.trial import unittest

from txpostgres import reconnection


class ArbitraryException(Exception):
    pass


class Reconnectable(object):
    def __init__(self):
        self.calls = []
        self.connects = []

    def call(self):
        self.calls.append(defer.Deferred())
        return self.calls[-1]

    def connect(self):
        self.connects.append(defer.Deferred())
        return self.connects[-1]

    def close(self):
        pass


class BrokenReconnectable(Reconnectable):

    def close(self):
        raise RuntimeError()


class TestDeadConnectionDetector(unittest.TestCase):

    def setUp(self):
        self.recoveries = 0
        self.reconnectable = Reconnectable()
        self.detector = reconnection.DeadConnectionDetector(self.deathChecker)

        self.detector.setReconnectable(self.reconnectable)
        self.detector.addRecoveryHandler(self.recovery)

    def deathChecker(self, f):
        return f.check(ArbitraryException)

    def recovery(self):
        self.recoveries += 1

    def brokenRecovery(self):
        self.recoveries += 1
        raise RuntimeError()

    def test_basic(self):
        """
        Only the failure recognized by the death checker causes reconnection to
        trigger. Until the connection recovers, all calls through the detector
        are immediately failed.
        """
        # the first call is successful
        d1 = self.detector.callChecking(self.reconnectable.call)
        self.reconnectable.calls.pop().callback(None)

        self.assertEquals(len(self.reconnectable.connects), 0)

        # the second call has an error, but the death checker does not
        # recognize it
        d2 = self.detector.callChecking(self.reconnectable.call)
        self.reconnectable.calls.pop().errback(RuntimeError())

        self.assertFailure(d2, RuntimeError)

        self.assertEquals(len(self.reconnectable.connects), 0)

        # the third and the fourth call discover that the connection is dead,
        # but only one reconnection is triggered
        d3 = self.detector.callChecking(self.reconnectable.call)
        d4 = self.detector.callChecking(self.reconnectable.call)
        self.reconnectable.calls.pop().errback(ArbitraryException())
        self.reconnectable.calls.pop().errback(ArbitraryException())

        self.assertFailure(d3, ArbitraryException)
        self.assertFailure(d4, ArbitraryException)

        # only one reconnection
        self.assertEquals(len(self.reconnectable.connects), 1)

        # the fifth call finds the connection dead
        d5 = self.detector.callChecking(self.reconnectable.call)
        self.assertEquals(len(self.reconnectable.calls), 0)

        self.assertFailure(d5, reconnection.ConnectionDead)

        rd = self.reconnectable.connects.pop()

        self.assertEquals(self.recoveries, 0)
        rd.callback(None)
        self.assertEquals(self.recoveries, 1)

        d6 = self.detector.callChecking(self.reconnectable.call)
        self.reconnectable.calls.pop().callback(None)

        d = defer.gatherResults([d1, d2, d3, d4, d5, d6])
        return d.addCallback(lambda ret: self.assertEquals(ret[5], None))

    def test_brokenRecovery(self):
        """
        Errors in recovery handlers are logged and discarded.
        """
        self.detector.removeRecoveryHandler(self.recovery)
        self.detector.addRecoveryHandler(self.brokenRecovery)

        d = self.detector.callChecking(self.reconnectable.call)
        self.reconnectable.calls.pop().errback(ArbitraryException())
        self.assertFailure(d, ArbitraryException)

        self.reconnectable.connects.pop().callback(None)
        # the error gets logged and discarded
        self.assertEquals(len(self.flushLoggedErrors(RuntimeError)), 1)

        d = self.detector.callChecking(self.reconnectable.call)
        self.reconnectable.calls.pop().callback(None)
        return d.addCallback(self.assertEquals, None)

    def test_brokenReconnectable(self):
        """
        Errors when closing the reconnectable are logged and discarded.
        """
        reconnectable = BrokenReconnectable()
        self.detector.setReconnectable(reconnectable)

        d = self.detector.callChecking(reconnectable.call)
        reconnectable.calls.pop().errback(ArbitraryException())
        self.assertFailure(d, ArbitraryException)

        reconnectable.connects.pop().callback(None)
        # the the error in BrokenReconnectable.close got ignored
        self.assertEquals(len(self.flushLoggedErrors()), 0)

        d = self.detector.callChecking(reconnectable.call)
        reconnectable.calls.pop().callback(None)
        return d.addCallback(self.assertEquals, None)
