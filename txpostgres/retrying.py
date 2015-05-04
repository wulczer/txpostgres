"""
Simple implementation of a retrying call.

This code is based on a snippet send to the twisted-python mailing list:

http://twistedmatrix.com/pipermail/twisted-python/2009-November/020818.html

as well as published as txretry:

https://github.com/fluidinfo/txretry

It has been modified to allow resetting the backoff iterator and not store a
list of past failures.
"""

import random

from twisted.internet import reactor, defer, task
from twisted.python import failure


def simpleBackoffIterator(initialDelay=1.0, maxDelay=3600,
                          factor=2.7182818284590451, jitter=0.11962656472,
                          maxRetries=10, now=True):
    """
    Yields increasing timeout values between retries of a call. The default
    factor and jitter are taken from Twisted's :tm:`ReconnectingClientFactory
    <internet.protocol.ReconnectingClientFactory>`.

    :var initialDelay: Initial delay, in seconds.
    :vartype initialDelay: :class:`float`

    :var maxDelay: Maximum cap for the delay, if zero then no maximum is
        applied.
    :vartype maxDelay: :class:`float`

    :var factor: Multiplicative factor for increasing the delay.
    :vartype factor: :class:`float`

    :var jitter: Randomness factor to include when increasing the delay, to
        prevent stampeding.
    :vartype jitter: :class:`float`

    :var maxRetries: If non-zero, only yield so many values after exhausting
        the iterator.
    :vartype maxRetries: :class:`int`

    :var now: If the very first delay yielded should always be zero.
    :vartype now: :class:`bool`
    """
    retries = 0
    delay = initialDelay

    if now:
        retries += 1
        yield 0.0

    while not maxRetries or retries < maxRetries:
        retries += 1

        delay = delay * factor
        if jitter:
            delay = random.normalvariate(delay, delay * jitter)

        if maxDelay:
            delay = min(delay, maxDelay)
        yield delay


class RetryingCall(object):
    """
    Calls a function repeatedly, passing it args and keyword args. Failures are
    passed to a user-supplied failure testing function. If the failure is
    ignored, the function is called again after a delay whose duration is
    obtained from a user-supplied iterator. The start method (below) returns a
    :d:`Deferred` that fires with the eventual non-error result of calling the
    supplied function, or fires its errback if no successful result can be
    obtained before the delay backoff iterator raises :class:`StopIteration`.

    It is important to note the behaviour when the delay of any of the steps is
    zero. The function is the called synchronously, ie. control does not go
    back to the reactor between obtaining the delay from the iterator and
    calling the function if the iterator returns zero.

    The :meth:`.resetBackoff` method replaces the backoff iterator with another
    one and is useful to reset the delay if some phase of the process has
    succeeded and that makes the desirable initial delay different again.
    """
    reactor = None

    def __init__(self, f, *args, **kw):
        if self.reactor is None:
            self.reactor = reactor
        self._f = f
        self._args = args
        self._kw = kw

    def _err(self, fail):
        if self.failure is None:
            self.failure = fail
        try:
            if not self.cancelled:
                fail = self._failureTester(fail)
        except:
            self._deferred.errback()
        else:
            if isinstance(fail, failure.Failure):
                self._deferred.errback(fail)
            else:
                self._call()

    def _call(self):
        try:
            delay = self._backoffIterator.next()
        except StopIteration:
            self._deferred.errback(self.failure)
        else:
            self._callWithDelay(delay)

    def _callWithDelay(self, delay):
        # if the delay is 0, call the function synchronously
        if not delay:
            self._inProgress = defer.maybeDeferred(
                self._f, *self._args, **self._kw)
        else:
            self._inProgress = task.deferLater(
                self.reactor, delay, self._f, *self._args, **self._kw)
        self._inProgress.addCallbacks(self._deferred.callback, self._err)

    def _cancel(self, d):
        self.cancelled = True
        self._inProgress.cancel()

    def start(self, backoffIterator=None, failureTester=None):
        """
        Start the call and retry it until it succeeds and fails.

        :param backoffIterator: A zero-argument callable that should
            return a iterator yielding reconnection delay periods. If
            :class:`None` then :func:`.simpleBackoffIterator` will be
            used.
        :type backoffIterator: callable

        :param failureTester: A one-argument callable that will be called with
            a :tm:`Failure <python.failure.Failure>` instance each time the
            function being retried fails. It should return
            :class:`None` if the call should be retried or a
            :tm:`Failure <python.failure.Failure>` if the retrying process
            should be stopped. If :class:`None` is used for this parameter,
            retrying will never stop until the backoff iterator is exhausted.
        :type failureTester: callable
        """
        self.resetBackoff(backoffIterator)

        if failureTester is None:
            failureTester = lambda _: None
        self._failureTester = failureTester

        self._deferred = defer.Deferred(self._cancel)
        self._inProgress = None
        self.failure = None
        self.cancelled = False

        self._call()
        return self._deferred

    def resetBackoff(self, backoffIterator=None):
        """
        Replace the current backoff iterator with a new one.
        """
        if backoffIterator is None:
            backoffIterator = simpleBackoffIterator()
        self._backoffIterator = iter(backoffIterator)
