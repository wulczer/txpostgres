from txpostgres import txpostgres, reconnection

from twisted.internet import reactor, task


class LoggingDetector(reconnection.DeadConnectionDetector):

    def startReconnecting(self, f):
        print '[*] database connection is down (error: %r)' % f.value
        return reconnection.DeadConnectionDetector.startReconnecting(self, f)

    def reconnect(self):
        print '[*] reconnecting...'
        return reconnection.DeadConnectionDetector.reconnect(self)

    def connectionRecovered(self):
        print '[*] connection recovered'
        return reconnection.DeadConnectionDetector.connectionRecovered(self)


def result(res):
    print '-> query returned result: %s' % res


def error(f):
    print '-> query failed with %r' % f.value


def connectionError(f):
    print '-> connecting failed with %r' % f.value


def runLoopingQuery(conn):
    d = conn.runQuery('select 1')
    d.addCallbacks(result, error)


def connected(_, conn):
    print '-> connected, running a query periodically'
    lc = task.LoopingCall(runLoopingQuery, conn)
    return lc.start(2)


# connect to the database using reconnection
conn = txpostgres.Connection(detector=LoggingDetector())
d = conn.connect('dbname=postgres')

# if the connection failed, log the error and start reconnecting
d.addErrback(conn.detector.checkForDeadConnection)
d.addErrback(connectionError)
d.addCallback(connected, conn)

# process events until killed
reactor.run()
