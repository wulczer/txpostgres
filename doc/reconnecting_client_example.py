import logging
from twisted.internet import reactor, task, defer
from twisted.python.util import println
from txpostgres.txpostgres import Connection

# noinspection PyProtectedMember
class ReconnectingConnection(object):
    """
    Wrapper around txpostgres.connection that will keep trying to reconnect forever
    The goals here are to suppress all connection errors and running queries without
    having to worry about the underlying connection state.
    """

    def __init__(self, logger=None, recon_delay=3, debug=False, **connect_kwargs):
        # For debugging purposes
        defer.setDebugging(debug)
        self.logger = logger if logger else logging.getLogger(__name__)
        if not logger:
            logging.basicConfig()
            self.logger.setLevel(logging.DEBUG)

        self.conn = None
        # We use a state-machine like
        self.state = 'not_connected'
        # Delay before attempting to reconnect
        self.recon_delay = recon_delay
        # See self.connect
        self.connection_args = connect_kwargs

        self.pending_requests = {}

    def connect(self):
        """
        Call the wrapped connection's connect, but catch whatever comes back, inlcuding future connection errors
        """
        self.conn = Connection(reconnector=self.connection_failed)
        d = self.conn.connect(**self.connection_args)
        d.addCallback(self.connection_made)
        d.addErrback(self.connection_failed)

    def connection_made(self, conn):
        self.logger.debug('Connection was made!')
        self.state = 'connected'
        #If we had pending requests, now is the time to run them
        for requestD, query in self.pending_requests.items():
            d = self.conn.runQuery(*query[0], **query[1])
            d.addCallback(requestD.callback)
            d.addErrback(requestD.errback)
            del(self.pending_requests[requestD])

    def connection_failed(self, exc):
        self.logger.info('Connection Failed, resetting it! (%s)', exc)

        # Don't close the connection before it had a chance to reconnect
        if self.state != 'reconnecting':
            self.state = 'disconnected'
            try:
                self.conn.close()
                del self.conn
            except:
                # Just. Keep. Running.
                self.logger.warning('Could not cleanly close the connection.')
        reactor.callLater(self.recon_delay, self.reconnect)

    def reconnect(self):
        self.connect()
        self.state = 'reconnecting'

    def runQuery(self, *args, **kwargs):
        #TODO: add a some sort of deffered magic here, for when we are disconnected and want to start querying later
        if self.state == 'connected':
            return self.conn.runQuery(*args, **kwargs)
        else:
            #If we are not connected, remember the query to call it later, when we have a connection
            d = defer.Deferred()
            self.pending_requests[d] = (args, kwargs)
            return d


def print_query(conn, *_):
    conn.logger.debug('I am currently... %s, with %i pending requests', conn.state, len(conn.pending_requests.keys()))

    conn.runQuery('select tablename from pg_tables').addCallback(println)


def myprint(self, *args):
    print args

if __name__ == '__main__':
    connection = ReconnectingConnection(debug=True,
                                        database='postgres',
                                        user='',
                                        password='',
                                        port=5432,
                                        host='localhost')
    connection.connect()

    task.LoopingCall(print_query, connection).start(1)

    reactor.run()