import logging
from twisted.internet import reactor, task
from twisted.python.util import println
from txpostgres.txpostgres import ReconnectingConnection


def print_query(conn, *_):
    conn.logger.debug('I am currently... %s, with %i pending requests', conn.state, len(conn.pending_requests.keys()))

    conn.runQuery('select tablename from pg_tables').addCallback(println)

if __name__ == '__main__':
    logging.basicConfig()
    connection = ReconnectingConnection(database='postgres',
                                        user='diallictive',
                                        password='diallictive',
                                        port=5432,
                                        host='localhost')
    connection.connect()

    task.LoopingCall(print_query, connection).start(1)

    reactor.run()