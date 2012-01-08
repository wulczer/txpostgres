import psycopg2
import psycopg2.extras
from txpostgres import txpostgres

from twisted.internet import reactor
from twisted.python import log, util


def dict_connect(*args, **kwargs):
    kwargs['connection_factory'] = psycopg2.extras.DictConnection
    return psycopg2.connect(*args, **kwargs)


class DictConnection(txpostgres.Connection):
    connectionFactory = staticmethod(dict_connect)


# connect using the custom connection class
conn = DictConnection()
d = conn.connect('dbname=postgres')

# run a query and print the result
d.addCallback(lambda _: conn.runQuery('select * from pg_tablespace'))
# access the column by its name
d.addCallback(lambda result: util.println('All tablespace names:',
                                          [row['spcname'] for row in result]))

# close the connection, log any errors and stop the reactor
d.addCallback(lambda _: conn.close())
d.addErrback(log.err)
d.addBoth(lambda _: reactor.stop())

# start the reactor to kick off connection estabilishing
reactor.run()
