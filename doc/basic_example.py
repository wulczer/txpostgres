from txpostgres import txpostgres

from twisted.internet import reactor
from twisted.python import log, util

# connect to the database
conn = txpostgres.Connection()
d = conn.connect('dbname=postgres')

# run the query and print the result
d.addCallback(lambda _: conn.runQuery('select tablename from pg_tables'))
d.addCallback(lambda result: util.println('All tables:', result))

# close the connection, log any errors and stop the reactor
d.addCallback(lambda _: conn.close())
d.addErrback(log.err)
d.addBoth(lambda _: reactor.stop())

# start the reactor to kick off connection estabilishing
reactor.run()
