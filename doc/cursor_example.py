from txpostgres import txpostgres

from twisted.internet import reactor
from twisted.python import log, util

# define the libpq connection string and the query to use
connstr = 'dbname=postgres'
query = 'select tablename from pg_tables order by tablename'

# connect to the database
conn = txpostgres.Connection()
d = conn.connect('dbname=postgres')

def useCursor(cur):
    # execute a query
    d = cur.execute(query)
    # fetch the first row from the result
    d.addCallback(lambda _: cur.fetchone())
    # output it
    d.addCallback(lambda result: util.println('First table name:', result[0]))
    # and close the cursor
    return d.addCallback(lambda _: cur.close())

# create a cursor and use it
d.addCallback(lambda _: conn.cursor())
d.addCallback(useCursor)

# log any errors and stop the reactor
d.addErrback(log.err)
d.addBoth(lambda _: reactor.stop())

# start the reactor to kick off connection estabilishing
reactor.run()
