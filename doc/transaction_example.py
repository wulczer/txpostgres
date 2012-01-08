from txpostgres import txpostgres

from twisted.internet import reactor
from twisted.python import log, util

# connect to the database
conn = txpostgres.Connection()
d = conn.connect('dbname=postgres')

# define a callable that will execute inside a transaction
def interaction(cur):
    # the parameter is a txpostgres Cursor
    d = cur.execute('create table test(x integer)')
    d.addCallback(lambda _: cur.execute('insert into test values (%s)', (1, )))
    return d

# run the interaction, making sure that if the insert fails, the table won't be
# left behind created but empty
d.addCallback(lambda _: conn.runInteraction(interaction))

# close the connection, log any errors and stop the reactor
d.addCallback(lambda _: conn.close())
d.addErrback(log.err)
d.addBoth(lambda _: reactor.stop())

# start the reactor to kick off connection estabilishing
reactor.run()
