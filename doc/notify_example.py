from txpostgres import txpostgres

from twisted.internet import reactor
from twisted.python import util


def outputResults(results, payload):
    print "Tables with `%s' in their name:" % payload
    for result in results:
        print result[0]


def observer(notify):
    if not notify.payload:
        print "No payload"
        return

    query = ("select tablename from pg_tables "
             "where tablename like '%%' || %s || '%%'")
    d = conn.runQuery(query, (notify.payload, ))
    d.addCallback(outputResults, notify.payload)


# connect to the database
conn = txpostgres.Connection()
d = conn.connect('dbname=postgres')

# add a NOTIFY observer
conn.addNotifyObserver(observer)
# start listening for NOTIFY events on the 'list' channel
d.addCallback(lambda _: conn.runOperation("listen list"))
d.addCallback(lambda _: util.println("Listening on the `list' channel"))

# process events until killed
reactor.run()
