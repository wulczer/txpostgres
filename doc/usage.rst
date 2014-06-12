Module usage
============

Basic usage of the module is not very different from using Twisted's adbapi:

.. literalinclude:: basic_example.py

If you want you can use the :class:`~txpostgres.txpostgres.Cursor` class directly, with a
interface closer to Psycopg. Note that using this method you have to make sure
never to execute a query before the previous one finishes, as that would
violate the PostgreSQL asynchronous protocol.

.. literalinclude:: cursor_example.py

Using transactions
------------------

Every query executed by txpostgres is committed immediately. If you need to
execute a series of queries in a transaction, use the
:meth:`~txpostgres.txpostgres.Connection.runInteraction` method:

.. literalinclude:: transaction_example.py

Customising the connection and cursor factories
-----------------------------------------------

You might want to customise the way txpostgres creates connections and cursors
to take advantage of Psycopg features like :psycopg:`dictionary cursors
<extras.html#dictionary-like-cursor>`. To do that, define a subclass of
:class:`~txpostgres.txpostgres.Connection` and override
:attr:`connectionFactory` or :attr:`cursorFactory` class attributes to use your
custom code. Here's an example of how to use dict cursors:

.. literalinclude:: dictcursor_example.py


Listening for database notifications
------------------------------------

Being an asynchronous driver, txpostgres supports the PostgreSQL NOTIFY_
feature for sending asynchronous notifications to connections. Here is an
example script that connects to the database and listens for notifications on
the `list` channel. Every time a notification is received, it interprets the
payload as part of the name of a table and outputs a list of tables with names
containing that payload.

.. literalinclude:: notify_example.py

To try it execute the example code and then open another session using psql_
and try sending some NOTIFY_ events::

  $ psql postgres
  psql (9.1.2)
  Type "help" for help.

  postgres=> notify list, 'user';
  NOTIFY
  postgres=> notify list, 'auth';
  NOTIFY

You should see the example program outputting lists of table names containing
the payload::

  $ python notify_example.py
  Listening on the `list' channel
  Tables with `user' in their name:
  pg_user_mapping
  Tables with `auth' in their name:
  pg_authid
  pg_auth_members

.. _NOTIFY: http://www.postgresql.org/docs/current/static/sql-notify.html
.. _psql: http://www.postgresql.org/docs/current/static/app-psql.html

Automatic reconnection
----------------------

The module includes provision for automatically reconnecting to the database in
case the connection gets broken. To use it, pass a
:class:`~txpostgres.reconnection.DeadConnectionDetector` instance to
:class:`~txpostgres.txpostgres.Connection`. You can customise the detector
instance or subclass it to add custom logic. See the documentation for
:class:`~txpostgres.reconnection.DeadConnectionDetector` for details.

When a :class:`~txpostgres.txpostgres.Connection` is configured with a
detector, it will automatically start the reconnection process whenever it
encounters a certain class of errors indicative of a disconnect. See
:func:`~txpostgres.reconnection.defaultDeathChecker` for more.

While the connection is down, all attempts to use it will result in immediate
failures with :exc:`~txpostgres.reconnection.ConnectionDead`. This is to
prevent sending additional queries down a link that's known to be down.

Here's an example of using automatic reconnection in txpostgres:

.. literalinclude:: reconnection_example.py

You can run this snippet and then try restarting the database. Logging lines
should appear, as the connection gets automatically recovered.

Choosing a Psycopg implementation
---------------------------------

To use txpostgres, you will need a recent enough version of Psycopg_, namely
2.2.0 or later. Since parts of Psycopg are written in C, it is not available
on some Python implementations, like PyPy. When first imported, txpostgres
will try to detect if an API-compatible implementation of Psycopg is available.

You can force a certain implementation to be used by exporing an environment
variable `TXPOSTGRES_PSYCOPG_IMPL`. Recognized values are:

psycopg2
  Force using Psycopg_, do not try any fallbacks.

psycopg2cffi
  Use psycopg2cffi_, a psycopg2 implementation based on cffi, known to work on
  PyPy.

psycopg2ct
  Use psycopg2ct_, an older psycopg2 implementation using ctypes, also
  compatible with PyPy.

.. _Psycopg: http://initd.org/psycopg/
.. _psycopg2cffi: https://github.com/chtd/psycopg2cffi
.. _psycopg2ct: https://github.com/mvantellingen/psycopg2-ctypes
