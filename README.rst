This is txpostgres is a library for accessing a PostgreSQL_ database from the Twisted_
framework. It builds upon asynchronous features of the Psycopg_ database
library, which in turn exposes the asynchronous features of libpq_, the
PostgreSQL C library.

It requires a version of Psycopg that includes support for `asynchronous
connections`_ (versions 2.2.0 and later) and a reasonably recent Twisted (it
has been tested with Twisted 10.2 onward). Alternatively, psycopg2cffi_ or
psycopg2-ctypes_ can be used in lieu of Psycopg.

txpostgres tries to present an interface that will be familiar to users of both
Twisted and Psycopg. It features a Cursor_ wrapper class that mimics the
interface of a `Psycopg cursor`_ but returns Deferred_ objects. It also provides
a Connection_ class that is meant to be a drop-in replacement for Twisted's
`adbapi.Connection`_ with some small differences regarding connection
establishing.

The main advantage of txpostgres over Twisted's built-in database support is
non-blocking connection building and complete lack of thread usage.

It runs on Python 2.6, 2.7, 3.4, 3.5 and PyPy_.

If you got txpostgres as a source tarball, you can run the automated test suite
and install the library with::

  tar xjf txpostgres-x.x.x.tar.bz2
  cd txpostgres-x.x.x
  trial test
  sudo python setup.py install

Alternatively, just install it from PyPI_::

  pip install txpostgres

The library is distributed under the MIT License, see the LICENSE file for
details. You can contact the author, Jan Urbański, at wulczer@wulczer.org. Feel
free to download the source_, file bugs in the `issue tracker`_ and consult the
documentation_

.. _PostgreSQL: http://www.postgresql.org/
.. _Twisted: http://twistedmatrix.com/
.. _Psycopg: http://initd.org/psycopg/
.. _Python: http://www.python.org/
.. _libpq: http://www.postgresql.org/docs/current/static/libpq-async.html
.. _`asynchronous connections`: http://initd.org/psycopg/docs/advanced.html#async-support
.. _psycopg2cffi: https://github.com/chtd/psycopg2cffi
.. _psycopg2-ctypes: http://pypi.python.org/pypi/psycopg2ct
.. _Cursor: http://wulczer.github.com/txpostgres/txpostgres.html#txpostgres.Cursor
.. _Psycopg cursor: http://initd.org/psycopg/docs/cursor.html#cursor
.. _Deferred: http://twistedmatrix.com/documents/current/api/twisted.internet.defer.Deferred.html
.. _Connection: http://wulczer.github.com/txpostgres/txpostgres.html#txpostgres.Connection
.. _adbapi.Connection: http://twistedmatrix.com/documents/current/api/twisted.enterprise.adbapi.Connection.html
.. _PyPy: http://pypy.org/
.. _PyPI: http://pypi.python.org/pypi/txpostgres
.. _source: https://github.com/wulczer/txpostgres
.. _issue tracker: https://github.com/txpostgres/issues
.. _documentation: http://txpostgres.readthedocs.org/

.. image:: https://secure.travis-ci.org/wulczer/txpostgres.png?branch=master
