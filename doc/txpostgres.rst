API documentation
-----------------

All txpostgres APIs are documented here.

txpostgres.txpostgres
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: txpostgres.txpostgres.Connection
    :exclude-members: connectionFactory, cursorFactory

.. autoclass:: txpostgres.txpostgres.Cursor

.. autoclass:: txpostgres.txpostgres.ConnectionPool
    :exclude-members: connectionFactory

    .. automethod:: txpostgres.txpostgres.ConnectionPool.__init__

.. autoclass:: txpostgres.txpostgres._PollingMixin

.. autoexception:: txpostgres.txpostgres.AlreadyConnected

.. autoexception:: txpostgres.txpostgres.RollbackFailed

.. autoexception:: txpostgres.txpostgres.UnexpectedPollResult

.. autoexception:: txpostgres.txpostgres.AlreadyPolling


.. autoexception:: txpostgres.AlreadyConnected

.. autoexception:: txpostgres.RollbackFailed

.. autoexception:: txpostgres.UnexpectedPollResult

.. autoexception:: txpostgres.AlreadyPolling
