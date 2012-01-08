API documentation
-----------------

.. autoclass:: txpostgres.Connection
    :exclude-members: connectionFactory, cursorFactory

.. autoclass:: txpostgres.Cursor

.. autoclass:: txpostgres.ConnectionPool
    :exclude-members: connectionFactory

    .. automethod:: txpostgres.ConnectionPool.__init__

.. autoclass:: txpostgres._PollingMixin

.. autoexception:: txpostgres.AlreadyConnected

.. autoexception:: txpostgres.RollbackFailed

.. autoexception:: txpostgres.UnexpectedPollResult

.. autoexception:: txpostgres.AlreadyPolling
