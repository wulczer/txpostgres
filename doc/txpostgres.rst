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

txpostgres.reconnection
~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: txpostgres.reconnection.DeadConnectionDetector

.. autofunction:: txpostgres.reconnection.defaultDeathChecker

.. autofunction:: txpostgres.reconnection.defaultReconnectionIterator

.. autoexception:: txpostgres.reconnection.ConnectionDead

txpostgres.retrying
~~~~~~~~~~~~~~~~~~~

.. autoclass:: txpostgres.retrying.RetryingCall
