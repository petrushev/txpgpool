TxPgPool
========

A simple connection pooling for postgresql in Twisted.

Provides three ways of pooling:

* ``QueuePgPool`` - Tries to maintain ``min`` number of ready connections - if there are
  more clients will connect on demand but will not exceed ``max``
* ``StaticPgPool`` - Will maintain only one connection and keep the clients in queue for
  its usage
* ``NullPgPool`` - Does not maintain any live connections on its own - clients will
  connect to a maximum one connection through it and when done it will close it

All types provide a method ``runQuery`` which will return a deferred that will fire the
callback with the result of the query when the latter is available.

Requirements
------------

* ``psycopg2``
* ``txpostgresql``
* ``Twisted``
