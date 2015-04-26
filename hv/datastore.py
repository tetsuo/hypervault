import os
import psycopg2
from contextlib import contextmanager
from psycopg2.extras import DictConnection, \
  LoggingConnection, register_hstore as _register_hstore
from Queue import LifoQueue, Empty, Full
from hv.util import memoize
from hv.entity import Key


class DatastoreError(Exception):
  pass


class PoolError(DatastoreError):
  pass


class ConnectionError(PoolError):
  """Could not get connection from pool."""


def _gen_get_pool_id(r):
  """Generates a `get_pool_id` function for the given interval.
  """
  @memoize
  def get_pool_id(n):
    i = z = 0
    if n >= r[-1:][0]:
      return None
    while i < len(r):
      if r[i] <= n < r[i+1]:
        break
      i += 2
      z += 1
    return z    
  return get_pool_id


class Datastore(object):
  """PostgreSQL connection manager.

  ``connections``
    Holds an array of dicts which are being passed to `psycopg2.connect` respectively.
    However those dicts should also contain a special `shards` value which adds
    meaning to all that fuss going around.

  ``pool_max``
    Maximum number of psycopg2 connections that are going to be kept-alive for each pool.

  ``pool_block_timeout``
    Maximum number of seconds to wait for getting a connection from a pool before
    the request is dropped.

  ``logger``
    Should hold a Logger object if LoggingConnection is needed.
    By default every connection is an instance of DictConnection.
  """

  def __init__(self, connections, pool_max=10, pool_block_timeout=5, logger=None):
    if logger:
      # XXX: LoggingConnection doesn't return dicts.
      connection_factory = LoggingConnection
    else:
      connection_factory = DictConnection

    r, self._pools = [], []
    for pool_id, connection in enumerate(connections):
      # we are copying these dicts before popping
      connection = connection.copy()
      # we store the logical shards interval for this pool in r
      r.extend([int(m) for m in connection.pop('shards').split('-')])
      self._pools.append(ConnectionPool(max_connections=pool_max,
        block_timeout=pool_block_timeout,
        connection_factory=connection_factory,
        pool_id=pool_id, logobj=logger, **connection))

    # this returns the pool_id for the given interval
    self._get_pool_id = _gen_get_pool_id(r)

  def get_connection(self, shard_id):
    """Returns a psycopg2 connection for the given shard_id.
    Beware that this connection should be sent back into the pool
    when you are finished.
    """
    return self._pools[self._get_pool_id(shard_id)].get_connection()

  def put_connection(self, connection):
    """Sends connection back into the pool where it belonged.
    """
    self._pools[connection.pool_id].put_connection(connection)
  
  def disconnect(self):
    """Closes every connection in every pool.
    """
    for pool in self._pools:
      pool.disconnect()
      
  def reinstantiate(self):
    """Reinstantiates connection pools.
    Make sure you have closed every connection before calling this method.
    """
    self.disconnect()
    for pool in self._pools:
      pool.reinstantiate()

  @contextmanager
  def cursor(self, shard_id):
    """Returns a context manager delivering a connection for the given shard_id.
    """
    connection = self.get_connection(shard_id)
    cursor = connection.cursor()
    try:
      yield cursor
    finally:
      cursor.close()
      self.put_connection(connection)

  def put(self, shard_id, kind, **kwargs):
    """Writes data to the specified shard, where kind is an integer
    which is not stored within hstore field and used for differentiating 
    between entity types.
    """
    key = None
    with self.cursor(shard_id) as cursor:
      q = 'INSERT INTO shard%04d.entities (type, body) ' % shard_id
      cursor.execute(q + 'VALUES (%s, %s) RETURNING id', (kind, kwargs))
      key = cursor.fetchone()[0]
    return Key(key)

  def get(self, key):
    if not isinstance(key, Key):
      raise DatastoreError('key must be of type Key')
    result = None
    with self.cursor(key.shard_id) as cursor:
      q = 'select * from shard%04d.entities' % key.shard_id
      cursor.execute(q + ' where id = %s', (key.id,))
      result = cursor.fetchone()
    return result

class ConnectionPool(object):
  """Thread-safe connection pool implementation.

  ``max_connections``
    Maximum number of connections.

  ``block_timeout``
    Maximum number of seconds to wait.
  """

  def __init__(self, max_connections=10, block_timeout=5,
         **kwds):
    self.max_connections = max_connections
    self.block_timeout = block_timeout
    
    # kwds is being sent directly to dbapi, so we pop
    self._pool_id = kwds.pop('pool_id')
    self._logobj = kwds.pop('logobj')
    self.kwds = kwds
    
    # make sure max_connections is valid
    is_valid = isinstance(max_connections, int) and \
      max_connections > 0
    if not is_valid:
      raise ValueError('max_connections must be a positive int')

    # if process id is changed, we will close all connections,
    # and reinstantiate this object
    self._pid = os.getpid()

    # this is where we define the pool, and fill it with None values
    self._pool = LifoQueue(max_connections)
    while True:
      try:
        self._pool.put_nowait(None)
      except Full:
        break

    # open connections
    self._connections = []

  def _checkpid(self):
    """Closes all connections and reinstantiates the object if pid is changed.
    """
    if self._pid == os.getpid():
      return

    self.disconnect()
    self.reinstantiate()
    
  def _make_connection(self):
    """Creates a fresh connection.
    """
    connection = psycopg2.connect(**self.kwds)
    
    if not hasattr(connection, 'pool_id'):
      connection.pool_id = self._pool_id

    # we don't need transaction, so let's turn autocommit on
    connection.autocommit = True

    # pass the logger object if needed
    if isinstance(connection, LoggingConnection):
      connection.initialize(self._logobj)

    _register_hstore(connection, unicode=True)

    # encoding
    connection.set_client_encoding('UTF8')

    # timezone
    cursor = connection.cursor()
    cursor.execute("set timezone to 'UTC'")
    cursor.close()

    self._connections.append(connection)
    return connection
  
  def get_connection(self):
    """Returns a psycopg2 connection for the given shard_id.
    Raises `ConnectionError` if necessary.
    """
    self._checkpid()

    connection = None
    try:
      # wait for a connection
      connection = self._pool.get(block=True, 
                    timeout=self.block_timeout)
    except Empty:
      # timeout
      raise ConnectionError('no connection available')

    # create a new connection if it is not initialized yet
    if connection is None:
      connection = self._make_connection()

    return connection

  def put_connection(self, connection):
    """Sends connection back into the pool.
    """
    self._checkpid()

    try:
      self._pool.put_nowait(connection)
    except Full:
      # reinstantiate may have caused this.
      # this connection is useless now.
      pass

  def disconnect(self):
    """Closes every connection in every pool.
    """
    for connection in self._connections:
      connection.close()
    self._connections = []

  def reinstantiate(self):
    """Reinstantiates connection pools.
    Make sure you have closed every connection before calling this method.
    """
    # let's add these back to kwds
    self.kwds['pool_id'] = self._pool_id
    self.kwds['logobj'] = self._logobj

    self.__init__(max_connections=self.max_connections,
            block_timeout=self.block_timeout,
            **self.kwds)
