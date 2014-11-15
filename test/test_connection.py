
import threading
import unittest
from hv.datastore import Datastore, ConnectionPool, \
  ConnectionError
from config import conf

class ConnectionTest(unittest.TestCase):
  def setUp(self):
    self.db = Datastore(conf['db_conns'],
      conf['db_pool_max'],
      conf['db_pool_block_timeout'])

  def tearDown(self):
    self.db.disconnect()

  def test_get_pool_id(self):
    for n in range(1, 30):
      x = self.db._get_pool_id(n)
      if n < 9:
        assert x == 0
      elif n >= 9 and n < 17:
        assert x == 1
      else:
        assert x == None

  def test_pool(self):
    p = self.db._pools[0]
    q = p._pool

    assert q.empty() == False
    assert q.full() == True
    assert len(p._connections) == 0

    c = p.get_connection()
    assert q.full() == False
    assert len(p._connections) == 1
    assert c.pool_id == 0
    assert c.autocommit == True

    p.put_connection(c)
    assert q.empty() == False
    assert q.full() == True
    assert len(p._connections) == 1

    n = 0
    while n < conf['db_pool_max']:
      c = p.get_connection()
      n += 1
    assert q.empty() == True
    assert len(p._connections) == conf['db_pool_max']
    self.assertRaises(ConnectionError, p.get_connection)

    p.put_connection(p._connections[0])
    p.put_connection(p._connections[1])
    assert q.empty() == False
    assert q.full() == False

    c1 = p.get_connection()
    c2 = p.get_connection()
    assert c1.get_backend_pid() != c2.get_backend_pid()

  def test_pool_multithread(self):
    p = self.db._pools[0]
    q = p._pool

    def worker(n):
      assert len(p._connections) == n

      if n >= conf['db_pool_max']:
        self.assertRaises(ConnectionError, p.get_connection)
        assert q.empty() == True
        assert len(p._connections) == conf['db_pool_max']
      else:
        assert q.empty() == False
        c = p.get_connection()
        if n == conf['db_pool_max']:
          assert q.empty() == True
          assert len(p._connections) == conf['db_pool_max']

    for i in range(conf['db_pool_max'] + 1):
      t = threading.Thread(target=worker, args=(i,))
      t.start()
      t.join()

  def test_pool_multithread2(self):
    p = self.db._pools[0]
    q = p._pool

    def worker():
      c = p.get_connection()
      self.db.put_connection(c)

    for i in range(conf['db_pool_max'] * 5):
      t = threading.Thread(target=worker)
      t.start()
      t.join()

    assert q.full() == True

  def test_disconnect(self):
    p = self.db._pools[0]

    n = 0
    while n < conf['db_pool_max']:
      c = p.get_connection()
      assert c.closed == False
      n += 1

    assert len(p._connections) == conf['db_pool_max']
    self.db.disconnect()
    assert len(p._connections) == 0

  def test_cursor(self):
    p = self.db._pools[0]
    q = p._pool

    assert q.full() == True
    assert len(p._connections) == 0

    c = None
    ver = None
    with self.db.cursor(5) as cur:
      assert q.full() == False
      assert len(p._connections) == 1
      c = cur
      cur.execute('SELECT version()')
      ver = cur.fetchone()

    assert c.closed == True
    assert c.connection.closed == False
    assert q.full() == True
    assert len(p._connections) == 1
