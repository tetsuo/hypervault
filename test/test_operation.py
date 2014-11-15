
import threading
import unittest
from hv.datastore import Datastore, ConnectionPool, \
  ConnectionError
from hv.entity import Key
from config import conf

class OperationTest(unittest.TestCase):
  def setUp(self):
    self.db = Datastore(conf['db_conns'],
      conf['db_pool_max'],
      conf['db_pool_block_timeout'])

  def tearDown(self):
    self.db.disconnect()

  def test_put_get(self):
    data = dict(beep='boop')
    key = self.db.put(12, 1, **data)
    assert key.shard_id == 12
    res = self.db.get(key)
    assert res[2].get('beep') == 'boop'
