
# -*- coding: utf-8 -*-

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
  """Havuzdan bağlantı alınamadı."""


def _gen_get_pool_id(r):
  """Verilen sanal shard aralıkları listesine göre `get_pool_id`
  fonksiyonunu oluşturur.

  `get_pool_id` verilen ``n`` (sanal shard id) değerine göre havuz id'si 
  dönmeye yarar. Bu fonksiyon daha sonra aynı değer ile çağrıldığında
  tekrar çalıştırılmaz, cache edilmiş sonucu döner.

  ``r``
    Sanal shard aralıkları listesi.
    
    5 dahil olmamak üzere, 1'den 5'e kadar, ve 11 dahil olmamak üzere
    5'den 11'e kadar iki aralığı örnekteki gibi tanımlıyoruz::

      [1, 5, 5, 11]
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
  """PostgreSQL veritabanı yöneticisi.

  Bağlantı havuzlarını oluşturur ve bağlantıların yönetimini sağlar.

  ``connections``
    `psycopg2.connect` argümanlarıyla beraber, her bağlantının hangi
    sanal shardlar (şemalar) için geçerli olduğunu belirten `shards`
    değerlerini de barındıran bağlantı listesi. örn::
      
      db_conns = [
        dict(shards='1-5', host='', port='5432',
           user='kullanici', password='parola', database='mydb'), 
      ...]

  ``pool_max``
    Her bağlantı için havuzda açık tutulacak bağlantı sayısı.

  ``pool_block_timeout``
    Havuzdan bu kadar saniye içinde bağlantı alınamazsa request iptal edilir.

  ``logger``
    Tanımlandığı durumda log mesajları bu loggera yazdırılır.
  """

  def __init__(self, connections, pool_max=10, pool_block_timeout=5, logger=None):
    if logger:
      # logger tanımlıysa bağlantıları LoggingConnection
      # classından oluşturuyoruz.
      # XXX: LoggingConnection sonuçları dict olarak dönmüyor.
      connection_factory = LoggingConnection
    else:
      connection_factory = DictConnection

    r, self._pools = [], []
    for pool_id, connection in enumerate(connections):
      # bu dictleri kopyalayalım, çünkü pop işlemi yapıyoruz.
      connection = connection.copy()
      # r değişkeninde bu havuza ait bağlantının barındırdığı
      # sanal shard aralılığını saklıyoruz.
      r.extend([int(m) for m in connection.pop('shards').split('-')])
      self._pools.append(ConnectionPool(max_connections=pool_max,
        block_timeout=pool_block_timeout,
        connection_factory=connection_factory,
        pool_id=pool_id, logobj=logger, **connection))

    # sanal shard aralıklarına göre havuz id'si dönecek fonksiyon
    self._get_pool_id = _gen_get_pool_id(r)

  def get_connection(self, shard_id):
    """Verilen sanal ``shard_id`` değeriyle ilişkilendirilmiş havuzdan
    bağlantı döner. Alınan bağlantıların işlem sonucunda `put_connection`
    ile tekrar havuza geri gönderilmesi elzemdir.
    """
    return self._pools[self._get_pool_id(shard_id)].get_connection()

  def put_connection(self, connection):
    """Verilen ``connection`` (bağlantıyı) ait olduğu havuza geri gönderir.
    """
    self._pools[connection.pool_id].put_connection(connection)
  
  def disconnect(self):
    """Tüm havuzlardaki tüm bağlantıları kapatır.
    """
    for pool in self._pools:
      pool.disconnect()
      
  def reinstantiate(self):
    """Tüm havuzları tekrar oluşturur.
    """
    self.disconnect()
    for pool in self._pools:
      pool.reinstantiate()

  @contextmanager
  def cursor(self, shard_id):
    """Cursor kullanımı için kolaylık sağlar.

    Verilen ``shard_id`` değerine göre ilgili havuzdan yeni bir bağlantı alır,
    bu bağlantı için `cursor` oluşturur. Context dışına çıkıldığında `cursor`
    kapatılır, bağlantı havuza geri gönderilir. örn::

      with g.db.cursor(5) as cursor:
        cursor.execute('SELECT version()')
        print cursor.fetchone()
    """
    connection = self.get_connection(shard_id)
    cursor = connection.cursor()
    try:
      yield cursor
    finally:
      cursor.close()
      self.put_connection(connection)

  def put(self, shard_id, kind, **kwargs):
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
  """Thread-safe bağlantı havuzu.

  Maksimum bağlantı sayısına ulaşılmışsa client'ın belirtilen süre
  kadar yeni bağlantı için beklemesini sağlar.

  ``max_connections``
    Açık tutulacak maksimum bağlantı sayısı.

  ``block_timeout``
    `LifoQueue`'dan bağlantı almak için saniye cinsinden maksimum 
    beklenecek süre.
  """

  def __init__(self, max_connections=10, block_timeout=5,
         **kwds):
    self.max_connections = max_connections
    self.block_timeout = block_timeout
    
    # kwds direkt olarak dbapi'a gönderileceği için
    # bu değerlerin bu dict içerisinde bulunmaması gerekiyor.
    self._pool_id = kwds.pop('pool_id')
    self._logobj = kwds.pop('logobj')
    self.kwds = kwds
    
    # max_connections değerinin geçerli olduğundan emin olalım.
    is_valid = isinstance(max_connections, int) and \
      max_connections > 0
    if not is_valid:
      raise ValueError('max_connections must be a positive int')

    # eğer process id değişmişse tüm bağlantıları kapatıp, bu objeyi
    # yeniden instantiate edeceğiz.
    self._pid = os.getpid()

    # havuzu tanımlıyoruz ve max_connections adet None
    # değerleriyle dolduruyoruz.
    self._pool = LifoQueue(max_connections)
    while True:
      try:
        self._pool.put_nowait(None)
      except Full:
        break

    # açık bağlantıların listesi
    self._connections = []

  def _checkpid(self):
    """Mevcut process id'yi kontrol et, eğer değişmişse tüm bağlantıları
    kapat ve objeyi tekrar instantiate et.
    """
    if self._pid == os.getpid():
      # değişmediyse sorun yok.
      return

    self.disconnect()
    self.reinstantiate()
    
  def _make_connection(self):
    """Taptaze bağlantı oluştur.
    """
    connection = psycopg2.connect(**self.kwds)
    
    # yönetici ile put_connection işlemi yaparken bu değerin
    # tanımlanmış olması gerekiyor.
    if not hasattr(connection, 'pool_id'):
      connection.pool_id = self._pool_id

    # transaction ihtiyacimiz yok, fazladan sql 
    # islemi yapmamak icin 'autocommit' secenegini aciyoruz.
    connection.autocommit = True

    # eger baglanti LoggingConnection ise initialize
    # etmek gerekiyor.
    if isinstance(connection, LoggingConnection):
      connection.initialize(self._logobj)

    # hstore kurulumu.
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
    """Havuzdan bağlantı döner.
    
    Bağlantı almak için istemci en fazla ``block_timeout`` ile belirtilmiş
    süre kadar bekletilir. Eğer süre sonunda bağlantı alınamadıysa 
    `ConnectionError` ile işlem sonlandırılır.
    """
    self._checkpid()

    connection = None
    try:
      # queue blok ederek havuzdan bağlantı almayı bekliyoruz.
      connection = self._pool.get(block=True, 
                    timeout=self.block_timeout)
    except Empty:
      # eğer süre sonunda bağlantı alınamamışsa ConnectionError
      # ile işlemi sonlandırıyoruz. 
      raise ConnectionError('no connection available')

    # eğer bağlantı aldık ama değeri None ise, o halde yeni bir
    # bağlantı oluşturuyoruz.
    if connection is None:
      connection = self._make_connection()

    return connection

  def put_connection(self, connection):
    """Bağlantıyı havuza geri gönderir.
    """
    self._checkpid()

    try:
      self._pool.put_nowait(connection)
    except Full:
      # bu hataya reinstantiate işlemi sebep olmuş olabilir, bu durumda
      # bağlantı havuza geri göndermek yerine uzay boşluğuna yolluyoruz.
      pass

  def disconnect(self):
    """Tüm açık bağlantıları kapat.
    """
    for connection in self._connections:
      connection.close()
    # disconnect tek başına kullanılmış olabilir.
    self._connections = []

  def reinstantiate(self):
    """Bu objeyi yeni process dahilinde tekrar instantiate et.
    Bunu çalıştırmadan önce tüm bağlantıların `disconnect` ile kapatılmış
    olduğundan emin olmak gerekiyor.
    """
    # şu değerleri tekrar kwds'e ekleyelim
    self.kwds['pool_id'] = self._pool_id
    self.kwds['logobj'] = self._logobj

    self.__init__(max_connections=self.max_connections,
            block_timeout=self.block_timeout,
            **self.kwds)
