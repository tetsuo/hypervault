
_KEY_EPOCH = 1379365531352

from datetime import datetime

class Key(object):
  def __init__(self, key):
    self._key = key
    # ilk 41-bit eklenme tarihi
    self.created = datetime.utcfromtimestamp((_KEY_EPOCH + (key >> 23))/1000)
    # sonraki 13-bit shard_id
    self.shard_id = (key & (1<<23)-1) >> 10
    # kalan 10-bit eklenme sirasi
    # 1 sn icinde en fazla toplam 1024 giris yapilabilir
    self.added_id = key & 1023

  def __repr__(self):
    return self._key
