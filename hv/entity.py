_KEY_EPOCH = 1379365531352

from datetime import datetime

class Key(object):
  def __init__(self, key):
    self.id = key
    # the UTC datetime corresponding to the first 41-bits of the numeric id
    self.created = datetime.utcfromtimestamp((_KEY_EPOCH + (key >> 23))/1000)
    # 13-bits integer and represents the logical shard
    self.shard_id = (key & (1<<23)-1) >> 10
    # represents an auto-incrementing sequence, modulus 1024. 
    # This means we can generate 1024 IDs, per shard, per millisecond.
    self.added_id = key & 1023
