
conf = dict(
  db_conns = [
    dict(shards='1-9', host='', port='5432',
      user='x', password='x', database='hvtest'),
    dict(shards='9-17', host='192.168.2.24', port='5432',
      user='x', password='x', database='hvtest')
  ],
  db_num_shards = 20,
  db_pool_max = 10,
  db_pool_block_timeout = 2
)
