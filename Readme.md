# hypervault

pgsql connection manager for scalability freaks.

```
pip install hv
```

This is the implementation of the pattern described by Instagram in ["Sharding & IDs at Instagram"](http://instagram-engineering.tumblr.com/post/10853187575/sharding-ids-at-instagram) article.

Besides for that, it wraps over [psycopg2](http://initd.org/psycopg/) with custom connection pooling support and stores dicts in [hstore](http://www.postgresql.org/docs/9.3/static/hstore.html) k/v pairs which may be indexed by PostgreSQL.

## api

### hv.entity.Key(id)

An instance of the `Key` represents a unique key (64-bits long) for an entity and has the following attributes:

`created` returns the UTC datetime corresponding to the first 41-bits of the numeric id.

`shard_id` holds a 13-bits integer and represents the logical shard.

`added_id` is the remaining 10-bits and represents an auto-incrementing sequence, modulus 1024. This means we can generate 1024 IDs, per shard, per millisecond.

### hv.datastore.Datastore(connections, pool_max, pool_block_timeout, logger)

`connections` holds an array of dicts which are being passed to psycopg2 respectively. However those dicts should also contain a special `shards` value which adds meaning to all that fuss going around.

This example shows the bare minimum you need to create a `Datastore` instance:

```py
connections = [
  dict(shards='1-9', host='192.168.2.23', port='5432', user='x', password='x', database='x'),
  dict(shards='9-17', host='192.168.2.24', port='5432', user='x', password='x', database='x')
]
db = Datastore(connections)
```

In this case, we assume PostgreSQL running on 192.168.2.23 contains shards (schemas) starting from 1 to 9 (9 not included) and on 192.168.2.24 we have shards from 9 to 17.

`pool_max` is the maximum number of psycopg2 connections that are going to be kept alive for every _connection_ we have passed. (default: 10)

`pool_block_timeout` is the maximum number of seconds to wait for getting a connection from pool before the request is dropped. (default: 5)

`logger` should hold a [Logger object](https://docs.python.org/2/library/logging.html#logger-objects) if you want to use [LoggingConnection](http://initd.org/psycopg/docs/extras.html#psycopg2.extras.LoggingConnection). By default every connection is an instance of [DictConnection](http://initd.org/psycopg/docs/extras.html#psycopg2.extras.DictConnection).

A `Datastore` instance has the following methods:

### get_connection(shard_id)

Returns a psycopg2 connection for the given `shard_id`. 

Beware that this connection should be sent back into the pool when you are finished, or otherwise you know- universe will collapse and Trinity will die :(

### put_connection(connection)

Sends `connection` back into the pool where it belonged.

### cursor(shard_id)

Returns a context manager delivering a connection for the given `shard_id`.

This is a convenience method that saves you from forgetting to call `put_connection`.

Example:

```py
with db.cursor(5) as cur:
  cur.execute('SELECT version()')
  ver = cur.fetchone()
```

### put(shard_id, kind, **kwargs)

Writes data to the specified shard, where `kind` is an integer which is not stored within hstore field and used for differentiating between entity types.

Returns a `hv.entity.Key`.

Example:

```py
data = dict(beep='boop')
key = db.put(12, 1, **data)
```

### get(key)

Fetches the data with the given `key`.

`key` must be of type `hv.entity.Key`.

Example:

```py
key = Key(307821103844175873)
res = db.get(key)
```

### disconnect()

Closes every connection in every pool.

### reinstantiate()

Reinstantiates connection pools. Make sure you have closed every connection before calling this method.

## license

mit