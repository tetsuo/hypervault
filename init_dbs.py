# You will most probably need to tweak these a bit according to your specs:
conns = [
  dict(shards='1-9', host='', port='5432', user='onur', password='z', database='hvtest'),
  dict(shards='9-17', host='192.168.2.24', port='5432', user='onur', password='z', database='hvtest')
]
init_fn_sql = \
    """
    CREATE OR REPLACE FUNCTION hv_init() RETURNS VOID AS $func$
    DECLARE
        first int := %s;
        last int := %s;
        schema_name_prefix varchar := 'shard';
        schema_name varchar;
        search_path varchar = 'public';
    BEGIN
        FOR i IN first..last LOOP
            schema_name := schema_name_prefix || lpad(i::varchar, 4, '0');
            search_path := search_path || ',' || schema_name;
            EXECUTE 'CREATE SCHEMA ' || schema_name;
            EXECUTE 'CREATE SEQUENCE ' || schema_name || '.table_id_seq';
            EXECUTE 'CREATE OR REPLACE FUNCTION ' || schema_name || '.next_id(OUT result bigint) AS $$ DECLARE our_epoch bigint := 1379365531352;seq_id bigint;now_millis bigint;shard_id int := ' || i || ';BEGIN SELECT nextval(''' || schema_name || '.table_id_seq'') %% 1024 INTO seq_id;SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000) INTO now_millis;result := (now_millis - our_epoch) << 23;result := result | (shard_id << 10);result := result | (seq_id);END;$$ LANGUAGE PLPGSQL;';
            EXECUTE 'CREATE TABLE ' || schema_name || '.entities (id bigint NOT NULL DEFAULT ' || schema_name || '.next_id(), type smallint NOT NULL, body HSTORE NOT NULL, updated TIMESTAMP, CONSTRAINT entities_pk PRIMARY KEY (id));';
        END LOOP;
        EXECUTE 'SET search_path TO ' || search_path;
    END;
    $func$ LANGUAGE PLPGSQL;
    """

from hv.datastore import Datastore
db = Datastore(conns)

for i, pool in enumerate(db._pools):
  first, last = \
    (int(m) for m in conns[i].get('shards').split('-'))
  conn = pool.get_connection()
  cur = conn.cursor();
  cur.execute(init_fn_sql, (first, last-1))
  if cur.statusmessage == 'CREATE FUNCTION':
    cur.callproc('hv_init')
    print cur.statusmessage
  cur.close()
  pool.put_connection(conn)
db.disconnect()
