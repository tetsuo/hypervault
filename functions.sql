CREATE OR REPLACE FUNCTION initdb() RETURNS VOID AS $func$
DECLARE
  first int := 1;
  last int := 8;
  schema_name_prefix varchar := 'shard';
  schema_name varchar;
  search_path varchar = 'public';
BEGIN
  EXECUTE 'CREATE EXTENSION hstore';
  FOR i IN first..last LOOP
    schema_name := schema_name_prefix || lpad(i::varchar, 4, '0');
    search_path := search_path || ',' || schema_name;
    RAISE NOTICE 'CREATING SHARD: %', schema_name;
    EXECUTE 'CREATE SCHEMA ' || schema_name;
    EXECUTE 'CREATE SEQUENCE ' || schema_name || '.table_id_seq';
    EXECUTE 'CREATE OR REPLACE FUNCTION ' || schema_name || '.next_id(OUT result bigint) AS $$ DECLARE our_epoch bigint := 1379365531352;seq_id bigint;now_millis bigint;shard_id int := ' || i || ';BEGIN SELECT nextval(''' || schema_name || '.table_id_seq'') % 1024 INTO seq_id;SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000) INTO now_millis;result := (now_millis - our_epoch) << 23;result := result | (shard_id << 10);result := result | (seq_id);END;$$ LANGUAGE PLPGSQL;';
    EXECUTE 'CREATE TABLE ' || schema_name || '.entities (id bigint NOT NULL DEFAULT ' || schema_name || '.next_id(), type smallint NOT NULL, body HSTORE NOT NULL, CONSTRAINT entities_pk PRIMARY KEY (id));';
    EXECUTE 'CREATE INDEX entities_idx_user_id ON ' || schema_name || '.entities USING BTREE(((body->''user_id'')::bigint) DESC NULLS LAST) WHERE type = 2;';
  END LOOP;
  RAISE NOTICE 'SETTING search_path';
  EXECUTE 'SET search_path TO ' || search_path;
END;
$func$ LANGUAGE PLPGSQL;