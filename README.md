# PostgreSQL

It starts from the urge to use `PostgreSQL` as `Redis` for a key-value store to quickly insert, delete and update key-value pairs.

This table can be identified easily, as there is no foreign-key contraint on it.

![the_table](pix/schema.png)

![the_meeting_at_the_golden_gate_giotto](pix/the_meeting_at_the_golden_gate_giotto.jpg)

## Benchmark

### Test
```python
CREATE UNLOGGED TABLE kv_test(key text, value int);
CREATE INDEX ON kv_test (key);
 
for $row_num in [1000, 10000, 100000, 1000000]:
  TRUNCATE kv_test
  generate $row_num random values $rand[$row_num]
  for $i := 1 .. $row_num:
    INSERT INTO kv_test VALUES ($rand[i], $rand[i])
  VACUUM ANALYZE kv_test
  for $i := 1 .. $row_num:
    SELECT * FROM kv_test WHERE key = $rand[i]
```

### Result

The calculated time to `write` key-value pairs is in milliseconds:
<p float="left">
	<img src="pix/write.png" width=600 />
</p>

The calculated time to `read` key-value pairs is in milliseconds:
<p float="left">
	<img src="pix/read.png" width=600 />
</p>

## Bloat

### Root Cause

The `PostgreSQL` system implements the `MVCC` (Multiversion Concurrency Control) and the way MVCC is implemented, it can introduce bloat’s in your system.

Bloat happens due to frequent `DELETE` and `UPDATE` operations, which can leave a lot of unused space in table or index relation files on disk. Over the time this space can build up and cause the performance degradation for both tables and indexes.

When a `DELETE` or `UPDATE` statement is used in `PostgreSQL`, it does not physically remove that row from the disk. For `DELETE`, the affected rows are just marked as invisible; For `UPDATE`, the affected rows are marked as invisible and the new version of those rows are `INSERT`ed. These invisible rows are also called `dead rows` or `dead tuples`.

### Analysis

```sql
CREATE EXTENSION IF NOT EXISTS pgstattuple;

CREATE TABLE test as SELECT x, md5(random()::text) as y FROM generate_series(1, 1000000) x;
CREATE INDEX ON test (x);

SELECT 
	pg_size_pretty(pg_relation_size('test')) as table_size, 
	pg_size_pretty(pg_relation_size('test_x_idx')) as index_size, 
	(pgstattuple('test')).dead_tuple_percent;
```

```bash
table_size | index_size | dead_tuple_percent 
------------+------------+--------------------
 65 MB     | 21 MB      |                  0
```

```sql
DELETE FROM test WHERE x % 3 = 0;

ANALYZE test;
SELECT 
	pg_size_pretty(pg_relation_size('test')) as table_size, 
	pg_size_pretty(pg_relation_size('test_x_idx')) as index_size, 
	(pgstattuple('test')).dead_tuple_percent;
```

After the `deletion` operation, the table size remains the same, however the output of `pgstattuple` shows that 29.78% of disk space is wasted. It’s taking the space in table but not useable anymore.
```bash
table_size | index_size | dead_tuple_percent 
------------+------------+--------------------
 65 MB     | 21 MB      |              29.78
```

Now let's take a look at the index.
```sql
SELECT 
	pg_size_pretty(pg_relation_size('test')) as table_size, 
	pg_size_pretty(pg_relation_size('test_x_idx')) as index_size, 
	100-(pgstatindex('test_x_idx')).avg_leaf_density as bloat_ratio;
```

```bash
table_size  | index_size |    bloat_ratio    
------------+------------+-------------------
   65 MB    |   21 MB    | 9.939999999999998
````

```sql
UPDATE test SET x = x + 2 WHERE x % 2 = 0;

SELECT 
	pg_size_pretty(pg_relation_size('test')) as table_size, 
	pg_size_pretty(pg_relation_size('test_x_idx')) as index_size, 
	100-(pgstatindex('test_x_idx')).avg_leaf_density as bloat_ratio;
```

After the `update` operation, index has become 41.08% bloated. This means that the performance of this index will degrade because these many entries are either empty or pointing to dead tuples.
```bash
 table_size | index_size | bloat_ratio 
------------+------------+-------------
   65 MB    |   21 MB    |       41.08
````

### Solution

`VACUUM`: 
* Remove the dead tuples by adding that space to free space map so that it can be reutilised by the database system;
* Remove index entries that point to these dead tuples;
* However this space will not be returned to the OS and will not shrink the table size.
* Normally `autovacuum` background process can take care of that. The default setting are set to vacuum tables whenever their is `20% change` in the table.
```sql
VACUUM test;
```

`VACUUM FULL`:
* Remove all bloats in a table and its associated indexes completely;
* Reclaim the disk space to the OS. This will reduce the table size on disk.
```sql
VACUUM FULL test;
VACUUM(FULL, ANALYZE, VERBOSE) test;
```

`REINDEX`:
* Remove the bloat from indexes;
* Rebuild the index specified or all indexes on the table;
* This option does not blocks the reads but will `block the writes`;
* However `CONCURRENTLY` option can be used to avoid that but it may take longer to complete than standard index creation.
```sql
REINDEX TABLE test;
REINDEX INDEX test_x_idx;
REINDEX SCHEMA public;
```

## Query

### Create user

Create a user:
```sql
CREATE USER admin WITH ENCRYPTED PASSWORD 'admin';
```

Grant the operation rights:
```sql
GRANT ALL PRIVILEGES ON DATABASE test TO admin;
GRANT USAGE ON SCHEMA test_schema TO admin;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA test_schema TO admin;
```

### Index

Create an index:
```sql
CREATE INDEX test_index ON test.test_schema(key);
```

Select all the indexes given a specified table:
```sql
SELECT * FROM pg_indexes WHERE tablename like 'test%';
```

### Temp Table

Create a temp table:
```sql
CREATE TEMP TABLE temp_test AS SELECT * FROM test WHERE key > 10;
```

Find the schema for a specified temp table:
```sql
SELECT pn.nspname, pc.relname 
FROM pg_class pc, pg_namespace pn 
WHERE pc.relnamespace = pn.oid and pc.relname ilike 'temp_test';
```

## Performance

### Options

Option 1 is to delete the rows that match the condition directly from the big table.

```sql
CREATE TEMP TABLE temp_test AS
SELECT t.key FROM test t WHERE t.key > 100 AND t.key <= 200;

DELETE FROM test t
USING temp_test tt
WHERE tt.key = t.key;
			
DROP TABLE temp_test;
```

Option 2 is to save the rows that DON'T match the condition into a temp table, truncate the big table and insert the rows from the temp table back into the big table.

```sql
BEGIN; -- typically faster and safer wrapped in a single transaction

SET LOCAL temp_buffers = '1000MB'; -- enough to hold the temp table

CREATE TEMP TABLE temp_test AS
SELECT t.* FROM test t WHERE t.key > 100 AND t.key <= 200; -- copy surviving rows into temporary table

TRUNCATE test; -- empty table - truncate is very fast for big tables

INSERT INTO test SELECT * FROM temp_test; -- insert back surviving rows

COMMIT;
```

### Result

For a million rows of data, the average deletion time is round 3-5 seconds. The above two options vary slightly with respect to the execution time.

The factors `index` as below and `transaction` as above don't contribute noticeably to the performance.
```sql
CREATE INDEX test_key_idx on test(key);

SELECT t.* FROM test t 
WHERE t.key > 100 AND t.key <= 200;
```

With the column `created` introduced for all the rows, it takes some time to filter the condition on timestamp. However, `index` on the `created` column contributes unnoticeably to the performance.
```sql
CREATE INDEX test_created_idx on test(created);

SELECT * FROM test t
WHERE DATE_PART('day', now()::timestamp - t.created::timestamp) > 3
```

With the table altered as `unlogged`, to insert a million rows of data, the insertion time is `320 seconds`.
```sql
ALTER TABLE test SET UNLOGGED;
```

With the table altered as `logged`, to insert a million rows of data, the insertion time is `320 seconds`.
```sql
ALTER TABLE test SET LOGGED;
```

## References
* https://www.cybertec-postgresql.com/en/postgresql-vs-redis-vs-memcached-performance/
* https://spin.atomicobject.com/2021/02/04/redis-postgresql/
* https://www.compose.com/articles/faster-performance-with-unlogged-tables-in-postgresql/
* https://stackoverflow.com/questions/8290900/best-way-to-delete-millions-of-rows-by-id
* https://stackoverflow.com/questions/13998139/optimize-postgres-query-on-timestamp-range/14007963
* https://wiki.postgresql.org/wiki/Slow_Query_Questions
* https://wiki.postgresql.org/wiki/Performance_Optimization
* https://www.highgo.ca/2021/03/20/how-to-check-and-resolve-bloat-in-postgresql/
