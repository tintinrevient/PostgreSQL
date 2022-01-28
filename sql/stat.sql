VACUUM test_schema.test_table;
-- 42 seconds
VACUUM FULL test_schema.test_table;
-- 21 seconds
REINDEX SCHEMA test_schema;

-- AUTOVACUUM
SELECT name, setting FROM pg_settings WHERE name='track_counts';
SELECT name, setting FROM pg_settings WHERE category = 'Autovacuum';
SELECT schemaname, relname, last_vacuum, last_autovacuum, last_analyze, last_autoanalyze, vacuum_count, autovacuum_count, analyze_count, autoanalyze_count FROM pg_stat_user_tables;
SELECT relname, n_live_tup, n_dead_tup FROM pg_stat_user_tables;

ALTER TABLE test_schema.test_table SET (autovacuum_enabled = true, autovacuum_vacuum_scale_factor = 0.1);

-- BLOAT_RATIO
SELECT
	pg_size_pretty(pg_relation_size('test_schema.test_table')) as table_size_bytes,
	(pgstattuple('test_schema.test_table')).dead_tuple_percent,
	pg_size_pretty(pg_relation_size('test_schema.test_table_pkey')) as pkey_index_size,
	100-(pgstatindex('test_schema.test_table_pkey')).avg_leaf_density as pkey_bloat_ratio,
	pg_size_pretty(pg_relation_size('test_schema.test_table_uuid')) as uuid_index_size,
	100-(pgstatindex('test_schema.test_table_uuid')).avg_leaf_density as uuid_bloat_ratio,
	pg_size_pretty(pg_relation_size('test_schema.test_table_feature_entity')) as feature_entity_index_size,
	100-(pgstatindex('test_schema.test_table_feature_entity')).avg_leaf_density as feature_entity_bloat_ratio;

-- SLOW QUERY
SELECT pid, user, query_start, now() - query_start AS query_time, query, state, wait_event_type, wait_event
FROM pg_stat_activity WHERE (now() - pg_stat_activity.query_start) > interval '5 minutes';

SELECT * FROM pg_stat_statements LIMIT 1;

-- LIVE TUPLES
SELECT reltuples FROM pg_class WHERE relname='test_table';
SELECT n_live_tup, n_dead_tup FROM pg_stat_user_tables WHERE relname='test_table';

-- TABLE SIZE
SELECT relname, pg_size_pretty(pg_table_size(oid)) FROM pg_class WHERE relname = 'test_table';

-- TABLE SETTING
SELECT reloptions FROM pg_class WHERE relname='test_tables';

-- AUTOVACUUM WORKER PROCESS
-- ps -axww | grep autovacuum
SELECT * FROM pg_stat_progress_vacuum;
