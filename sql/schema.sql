CREATE SCHEMA test_schema;

CREATE SEQUENCE test_schema.id_seq;

CREATE TABLE test_schema.test_table (
	featureId		bigint		DEFAULT nextval('test_schema.id_seq') NOT NULL,
	featureDefRef	varchar,
	entityId		varchar,
	valueStr		varchar,
	active			boolean		DEFAULT true,
	uuid			varchar		NOT NULL,
	timestamp		timestamp	NOT NULL,
	expire			timestamp	NOT NULL,

	PRIMARY KEY (featureDefRef, entityId, uuid)
);

-- INDEX
CREATE INDEX test_table_uuid ON test_schema.test_table(uuid);
CREATE INDEX test_table_feature_entity ON test_schema.test_table(md5(featureDefRef::text || entityId::text));

SELECT * FROM pg_indexes WHERE tablename='test_table';
SELECT * FROM pg_tables WHERE tablename = 'test_table';

-- GRANT
ALTER TABLE test_schema.test_table OWNER TO admin;
CREATE USER admin WITH ENCRYPTED PASSWORD 'admin';
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA test_schema TO admin;
GRANT ALL ON SEQUENCE test_schema.id_seq TO admin;

-- EXTENSION
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
GRANT ALL ON TABLE pg_stat_statements TO admin;

CREATE EXTENSION IF NOT EXISTS pgstattuple;
GRANT EXECUTE ON FUNCTION pgstattuple(text) TO admin;
GRANT EXECUTE ON FUNCTION pgstatindex(text) TO admin;

SELECT * FROM pg_available_extensions WHERE name = 'pg_stat_statements' and installed_version is not null;

-- UNLOGGED
ALTER TABLE test_schema.test_table SET UNLOGGED;