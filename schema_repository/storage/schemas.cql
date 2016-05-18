CREATE KEYSPACE IF NOT EXISTS schema_registry WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE schema_registry.schemas (
  client varchar,
  subject varchar,
  version int,
  id int,
  avro_schema text,
  PRIMARY KEY (client, subject, version),
);

CREATE TABLE schema_registry.configs (
  client varchar,
  global boolean,
  subject varchar,
  level varchar,
  PRIMARY KEY (client, global, subject),
);
