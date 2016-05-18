CREATE KEYSPACE avro WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

CREATE TABLE schemas (
  client varchar,
  id int,
  subject string,
  version int,
  schema varchar,
  PRIMARY KEY (client, id, subject, version)
);

CREATE TABLE schemaids (
  id counter,
  client varchar,
  PRIMARY KEY (client)
);
