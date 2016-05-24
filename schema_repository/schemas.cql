CREATE KEYSPACE avro WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE TABLE avro.schemas (
  client varchar,
  id int,
  subject varchar,
  version int,
  avro_schema varchar,
  PRIMARY KEY (client, id, subject, version)
);

CREATE TABLE avro.schemaids (
  id counter,
  client varchar,
  PRIMARY KEY (client)
);

CREATE TABLE avro.configs (
    client text,
    global boolean,
    subject text,
    level text,
    PRIMARY KEY (client, global, subject)
);
