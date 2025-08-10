-- Basic schema for integration tests
CREATE KEYSPACE IF NOT EXISTS app
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE app;

CREATE TABLE IF NOT EXISTS users (
  org_id int,
  id uuid,
  name text,
  age int,
  PRIMARY KEY ((org_id), id)
) WITH CLUSTERING ORDER BY (id ASC);

CREATE TABLE IF NOT EXISTS storage (
  filename text,
  ukey text,
  value map<text, text>,
  PRIMARY KEY (filename, ukey)
) WITH CLUSTERING ORDER BY (ukey ASC);


