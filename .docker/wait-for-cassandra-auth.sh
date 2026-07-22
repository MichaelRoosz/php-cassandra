#!/usr/bin/env bash
set -euo pipefail

host="127.0.0.1"
port="9042"
user="cassandra"
password="cassandra"
retries=60

compose="docker compose -f docker-compose.auth.yml"

until $compose exec -T cassandra bash -lc "/opt/cassandra/bin/cqlsh -u ${user} -p ${password} ${host} ${port} -e \"DESCRIBE KEYSPACES\" > /dev/null 2>&1"; do
  ((retries--)) || { echo "Cassandra did not become ready in time" >&2; exit 1; }
  echo "Waiting for Cassandra (auth) at ${host}:${port}..." >&2
  sleep 5
done

echo "Cassandra (auth) is up"
