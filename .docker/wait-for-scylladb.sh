#!/usr/bin/env bash
set -euo pipefail

host="127.0.0.1"
port="9042"
retries=60

until docker compose  -f docker-compose.scylladb.yml exec -T scylladb bash -lc "cqlsh ${host} ${port} -e \"DESCRIBE KEYSPACES\" > /dev/null 2>&1"; do
  ((retries--)) || { echo "Cassandra did not become ready in time" >&2; exit 1; }
  echo "Waiting for Cassandra at ${host}:${port}..." >&2
  sleep 5
done

echo "Cassandra is up"
