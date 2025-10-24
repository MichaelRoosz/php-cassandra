#!/bin/bash

# Benchmark Comparison Script
# Runs benchmarks for both php-cassandra and DataStax driver and compares results

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.bench.yml"

echo "================================================"
echo "Cassandra PHP Driver Benchmark Comparison"
echo "================================================"
echo ""
echo "This will compare performance between:"
echo "  1. php-cassandra library (PHP 8.2)"
echo "  2. DataStax PHP driver via PECL (PHP 7.1)"
echo "  3. ScyllaDB PHP driver (PHP 8.2)"
echo ""
echo "Ensuring Cassandra is ready..."
echo ""

cd "$PROJECT_ROOT"

# Start Cassandra if not already running
docker-compose -f "$COMPOSE_FILE" up -d cassandra

# Wait for Cassandra to be healthy
echo "Waiting for Cassandra to be healthy..."
timeout=300
elapsed=0
until docker-compose -f "$COMPOSE_FILE" exec -T cassandra cqlsh -e "DESCRIBE KEYSPACES" > /dev/null 2>&1; do
    if [ $elapsed -ge $timeout ]; then
        echo "Error: Cassandra did not become ready in time"
        exit 1
    fi
    echo -n "."
    sleep 5
    elapsed=$((elapsed + 5))
done
echo ""
echo "Cassandra is ready!"
echo ""

# Create results directory
RESULTS_DIR="$PROJECT_ROOT/benchmarks/results"
mkdir -p "$RESULTS_DIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

echo "================================================"
echo "Running php-cassandra Benchmarks (PHP 8.2)"
echo "================================================"
echo ""

# Build and run php-cassandra benchmarks
docker-compose -f "$COMPOSE_FILE" build bench-php-cassandra
docker-compose -f "$COMPOSE_FILE" run --rm bench-php-cassandra > "$RESULTS_DIR/php-cassandra_${TIMESTAMP}.txt"

echo "Results saved to: $RESULTS_DIR/php-cassandra_${TIMESTAMP}.txt"
echo ""

echo "================================================"
echo "Running DataStax Driver Benchmarks (PHP 7.1)"
echo "================================================"
echo ""

# Build and run DataStax benchmarks
docker-compose -f "$COMPOSE_FILE" build bench-datastax
docker-compose -f "$COMPOSE_FILE" run --rm bench-datastax > "$RESULTS_DIR/datastax_${TIMESTAMP}.txt"

echo "Results saved to: $RESULTS_DIR/datastax_${TIMESTAMP}.txt"
echo ""

echo "================================================"
echo "Running ScyllaDB Driver Benchmarks (PHP 8.2)"
echo "================================================"
echo ""

# Build and run ScyllaDB benchmarks
docker-compose -f "$COMPOSE_FILE" build bench-scylladb
docker-compose -f "$COMPOSE_FILE" run --rm bench-scylladb > "$RESULTS_DIR/scylladb_${TIMESTAMP}.txt"

echo "Results saved to: $RESULTS_DIR/scylladb_${TIMESTAMP}.txt"
echo ""

echo "================================================"
echo "Raw Benchmark Output"
echo "================================================"
echo ""

echo "--- php-cassandra Library (PHP 8.2) ---"
grep -A 50 "=== php-cassandra Library Benchmarks ===" "$RESULTS_DIR/php-cassandra_${TIMESTAMP}.txt" | grep -v "JSON Results" | head -n 20
echo ""
echo "--- DataStax Driver (PHP 7.1) ---"
grep -A 50 "=== DataStax PHP Driver Benchmarks ===" "$RESULTS_DIR/datastax_${TIMESTAMP}.txt" | grep -v "JSON Results" | head -n 20
echo ""
echo "--- ScyllaDB Driver (PHP 8.2) ---"
grep -A 50 "=== ScyllaDB PHP Driver Benchmarks ===" "$RESULTS_DIR/scylladb_${TIMESTAMP}.txt" | grep -v "JSON Results" | head -n 20
echo ""

# Parse and compare JSON results
echo "================================================"
echo "Detailed Comparison"
echo "================================================"

php "$SCRIPT_DIR/compare-results.php" \
    "$RESULTS_DIR/php-cassandra_${TIMESTAMP}.txt" \
    "$RESULTS_DIR/datastax_${TIMESTAMP}.txt" \
    "$RESULTS_DIR/scylladb_${TIMESTAMP}.txt" 2>/dev/null || true

echo ""
echo "Full results available in:"
echo "  - $RESULTS_DIR/php-cassandra_${TIMESTAMP}.txt"
echo "  - $RESULTS_DIR/datastax_${TIMESTAMP}.txt"
echo "  - $RESULTS_DIR/scylladb_${TIMESTAMP}.txt"
echo ""
echo "To run individual benchmarks:"
echo "  docker-compose -f benchmarks/docker-compose.bench.yml run --rm bench-php-cassandra"
echo "  docker-compose -f benchmarks/docker-compose.bench.yml run --rm bench-datastax"
echo "  docker-compose -f benchmarks/docker-compose.bench.yml run --rm bench-scylladb"
echo ""
echo "To stop and clean up:"
echo "  docker-compose -f benchmarks/docker-compose.bench.yml down"
echo ""

