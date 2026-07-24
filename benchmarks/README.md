# Cassandra PHP Driver Benchmarks

This directory contains benchmarks comparing the performance of `php-cassandra` library against two other PHP Cassandra drivers.

## Overview

The benchmarks compare:
- **php-cassandra**: Modern PHP library (PHP 8.2) - this project
- **DataStax PHP Driver**: Legacy PECL extension (PHP 7.1)
- **ScyllaDB PHP Driver**: Modern PECL extension fork (PHP 8.2) - [he4rt/scylladb-php-driver](https://github.com/he4rt/scylladb-php-driver)

All drivers are tested against the same Cassandra instance running in Docker, performing identical operations to ensure fair comparison.

## Benchmarks

The following operations are benchmarked:

1. **benchInsertAndSelectWithoutTypeInfo**: Insert 100 rows and select 100 without explicit type information
2. **benchInsertAndSelectWithTypeInfo**: Insert 100 rows and select one with explicit type information
3. **benchPagedQuery**: Query 500 rows with pagination (page size 50)
4. **benchPreparedInsert**: Use prepared statements to insert 100 rows
5. **benchSimpleSelect**: One simple select from a table

## Prerequisites

- Docker and Docker Compose installed
- PHP CLI (for comparison script)
- Sufficient disk space for Docker images (~2GB)
- Available ports: 9142 (Cassandra)

## Running Benchmarks

### Quick Start - Run Comparison

The easiest way to run and compare both benchmarks:

```bash
cd /path/to/php-cassandra
chmod +x benchmarks/run-comparison.sh
./benchmarks/run-comparison.sh
```

This will:
1. Start Cassandra in Docker
2. Build all three benchmark containers
3. Run all three benchmarks
4. Display comparison results
5. Save detailed results to `benchmarks/results/`

### Run Individual Benchmarks

The benchmarks use a dedicated `docker-compose.bench.yml` file to keep them separate from the main project.

Start Cassandra first:
```bash
docker-compose -f benchmarks/docker-compose.bench.yml up -d cassandra
```

Wait for Cassandra to be ready (check with `docker-compose -f benchmarks/docker-compose.bench.yml logs -f cassandra`).

Run php-cassandra benchmarks:
```bash
docker-compose -f benchmarks/docker-compose.bench.yml run --rm bench-php-cassandra
```

Run DataStax driver benchmarks:
```bash
docker-compose -f benchmarks/docker-compose.bench.yml run --rm bench-datastax
```

Run ScyllaDB driver benchmarks:
```bash
docker-compose -f benchmarks/docker-compose.bench.yml run --rm bench-scylladb
```

Clean up when done:
```bash
docker-compose -f benchmarks/docker-compose.bench.yml down
```

### Using PHPBench (php-cassandra only)

For more detailed PHPBench reports on the php-cassandra library:

```bash
# Start Cassandra
docker-compose up -d cassandra

# Run PHPBench locally (requires PHP 8.2+ and composer install)
vendor/bin/phpbench run --report=default

# Or run specific benchmarks
vendor/bin/phpbench run benchmarks/QueryBench.php --report=aggregate
```

### Compression benchmarks

LZ4 (de)compression uses the optional native `lz4` PHP extension when it is
installed, and falls back to a pure-PHP implementation otherwise — so it is
worth knowing whether compressing frames actually pays off or just burns CPU,
and how much the extension matters. Two benchmarks — both against a real node —
answer this:

```bash
# 1) Real server, several-MB blob, compression on vs. off (full local speed).
#    Requires a reachable node (set APP_CASSANDRA_HOST / APP_CASSANDRA_PORT).
vendor/bin/phpbench run benchmarks/CompressionBench.php --report=aggregate

# 2) Real server round-trips of a several-MB blob over a range of SIMULATED
#    network bandwidths, compression on vs. off, printed as a speed-up table.
APP_CASSANDRA_HOST=127.0.0.1 APP_CASSANDRA_PORT=9042 \
    php benchmarks/compression-network-bench.php
```

The network benchmark (2) is the one that answers the practical question. It
uses a `ThrottledSocket` transport that sleeps in proportion to the bytes it
transfers, so slower links spend proportionally more time on the wire.

With the native `lz4` extension installed, compression wins everywhere — even on
a full-speed local socket:

```
network link           |   uncompressed |     compressed |  speed-up
---------------------------------------------------------------------
full speed (local)     |       0.070 s |       0.050 s |    1.41x
50 MB/s (fast LAN)     |       0.240 s |       0.054 s |    4.46x
10 MB/s (100 Mbit)     |       0.923 s |       0.054 s |   16.95x
2 MB/s (slow WAN)      |       4.266 s |       0.069 s |   61.87x
0.5 MB/s (mobile)      |      16.855 s |       0.131 s |  128.88x
```

With the pure-PHP fallback, compression costs more CPU than a fast local socket
saves, but is still a large win from a fast LAN downwards:

```
network link           |   uncompressed |     compressed |  speed-up
---------------------------------------------------------------------
full speed (local)     |       0.058 s |       0.189 s |    0.31x
50 MB/s (fast LAN)     |       0.233 s |       0.188 s |    1.24x
10 MB/s (100 Mbit)     |       0.904 s |       0.183 s |    4.93x
2 MB/s (slow WAN)      |       4.264 s |       0.204 s |   20.91x
0.5 MB/s (mobile)      |      16.859 s |       0.263 s |   64.02x
```

In other words: install `ext-lz4` if you use compression; without it,
compression is a net loss only on a fast local socket and a growing win
as the network gets slower.

## Results

Benchmark results are saved to `benchmarks/results/` with timestamps. Each run produces three files:
- `php-cassandra_YYYYMMDD_HHMMSS.txt` - php-cassandra results
- `datastax_YYYYMMDD_HHMMSS.txt` - DataStax driver results
- `scylladb_YYYYMMDD_HHMMSS.txt` - ScyllaDB driver results

The comparison script will show:
- Execution times for each benchmark
- Operations per second
- Speedup factors (which driver is faster and by how much)
- Head-to-head comparison of all three drivers

## Customizing Benchmarks

### Modify Cassandra Version

Edit `docker-compose.yml` or set environment variable:
```bash
CASSANDRA_VERSION=4.1 ./benchmarks/run-comparison.sh
```

### Add New Benchmarks

1. Add method to `benchmarks/QueryBench.php` (for PHPBench)
2. Add equivalent method to `benchmarks/run-bench-php-cassandra.php`
3. Add equivalent method to `benchmarks/datastax/run-bench.php`
4. Add equivalent method to `benchmarks/scylladb/run-bench.php`
5. Update benchmark configuration arrays in all runner scripts

## Benchmark Environment

### php-cassandra Container
- **Base Image**: php:8.2-cli
- **PHP Version**: 8.2
- **Extensions**: zip, sockets
- **Dependencies**: Full project dependencies via Composer
- **Connection**: Socket or Stream mode (configurable via `APP_CASSANDRA_CONNECTION_MODE`)

### DataStax Driver Container
- **Base Image**: php:7.1-cli (Debian Buster)
- **PHP Version**: 7.1
- **Extensions**: cassandra (1.3.2 from PECL)
- **Dependencies**: DataStax C++ driver (2.16.2, built from source)
- **Connection**: Native driver connection

### ScyllaDB Driver Container
- **Base Image**: php:8.2-cli
- **PHP Version**: 8.2
- **Extensions**: cassandra (1.3.x from source)
- **Dependencies**: ScyllaDB C++ driver (2.17.1, built from source)
- **Connection**: Native driver connection with ScyllaDB shard-aware optimizations

### Cassandra Container
- **Image**: cassandra:5.0 (configurable)
- **Port**: 9042 (exposed as 9142 on host)
- **Configuration**: 512MB heap, SimpleStrategy replication

## Interpreting Results

### Metrics Explained

- **Total Time**: Sum of all iterations
- **Average Time**: Mean time per iteration
- **Ops/s**: Operations per second (higher is better)
- **Speedup**: Comparison factor between drivers

### Factors Affecting Performance

1. **Network Overhead**: Each driver uses different protocols and connection handling
2. **Serialization**: Type handling and encoding differ between drivers
3. **Connection Mode**: Socket vs Stream (php-cassandra only)
4. **PHP Version**: PHP 8.2 vs PHP 8.2 vs PHP 7.1 have different performance characteristics
5. **Prepared Statements**: Caching and reuse strategies differ
6. **C++ Driver**: ScyllaDB driver includes shard-aware optimizations not present in DataStax driver

### Fair Comparison Notes

- All drivers perform identical operations
- Same Cassandra instance and keyspace
- Same consistency levels
- Same data types and volumes
- Isolated containers prevent resource contention
- PHP version differences are noted in results

## Troubleshooting

### Cassandra Not Ready

If benchmarks fail with connection errors:
```bash
# Check Cassandra status
docker-compose logs cassandra

# Restart Cassandra
docker-compose down
docker-compose up -d cassandra
```

Wait for "Created default superuser role 'cassandra'" in logs.

### Build Errors

Clear Docker cache and rebuild:
```bash
docker-compose -f benchmarks/docker-compose.bench.yml down
docker system prune -f
docker-compose -f benchmarks/docker-compose.bench.yml build --no-cache bench-php-cassandra
docker-compose -f benchmarks/docker-compose.bench.yml build --no-cache bench-datastax
docker-compose -f benchmarks/docker-compose.bench.yml build --no-cache bench-scylladb
```

### Driver Extension Installation Fails

The DataStax C++ driver and PHP extension are from 2020 and may have compatibility issues with newer systems. The ScyllaDB driver is more modern but still requires compilation. If builds fail:

1. Check the Dockerfile.datastax or Dockerfile.scylladb for hardcoded versions
2. Try different C++ driver versions
3. Check [DataStax driver downloads](https://downloads.datastax.com/cpp-driver/) or [ScyllaDB driver releases](https://github.com/scylladb/cpp-driver/releases)
4. Ensure you have sufficient disk space and memory for compilation

### Permission Issues

Make the comparison script executable:
```bash
chmod +x benchmarks/run-comparison.sh
chmod +x benchmarks/compare-results.php
```

## Contributing

When adding new benchmarks:
1. Ensure they test equivalent operations in all three drivers
2. Document what is being tested
3. Use consistent naming across all implementations
4. Update this README with benchmark descriptions
5. Test all three drivers to ensure compatibility

## License

Same as the parent project.

