<?php

declare(strict_types=1);

/*
 * Slow-network comparison for CQL LZ4 compression, against a real server.
 *
 * Compression on the client runs in plain PHP, so whether it is worth enabling
 * depends entirely on the network: the CPU it costs is fixed, but the bytes it
 * saves only matter if the link is slow enough that transmitting them would
 * have taken longer than compressing them.
 *
 * This script makes that trade-off visible. It round-trips a several-megabyte,
 * highly compressible blob against a live Cassandra/ScyllaDB node, with
 * compression on vs. off, over a range of simulated bandwidths (implemented by
 * a ThrottledSocket that sleeps in proportion to bytes transferred). It prints
 * the wall-clock time for each and the resulting speed-up.
 *
 * Usage (defaults to 127.0.0.1:9142, override as needed):
 *   APP_CASSANDRA_HOST=127.0.0.1 APP_CASSANDRA_PORT=9042 \
 *       php benchmarks/compression-network-bench.php
 *
 * Note: outgoing frames are only actually compressed on protocol v5 (Cassandra
 * 4.0+). On v3/v4 (incl. ScyllaDB) the download path is still compressed by the
 * server, so the read comparison remains meaningful.
 */

use Cassandra\Connection;

require __DIR__ . '/bootstrap.php';

const PAYLOAD_BYTES = 4 * 1024 * 1024;
const ROUND_TRIPS = 3;

/**
 * Simulated network links to compare, in bytes per second (0 = full speed).
 *
 * @var array<string, int>
 */
$bandwidths = [
    'full speed (local)' => 0,
    '50 MB/s (fast LAN)' => 50_000_000,
    '10 MB/s (100 Mbit)' => 10_000_000,
    '2 MB/s (slow WAN)' => 2_000_000,
    '0.5 MB/s (mobile)' => 500_000,
];

/**
 * Time a number of full round-trips (write the blob, read it back).
 */
function timeRoundTrips(Connection $conn, string $payload, int $count): float {
    $prepared = $conn->prepare('INSERT INTO docs (id, size, data) VALUES (?, ?, ?)');

    $start = hrtime(true);
    for ($i = 0; $i < $count; $i++) {
        $conn->execute($prepared, [BenchEnv::COMPRESSION_ROW_ID, PAYLOAD_BYTES, new Cassandra\Value\Blob($payload)]);

        $result = $conn->query('SELECT data FROM docs WHERE id = ?', [BenchEnv::COMPRESSION_ROW_ID])->asRowsResult();
        $row = $result->fetch();
        if (!is_array($row) || !is_string($row['data'] ?? null) || $row['data'] !== $payload) {
            throw new RuntimeException('Round-trip payload mismatch');
        }
    }

    return (hrtime(true) - $start) / 1e9 / $count;
}

echo "CQL LZ4 compression over a simulated network (real server round-trips)\n";
printf("PHP %s, payload %.1f MB, %d round-trip(s) per measurement\n\n", PHP_VERSION, PAYLOAD_BYTES / 1e6, ROUND_TRIPS);

BenchEnv::ensureCompressionFixture(PAYLOAD_BYTES);
$payload = BenchEnv::compressionPayload(PAYLOAD_BYTES);

$header = sprintf("%-22s | %14s | %14s | %9s\n", 'network link', 'uncompressed', 'compressed', 'speed-up');
echo $header;
echo str_repeat('-', strlen($header)) . "\n";

foreach ($bandwidths as $label => $bytesPerSecond) {
    $uncompressedConn = BenchEnv::compressionConnection(compress: false, bytesPerSecond: $bytesPerSecond);
    $compressedConn = BenchEnv::compressionConnection(compress: true, bytesPerSecond: $bytesPerSecond);

    $uncompressedTime = timeRoundTrips($uncompressedConn, $payload, ROUND_TRIPS);
    $compressedTime = timeRoundTrips($compressedConn, $payload, ROUND_TRIPS);

    $uncompressedConn->disconnect();
    $compressedConn->disconnect();

    printf(
        "%-22s | %11.3f s | %11.3f s | %7.2fx\n",
        $label,
        $uncompressedTime,
        $compressedTime,
        $compressedTime > 0 ? $uncompressedTime / $compressedTime : 0.0
    );
}

echo "\nspeed-up > 1 means compression was faster overall on that link.\n";
