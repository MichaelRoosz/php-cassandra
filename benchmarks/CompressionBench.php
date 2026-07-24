<?php

declare(strict_types=1);

use Cassandra\Connection;

/**
 * Real-server benchmarks for outgoing/incoming LZ4 compression on the CQL
 * protocol, using a several-megabyte payload.
 *
 * These run against a live Cassandra/ScyllaDB node (see benchmarks/README.md)
 * and measure a full round-trip of a large, compressible blob with compression
 * enabled vs. disabled:
 *
 *   - `benchReadLargeBlob*`  : SELECT the blob back (server -> client, the
 *                              incoming decompression path).
 *   - `benchWriteLargeBlob*` : re-INSERT the blob (client -> server, the
 *                              outgoing compression path added in this release;
 *                              only actually compressed on protocol v5).
 *
 * At full local speed compression usually loses (pure-PHP CPU dominates); the
 * standalone `compression-network-bench.php` shows how the trade-off flips once
 * a slow network link is simulated.
 *
 * Requires a reachable node: set `APP_CASSANDRA_HOST` / `APP_CASSANDRA_PORT`
 * (the bootstrap defaults to 127.0.0.1:9142).
 */
final class CompressionBench {
    private const PAYLOAD_BYTES = 4 * 1024 * 1024;

    private Connection $compressed;

    private string $payload;

    private Connection $uncompressed;

    public function __construct() {
        BenchEnv::ensureCompressionFixture(self::PAYLOAD_BYTES);

        $this->payload = BenchEnv::compressionPayload(self::PAYLOAD_BYTES);
        $this->compressed = BenchEnv::compressionConnection(compress: true);
        $this->uncompressed = BenchEnv::compressionConnection(compress: false);
    }

    /**
     * @Revs(3)
     * @Iterations(3)
     */
    public function benchReadLargeBlobCompressed(): void {
        $this->readBlob($this->compressed);
    }

    /**
     * @Revs(3)
     * @Iterations(3)
     */
    public function benchReadLargeBlobUncompressed(): void {
        $this->readBlob($this->uncompressed);
    }

    /**
     * @Revs(3)
     * @Iterations(3)
     */
    public function benchWriteLargeBlobCompressed(): void {
        $this->writeBlob($this->compressed);
    }

    /**
     * @Revs(3)
     * @Iterations(3)
     */
    public function benchWriteLargeBlobUncompressed(): void {
        $this->writeBlob($this->uncompressed);
    }

    private function readBlob(Connection $conn): void {
        $result = $conn->query(
            'SELECT data FROM docs WHERE id = ?',
            [BenchEnv::COMPRESSION_ROW_ID]
        )->asRowsResult();

        $row = $result->fetch();
        if (!is_array($row) || !isset($row['data']) || !is_string($row['data'])) {
            throw new RuntimeException('Benchmark fixture row is missing');
        }
    }

    private function writeBlob(Connection $conn): void {
        $prepared = $conn->prepare('INSERT INTO docs (id, size, data) VALUES (?, ?, ?)');
        $conn->execute($prepared, [
            BenchEnv::COMPRESSION_ROW_ID,
            self::PAYLOAD_BYTES,
            new Cassandra\Value\Blob($this->payload),
        ]);
    }
}
