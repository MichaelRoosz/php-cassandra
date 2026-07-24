<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Connection\ConnectionOptions;
use Cassandra\Consistency;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Value\Blob;

/**
 * Exercises the modern (protocol v5) frame codec when a single request exceeds
 * the maximum frame payload size (128 KiB) and must be split across several
 * frames.
 *
 * This is a regression test for the {@see \Cassandra\Connection\FrameCodec}
 * chunking bug: the offset and remaining-length counters were reset on every
 * loop iteration, so any request larger than one frame looped forever. A 1 MiB
 * value forces ~8 frames, so before the fix these tests would hang.
 *
 * The v5 framing (and therefore the multi-frame path) only exists on Cassandra
 * 4.0+; the tests skip where protocol v5 is unavailable (e.g. ScyllaDB).
 */
final class FrameCodecMultiFrameTest extends AbstractIntegrationTestCase {
    private const PAYLOAD_BYTES = 1024 * 1024;

    public function testLargeRequestSplitIntoMultipleFramesCompressed(): void {

        $this->skipIfProtocolV5Unsupported();

        // Compressible payload: each frame is compressed before being sent.
        $payload = str_repeat('multi-frame-compressed-payload;', self::PAYLOAD_BYTES);
        $payload = substr($payload, 0, self::PAYLOAD_BYTES);

        $this->assertLargeBlobRoundTrips(
            enableCompression: true,
            id: 1,
            payload: $payload,
        );
    }

    public function testLargeRequestSplitIntoMultipleFramesCompressedIncompressible(): void {

        $this->skipIfProtocolV5Unsupported();

        // Incompressible payload with compression enabled: exercises the
        // per-frame "send uncompressed" fallback across many frames.
        $this->assertLargeBlobRoundTrips(
            enableCompression: true,
            id: 2,
            payload: random_bytes(self::PAYLOAD_BYTES),
        );
    }

    public function testLargeRequestSplitIntoMultipleFramesUncompressed(): void {

        $this->skipIfProtocolV5Unsupported();

        // Incompressible payload, no compression: the raw multi-frame chunking
        // path where the original infinite-loop bug lived.
        $this->assertLargeBlobRoundTrips(
            enableCompression: false,
            id: 3,
            payload: random_bytes(self::PAYLOAD_BYTES),
        );
    }

    protected static function setupTable(): void {
        $conn = self::newConnection(self::$defaultKeyspace);
        $conn->query('CREATE TABLE IF NOT EXISTS large_blobs (id int PRIMARY KEY, data blob)');
        $conn->disconnect();
    }

    private function assertLargeBlobRoundTrips(bool $enableCompression, int $id, string $payload): void {

        $options = new ConnectionOptions(
            enableCompression: $enableCompression,
            allowedProtocolVersions: [ProtocolVersion::V5],
            initialProtocolVersion: ProtocolVersion::V5,
        );

        $conn = self::newConnection(self::$defaultKeyspace, true, $options, forceInitialProtocolVersion: true);
        $conn->setConsistency(Consistency::ONE);

        $insert = $conn->prepare("INSERT INTO {$this->keyspace}.large_blobs (id, data) VALUES (?, ?)");
        $conn->execute($insert, [$id, new Blob($payload)]);

        $rows = $conn->query(
            "SELECT data FROM {$this->keyspace}.large_blobs WHERE id = ?",
            [$id]
        )->asRowsResult();

        $row = $rows->fetch();
        $this->assertIsArray($row);
        $this->assertIsString($row['data'] ?? null);
        $this->assertSame(strlen($payload), strlen($row['data']));
        $this->assertTrue($payload === $row['data'], 'Multi-frame round-trip corrupted the payload');

        $conn->disconnect();
    }

    private function skipIfProtocolV5Unsupported(): void {
        if (self::isProtocolVersionSupported(ProtocolVersion::V5) === false) {
            $this->markTestSkipped('Protocol V5 (modern framing) is not supported by the server.');
        }
    }
}
