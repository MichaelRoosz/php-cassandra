<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Connection;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Consistency;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Value\Blob;

final class CompressionTest extends AbstractIntegrationTestCase {
    public function testNegotiatesCompressionAndDecodesCompressedFramesV3(): void {

        $options = new ConnectionOptions(
            enableCompression: true,
            allowedProtocolVersions: [ProtocolVersion::V3],
            initialProtocolVersion: ProtocolVersion::V3,
        );

        $this->testCompression($options);
    }

    public function testNegotiatesCompressionAndDecodesCompressedFramesV4(): void {

        if (self::isProtocolVersionSupported(ProtocolVersion::V4) === false) {
            $this->markTestSkipped('Protocol V4 is not supported by the server.');
        }

        $options = new ConnectionOptions(
            enableCompression: true,
            allowedProtocolVersions: [ProtocolVersion::V4],
            initialProtocolVersion: ProtocolVersion::V4,
        );

        $this->testCompression($options);
    }

    public function testNegotiatesCompressionAndDecodesCompressedFramesV5(): void {

        if (self::isProtocolVersionSupported(ProtocolVersion::V5) === false) {
            $this->markTestSkipped('Protocol V5 is not supported by the server.');
        }

        $options = new ConnectionOptions(
            enableCompression: true,
            allowedProtocolVersions: [ProtocolVersion::V5],
        );

        $this->testCompression($options);
    }

    protected static function setupTable(): void {
        $conn = self::newConnection(self::$defaultKeyspace);
        $conn->query('CREATE TABLE IF NOT EXISTS compressed_blobs (id int PRIMARY KEY, data blob)');
        $conn->disconnect();
    }

    private function testCompression(ConnectionOptions $options): void {

        $nodes = [
            new SocketNodeConfig(
                host: self::getHost(),
                port: self::getPort(),
                username: self::getUsername(),
                password: self::getPassword()
            ),
        ];

        $conn = new Connection($nodes, self::$defaultKeyspace, $options);
        $conn->setConsistency(Consistency::ONE);
        $conn->connect();

        // Run a query that returns a small payload; intent is to ensure decoding path works under compression
        $rows = $conn->query('SELECT key, cluster_name, release_version FROM system.local')->asRowsResult();
        $this->assertSame(1, $rows->getRowCount());
        $row = $rows->fetch();
        $this->assertSame('local', $row['key'] ?? null);
        $this->assertIsString($row['cluster_name'] ?? null);
        $this->assertIsString($row['release_version'] ?? null);

        // Also try an async path under compression
        $stmt = $conn->queryAsync('SELECT key FROM system.local', [], Consistency::ONE, new QueryOptions());
        $res = $stmt->getRowsResult();
        $this->assertSame(1, $res->getRowCount());

        // Round-trip large payloads to exercise the actual compression codec.
        // On every negotiated protocol version the outgoing (client -> server)
        // request frames are compressed by us (via RequestCompressor on v3/v4
        // and FrameCodec on v5), and the server compresses the response, which
        // we decompress. A highly compressible payload is actually shrunk on
        // the wire, while random data is incompressible and must survive the
        // "send uncompressed" fallback unchanged.
        $compressible = str_repeat('The quick brown fox jumps over the lazy dog. ', 6000); // ~270 KB
        $incompressible = random_bytes(256 * 1024);

        $insert = $conn->prepare("INSERT INTO {$this->keyspace}.compressed_blobs (id, data) VALUES (?, ?)");

        foreach ([1 => $compressible, 2 => $incompressible] as $id => $payload) {
            $conn->execute($insert, [$id, new Blob($payload)]);

            $rows = $conn->query(
                "SELECT data FROM {$this->keyspace}.compressed_blobs WHERE id = ?",
                [$id]
            )->asRowsResult();

            $row = $rows->fetch();
            $this->assertIsArray($row);
            $this->assertIsString($row['data'] ?? null);
            $this->assertSame($payload, $row['data'], "Compressed round-trip corrupted payload {$id}");
        }

        $conn->disconnect();
    }
}
