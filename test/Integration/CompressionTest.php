<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Connection;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Consistency;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Options\QueryOptions;

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

        $conn->disconnect();
    }
}
