<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection as ConnectionClass;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Connection\Node;
use Cassandra\Connection\NodeConfig;
use Cassandra\Connection\Stream;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use ReflectionProperty;

final class ConnectionTest extends AbstractUnitTestCase {
    public function testSetAllowedProtocolVersionsWhenNotConnected(): void {
        $conn = new ConnectionClass(nodes: [], keyspace: '', options: new \Cassandra\Connection\ConnectionOptions());

        $versions = [ProtocolVersion::V4, ProtocolVersion::V3];
        $conn->setAllowedProtocolVersions($versions);

        $this->assertSame($versions, $conn->getAllowedProtocolVersions());
    }

    public function testSetAllowedProtocolVersionsWhenConnectedThrows(): void {
        $conn = new ConnectionClass(nodes: [], keyspace: '', options: new \Cassandra\Connection\ConnectionOptions());

        // Create a stub Node implementation to mark the connection as "connected"
        $stubNode = new class() implements Node {
            public function close(): void {}
            public function getConfig(): NodeConfig {
                return new class extends NodeConfig {
                    public function getNodeClass(): string { return Stream::class; }
                };
            }
            public function read(int $length, bool $waitForData): string { return ''; }
            public function readAvailableData(int $expectedLength, int $maxLength, bool $waitForData): string { return ''; }
            public function readAvailableDataFromSource(int $expectedLength, int $upperBoundaryLength, bool $waitForData): string { return ''; }
            public function write(string $data): void {}
            public function writeRequest(\Cassandra\Request\Request $request): void {}
        };

        // Inject stub node into protected property using reflection
        $rp = new ReflectionProperty(ConnectionClass::class, 'node');
        $rp->setAccessible(true);
        $rp->setValue($conn, $stubNode);

        $this->expectException(ConnectionException::class);
        $this->expectExceptionCode(ExceptionCode::CONNECTION_SET_ALLOWED_PROTOCOL_VERSIONS_WHEN_ALREADY_CONNECTED->value);

        $conn->setAllowedProtocolVersions([ProtocolVersion::V3]);
    }
}
