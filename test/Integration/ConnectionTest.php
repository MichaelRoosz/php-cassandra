<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Connection;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Consistency;
use PHPUnit\Framework\TestCase;

final class ConnectionTest extends TestCase {
    public function testConnectAndProtocolNegotiation(): void {
        $conn = $this->newConnection();
        $conn->connect();
        $this->assertTrue($conn->isConnected());

        $version = $conn->getVersion();
        $this->assertGreaterThanOrEqual(3, $version);

        if ($version >= 5) {
            $this->assertTrue($conn->supportsKeyspaceRequestOption());
            $this->assertTrue($conn->supportsNowInSecondsRequestOption());
        }
    }

    public function testSetConsistencyAffectsDefault(): void {
        $conn = $this->newConnection();
        $conn->setConsistency(Consistency::ONE);
        $rows = $conn->query('SELECT key FROM system.local')->asRowsResult();
        $this->assertSame(1, $rows->getRowCount());
        $row = $rows->fetch();
        $this->assertSame(['key' => 'local'], $row);
    }

    private static function getHost(): string {
        return getenv('APP_CASSANDRA_HOST') ?: '127.0.0.1';
    }

    private static function getPort(): int {
        $port = getenv('APP_CASSANDRA_PORT') ?: '9042';

        return (int) $port;
    }

    private function newConnection(string $keyspace = 'app'): Connection {
        $nodes = [
            new SocketNodeConfig(
                host: self::getHost(),
                port: self::getPort(),
                username: '',
                password: ''
            ),
        ];

        $conn = new Connection($nodes, $keyspace);

        return $conn;
    }
}
