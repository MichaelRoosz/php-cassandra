<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Connection;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Consistency;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Response\Exception as ServerException;
use Cassandra\Type;
use PHPUnit\Framework\TestCase;

final class QueryTest extends TestCase {
    public function testPositionalBindAndTypes(): void {
        $conn = $this->newConnection();
        $rows = $conn->querySync(
            'SELECT key FROM system.local WHERE key = ?',
            [new Type\Ascii('local')],
            Consistency::ONE,
            new QueryOptions()
        )->asRowsResult();

        $this->assertSame(1, $rows->getRowCount());
        $this->assertSame(['key' => 'local'], $rows->fetch());
    }
    public function testSimpleSelect(): void {
        $conn = $this->newConnection();
        $rows = $conn->querySync('SELECT key FROM system.local')->asRowsResult();
        $this->assertSame(1, $rows->getRowCount());
        $this->assertSame(['key' => 'local'], $rows->fetch());
    }

    public function testSyntaxErrorRaisesServerException(): void {
        $conn = $this->newConnection();
        $this->expectException(ServerException::class);
        $conn->querySync('SELECT * FROM does_not_exist_123');
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
        $conn->setConsistency(Consistency::ONE);
        $this->assertTrue($conn->connect());
        $this->assertTrue($conn->isConnected());

        return $conn;
    }
}
