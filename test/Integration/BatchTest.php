<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Connection;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Consistency;
use Cassandra\Request\Batch;
use Cassandra\Request\BatchType;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Type;
use PHPUnit\Framework\TestCase;

final class BatchTest extends TestCase {
    public function testUnloggedBatchInsert(): void {
        $conn = $this->newConnection();

        $filename = 'itest_' . bin2hex(random_bytes(6));
        $batch = new Batch(BatchType::UNLOGGED, Consistency::ONE);
        for ($i = 0; $i < 10; $i++) {
            $batch->appendQuery(
                'INSERT INTO storage(filename, ukey, value) VALUES (?, ?, ?)',
                [
                    Type\Varchar::fromValue($filename),
                    Type\Varchar::fromValue('k' . $i),
                    Type\MapCollection::fromValue(['a' => 'b'], Type::VARCHAR, Type::VARCHAR),
                ]
            );
        }

        $r = $conn->batch($batch);
        $this->assertSame(0, $r->getStream());

        $rows = $conn->query(
            'SELECT COUNT(*) FROM storage WHERE filename = ?',
            [Type\Varchar::fromValue($filename)],
            Consistency::ONE,
            new QueryOptions(namesForValues: false)
        )->asRowsResult();

        $this->assertSame(10, (int) $rows->fetchColumn(0));
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
