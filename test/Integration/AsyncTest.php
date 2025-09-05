<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Connection;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Consistency;
use Cassandra\Request\Batch;
use Cassandra\Request\BatchType;
use Cassandra\Type;
use Cassandra\Value;
use PHPUnit\Framework\TestCase;

final class AsyncTest extends TestCase {
    public function testAsyncBatchAndFlush(): void {
        $conn = $this->newConnection();
        $filename = 'itest_' . bin2hex(random_bytes(6));

        $pending = [];
        for ($j = 0; $j < 3; $j++) {
            $batch = new Batch(BatchType::UNLOGGED, Consistency::ONE);
            for ($i = 0; $i < 10; $i++) {
                $batch->appendQuery(
                    'INSERT INTO storage(filename, ukey, value) VALUES (?, ?, ?)',
                    [
                        Value\Varchar::fromValue($filename),
                        Value\Varchar::fromValue('k' . $j . '_' . $i),
                        Value\MapCollection::fromValue(['x' => 'y'], Type::VARCHAR, Type::VARCHAR),
                    ]
                );
            }
            $pending[] = $conn->batchAsync($batch);
        }

        $conn->flush();

        $countValue = $conn->query(
            'SELECT COUNT(*) FROM storage WHERE filename = ?',
            [Value\Varchar::fromValue($filename)]
        )->asRowsResult()->fetchColumn(0);
        $count = is_int($countValue) ? $countValue : 0;

        $this->assertGreaterThanOrEqual(30, $count);
    }
    public function testConcurrentAsyncQueries(): void {
        $conn = $this->newConnection();

        $s1 = $conn->queryAsync('SELECT key FROM system.local');
        $s2 = $conn->queryAsync('SELECT release_version FROM system.local');

        $r2 = $s2->getRowsResult();
        $r1 = $s1->getRowsResult();

        $this->assertSame(1, $r1->getRowCount());
        $this->assertSame(['key' => 'local'], $r1->fetch());
        $this->assertSame(1, $r2->getRowCount());
        $this->assertIsString($r2->fetch()['release_version'] ?? null);
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
        $conn->connect();
        $this->assertTrue($conn->isConnected());

        return $conn;
    }
}
