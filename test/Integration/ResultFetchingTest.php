<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Connection;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Consistency;
use Cassandra\Response\Result\FetchType;
use Cassandra\Type;
use PHPUnit\Framework\TestCase;

final class ResultFetchingTest extends TestCase {
    public function testFetchVariantsAndIterator(): void {
        $conn = $this->newConnection();

        // Seed a few simple rows
        $filename = 'itest_' . bin2hex(random_bytes(4));
        for ($i = 0; $i < 3; $i++) {
            $conn->querySync(
                'INSERT INTO storage(filename, ukey, value) VALUES (?, ?, ?)',
                [
                    new Type\Varchar($filename),
                    new Type\Varchar('k' . $i),
                    new Type\MapCollection(['a' => (string) $i], Type::VARCHAR, Type::VARCHAR),
                ]
            );
        }

        $rows = $conn->querySync(
            'SELECT ukey FROM storage WHERE filename = ?',
            [new Type\Varchar($filename)]
        )->asRowsResult();

        // Iterator
        $all = [];
        foreach ($rows as $row) {
            $all[] = $row['ukey'];
        }
        $sorted = $all;
        sort($sorted, SORT_STRING);
        $this->assertSame(['k0', 'k1', 'k2'], $sorted);

        // Rewind and fetch variants
        $rows->rewind();

        $first = $rows->fetch(FetchType::ASSOC);
        $this->assertIsArray($first);
        $this->assertArrayHasKey('ukey', $first);

        $secondNum = $rows->fetch(FetchType::NUM);
        $this->assertIsArray($secondNum);
        $this->assertArrayHasKey(0, $secondNum);

        $thirdBoth = $rows->fetch(FetchType::BOTH);
        $this->assertIsArray($thirdBoth);
        $this->assertArrayHasKey('ukey', $thirdBoth);
        $this->assertArrayHasKey(0, $thirdBoth);
        $this->assertSame($thirdBoth['ukey'], $thirdBoth[0]);

        $fetchedKeys = [$first['ukey'], $secondNum[0], $thirdBoth['ukey']];
        sort($fetchedKeys, SORT_STRING);
        $this->assertSame(['k0', 'k1', 'k2'], $fetchedKeys);

        // Fetch column on exhausted should be false
        $this->assertFalse($rows->fetchColumn(0));
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
