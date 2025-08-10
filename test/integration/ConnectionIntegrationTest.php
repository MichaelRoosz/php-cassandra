<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Connection;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Consistency;
use Cassandra\Request\Batch;
use Cassandra\Request\BatchType;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Response\Exception as ServerException;
use Cassandra\Type;
use PHPUnit\Framework\TestCase;

final class ConnectionIntegrationTest extends TestCase {
    public function testBatchInsert(): void {
        $conn = $this->newConnection();
        $batch = new Batch(BatchType::UNLOGGED, Consistency::ONE);
        for ($i = 0; $i < 10; $i++) {
            $batch->appendQuery(
                'INSERT INTO storage(filename, ukey, value) VALUES (?, ?, ?)',
                [
                    new Type\Varchar('fileA'),
                    new Type\Varchar('k' . $i),
                    new Type\CollectionMap(['a' => 'b'], Type::VARCHAR, Type::VARCHAR),
                ]
            );
        }
        $r = $conn->batchSync($batch);
        $this->assertSame(0, $r->getStream()); // stream 0 for sync

        $rows = $conn->querySync(
            'SELECT COUNT(*) FROM storage WHERE filename = ?',
            [new Type\Varchar('fileA')],
            Consistency::ONE,
            new QueryOptions(namesForValues: false)
        )->asRowsResult();

        $count = (int) $rows->fetchColumn(0);

        $this->assertGreaterThanOrEqual(10, $count);
    }

    public function testInsertSelectPreparedAndPaging(): void {

        $conn = $this->newConnection();

        // insert multiple users
        $prepared = $conn->prepareSync('INSERT INTO users (id, org_id, name, age) VALUES (:id, :org_id, :name, :age)');
        for ($i = 0; $i < 150; $i++) {
            $conn->executeSync(
                $prepared,
                [
                    'id' => new Type\Uuid(self::uuidV4()),
                    'org_id' => 42,
                    'name' => 'u' . $i,
                    'age' => 20 + ($i % 10),
                ],
                Consistency::ONE,
                new ExecuteOptions(namesForValues: true)
            );
        }

        $prepSel = $conn->prepareSync('SELECT id, name FROM users WHERE org_id = :org_id');
        $rows = $conn->executeSync(
            $prepSel,
            ['org_id' => 42],
            Consistency::ONE,
            new ExecuteOptions(pageSize: 50, namesForValues: true)
        )->asRowsResult();

        $count = 0;
        do {
            foreach ($rows as $row) {
                $this->assertArrayHasKey('name', $row);
                $count++;
            }
            $pagingState = $rows->getMetadata()->pagingState;
            if ($pagingState === null) {
                break;
            }

            $rows = $conn->executeSync(
                $rows,
                ['org_id' => 42],
                Consistency::ONE,
                new ExecuteOptions(pageSize: 50, namesForValues: true, pagingState: $pagingState)
            )->asRowsResult();

        } while (true);

        $this->assertGreaterThanOrEqual(150, $count);

    }

    public function testSimpleQuery(): void {
        $conn = $this->newConnection();
        $r = $conn->querySync('SELECT key FROM system.local');
        $this->assertSame(1, iterator_count($r));
    }

    public function testSyntaxErrorRaisesException(): void {
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

    private static function uuidV4(): string {
        $data = random_bytes(16);
        $data[6] = chr((ord($data[6]) & 0x0f) | 0x40);
        $data[8] = chr((ord($data[8]) & 0x3f) | 0x80);

        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
    }
}
