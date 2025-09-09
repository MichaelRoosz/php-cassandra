<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Consistency;
use Cassandra\Request\Batch;
use Cassandra\Request\BatchType;
use Cassandra\Request\Options\BatchOptions;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Type;
use Cassandra\Value;

final class ConnectionIntegrationTest extends AbstractIntegrationTestCase {
    public function testBatchInsert(): void {

        $conn = $this->connection;
        $batch = new Batch(BatchType::UNLOGGED, Consistency::ONE, new BatchOptions(keyspace: $this->keyspace));
        for ($i = 0; $i < 10; $i++) {
            $batch->appendQuery(
                'INSERT INTO storage(filename, ukey, value) VALUES (?, ?, ?)',
                [
                    Value\Varchar::fromValue('fileA'),
                    Value\Varchar::fromValue('k' . $i),
                    Value\MapCollection::fromValue(['a' => 'b'], Type::VARCHAR, Type::VARCHAR),
                ]
            );
        }
        $r = $conn->batch($batch);
        $this->assertSame(0, $r->getStream()); // stream 0 for sync

        $rows = $conn->query(
            'SELECT COUNT(*) FROM storage WHERE filename = ?',
            [Value\Varchar::fromValue('fileA')],
            Consistency::ONE,
            new QueryOptions(namesForValues: false)
        )->asRowsResult();

        $countValue = $rows->fetchColumn(0);
        $count = is_int($countValue) ? $countValue : 0;

        $this->assertGreaterThanOrEqual(10, $count);
    }

    public function testInsertSelectPreparedAndPaging(): void {

        $conn = $this->connection;

        // insert multiple users
        $prepared = $conn->prepare("INSERT INTO {$this->keyspace}.users (id, org_id, name, age) VALUES (:id, :org_id, :name, :age)");
        for ($i = 0; $i < 150; $i++) {
            $conn->execute(
                $prepared,
                [
                    'id' => Value\Uuid::fromValue(self::uuidV4()),
                    'org_id' => 42,
                    'name' => 'u' . $i,
                    'age' => 20 + ($i % 10),
                ],
                Consistency::ONE,
                new ExecuteOptions(namesForValues: true)
            );
        }

        $prepSel = $conn->prepare("SELECT id, name FROM {$this->keyspace}.users WHERE org_id = :org_id");
        $rows = $conn->execute(
            $prepSel,
            ['org_id' => 42],
            Consistency::ONE,
            new ExecuteOptions(pageSize: 50, namesForValues: true)
        )->asRowsResult();

        $count = 0;
        do {
            foreach ($rows as $row) {
                $this->assertIsArray($row);
                $this->assertArrayHasKey('name', $row);
                $count++;
            }
            $pagingState = $rows->getRowsMetadata()->pagingState;
            if ($pagingState === null) {
                break;
            }

            $rows = $conn->execute(
                $rows,
                ['org_id' => 42],
                Consistency::ONE,
                new ExecuteOptions(pageSize: 50, namesForValues: true, pagingState: $pagingState)
            )->asRowsResult();

        } while (true);

        $this->assertGreaterThanOrEqual(150, $count);

    }

    protected static function setupTable(): void {
        $conn = self::newConnection(self::$defaultKeyspace);
        $conn->query('CREATE TABLE IF NOT EXISTS storage(filename varchar, ukey varchar, value map<varchar, varchar>, PRIMARY KEY (filename, ukey))');
        $conn->query('CREATE TABLE IF NOT EXISTS users(org_id int, id uuid, name varchar, age int, PRIMARY KEY ((org_id), id))');
        $conn->disconnect();
    }

    private static function uuidV4(): string {
        $data = random_bytes(16);
        $data[6] = chr((ord($data[6]) & 0x0f) | 0x40);
        $data[8] = chr((ord($data[8]) & 0x3f) | 0x80);

        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
    }
}
