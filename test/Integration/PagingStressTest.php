<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Consistency;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Request\BatchType;
use Cassandra\Type;
use Cassandra\Value;

final class PagingStressTest extends AbstractIntegrationTestCase {
    public function testLargeResultSetPagingPrepared(): void {

        $conn = $this->connection;

        $conn->query('TRUNCATE big_kv');

        $filename = 'pf_' . bin2hex(random_bytes(6));
        $numRows = 1200;

        $ins = $conn->prepare("INSERT INTO {$this->keyspace}.big_kv (filename, ukey, value) VALUES (:filename, :ukey, :value)");
        for ($i = 0; $i < $numRows; $i++) {
            $conn->execute(
                $ins,
                [
                    'filename' => Value\Varchar::fromValue($filename),
                    'ukey' => Value\Varchar::fromValue('k' . ($i + 1)),
                    'value' => Value\MapCollection::fromValue(['v' => (string) $i], Type::VARCHAR, Type::VARCHAR),
                ],
                Consistency::ONE,
                new ExecuteOptions(namesForValues: true)
            );
        }

        $sel = $conn->prepare("SELECT filename, ukey, value FROM {$this->keyspace}.big_kv WHERE filename = :filename");
        $pageSize = 300;
        $rows = $conn->execute(
            $sel,
            ['filename' => Value\Varchar::fromValue($filename)],
            Consistency::ONE,
            new ExecuteOptions(pageSize: $pageSize, namesForValues: true)
        )->asRowsResult();

        $seen = 0;
        do {
            foreach ($rows as $_row) {
                $seen++;
            }
            $state = $rows->getRowsMetadata()->pagingState;
            if ($state === null) {
                break;
            }
            $rows = $conn->execute(
                $rows,
                ['filename' => Value\Varchar::fromValue($filename)],
                Consistency::ONE,
                new ExecuteOptions(pageSize: $pageSize, namesForValues: true, pagingState: $state)
            )->asRowsResult();
        } while (true);

        $this->assertSame($numRows, $seen);
    }

    public function testLargeResultSetPagingQuery(): void {

        $conn = $this->connection;

        $conn->query('TRUNCATE big_kv');

        // Seed a large number of rows under single partition key
        $filename = 'pf_' . bin2hex(random_bytes(6));
        $numRows = 1500; // Big enough to require many pages (min clamp 100)

        // Insert using async batches to speed up
        $pending = [];
        $batchSize = 100;
        for ($offset = 0; $offset < $numRows; $offset += $batchSize) {
            $batch = $conn->createBatchRequest(BatchType::UNLOGGED);
            for ($i = 0; $i < $batchSize && ($offset + $i) < $numRows; $i++) {
                $ukey = 'k' . ($offset + $i + 1);
                $batch->appendQuery(
                    'INSERT INTO big_kv(filename, ukey, value) VALUES (?, ?, ?)',
                    [
                        Value\Varchar::fromValue($filename),
                        Value\Varchar::fromValue($ukey),
                        Value\MapCollection::fromValue(['v' => (string) ($offset + $i)], Type::VARCHAR, Type::VARCHAR),
                    ]
                );
            }
            $pending[] = $conn->batchAsync($batch);
        }

        $conn->flush();

        foreach ($pending as $stmt) {
            $stmt->waitForResponse();
        }

        // Query with explicit page size; driver clamps minimum to 100
        $pageSize = 200;
        $rows = $conn->query(
            'SELECT filename, ukey, value FROM big_kv WHERE filename = ?',
            [Value\Varchar::fromValue($filename)],
            Consistency::ONE,
            new QueryOptions(pageSize: $pageSize)
        )->asRowsResult();

        $seen = 0;
        do {
            foreach ($rows as $_row) {
                $seen++;
            }

            $state = $rows->getRowsMetadata()->pagingState;
            if ($state === null) {
                break;
            }
            $rows = $conn->query(
                'SELECT filename, ukey, value FROM big_kv WHERE filename = ?',
                [Value\Varchar::fromValue($filename)],
                Consistency::ONE,
                new QueryOptions(pageSize: $pageSize, pagingState: $state)
            )->asRowsResult();
        } while (true);

        $this->assertSame($numRows, $seen);
    }

    protected static function setupTable(): void {
        $conn = self::newConnection(self::$defaultKeyspace);
        $conn->query('CREATE TABLE IF NOT EXISTS big_kv(filename varchar, ukey varchar, value map<varchar, varchar>, PRIMARY KEY (filename, ukey))');
        $conn->disconnect();
    }
}
