<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Consistency;
use Cassandra\Request\Batch;
use Cassandra\Request\BatchType;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Type;
use Cassandra\Value;

final class StorageIntegrationTest extends AbstractIntegrationTest {
    public function testBatchAsyncAndPagingOnStorage(): void {
        $conn = $this->connection;

        $filename = 'itest_' . bin2hex(random_bytes(6));

        // Seed data using multiple async batches
        $numRows = 120;
        $batchSize = 40;
        $pending = [];

        for ($offset = 0; $offset < $numRows; $offset += $batchSize) {
            $batch = new Batch(BatchType::UNLOGGED, Consistency::ONE);
            for ($i = 0; $i < $batchSize && ($offset + $i) < $numRows; $i++) {
                $ukey = 'k' . ($offset + $i + 1);
                $batch->appendQuery(
                    'INSERT INTO storage(filename, ukey, value) VALUES(?, ?, ?)',
                    [
                        Value\Varchar::fromValue($filename),
                        Value\Varchar::fromValue($ukey),
                        Value\MapCollection::fromValue(['id' => '4003578', 'title' => 'Christmas By The River (CD)'], Type::VARCHAR, Type::VARCHAR),
                    ]
                );
            }
            $pending[] = $conn->batchAsync($batch);
        }

        foreach ($pending as $stmt) {
            $stmt->waitForResponse();
        }

        // Prepared select with paging
        $prepared = $conn->prepare('SELECT filename, ukey, value FROM storage WHERE filename = :filename');
        $rows = $conn->execute(
            $prepared,
            ['filename' => $filename],
            Consistency::ONE,
            new ExecuteOptions(pageSize: 50, namesForValues: true)
        )->asRowsResult();

        $count = 0;
        do {
            foreach ($rows as $_row) {
                $count++;
            }
            $pagingState = $rows->getRowsMetadata()->pagingState;
            if ($pagingState === null) {
                break;
            }
            $rows = $conn->execute(
                $rows,
                ['filename' => $filename],
                Consistency::ONE,
                new ExecuteOptions(pageSize: 50, namesForValues: true, pagingState: $pagingState)
            )->asRowsResult();
        } while (true);

        $this->assertSame($numRows, $count, 'Prepared+paging should return all inserted rows');

        // Simple query with paging
        $result = $conn->query(
            'SELECT ukey FROM storage WHERE filename = ? ORDER BY ukey ASC',
            [Value\Varchar::fromValue($filename)],
            Consistency::ONE,
            new QueryOptions(pageSize: 50)
        )->asRowsResult();

        $count2 = 0;
        do {
            foreach ($result as $_row) {
                $count2++;
            }
            $state = $result->getRowsMetadata()->pagingState;
            if ($state === null) {
                break;
            }
            $result = $conn->query(
                'SELECT ukey FROM storage WHERE filename = ? ORDER BY ukey ASC',
                [Value\Varchar::fromValue($filename)],
                Consistency::ONE,
                new QueryOptions(pageSize: 50, pagingState: $state)
            )->asRowsResult();
        } while (true);

        $this->assertSame($numRows, $count2, 'Query+paging should return all inserted rows');
    }

    protected static function setupTable(): void {
        $conn = self::newConnection(self::$keyspace);
        $conn->query('CREATE TABLE IF NOT EXISTS storage(filename varchar, ukey varchar, value map<varchar, varchar>, PRIMARY KEY (filename, ukey))');
        $conn->disconnect();
    }
}
