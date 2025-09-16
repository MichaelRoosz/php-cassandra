<?php

declare(strict_types=1);

use Cassandra\Connection;
use Cassandra\Consistency;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Value\Int32;
use Cassandra\Value\Varchar;

/**
 * Basic query micro-benchmarks reusing the integration connection.
 */
final class QueryBench {
    private Connection $conn;

    public function __construct() {
        $this->conn = BenchEnv::connection();
    }

    public function beforeEach(): void {
        $this->conn->query('TRUNCATE kv');
    }

    /**
     * @Revs(10)
     * @Iterations(5)
     */
    public function benchInsertAndSelectWithoutTypeInfo(): void {
        for ($i = 0; $i < 100; $i++) {
            $this->conn->query(
                'INSERT INTO kv (id, v) VALUES (?, ?)',
                [$i, (string) $i],
                Consistency::ONE
            );
        }

        $this->conn->query(
            'SELECT * FROM kv WHERE id = ?',
            [42],
            Consistency::ONE
        )->asRowsResult();
    }

    /**
     * @Revs(10)
     * @Iterations(5)
     */
    public function benchInsertAndSelectWithTypeInfo(): void {
        for ($i = 0; $i < 100; $i++) {
            $this->conn->query(
                'INSERT INTO kv (id, v) VALUES (?, ?)',
                [Int32::fromValue($i), Varchar::fromValue((string) $i)],
                Consistency::ONE
            );
        }

        $this->conn->query(
            'SELECT * FROM kv WHERE id = ?',
            [Int32::fromValue(42)],
            Consistency::ONE
        )->asRowsResult();
    }

    /**
     * Measure paging over a larger table to exercise network and parsing.
     *
     * @Revs(5)
     * @Iterations(3)
     */
    public function benchPagedQuery(): void {
        // ensure some data exists
        for ($i = 0; $i < 500; $i++) {
            $this->conn->query(
                'INSERT INTO kv (id, v) VALUES (?, ?) IF NOT EXISTS',
                [Int32::fromValue($i), Varchar::fromValue((string) $i)],
                Consistency::ONE
            );
        }

        $opts = (new QueryOptions(pageSize: 50));
        $responses = $this->conn->queryAll('SELECT * FROM kv', [], Consistency::ONE, $opts);

        $count = 0;
        foreach ($responses as $rowsResult) {
            $count += $rowsResult->getRowCount();
        }
        if ($count < 100) {
            throw new RuntimeException('Unexpected low row count: ' . $count);
        }
    }

    /**
     * @Revs(10)
     * @Iterations(5)
     */
    public function benchPreparedInsert(): void {
        $prepared = $this->conn->prepare('INSERT INTO kv (id, v) VALUES (?, ?)');

        for ($i = 0; $i < 100; $i++) {
            $this->conn->execute($prepared, [$i, (string) $i]);
        }
    }

    /**
     * @Revs(10)
     * @Iterations(5)
     */
    public function benchSimpleSelect(): void {
        $this->conn->query('SELECT key FROM system.local')->asRowsResult();
    }
}
