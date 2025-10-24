<?php

/**
 * Simple benchmark runner for php-cassandra library
 * Mimics the operations from QueryBench.php with manual timing
 */

declare(strict_types=1);

use Cassandra\Connection;
use Cassandra\Consistency;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Value\Int32;
use Cassandra\Value\Varchar;

require __DIR__ . '/bootstrap.php';

class PhpCassandraQueryBench {
    private Connection $conn;

    public function __construct() {
        $this->conn = BenchEnv::connection();
    }

    public function beforeEach(): void {
        $this->conn->query('TRUNCATE kv');
    }

    /**
     * Insert 100 rows and select one without prepared statements
     */
    public function benchInsertAndSelectWithoutTypeInfo(int $revs, int $iterations): array {
        $totalTime = 0.0;

        for ($iter = 0; $iter < $iterations; $iter++) {
            $this->beforeEach();
            $start = microtime(true);

            for ($rev = 0; $rev < $revs; $rev++) {
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

            $elapsed = microtime(true) - $start;
            $totalTime += $elapsed;
        }

        return [
            'name' => 'benchInsertAndSelectWithoutTypeInfo',
            'revs' => $revs,
            'iterations' => $iterations,
            'total_time' => $totalTime,
            'avg_time' => $totalTime / $iterations,
            'ops_per_second' => ($revs * $iterations) / $totalTime,
        ];
    }

    /**
     * Insert 100 rows and select one with type info
     */
    public function benchInsertAndSelectWithTypeInfo(int $revs, int $iterations): array {
        $totalTime = 0.0;

        for ($iter = 0; $iter < $iterations; $iter++) {
            $this->beforeEach();
            $start = microtime(true);

            for ($rev = 0; $rev < $revs; $rev++) {
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

            $elapsed = microtime(true) - $start;
            $totalTime += $elapsed;
        }

        return [
            'name' => 'benchInsertAndSelectWithTypeInfo',
            'revs' => $revs,
            'iterations' => $iterations,
            'total_time' => $totalTime,
            'avg_time' => $totalTime / $iterations,
            'ops_per_second' => ($revs * $iterations) / $totalTime,
        ];
    }

    /**
     * Paged query benchmark
     */
    public function benchPagedQuery(int $revs, int $iterations): array {
        $totalTime = 0.0;

        // Setup data once
        $this->beforeEach();
        for ($i = 0; $i < 500; $i++) {
            $this->conn->query(
                'INSERT INTO kv (id, v) VALUES (?, ?) IF NOT EXISTS',
                [Int32::fromValue($i), Varchar::fromValue((string) $i)],
                Consistency::ONE
            );
        }

        for ($iter = 0; $iter < $iterations; $iter++) {
            $start = microtime(true);

            for ($rev = 0; $rev < $revs; $rev++) {

                $count = 0;

                $query = 'SELECT * FROM kv';
                $opts = (new QueryOptions(pageSize: 50));
                $response = $this->conn->query($query, [], Consistency::ONE, $opts)->asRowsResult();

                foreach ($response as $_row) {
                    $count++;
                }

                $pagingState = $response->getRowsMetadata()->pagingState;
                while ($pagingState !== null) {
                    $response = $this->conn->query(
                        query: $query,
                        values: [],
                        consistency: Consistency::ONE,
                        options: $opts->withPagingState(
                            $pagingState
                        )
                    )->asRowsResult();

                    foreach ($response as $_row) {
                        $count++;
                    }

                    $pagingState = $response->getRowsMetadata()->pagingState;
                }

                if ($count < 100) {
                    throw new RuntimeException('Unexpected low row count: ' . $count);
                }
            }

            $elapsed = microtime(true) - $start;
            $totalTime += $elapsed;
        }

        return [
            'name' => 'benchPagedQuery',
            'revs' => $revs,
            'iterations' => $iterations,
            'total_time' => $totalTime,
            'avg_time' => $totalTime / $iterations,
            'ops_per_second' => ($revs * $iterations) / $totalTime,
        ];
    }

    /**
     * Prepared statement insert benchmark
     */
    public function benchPreparedInsert(int $revs, int $iterations): array {
        $totalTime = 0.0;

        for ($iter = 0; $iter < $iterations; $iter++) {
            $this->beforeEach();
            $start = microtime(true);

            $prepared = $this->conn->prepare('INSERT INTO kv (id, v) VALUES (?, ?)');

            for ($rev = 0; $rev < $revs; $rev++) {
                for ($i = 0; $i < 100; $i++) {
                    $this->conn->execute($prepared, [$i, (string) $i]);
                }
            }

            $elapsed = microtime(true) - $start;
            $totalTime += $elapsed;
        }

        return [
            'name' => 'benchPreparedInsert',
            'revs' => $revs,
            'iterations' => $iterations,
            'total_time' => $totalTime,
            'avg_time' => $totalTime / $iterations,
            'ops_per_second' => ($revs * $iterations) / $totalTime,
        ];
    }

    /**
     * Simple select benchmark
     */
    public function benchSimpleSelect(int $revs, int $iterations): array {
        $totalTime = 0.0;

        for ($iter = 0; $iter < $iterations; $iter++) {
            $start = microtime(true);

            for ($rev = 0; $rev < $revs; $rev++) {
                $this->conn->query('SELECT key FROM system.local')->asRowsResult();
            }

            $elapsed = microtime(true) - $start;
            $totalTime += $elapsed;
        }

        return [
            'name' => 'benchSimpleSelect',
            'revs' => $revs,
            'iterations' => $iterations,
            'total_time' => $totalTime,
            'avg_time' => $totalTime / $iterations,
            'ops_per_second' => ($revs * $iterations) / $totalTime,
        ];
    }
}

// Load benchmark configuration
$benchConfig = require __DIR__ . '/bench-config.php';

// Run benchmarks
echo "=== php-cassandra Library Benchmarks ===\n";
echo 'PHP Version: ' . phpversion() . "\n\n";

$bench = new PhpCassandraQueryBench();

$benchmarks = [];
foreach ($benchConfig as $method => $params) {
    $benchmarks[] = array_merge(['method' => $method], $params);
}

$results = [];
foreach ($benchmarks as $config) {
    echo "Running {$config['method']}...\n";
    $result = $bench->{$config['method']}($config['revs'], $config['iterations']);
    $results[] = $result;

    printf("  Total: %.4fs | Avg: %.4fs | Ops/s: %.2f\n",
        $result['total_time'],
        $result['avg_time'],
        $result['ops_per_second']
    );
}

// Output JSON results for comparison
echo "\n=== JSON Results ===\n";
echo json_encode(['driver' => 'php-cassandra', 'results' => $results], JSON_PRETTY_PRINT) . "\n";
