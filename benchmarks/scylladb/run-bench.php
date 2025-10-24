<?php

declare(strict_types=1);

/**
 * Simple benchmark runner for ScyllaDB PHP driver
 * Mimics the operations from QueryBench.php but uses ScyllaDB driver API
 */

require __DIR__ . '/bootstrap.php';

class ScyllaDBQueryBench {
    private $session;

    public function __construct() {
        $this->session = ScyllaDBBenchEnv::session();
    }

    public function beforeEach() {
        $this->session->execute('TRUNCATE kv', ['consistency' => \Cassandra::CONSISTENCY_ONE]);
    }

    /**
     * Insert 100 rows and select one without prepared statements
     */
    public function benchInsertAndSelectWithoutTypeInfo($revs, $iterations) {
        $totalTime = 0;

        for ($iter = 0; $iter < $iterations; $iter++) {
            $this->beforeEach();
            $start = microtime(true);

            for ($rev = 0; $rev < $revs; $rev++) {
                for ($i = 0; $i < 100; $i++) {
                    $this->session->execute(
                        'INSERT INTO kv (id, v) VALUES (?, ?)',
                        ['arguments' => [$i, (string) $i], 'consistency' => \Cassandra::CONSISTENCY_ONE]
                    );
                }

                $this->session->execute(
                    'SELECT * FROM kv WHERE id = ?',
                    ['arguments' => [42], 'consistency' => \Cassandra::CONSISTENCY_ONE]
                );
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
    public function benchInsertAndSelectWithTypeInfo($revs, $iterations) {
        $totalTime = 0;

        for ($iter = 0; $iter < $iterations; $iter++) {
            $this->beforeEach();
            $start = microtime(true);

            for ($rev = 0; $rev < $revs; $rev++) {
                for ($i = 0; $i < 100; $i++) {
                    $this->session->execute(
                        'INSERT INTO kv (id, v) VALUES (?, ?)',
                        ['arguments' => [
                            $i,
                            (string) $i,
                        ], 'consistency' => \Cassandra::CONSISTENCY_ONE]
                    );
                }

                $this->session->execute(
                    'SELECT * FROM kv WHERE id = ?',
                    ['arguments' => [
                        42,
                    ], 'consistency' => \Cassandra::CONSISTENCY_ONE]
                );
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
    public function benchPagedQuery($revs, $iterations) {
        $totalTime = 0;

        // Setup data once
        $this->beforeEach();
        for ($i = 0; $i < 500; $i++) {
            $statement = new \Cassandra\SimpleStatement(
                'INSERT INTO kv (id, v) VALUES (?, ?) IF NOT EXISTS'
            );
            $this->session->execute($statement, [
                'arguments' => [$i, (string) $i],
                'consistency' => \Cassandra::CONSISTENCY_ONE,
            ]);
        }

        for ($iter = 0; $iter < $iterations; $iter++) {
            $start = microtime(true);

            for ($rev = 0; $rev < $revs; $rev++) {
                $statement = new \Cassandra\SimpleStatement('SELECT * FROM kv');
                $options = ['page_size' => 50, 'consistency' => \Cassandra::CONSISTENCY_ONE];
                $result = $this->session->execute($statement, $options);

                $count = 0;
                do {
                    foreach ($result as $row) {
                        $count++;
                    }
                    $result = $result->nextPage();
                } while ($result !== null);

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
    public function benchPreparedInsert($revs, $iterations) {
        $totalTime = 0;

        for ($iter = 0; $iter < $iterations; $iter++) {
            $this->beforeEach();
            $start = microtime(true);

            $prepared = $this->session->prepare('INSERT INTO kv (id, v) VALUES (?, ?)');

            for ($rev = 0; $rev < $revs; $rev++) {
                for ($i = 0; $i < 100; $i++) {
                    $this->session->execute($prepared, ['arguments' => [$i, (string) $i], 'consistency' => \Cassandra::CONSISTENCY_ONE]);
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
    public function benchSimpleSelect($revs, $iterations) {
        $totalTime = 0;

        for ($iter = 0; $iter < $iterations; $iter++) {
            $start = microtime(true);

            for ($rev = 0; $rev < $revs; $rev++) {
                $this->session->execute('SELECT key FROM system.local', ['consistency' => \Cassandra::CONSISTENCY_ONE]);
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
$benchConfig = require __DIR__ . '/../bench-config.php';

// Run benchmarks
echo "=== ScyllaDB PHP Driver Benchmarks ===\n";
echo 'PHP Version: ' . phpversion() . "\n";
echo 'ScyllaDB Extension Version: ' . phpversion('cassandra') . "\n\n";

$bench = new ScyllaDBQueryBench();

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
echo json_encode(['driver' => 'scylladb', 'results' => $results], JSON_PRETTY_PRINT) . "\n";
