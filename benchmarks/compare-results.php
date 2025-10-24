#!/usr/bin/env php
<?php

declare(strict_types=1);

/**
 * Compare benchmark results from both drivers and display them in a table.
 */

function extractJsonResults(string $filePath): array {
    if (!file_exists($filePath)) {
        return [];
    }

    $content = file_get_contents($filePath);

    // Find JSON results section
    if (preg_match('/=== JSON Results ===\s*(\{.*?\})\s*$/s', $content, $matches)) {
        $decoded = json_decode($matches[1], true);

        return $decoded ?? [];
    }

    return [];
}

function formatTime(float $seconds): string {
    if ($seconds < 0.001) {
        return sprintf('%.2fμs', $seconds * 1000000);
    } elseif ($seconds < 1) {
        return sprintf('%.2fms', $seconds * 1000);
    } else {
        return sprintf('%.4fs', $seconds);
    }
}

function calculateSpeedup(float $phpCassandraTime, float $datastaxTime): string {
    if ($phpCassandraTime == 0 || $datastaxTime == 0) {
        return 'N/A';
    }

    $ratio = $datastaxTime / $phpCassandraTime;

    if ($ratio > 1.0) {
        return sprintf('%.2fx faster', $ratio);
    } elseif ($ratio < 1.0) {
        return sprintf('%.2fx slower', 1 / $ratio);
    } else {
        return 'same';
    }
}

function main(array $argv): int {
    if (count($argv) !== 4) {
        echo "Usage: compare-results.php <php-cassandra_results.txt> <datastax_results.txt> <scylladb_results.txt>\n";

        return 1;
    }

    $phpCassandraFile = $argv[1];
    $datastaxFile = $argv[2];
    $scylladbFile = $argv[3];

    try {
        $phpCassandraData = extractJsonResults($phpCassandraFile);
        $datastaxData = extractJsonResults($datastaxFile);
        $scylladbData = extractJsonResults($scylladbFile);

        if (empty($phpCassandraData) || empty($datastaxData) || empty($scylladbData)) {
            echo "Warning: Could not extract JSON results from one or more files\n";

            return 0;
        }

        // Index results by benchmark name
        $phpCassandraResults = [];
        foreach ($phpCassandraData['results'] ?? [] as $result) {
            $phpCassandraResults[$result['name']] = $result;
        }

        $datastaxResults = [];
        foreach ($datastaxData['results'] ?? [] as $result) {
            $datastaxResults[$result['name']] = $result;
        }

        $scylladbResults = [];
        foreach ($scylladbData['results'] ?? [] as $result) {
            $scylladbResults[$result['name']] = $result;
        }

        // First table: Benchmark descriptions
        echo "\n=== Benchmark Descriptions ===\n\n";

        foreach ($phpCassandraResults as $benchName => $phpCassandra) {
            if (isset($datastaxResults[$benchName]) && isset($scylladbResults[$benchName])) {
                $description = $phpCassandra['description'] ?? 'N/A';
                $rounds = $phpCassandra['rounds'] ?? 0;
                $iterations = $phpCassandra['iterations'] ?? 0;

                printf("%s\n  %s\n  -> 1 iteration = %d rounds, testing with %d iterations\n\n",
                    $benchName,
                    $description,
                    $rounds,
                    $iterations
                );
            }
        }

        // Second table: Performance comparison
        echo "\n=== Performance Comparison (avg time per iteration, lower is better) ===\n";
        echo str_repeat('=', 130) . "\n";
        printf("%-45s %-15s %-15s %-15s %-20s %-20s\n",
            'Benchmark',
            'php-cassandra',
            'DataStax',
            'ScyllaDB',
            'vs DataStax',
            'vs ScyllaDB'
        );
        echo str_repeat('-', 130) . "\n";

        foreach ($phpCassandraResults as $benchName => $phpCassandra) {
            if (isset($datastaxResults[$benchName]) && isset($scylladbResults[$benchName])) {
                $datastax = $datastaxResults[$benchName];
                $scylladb = $scylladbResults[$benchName];

                $phpCassandraAvg = $phpCassandra['avg_time'];
                $datastaxAvg = $datastax['avg_time'];
                $scylladbAvg = $scylladb['avg_time'];

                $comparisonDataStax = calculateSpeedup($phpCassandraAvg, $datastaxAvg);
                $comparisonScyllaDB = calculateSpeedup($phpCassandraAvg, $scylladbAvg);

                // Print benchmark name and times
                printf("%-45s %-15s %-15s %-15s %-20s %-20s\n",
                    $benchName,
                    formatTime($phpCassandraAvg),
                    formatTime($datastaxAvg),
                    formatTime($scylladbAvg),
                    $comparisonDataStax,
                    $comparisonScyllaDB
                );
            }
        }

        echo str_repeat('=', 130) . "\n";
        echo "\nNotes:\n";
        echo "  Times are average per iteration. Each iteration runs multiple rounds of operations.\n";
        echo "  'Xx faster/slower' compares php-cassandra to the other driver (lower time is better)\n";
        echo "  php-cassandra: PHP 8.2 | DataStax: PHP 7.1 | ScyllaDB: PHP 8.2\n";

        return 0;
    } catch (Exception $e) {
        echo "Error comparing results: {$e->getMessage()}\n";

        return 1;
    }
}

exit(main($argv));
