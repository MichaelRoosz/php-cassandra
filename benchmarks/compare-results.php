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

        // Print comparison table
        echo "\nBenchmark Comparison Table\n";
        echo str_repeat('=', 165) . "\n";
        printf("%-40s %-25s %-20s %-20s %-25s %-25s\n",
            'Benchmark',
            'php-cassandra (PHP 8.2)',
            'DataStax (PHP 7.1)',
            'ScyllaDB (PHP 8.2)',
            'vs DataStax',
            'vs ScyllaDB'
        );
        echo str_repeat('-', 165) . "\n";

        foreach ($phpCassandraResults as $benchName => $phpCassandra) {
            if (isset($datastaxResults[$benchName]) && isset($scylladbResults[$benchName])) {
                $datastax = $datastaxResults[$benchName];
                $scylladb = $scylladbResults[$benchName];

                $phpCassandraAvg = $phpCassandra['avg_time'];
                $datastaxAvg = $datastax['avg_time'];
                $scylladbAvg = $scylladb['avg_time'];

                $comparisonDataStax = calculateSpeedup($phpCassandraAvg, $datastaxAvg);
                $comparisonScyllaDB = calculateSpeedup($phpCassandraAvg, $scylladbAvg);

                printf("%-40s %-25s %-20s %-20s %-25s %-25s\n",
                    $benchName,
                    formatTime($phpCassandraAvg),
                    formatTime($datastaxAvg),
                    formatTime($scylladbAvg),
                    $comparisonDataStax,
                    $comparisonScyllaDB
                );
            }
        }

        echo str_repeat('=', 165) . "\n";
        echo "\nNotes:\n";
        echo "  - Lower time is better\n";
        echo "  - 'vs DataStax' shows php-cassandra (PHP 8.2) relative to DataStax (PHP 7.1)\n";
        echo "  - 'vs ScyllaDB' shows php-cassandra (PHP 8.2) relative to ScyllaDB driver (PHP 8.2)\n";
        echo "  - 'Xx faster' means php-cassandra is faster\n";
        echo "  - 'Xx slower' means php-cassandra is slower\n";

        return 0;
    } catch (Exception $e) {
        echo "Error comparing results: {$e->getMessage()}\n";

        return 1;
    }
}

exit(main($argv));
