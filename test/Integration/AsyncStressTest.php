<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Consistency;
use Cassandra\Request\Options\QueryOptions;

final class AsyncStressTest extends AbstractIntegrationTestCase {
    public function testManyConcurrentQueryAsync(): void {

        $conn = $this->connection;

        // Warm-up simple selects to ensure connection is stable
        $r = $conn->query('SELECT key FROM system.local')->asRowsResult();
        $this->assertSame(1, $r->getRowCount());

        $total = 250; // reasonably high but not extreme to keep CI stable
        $pending = [];

        for ($i = 0; $i < $total; $i++) {
            $pending[] = $conn->queryAsync(
                'SELECT key, release_version FROM system.local',
                [],
                Consistency::ONE,
                new QueryOptions()
            );
        }

        // Interleave some reads to exercise multiplexing
        $mid = (int) floor($total / 2);
        $pending[$mid - 1]->getRowsResult();
        $pending[$mid]->getRowsResult();

        // Drain remaining
        $conn->waitForAllPendingAsyncStatements();

        $validated = 0;
        foreach ($pending as $stmt) {
            $rows = $stmt->getRowsResult();
            $this->assertSame(1, $rows->getRowCount());
            $row = $rows->fetch();
            $this->assertSame('local', $row['key'] ?? null);
            $this->assertIsString($row['release_version'] ?? null);
            $validated++;
        }

        $this->assertSame($total, $validated);
    }

    protected static function setupTable(): void {
        // no-op
    }
}
