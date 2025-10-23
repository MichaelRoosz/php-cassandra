<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Value;

final class StatementResolutionTest extends AbstractIntegrationTestCase {
    public function testTryResolveStatementsMultipleCallsEventuallyResolvesAll(): void {

        $conn = $this->connection;

        // Create multiple async queries
        $statements = [];
        for ($i = 0; $i < 10; $i++) {
            $statements[] = $conn->queryAsync('SELECT key FROM system.local');
        }

        // Keep trying to resolve with small max until all are resolved
        $maxIterations = 100;
        $iterations = 0;
        while ($iterations < $maxIterations) {
            $resolved = $conn->tryResolveStatements($statements, 2);

            $allReady = true;
            foreach ($statements as $stmt) {
                if (!$stmt->isResultReady()) {
                    $allReady = false;

                    break;
                }
            }

            if ($allReady) {
                break;
            }

            $iterations++;
        }

        // All should be resolved by now
        foreach ($statements as $stmt) {
            $this->assertTrue($stmt->isResultReady());
        }
    }

    public function testTryResolveStatementsProgressiveResolution(): void {

        $conn = $this->connection;

        // Create async queries
        $statements = [];
        for ($i = 0; $i < 5; $i++) {
            $statements[] = $conn->queryAsync('SELECT key FROM system.local');
        }

        $totalResolved = 0;
        $maxIterations = 50;
        $iterations = 0;

        // Try to resolve one at a time
        while ($iterations < $maxIterations) {
            $resolved = $conn->tryResolveStatements($statements, 1);
            $totalResolved += $resolved;

            if ($totalResolved >= 5) {
                break;
            }

            $iterations++;
        }

        // Should have resolved all 5 statements
        $this->assertSame(5, $totalResolved);

        // All should be ready
        foreach ($statements as $stmt) {
            $this->assertTrue($stmt->isResultReady());
        }
    }

    public function testTryResolveStatementsReturnsCountOfNewlyResolved(): void {

        $conn = $this->connection;

        // Create multiple async queries
        $s1 = $conn->queryAsync('SELECT key FROM system.local');
        $s2 = $conn->queryAsync('SELECT release_version FROM system.local');
        $s3 = $conn->queryAsync('SELECT cluster_name FROM system.local');

        $statements = [$s1, $s2, $s3];

        // Try to resolve statements - should resolve at least some
        $resolved = $conn->tryResolveStatements($statements);

        // Should have resolved between 0 and 3 statements
        $this->assertGreaterThanOrEqual(0, $resolved);
        $this->assertLessThanOrEqual(3, $resolved);

        // Ensure all are eventually resolved
        $conn->waitForStatements($statements);

        // Now all should be ready
        $this->assertTrue($s1->isResultReady());
        $this->assertTrue($s2->isResultReady());
        $this->assertTrue($s3->isResultReady());
    }

    public function testTryResolveStatementsWithAlreadyResolvedStatements(): void {

        $conn = $this->connection;

        // Create async statements and resolve them immediately
        $s1 = $conn->queryAsync('SELECT key FROM system.local');
        $s2 = $conn->queryAsync('SELECT release_version FROM system.local');

        // Resolve them completely first
        $conn->waitForStatements([$s1, $s2]);

        $statements = [$s1, $s2];

        // Both already ready
        $this->assertTrue($s1->isResultReady());
        $this->assertTrue($s2->isResultReady());

        // Try to resolve - should return 0 since they're already resolved
        $resolved = $conn->tryResolveStatements($statements);

        $this->assertSame(0, $resolved);
    }

    public function testTryResolveStatementsWithEmptyArray(): void {

        $conn = $this->connection;

        // Try to resolve empty array - should return 0
        $resolved = $conn->tryResolveStatements([]);

        $this->assertSame(0, $resolved);
    }

    public function testTryResolveStatementsWithMaxLimit(): void {

        $conn = $this->connection;

        // Create multiple async queries
        $statements = [];
        for ($i = 0; $i < 5; $i++) {
            $statements[] = $conn->queryAsync('SELECT key FROM system.local');
        }

        // Try to resolve with max limit of 2
        $resolved = $conn->tryResolveStatements($statements, 2);

        // Should resolve at most 2
        $this->assertLessThanOrEqual(2, $resolved);
        $this->assertGreaterThanOrEqual(0, $resolved);

        // Complete the rest
        $conn->waitForStatements($statements);

        foreach ($statements as $stmt) {
            $this->assertTrue($stmt->isResultReady());
        }
    }

    public function testTryResolveStatementsWithMixedReadyAndPending(): void {

        $conn = $this->connection;

        // Create three async statements
        $s1 = $conn->queryAsync('SELECT key FROM system.local');
        $s2 = $conn->queryAsync('SELECT release_version FROM system.local');
        $s3 = $conn->queryAsync('SELECT cluster_name FROM system.local');

        // Resolve the first one immediately
        $conn->waitForStatements([$s1]);
        $this->assertTrue($s1->isResultReady());

        $statements = [$s1, $s2, $s3];

        // Try to resolve all three
        $resolved = $conn->tryResolveStatements($statements);

        // Should have resolved 0 to 2 new statements (s1 was already ready)
        $this->assertGreaterThanOrEqual(0, $resolved);
        $this->assertLessThanOrEqual(2, $resolved);

        // Complete the rest
        $conn->waitForStatements($statements);

        $this->assertTrue($s1->isResultReady());
        $this->assertTrue($s2->isResultReady());
        $this->assertTrue($s3->isResultReady());
    }

    public function testTryResolveStatementsWithZeroMax(): void {

        $conn = $this->connection;

        // Create async queries
        $s1 = $conn->queryAsync('SELECT key FROM system.local');
        $s2 = $conn->queryAsync('SELECT release_version FROM system.local');

        // Try to resolve with max = 0 should return 0
        $resolved = $conn->tryResolveStatements([$s1, $s2], 0);

        $this->assertSame(0, $resolved);

        // Complete them properly
        $conn->waitForStatements([$s1, $s2]);

        $this->assertTrue($s1->isResultReady());
        $this->assertTrue($s2->isResultReady());
    }

    public function testWaitForStatementsWithAlreadyResolvedStatements(): void {

        $conn = $this->connection;

        // Execute async queries and then immediately resolve them
        $s1 = $conn->queryAsync('SELECT key FROM system.local');
        $s2 = $conn->queryAsync('SELECT release_version FROM system.local');

        // Immediately wait for them to be ready
        $conn->waitForStatements([$s1, $s2]);

        // Both should be ready now
        $this->assertTrue($s1->isResultReady());
        $this->assertTrue($s2->isResultReady());

        // Wait again should return immediately since they're already resolved
        $conn->waitForStatements([$s1, $s2]);

        // Still ready
        $this->assertTrue($s1->isResultReady());
        $this->assertTrue($s2->isResultReady());
    }

    public function testWaitForStatementsWithEmptyArray(): void {

        $conn = $this->connection;

        // Waiting for empty array should return immediately without error
        $conn->waitForStatements([]);
    }

    public function testWaitForStatementsWithInserts(): void {

        $conn = $this->connection;

        $filename = 'test_wait_' . bin2hex(random_bytes(4));

        // Create async insert statements
        $statements = [];
        for ($i = 0; $i < 5; $i++) {
            $statements[] = $conn->queryAsync(
                'INSERT INTO storage(filename, ukey, value) VALUES (?, ?, ?)',
                [
                    Value\Varchar::fromValue($filename),
                    Value\Varchar::fromValue('key' . $i),
                    Value\MapCollection::fromValue(['data' => 'value' . $i], \Cassandra\Type::VARCHAR, \Cassandra\Type::VARCHAR),
                ]
            );
        }

        // Wait for all inserts to complete
        $conn->waitForStatements($statements);

        // Verify all completed
        foreach ($statements as $stmt) {
            $this->assertTrue($stmt->isResultReady());
        }

        // Verify data was inserted
        $countResult = $conn->query(
            'SELECT COUNT(*) as count FROM storage WHERE filename = ?',
            [Value\Varchar::fromValue($filename)]
        )->asRowsResult();

        $count = $countResult->fetchColumn(0);
        $this->assertGreaterThanOrEqual(5, $count);
    }

    public function testWaitForStatementsWithMixedReadyAndPending(): void {

        $conn = $this->connection;

        // Create three async statements
        $s1 = $conn->queryAsync('SELECT key FROM system.local');
        $s2 = $conn->queryAsync('SELECT release_version FROM system.local');
        $s3 = $conn->queryAsync('SELECT cluster_name FROM system.local');

        // Resolve the first one immediately
        $conn->waitForStatements([$s1]);
        $this->assertTrue($s1->isResultReady());

        // Now s1 is ready, s2 and s3 are potentially pending
        // Wait for all three (s1 already ready, s2 and s3 pending)
        $conn->waitForStatements([$s1, $s2, $s3]);

        // All should be ready now
        $this->assertTrue($s1->isResultReady());
        $this->assertTrue($s2->isResultReady());
        $this->assertTrue($s3->isResultReady());
    }
    public function testWaitForStatementsWithMultipleAsyncQueries(): void {

        $conn = $this->connection;

        // Create multiple async queries
        $s1 = $conn->queryAsync('SELECT key FROM system.local');
        $s2 = $conn->queryAsync('SELECT release_version FROM system.local');
        $s3 = $conn->queryAsync('SELECT cluster_name FROM system.local');

        // None of the statements should be ready yet (they might be, but we'll test the wait anyway)
        $statements = [$s1, $s2, $s3];

        // Wait for all statements to complete
        $conn->waitForStatements($statements);

        // All statements should now be ready
        $this->assertTrue($s1->isResultReady());
        $this->assertTrue($s2->isResultReady());
        $this->assertTrue($s3->isResultReady());

        // Verify we can get results from all statements
        $r1 = $s1->getRowsResult();
        $r2 = $s2->getRowsResult();
        $r3 = $s3->getRowsResult();

        $this->assertSame(1, $r1->getRowCount());
        $this->assertSame(1, $r2->getRowCount());
        $this->assertSame(1, $r3->getRowCount());
    }

    protected static function setupTable(): void {
        $conn = self::newConnection(self::$defaultKeyspace);
        $conn->query('CREATE TABLE IF NOT EXISTS storage(filename varchar, ukey varchar, value map<varchar, varchar>, PRIMARY KEY (filename, ukey))');
        $conn->disconnect();
    }
}
