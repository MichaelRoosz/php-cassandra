<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Request\BatchType;
use Cassandra\Type;
use Cassandra\Value;

final class AsyncTest extends AbstractIntegrationTestCase {
    public function testAsyncBatchAndWaitForAllPendingStatements(): void {

        $conn = $this->connection;

        $filename = 'itest_' . bin2hex(random_bytes(6));

        $pending = [];
        for ($j = 0; $j < 3; $j++) {
            $batch = $conn->createBatchRequest(BatchType::UNLOGGED);
            for ($i = 0; $i < 10; $i++) {
                $batch->appendQuery(
                    'INSERT INTO storage(filename, ukey, value) VALUES (?, ?, ?)',
                    [
                        Value\Varchar::fromValue($filename),
                        Value\Varchar::fromValue('k' . $j . '_' . $i),
                        Value\MapCollection::fromValue(['x' => 'y'], Type::VARCHAR, Type::VARCHAR),
                    ]
                );
            }
            $pending[] = $conn->batchAsync($batch);
        }

        $conn->waitForAllPendingStatements();

        $countValue = $conn->query(
            'SELECT COUNT(*) FROM storage WHERE filename = ?',
            [Value\Varchar::fromValue($filename)]
        )->asRowsResult()->fetchColumn(0);
        $count = is_int($countValue) ? $countValue : 0;

        $this->assertGreaterThanOrEqual(30, $count);
    }

    public function testConcurrentAsyncQueries(): void {

        $conn = $this->connection;

        $s1 = $conn->queryAsync('SELECT key FROM system.local');
        $s2 = $conn->queryAsync('SELECT release_version FROM system.local');

        $r2 = $s2->getRowsResult();
        $r1 = $s1->getRowsResult();

        $this->assertSame(1, $r1->getRowCount());
        $this->assertSame(['key' => 'local'], $r1->fetch());
        $this->assertSame(1, $r2->getRowCount());
        $this->assertIsString($r2->fetch()['release_version'] ?? null);
    }

    protected static function setupTable(): void {
        $conn = self::newConnection(self::$defaultKeyspace);
        $conn->query('CREATE TABLE IF NOT EXISTS storage(filename varchar, ukey varchar, value map<varchar, varchar>, PRIMARY KEY (filename, ukey))');
        $conn->disconnect();
    }
}
