<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Consistency;
use Cassandra\Request\BatchType;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Type;
use Cassandra\Value;

final class BatchTest extends AbstractIntegrationTestCase {
    public function testUnloggedBatchInsert(): void {
        $conn = $this->connection;

        $filename = 'itest_' . bin2hex(random_bytes(6));
        $batch = $conn->createBatchRequest(BatchType::UNLOGGED, Consistency::ONE);
        for ($i = 0; $i < 10; $i++) {
            $batch->appendQuery(
                'INSERT INTO storage(filename, ukey, value) VALUES (?, ?, ?)',
                [
                    Value\Varchar::fromValue($filename),
                    Value\Varchar::fromValue('k' . $i),
                    Value\MapCollection::fromValue(['a' => 'b'], Type::VARCHAR, Type::VARCHAR),
                ]
            );
        }

        $r = $conn->batch($batch);
        $this->assertSame(0, $r->getStream());

        $rows = $conn->query(
            'SELECT COUNT(*) FROM storage WHERE filename = ?',
            [Value\Varchar::fromValue($filename)],
            Consistency::ONE,
            new QueryOptions(namesForValues: false)
        )->asRowsResult();

        $countValue = $rows->fetchColumn(0);
        $count = is_int($countValue) ? $countValue : 0;
        $this->assertSame(10, $count);
    }

    protected static function setupTable(): void {
        $conn = self::newConnection(self::$defaultKeyspace);
        $conn->query('CREATE TABLE IF NOT EXISTS storage(filename varchar, ukey varchar, value map<varchar, varchar>, PRIMARY KEY (filename, ukey))');
        $conn->disconnect();
    }
}
