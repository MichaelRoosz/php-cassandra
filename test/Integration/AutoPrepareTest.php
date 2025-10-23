<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Consistency;
use Cassandra\Exception\ServerException\InvalidException;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Value;

final class AutoPrepareTest extends AbstractIntegrationTestCase {
    /**
     * Test that disabling auto-prepare does NOT trigger automatic preparation.
     * A php float value will be sent as a double, which is not supported by the server.
     */
    public function testQueryWithAutoPrepareDisabled(): void {
        $conn = $this->connection;
        $conn->query('TRUNCATE auto_prepare_test');

        $this->expectException(InvalidException::class);
        $conn->query(
            "INSERT INTO {$this->keyspace}.auto_prepare_test (id, name, age) VALUES (?, ?, ?)",
            [
                Value\Uuid::fromValue(self::uuidV4()),
                'Charlie',
                35.0,
            ],
            Consistency::ONE,
            new QueryOptions(autoPrepare: false)
        );
    }

    /**
     * Test that enabling auto-prepare does trigger automatic preparation.
     */
    public function testQueryWithAutoPrepareEnabled(): void {
        $conn = $this->connection;
        $conn->query('TRUNCATE auto_prepare_test');

        $conn->query(
            "INSERT INTO {$this->keyspace}.auto_prepare_test (id, name, age) VALUES (?, ?, ?)",
            [
                Value\Uuid::fromValue(self::uuidV4()),
                'Charlie',
                35.0,
            ],
            Consistency::ONE,
            new QueryOptions(autoPrepare: true)
        );

        $rows = $conn->query(
            "SELECT * FROM {$this->keyspace}.auto_prepare_test"
        )->asRowsResult();

        $this->assertSame(1, $rows->getRowCount());
        $row = $rows->fetch();
        $this->assertIsArray($row);
        $this->assertSame('Charlie', $row['name']);
        $this->assertSame(35.0, $row['age']);
    }

    protected static function setupTable(): void {
        $conn = self::newConnection(self::$defaultKeyspace);
        $conn->query(
            'CREATE TABLE IF NOT EXISTS auto_prepare_test(' .
            'id uuid PRIMARY KEY, ' .
            'name varchar, ' .
            'age float' .
            ')'
        );
        $conn->disconnect();
    }

    private static function uuidV4(): string {
        $data = random_bytes(16);
        $data[6] = chr((ord($data[6]) & 0x0f) | 0x40);
        $data[8] = chr((ord($data[8]) & 0x3f) | 0x80);

        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
    }
}
