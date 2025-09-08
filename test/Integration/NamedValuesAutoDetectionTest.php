<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Consistency;
use Cassandra\Value;

final class NamedValuesAutoDetectionTest extends AbstractIntegrationTestCase {
    public function testExecuteAutoDetectionWithNamedMarkersAndAssociativeValues(): void {

        $conn = $this->connection;
        $conn->query('TRUNCATE auto_users');

        // Prepare an INSERT with named markers
        $ins = $conn->prepare(
            "INSERT INTO {$this->keyspace}.auto_users (id, org_id, name, age) VALUES (:id, :org_id, :name, :age)"
        );

        $orgId = random_int(10000, 99999);

        // Execute with associative values; do NOT set namesForValues explicitly
        $conn->execute(
            $ins,
            [
                'id' => Value\Uuid::fromValue(self::uuidV4()),
                'org_id' => $orgId,
                'name' => 'bob',
                'age' => 31,
            ],
            Consistency::ONE
        );

        // Prepare a SELECT with named marker and execute with associative values; auto-detection should work
        $sel = $conn->prepare(
            "SELECT count(*) FROM {$this->keyspace}.auto_users WHERE org_id = :org_id"
        );

        $rows = $conn->execute($sel, ['org_id' => $orgId], Consistency::ONE)->asRowsResult();
        $countValue = $rows->fetchColumn(0);
        $count = is_int($countValue) ? $countValue : 0;
        $this->assertSame(1, $count);
    }

    public function testQueryAutoDetectionWithNamedMarkersAndAssociativeValues(): void {

        $conn = $this->connection;
        $conn->query('TRUNCATE auto_users');

        // INSERT with named markers and associative values; do NOT set namesForValues explicitly
        $conn->query(
            "INSERT INTO {$this->keyspace}.auto_users (id, org_id, name, age) VALUES (:id, :org_id, :name, :age)",
            [
                'id' => Value\Uuid::fromValue(self::uuidV4()),
                'org_id' => 7,
                'name' => 'alice',
                'age' => 28,
            ],
            Consistency::ONE
        );

        // SELECT with named marker and associative value; auto-detection should kick in again
        $rows = $conn->query(
            "SELECT count(*) FROM {$this->keyspace}.auto_users WHERE org_id = :org_id",
            ['org_id' => 7],
            Consistency::ONE
        )->asRowsResult();

        $countValue = $rows->fetchColumn(0);
        $count = is_int($countValue) ? $countValue : 0;
        $this->assertSame(1, $count);
    }

    protected static function setupTable(): void {
        $conn = self::newConnection(self::$defaultKeyspace);
        $conn->query('CREATE TABLE IF NOT EXISTS auto_users(org_id int, id uuid, name varchar, age int, PRIMARY KEY ((org_id), id))');
        $conn->disconnect();
    }

    private static function uuidV4(): string {
        $data = random_bytes(16);
        $data[6] = chr((ord($data[6]) & 0x0f) | 0x40);
        $data[8] = chr((ord($data[8]) & 0x3f) | 0x80);

        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
    }
}
