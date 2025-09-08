<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Consistency;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Value;

final class ReprepareFaultInjectionTest extends AbstractIntegrationTestCase {
    public function testReprepareAfterDisconnectAsync(): void {

        $conn = $this->connection;

        // Prepare, then disconnect to force UNPREPARED on next execute
        $ins = $conn->prepare("INSERT INTO {$this->keyspace}.reprepare_users (org_id, id, name) VALUES (:org_id, :id, :name)");
        $conn->disconnect();

        $stmt = $conn->executeAsync(
            $ins,
            [
                'org_id' => 9002,
                'id' => Value\Uuid::fromValue(self::uuidV4()),
                'name' => 'trinity',
            ],
            Consistency::ONE,
            new ExecuteOptions(namesForValues: true)
        );

        // Will block until reprepare+reexecute finishes
        $stmt->waitForResponse();

        $sel = $conn->prepare("SELECT count(*) FROM {$this->keyspace}.reprepare_users WHERE org_id = :org_id");
        $rows = $conn->execute($sel, ['org_id' => 9002], Consistency::ONE, new ExecuteOptions(namesForValues: true))->asRowsResult();
        $countValue = $rows->fetchColumn(0);
        $count = is_int($countValue) ? $countValue : 0;
        $this->assertSame(1, $count);
    }

    public function testReprepareAfterDisconnectSync(): void {

        $conn = $this->connection;

        $conn->query('TRUNCATE reprepare_users');

        $ins = $conn->prepare("INSERT INTO {$this->keyspace}.reprepare_users (org_id, id, name) VALUES (:org_id, :id, :name)");

        // Simulate node failure by disconnecting before first execute
        $conn->disconnect();

        // Should auto-reconnect, hit UNPREPARED, transparently reprepare and succeed
        $conn->execute(
            $ins,
            [
                'org_id' => 9001,
                'id' => Value\Uuid::fromValue(self::uuidV4()),
                'name' => 'neo',
            ],
            Consistency::ONE,
            new ExecuteOptions(namesForValues: true)
        );

        $sel = $conn->prepare("SELECT count(*) FROM {$this->keyspace}.reprepare_users WHERE org_id = :org_id");
        $rows = $conn->execute($sel, ['org_id' => 9001], Consistency::ONE, new ExecuteOptions(namesForValues: true))->asRowsResult();
        $countValue = $rows->fetchColumn(0);
        $count = is_int($countValue) ? $countValue : 0;
        $this->assertSame(1, $count);
    }

    protected static function setupTable(): void {
        $conn = self::newConnection(self::$defaultKeyspace);
        $conn->query('CREATE TABLE IF NOT EXISTS reprepare_users(org_id int, id uuid, name varchar, PRIMARY KEY ((org_id), id))');
        $conn->disconnect();
    }

    private static function uuidV4(): string {
        $data = random_bytes(16);
        $data[6] = chr((ord($data[6]) & 0x0f) | 0x40);
        $data[8] = chr((ord($data[8]) & 0x3f) | 0x80);

        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
    }
}
