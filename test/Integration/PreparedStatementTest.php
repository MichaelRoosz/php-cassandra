<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Consistency;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Value;

final class PreparedStatementTest extends AbstractIntegrationTest {
    public function testPrepareAndExecuteWithNamedBinds(): void {
        $conn = $this->connection;

        $conn->query('TRUNCATE users');

        $prepared = $conn->prepare('INSERT INTO users (id, org_id, name, age) VALUES (:id, :org_id, :name, :age)');
        $conn->execute(
            $prepared,
            [
                'id' => Value\Uuid::fromValue(self::uuidV4()),
                'org_id' => 7,
                'name' => 'alice',
                'age' => 28,
            ],
            Consistency::ONE,
            new ExecuteOptions(namesForValues: true)
        );

        $sel = $conn->prepare('SELECT count(*) FROM users WHERE org_id = :org_id');
        $rows = $conn->execute($sel, ['org_id' => 7], Consistency::ONE, new ExecuteOptions(namesForValues: true))->asRowsResult();
        $countValue = $rows->fetchColumn(0);
        $count = is_int($countValue) ? $countValue : 0;
        $this->assertSame(1, $count);
    }

    public function testPreparedPagingUsingPreviousResult(): void {
        $conn = $this->connection;

        // Seed deterministic number of rows for a unique org_id
        $orgId = random_int(10000, 99999);
        $ins = $conn->prepare('INSERT INTO users (id, org_id, name, age) VALUES (:id, :org_id, :name, :age)');
        $numRows = 60;
        for ($i = 0; $i < $numRows; $i++) {
            $conn->execute(
                $ins,
                [
                    'id' => Value\Uuid::fromValue(self::uuidV4()),
                    'org_id' => $orgId,
                    'name' => 'u' . $i,
                    'age' => 20 + ($i % 10),
                ],
                Consistency::ONE,
                new ExecuteOptions(namesForValues: true)
            );
        }

        // Prepared select with paging, assert exact count
        $prepared = $conn->prepare('SELECT id, name FROM users WHERE org_id = :org_id');
        $rows = $conn->execute(
            $prepared,
            ['org_id' => $orgId],
            Consistency::ONE,
            new ExecuteOptions(pageSize: 25, namesForValues: true)
        )->asRowsResult();

        $seen = 0;
        do {
            foreach ($rows as $row) {
                $this->assertIsArray($row);
                $this->assertArrayHasKey('name', $row);
                $this->assertIsString($row['name']);
                $this->assertMatchesRegularExpression('/^u\\d+$/', $row['name']);
                $seen++;
            }
            $state = $rows->getRowsMetadata()->pagingState;
            if ($state === null) {
                break;
            }
            $rows = $conn->execute(
                $rows,
                ['org_id' => $orgId],
                Consistency::ONE,
                new ExecuteOptions(pageSize: 25, namesForValues: true, pagingState: $state)
            )->asRowsResult();
        } while (true);

        $this->assertSame($numRows, $seen);
    }

    protected static function setupTable(): void {
        $conn = self::newConnection(self::$keyspace);
        $conn->query('CREATE TABLE IF NOT EXISTS users(org_id int, id uuid, name varchar, age int, PRIMARY KEY ((org_id), id))');
        $conn->disconnect();
    }

    private static function uuidV4(): string {
        $data = random_bytes(16);
        $data[6] = chr((ord($data[6]) & 0x0f) | 0x40);
        $data[8] = chr((ord($data[8]) & 0x3f) | 0x80);

        return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
    }
}
