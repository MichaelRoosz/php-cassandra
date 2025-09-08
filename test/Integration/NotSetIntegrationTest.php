<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Consistency;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Value;

final class NotSetIntegrationTest extends AbstractIntegrationTestCase {
    public function testUpdateUsingNotSetLeavesValueUnchangedNamed(): void {

        $conn = $this->connection;
        $conn->query('TRUNCATE kv');

        // Seed baseline row
        $ins = $conn->prepare("INSERT INTO {$this->keyspace}.kv (id, a, b) VALUES (:id, :a, :b)");
        $conn->execute(
            $ins,
            [
                'id' => 1,
                'a' => 3,
                'b' => 'xx',
            ],
            Consistency::ONE,
            new ExecuteOptions(namesForValues: true)
        );

        // Update with NOT SET for column a, should leave 'a' unchanged
        $upd = $conn->prepare("UPDATE {$this->keyspace}.kv SET a = :a, b = :b WHERE id = :id");
        $conn->execute(
            $upd,
            [
                'id' => 1,
                'a' => new Value\NotSet(),
                'b' => 'yy',
            ],
            Consistency::ONE,
            new ExecuteOptions(namesForValues: true)
        );

        $rows = $conn->query(
            "SELECT a, b FROM {$this->keyspace}.kv WHERE id = ?",
            [1]
        )->asRowsResult();

        $row = $rows->fetch();
        $this->assertIsArray($row);
        $this->assertSame(3, $row['a']);
        $this->assertSame('yy', $row['b']);
    }

    public function testUpdateUsingNotSetLeavesValueUnchangedPositional(): void {

        $conn = $this->connection;
        $conn->query('TRUNCATE kv');

        // Seed baseline row
        $ins = $conn->prepare("INSERT INTO {$this->keyspace}.kv (id, a, b) VALUES (?, ?, ?)");
        $conn->execute(
            $ins,
            [1, 1, 'x'],
            Consistency::ONE
        );

        // Update with NOT SET for column a, should leave 'a' unchanged
        $upd = $conn->prepare("UPDATE {$this->keyspace}.kv SET a = ?, b = ? WHERE id = ?");
        $conn->execute(
            $upd,
            [new Value\NotSet(), 'y', 1],
            Consistency::ONE
        );

        $rows = $conn->query(
            "SELECT a, b FROM {$this->keyspace}.kv WHERE id = ?",
            [1]
        )->asRowsResult();

        $row = $rows->fetch();
        $this->assertIsArray($row);
        $this->assertSame(1, $row['a']);
        $this->assertSame('y', $row['b']);
    }

    public function testUpdateUsingNullSetsColumnToNullPositional(): void {

        $conn = $this->connection;
        $conn->query('TRUNCATE kv');

        // Seed baseline row
        $ins = $conn->prepare("INSERT INTO {$this->keyspace}.kv (id, a, b) VALUES (?, ?, ?)");
        $conn->execute(
            $ins,
            [1, 2, 'x'],
            Consistency::ONE
        );

        // Update with NULL for column a, should set 'a' to null
        $upd = $conn->prepare("UPDATE {$this->keyspace}.kv SET a = ?, b = ? WHERE id = ?");
        $conn->execute(
            $upd,
            [null, 'z', 1],
            Consistency::ONE
        );

        $rows = $conn->query(
            "SELECT a, b FROM {$this->keyspace}.kv WHERE id = ?",
            [1]
        )->asRowsResult();

        $row = $rows->fetch();
        $this->assertIsArray($row);
        $this->assertNull($row['a']);
        $this->assertSame('z', $row['b']);
    }

    protected static function setupTable(): void {
        $conn = self::newConnection(self::$defaultKeyspace);
        $conn->query('CREATE TABLE IF NOT EXISTS kv (id int PRIMARY KEY, a int, b varchar)');
        $conn->disconnect();
    }
}
