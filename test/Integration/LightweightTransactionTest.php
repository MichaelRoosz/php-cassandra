<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Consistency;
use Cassandra\Request\BatchType;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Response\Result\RowsResult;
use Cassandra\SerialConsistency;

/**
 * Integration tests for lightweight transactions (compare-and-set).
 *
 * See https://docs.datastax.com/en/cql-oss/3.3/cql/cql_using/useInsertLWT.html
 *
 * A conditional statement always returns a rows result whose first column is
 * `[applied]`. When the condition fails, the current values of the compared
 * columns are returned alongside it.
 */
final class LightweightTransactionTest extends AbstractIntegrationTestCase {
    public function testConditionalDeleteIfExists(): void {
        $id = 40;
        $table = "{$this->keyspace}.lwt_cyclists";

        $missing = $this->connection->query("DELETE FROM {$table} WHERE id = ? IF EXISTS", [$id])
            ->asRowsResult()->fetch();

        $this->assertIsArray($missing);
        $this->assertFalse($missing['[applied]'], 'Deleting a missing row must not apply');

        $this->connection->query(
            "INSERT INTO {$table} (id, lastname, firstname) VALUES (?, ?, ?)",
            [$id, 'KNETEMANN', 'Roxane']
        );

        $deleted = $this->connection->query("DELETE FROM {$table} WHERE id = ? IF EXISTS", [$id])
            ->asRowsResult()->fetch();

        $this->assertIsArray($deleted);
        $this->assertTrue($deleted['[applied]']);
        $this->assertNull($this->fetchRow($table, $id));
    }

    public function testConditionalUpdateInBatch(): void {
        $id = 50;
        $table = "{$this->keyspace}.lwt_cyclists";

        $batch = $this->connection->createBatchRequest(BatchType::LOGGED, Consistency::ONE);
        $batch->appendQuery(
            "INSERT INTO {$table} (id, lastname, firstname) VALUES (?, ?, ?) IF NOT EXISTS",
            [$id, 'VOS', 'Marianne']
        );

        $applied = $this->connection->batch($batch)->asRowsResult()->fetch();
        $this->assertIsArray($applied);
        $this->assertTrue($applied['[applied]'], 'Conditional batch should apply on a fresh key');

        $batch = $this->connection->createBatchRequest(BatchType::LOGGED, Consistency::ONE);
        $batch->appendQuery(
            "INSERT INTO {$table} (id, lastname, firstname) VALUES (?, ?, ?) IF NOT EXISTS",
            [$id, 'OTHER', 'Someone']
        );

        $rejected = $this->connection->batch($batch)->asRowsResult()->fetch();
        $this->assertIsArray($rejected);
        $this->assertFalse($rejected['[applied]'], 'Conditional batch must not overwrite an existing key');
        $this->assertSame('VOS', $rejected['lastname']);
    }

    public function testConditionalUpdateWithInOperator(): void {
        $id = 31;
        $table = "{$this->keyspace}.lwt_cyclists";

        $this->connection->query(
            "INSERT INTO {$table} (id, lastname, firstname) VALUES (?, ?, ?)",
            [$id, 'VOS', 'Marianne']
        );

        $applied = $this->connection->query(
            "UPDATE {$table} SET firstname = ? WHERE id = ? IF lastname IN ?",
            ['Mariska', $id, ['VOS', 'BRAND']]
        )->asRowsResult()->fetch();

        $this->assertIsArray($applied);
        $this->assertTrue($applied['[applied]'], 'IN is a supported LWT condition operator');
        $this->assertSame('Mariska', $this->fetchColumn($table, 'firstname', $id));
    }

    public function testConditionalUpdateWithMultipleConditions(): void {
        $id = 30;
        $table = "{$this->keyspace}.lwt_cyclists";

        $this->connection->query(
            "INSERT INTO {$table} (id, lastname, firstname, age) VALUES (?, ?, ?, ?)",
            [$id, 'KNETEMANN', 'Roxxane', 30]
        );

        $rejected = $this->connection->query(
            "UPDATE {$table} SET firstname = ? WHERE id = ? IF lastname = ? AND age > ?",
            ['Roxane', $id, 'KNETEMANN', 40]
        )->asRowsResult()->fetch();

        $this->assertIsArray($rejected);
        $this->assertFalse($rejected['[applied]'], 'All IF conditions must hold for the update to apply');

        $applied = $this->connection->query(
            "UPDATE {$table} SET firstname = ? WHERE id = ? IF lastname = ? AND age > ?",
            ['Roxane', $id, 'KNETEMANN', 20]
        )->asRowsResult()->fetch();

        $this->assertIsArray($applied);
        $this->assertTrue($applied['[applied]']);
        $this->assertSame('Roxane', $this->fetchColumn($table, 'firstname', $id));
    }

    public function testConditionalUpdateWithSerialConsistency(): void {
        $table = "{$this->keyspace}.lwt_cyclists";

        foreach ([SerialConsistency::SERIAL, SerialConsistency::LOCAL_SERIAL] as $index => $serial) {
            $id = 60 + $index;

            $this->connection->query(
                "INSERT INTO {$table} (id, lastname, firstname) VALUES (?, ?, ?)",
                [$id, 'VOS', 'Marianne']
            );

            $result = $this->connection->query(
                "UPDATE {$table} SET firstname = ? WHERE id = ? IF lastname = ?",
                ['Mariska', $id, 'VOS'],
                Consistency::ONE,
                new QueryOptions(serialConsistency: $serial)
            )->asRowsResult()->fetch();

            $this->assertIsArray($result);
            $this->assertTrue(
                $result['[applied]'],
                "LWT with serial consistency {$serial->name} should apply"
            );
            $this->assertSame('Mariska', $this->fetchColumn($table, 'firstname', $id));
        }
    }

    public function testConditionalUpdateWithUpdateIfExists(): void {
        $table = "{$this->keyspace}.lwt_cyclists";

        $missing = $this->connection->query(
            "UPDATE {$table} SET firstname = ? WHERE id = ? IF EXISTS",
            ['Nobody', 41]
        )->asRowsResult()->fetch();

        $this->assertIsArray($missing);
        $this->assertFalse($missing['[applied]'], 'UPDATE ... IF EXISTS must not create a row');
        $this->assertNull($this->fetchRow($table, 41));
    }

    public function testInsertIfNotExists(): void {
        $id = 10;
        $table = "{$this->keyspace}.lwt_cyclists";

        $result = $this->connection->query(
            "INSERT INTO {$table} (id, lastname, firstname) VALUES (?, ?, ?) IF NOT EXISTS",
            [$id, 'KNETEMANN', 'Roxxane']
        );

        $this->assertInstanceOf(RowsResult::class, $result->asRowsResult());

        $row = $result->asRowsResult()->fetch();
        $this->assertIsArray($row);
        $this->assertTrue($row['[applied]'], 'A first insert with IF NOT EXISTS must apply');

        // `[applied]` is always the first column of a conditional result. Cassandra
        // returns it alone when the statement applied, ScyllaDB also echoes the row.
        $this->assertSame('[applied]', array_key_first($row));
    }

    public function testInsertIfNotExistsReturnsExistingRowWhenNotApplied(): void {
        $id = 11;
        $table = "{$this->keyspace}.lwt_cyclists";

        $this->connection->query(
            "INSERT INTO {$table} (id, lastname, firstname) VALUES (?, ?, ?) IF NOT EXISTS",
            [$id, 'KNETEMANN', 'Roxxane']
        );

        $row = $this->connection->query(
            "INSERT INTO {$table} (id, lastname, firstname) VALUES (?, ?, ?) IF NOT EXISTS",
            [$id, 'OVERWRITE', 'Nope']
        )->asRowsResult()->fetch();

        $this->assertIsArray($row);
        $this->assertFalse($row['[applied]'], 'A second insert with IF NOT EXISTS must not apply');
        $this->assertSame('KNETEMANN', $row['lastname'], 'The current row is returned when not applied');
        $this->assertSame('Roxxane', $row['firstname']);

        $this->assertSame(
            'KNETEMANN',
            $this->fetchColumn($table, 'lastname', $id),
            'The stored row must be untouched'
        );
    }

    public function testSerialReadSeesLatestCommittedValue(): void {
        $id = 70;
        $table = "{$this->keyspace}.lwt_cyclists";

        $this->connection->query(
            "INSERT INTO {$table} (id, lastname, firstname) VALUES (?, ?, ?) IF NOT EXISTS",
            [$id, 'VOS', 'Marianne']
        );

        $row = $this->connection->query(
            "SELECT firstname FROM {$table} WHERE id = ?",
            [$id],
            Consistency::SERIAL
        )->asRowsResult()->fetch();

        $this->assertIsArray($row);
        $this->assertSame('Marianne', $row['firstname'], 'A SERIAL read must see the committed LWT write');
    }

    protected static function setupTable(): void {
        $conn = self::newConnection(self::$defaultKeyspace);
        $conn->query(
            'CREATE TABLE IF NOT EXISTS lwt_cyclists ('
            . 'id int PRIMARY KEY, lastname varchar, firstname varchar, age int)'
        );
        $conn->disconnect();
    }

    private function fetchColumn(string $table, string $column, int $id): mixed {
        $row = $this->fetchRow($table, $id);
        $this->assertIsArray($row);

        return $row[$column];
    }

    /**
     * @return array<int|string, mixed>|null
     */
    private function fetchRow(string $table, int $id): ?array {
        $row = $this->connection->query("SELECT * FROM {$table} WHERE id = ?", [$id])
            ->asRowsResult()->fetch();

        return is_array($row) ? $row : null;
    }
}
