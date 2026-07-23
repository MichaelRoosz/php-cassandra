<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Consistency;
use Cassandra\Request\BatchType;
use Cassandra\Request\Options\BatchOptions;
use Cassandra\Request\Query;
use Cassandra\Request\Request;
use Cassandra\Response\Response;
use Cassandra\Response\Result\RowsResult;
use Cassandra\Response\Result\SchemaChangeResult;
use Cassandra\Value;

/**
 * Integration tests for the remaining CQL surface that has no home in a more
 * specific test class: TTL and write timestamps, schema evolution, the query
 * language built-ins, tracing, custom payloads and virtual tables.
 */
final class CqlFeaturesTest extends AbstractIntegrationTestCase {
    /**
     * A GROUP BY spanning every partition makes the server warn about an
     * aggregation without a partition key. That is expected for this test, so
     * let it through while still failing on anything else.
     */
    #[\Override]
    public function onWarnings(array $warnings, Request $request, Response $response): void {
        $unexpected = array_filter(
            $warnings,
            static fn(string $warning): bool => !str_contains($warning, 'Aggregation query used without partition key')
        );

        if ($unexpected !== []) {
            parent::onWarnings($unexpected, $request, $response);
        }
    }

    public function testAggregatesAndDistinct(): void {
        $table = "{$this->keyspace}.cql_numbers";

        $row = $this->connection->query(
            'SELECT COUNT(*) AS c, MIN(n) AS mn, MAX(n) AS mx, AVG(n) AS av, SUM(n) AS sm '
            . "FROM {$table} WHERE pk = ?",
            [1]
        )->asRowsResult()->fetch();

        $this->assertIsArray($row);
        $this->assertSame(3, $row['c']);
        $this->assertSame(10, $row['mn']);
        $this->assertSame(30, $row['mx']);
        $this->assertSame(20, $row['av']);
        $this->assertSame(60, $row['sm']);

        $distinct = $this->connection->query("SELECT DISTINCT pk FROM {$table}")
            ->asRowsResult()->fetchAll();
        $pks = array_column($distinct, 'pk');
        sort($pks);
        $this->assertSame([1, 2], $pks);
    }

    public function testAlterTableAddDropAndRename(): void {
        $table = "{$this->keyspace}.cql_alter";

        $this->assertInstanceOf(
            SchemaChangeResult::class,
            $this->connection->query("ALTER TABLE {$table} ADD added int")
        );

        $this->connection->query("INSERT INTO {$table} (pk, ck, added) VALUES (?, ?, ?)", [1, 1, 7]);
        $row = $this->connection->query("SELECT added FROM {$table} WHERE pk = ? AND ck = ?", [1, 1])
            ->asRowsResult()->fetch();
        $this->assertIsArray($row);
        $this->assertSame(7, $row['added']);

        $this->assertInstanceOf(
            SchemaChangeResult::class,
            $this->connection->query("ALTER TABLE {$table} DROP added")
        );

        // Only primary key columns can be renamed.
        $this->assertInstanceOf(
            SchemaChangeResult::class,
            $this->connection->query("ALTER TABLE {$table} RENAME ck TO ck_renamed")
        );
    }

    public function testAlterTypeAddAndRename(): void {
        $this->assertInstanceOf(
            SchemaChangeResult::class,
            $this->connection->query("ALTER TYPE {$this->keyspace}.cql_udt ADD zip int")
        );
        $this->assertInstanceOf(
            SchemaChangeResult::class,
            $this->connection->query("ALTER TYPE {$this->keyspace}.cql_udt RENAME zip TO postcode")
        );
    }

    public function testBatchWithDefaultTimestamp(): void {
        $table = "{$this->keyspace}.cql_writetime";
        $timestamp = 1700000000000000;

        $batch = $this->connection->createBatchRequest(
            BatchType::LOGGED,
            Consistency::ONE,
            new BatchOptions(defaultTimestamp: $timestamp)
        );
        $batch->appendQuery("INSERT INTO {$table} (id, v) VALUES (?, ?)", [30, 'batched']);
        $this->connection->batch($batch);

        $row = $this->connection->query("SELECT WRITETIME(v) AS w FROM {$table} WHERE id = ?", [30])
            ->asRowsResult()->fetch();

        $this->assertIsArray($row);
        $this->assertSame($timestamp, $row['w'], 'The batch default timestamp should be used for the write');
    }

    public function testBuiltInScalarFunctions(): void {
        $table = "{$this->keyspace}.cql_numbers";

        // CAST() was only added in Cassandra 3.2, so it is only exercised where
        // it is supported.
        $castSelector = self::isCastFunctionSupported() ? 'CAST(n AS text) AS as_text, ' : '';

        // ScyllaDB does not support arithmetic expressions (e.g. "n + 5") in
        // the selection clause, and Cassandra only added them in 4.0, so that
        // part is only exercised where it is supported.
        $arithmeticSelector = self::isArithmeticInSelectClauseSupported() ? 'n + 5 AS plus, ' : '';

        $row = $this->connection->query(
            "SELECT {$castSelector}{$arithmeticSelector}blobAsInt(intAsBlob(n)) AS roundtrip "
            . "FROM {$table} WHERE pk = ? AND ck = ?",
            [1, 1]
        )->asRowsResult()->fetch();

        $this->assertIsArray($row);
        if (self::isCastFunctionSupported()) {
            $this->assertSame('10', $row['as_text'], 'CAST converts an int column to text');
        }
        if (self::isArithmeticInSelectClauseSupported()) {
            $this->assertSame(15, $row['plus'], 'Arithmetic is evaluated server side');
        }
        $this->assertSame(10, $row['roundtrip'], 'intAsBlob/blobAsInt round-trip the value');

        $generated = $this->connection->query(
            "SELECT uuid() AS u, now() AS t FROM {$table} WHERE pk = ? AND ck = ?",
            [1, 1]
        )->asRowsResult()->fetch();

        $this->assertIsArray($generated);
        $this->assertIsString($generated['u']);
        $this->assertIsString($generated['t']);
    }

    public function testCounterBatch(): void {
        $table = "{$this->keyspace}.cql_counters";

        $batch = $this->connection->createBatchRequest(BatchType::COUNTER, Consistency::ONE);
        $batch->appendQuery(
            "UPDATE {$table} SET c = c + ? WHERE id = ?",
            [Value\Counter::fromValue(5), 1]
        );
        $batch->appendQuery(
            "UPDATE {$table} SET c = c + ? WHERE id = ?",
            [Value\Counter::fromValue(7), 1]
        );
        $this->connection->batch($batch);

        $row = $this->connection->query("SELECT c FROM {$table} WHERE id = ?", [1])
            ->asRowsResult()->fetch();

        $this->assertIsArray($row);
        $this->assertSame(12, $row['c'], 'Counter increments in a batch should accumulate');
    }

    public function testCustomPayloadIsAccepted(): void {
        if (!self::isCustomPayloadSupported()) {
            $this->markTestSkipped('ScyllaDB does not honor the custom payload frame flag');
        }

        $request = new Query("SELECT * FROM {$this->keyspace}.cql_numbers WHERE pk = 1 AND ck = 1");
        $request->setPayload(['my-key' => 'my-value']);

        $response = $this->connection->syncRequest($request);

        // Stock Cassandra does not echo a payload back - that needs a custom
        // QueryHandler - so this asserts the request side is accepted.
        $this->assertInstanceOf(RowsResult::class, $response);
        $this->assertSame(1, $response->getRowCount());
    }

    public function testDescribeOverCql(): void {
        if (!self::isDescribeOverCqlSupported()) {
            $this->markTestSkipped('DESCRIBE over CQL requires Cassandra 4.0+ (ScyllaDB returns a different shape)');
        }

        $rows = $this->connection->query("DESCRIBE TABLE {$this->keyspace}.cql_numbers")
            ->asRowsResult()->fetchAll();

        $this->assertNotEmpty($rows);
        $this->assertSame('cql_numbers', $rows[0]['name']);
        $this->assertIsString($rows[0]['create_statement']);
        $this->assertStringContainsString('CREATE TABLE', $rows[0]['create_statement']);
    }

    public function testGroupByAndPerPartitionLimit(): void {
        if (!self::isGroupBySupported()) {
            $this->markTestSkipped('GROUP BY / PER PARTITION LIMIT require Cassandra 3.10+');
        }

        $table = "{$this->keyspace}.cql_numbers";

        $grouped = $this->connection->query("SELECT pk, COUNT(*) AS c FROM {$table} GROUP BY pk")
            ->asRowsResult()->fetchAll();

        $counts = [];
        foreach ($grouped as $row) {
            $pk = $row['pk'];
            $this->assertIsInt($pk);
            $counts[$pk] = $row['c'];
        }
        ksort($counts);
        $this->assertSame([1 => 3, 2 => 1], $counts);

        $limited = $this->connection->query("SELECT pk, ck FROM {$table} PER PARTITION LIMIT 1")
            ->asRowsResult()->fetchAll();

        $this->assertCount(2, $limited, 'One row per partition');
    }

    public function testInPredicateAndTokenFunction(): void {
        $table = "{$this->keyspace}.cql_numbers";

        $rows = $this->connection->query(
            "SELECT ck FROM {$table} WHERE pk = ? AND ck IN ?",
            [1, [1, 3]]
        )->asRowsResult()->fetchAll();

        $cks = array_column($rows, 'ck');
        sort($cks);
        $this->assertSame([1, 3], $cks);

        $tokens = $this->connection->query("SELECT pk, token(pk) AS tk FROM {$table} LIMIT 1")
            ->asRowsResult()->fetch();

        $this->assertIsArray($tokens);
        $this->assertIsInt($tokens['tk'], 'token() returns a bigint');
    }

    public function testStaticColumnIsSharedAcrossPartition(): void {
        $table = "{$this->keyspace}.cql_static";

        $this->connection->query("INSERT INTO {$table} (pk, ck, s, v) VALUES (?, ?, ?, ?)", [1, 1, 99, 'a']);
        $this->connection->query("INSERT INTO {$table} (pk, ck, v) VALUES (?, ?, ?)", [1, 2, 'b']);

        $rows = $this->connection->query("SELECT ck, s, v FROM {$table} WHERE pk = ?", [1])
            ->asRowsResult()->fetchAll();

        $this->assertCount(2, $rows);
        foreach ($rows as $row) {
            $this->assertSame(99, $row['s'], 'A static column is shared by every row of the partition');
        }

        // Updating it once changes it for the whole partition.
        $this->connection->query("UPDATE {$table} SET s = ? WHERE pk = ?", [100, 1]);

        $rows = $this->connection->query("SELECT s FROM {$table} WHERE pk = ?", [1])
            ->asRowsResult()->fetchAll();
        foreach ($rows as $row) {
            $this->assertSame(100, $row['s']);
        }
    }

    public function testTracingProducesASession(): void {
        $request = new Query("SELECT * FROM {$this->keyspace}.cql_numbers WHERE pk = 1 AND ck = 1");
        $request->enableTracing();

        $response = $this->connection->syncRequest($request);
        $tracingUuid = $response->getTracingUuid();

        $this->assertIsString($tracingUuid, 'A traced request must return a tracing session id');

        $traces = self::newConnection('system_traces');

        try {
            $session = null;

            for ($attempt = 0; $attempt < 50; $attempt++) {
                $row = $traces->query(
                    'SELECT session_id, command FROM system_traces.sessions WHERE session_id = ?',
                    [Value\Uuid::fromValue($tracingUuid)]
                )->asRowsResult()->fetch();

                if (is_array($row)) {
                    $session = $row;

                    break;
                }

                usleep(100000);
            }

            $this->assertIsArray($session, 'The trace session should be written to system_traces');
            $this->assertSame('QUERY', $session['command']);
        } finally {
            $traces->disconnect();
        }
    }

    public function testTtlAndWriteTime(): void {
        $table = "{$this->keyspace}.cql_writetime";
        $timestamp = 1600000000000000;

        $this->connection->query(
            "INSERT INTO {$table} (id, v) VALUES (?, ?) USING TTL ? AND TIMESTAMP ?",
            [10, 'expires', 3600, $timestamp]
        );

        $row = $this->connection->query(
            "SELECT TTL(v) AS ttl, WRITETIME(v) AS w FROM {$table} WHERE id = ?",
            [10]
        )->asRowsResult()->fetch();

        $this->assertIsArray($row);
        $this->assertIsInt($row['ttl']);
        $this->assertGreaterThan(0, $row['ttl']);
        $this->assertLessThanOrEqual(3600, $row['ttl']);
        $this->assertSame($timestamp, $row['w'], 'USING TIMESTAMP should set the write time');

        // A row written without a TTL reports null.
        $this->connection->query("INSERT INTO {$table} (id, v) VALUES (?, ?)", [11, 'permanent']);
        $noTtl = $this->connection->query("SELECT TTL(v) AS ttl FROM {$table} WHERE id = ?", [11])
            ->asRowsResult()->fetch();

        $this->assertIsArray($noTtl);
        $this->assertNull($noTtl['ttl']);
    }

    public function testVirtualTables(): void {
        if (!self::isVirtualTablesSupported()) {
            $this->markTestSkipped('Virtual tables (system_views) require Cassandra 4.1+ (ScyllaDB exposes a different set)');
        }

        $views = self::newConnection('system_views');

        try {
            $keyspaces = $views->query(
                'SELECT keyspace_name FROM system_virtual_schema.keyspaces'
            )->asRowsResult()->fetchAll();

            $names = array_column($keyspaces, 'keyspace_name');
            $this->assertContains('system_views', $names);

            $setting = $views->query(
                'SELECT name, value FROM system_views.settings WHERE name = ?',
                ['materialized_views_enabled']
            )->asRowsResult()->fetch();

            $this->assertIsArray($setting);
            $this->assertSame('materialized_views_enabled', $setting['name']);
        } finally {
            $views->disconnect();
        }
    }

    protected static function setupTable(): void {
        $conn = self::newConnection(self::$defaultKeyspace);

        $conn->query('CREATE TABLE IF NOT EXISTS cql_numbers (pk int, ck int, n int, PRIMARY KEY (pk, ck))');
        foreach ([[1, 1, 10], [1, 2, 20], [1, 3, 30], [2, 1, 40]] as [$pk, $ck, $n]) {
            $conn->query('INSERT INTO cql_numbers (pk, ck, n) VALUES (?, ?, ?)', [$pk, $ck, $n]);
        }

        $conn->query('CREATE TABLE IF NOT EXISTS cql_writetime (id int PRIMARY KEY, v varchar)');
        $conn->query('CREATE TABLE IF NOT EXISTS cql_counters (id int PRIMARY KEY, c counter)');
        $conn->query('CREATE TABLE IF NOT EXISTS cql_alter (pk int, ck int, PRIMARY KEY (pk, ck))');
        $conn->query(
            'CREATE TABLE IF NOT EXISTS cql_static (pk int, ck int, s int STATIC, v varchar, PRIMARY KEY (pk, ck))'
        );
        $conn->query('CREATE TYPE IF NOT EXISTS cql_udt (street varchar, city varchar)');

        $conn->disconnect();
    }
}
