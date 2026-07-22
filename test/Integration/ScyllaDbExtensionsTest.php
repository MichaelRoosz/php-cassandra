<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Exception\ServerException\SyntaxErrorException;

/**
 * Integration tests for ScyllaDB-only CQL extensions.
 *
 * `BYPASS CACHE` and `USING TIMEOUT` are ScyllaDB additions to CQL; Apache
 * Cassandra rejects both as syntax errors. Each extension is therefore covered
 * twice: once asserting it works on ScyllaDB, once asserting Apache Cassandra
 * rejects it, so the divergence is pinned down on whichever backend the suite
 * runs against.
 *
 * See https://docs.scylladb.com/stable/cql/dml.html#bypass-cache and
 * https://docs.scylladb.com/stable/cql/time-to-live.html#using-timeout
 */
final class ScyllaDbExtensionsTest extends AbstractIntegrationTestCase {
    public function testBypassCacheCombinedWithUsingTimeout(): void {
        if (!self::isScyllaDb()) {
            $this->markTestSkipped('BYPASS CACHE / USING TIMEOUT are ScyllaDB-only');
        }

        // The grammar fixes the order: BYPASS CACHE precedes USING TIMEOUT.
        $rows = $this->connection->query(
            "SELECT pk, ck FROM {$this->keyspace}.scylla_ext WHERE pk = ? BYPASS CACHE USING TIMEOUT 5s",
            [1]
        )->asRowsResult()->fetchAll();

        $this->assertCount(2, $rows);
    }

    public function testBypassCacheIsRejectedByApacheCassandra(): void {
        if (self::isScyllaDb()) {
            $this->markTestSkipped('BYPASS CACHE is only rejected by Apache Cassandra');
        }

        $this->expectException(SyntaxErrorException::class);
        $this->connection->query(
            "SELECT pk FROM {$this->keyspace}.scylla_ext WHERE pk = ? BYPASS CACHE",
            [1]
        );
    }

    public function testBypassCacheReturnsRows(): void {
        if (!self::isScyllaDb()) {
            $this->markTestSkipped('BYPASS CACHE is ScyllaDB-only');
        }

        $table = "{$this->keyspace}.scylla_ext";

        // Single partition, full scan and a filtered read all accept BYPASS CACHE.
        $partition = $this->connection->query(
            "SELECT pk, ck, v FROM {$table} WHERE pk = ? BYPASS CACHE",
            [1]
        )->asRowsResult()->fetchAll();
        $this->assertCount(2, $partition);

        $scan = $this->connection->query("SELECT pk FROM {$table} BYPASS CACHE")
            ->asRowsResult()->fetchAll();
        $this->assertGreaterThanOrEqual(3, count($scan));

        $filtered = $this->connection->query(
            "SELECT pk FROM {$table} WHERE n > ? ALLOW FILTERING BYPASS CACHE",
            [5]
        )->asRowsResult()->fetchAll();
        $this->assertNotEmpty($filtered);
    }

    public function testUsingTimeoutIsRejectedByApacheCassandra(): void {
        if (self::isScyllaDb()) {
            $this->markTestSkipped('USING TIMEOUT is only rejected by Apache Cassandra');
        }

        $this->expectException(SyntaxErrorException::class);
        $this->connection->query(
            "SELECT pk FROM {$this->keyspace}.scylla_ext WHERE pk = ? USING TIMEOUT 5s",
            [1]
        );
    }

    public function testUsingTimeoutOnRead(): void {
        if (!self::isScyllaDb()) {
            $this->markTestSkipped('USING TIMEOUT is ScyllaDB-only');
        }

        $table = "{$this->keyspace}.scylla_ext";

        // Both second- and millisecond-granularity durations are accepted.
        $seconds = $this->connection->query(
            "SELECT pk, ck FROM {$table} WHERE pk = ? USING TIMEOUT 5s",
            [1]
        )->asRowsResult()->fetchAll();
        $this->assertCount(2, $seconds);

        $millis = $this->connection->query(
            "SELECT pk FROM {$table} WHERE pk = ? USING TIMEOUT 500ms",
            [1]
        )->asRowsResult()->fetchAll();
        $this->assertCount(2, $millis);
    }

    public function testUsingTimeoutOnWrites(): void {
        if (!self::isScyllaDb()) {
            $this->markTestSkipped('USING TIMEOUT is ScyllaDB-only');
        }

        $table = "{$this->keyspace}.scylla_ext";
        $id = 90;

        $this->connection->query(
            "INSERT INTO {$table} (pk, ck, v) VALUES (?, ?, ?) USING TIMEOUT 5s",
            [$id, 1, 'inserted']
        );
        $this->assertSame('inserted', $this->fetchValue($table, $id));

        $this->connection->query(
            "UPDATE {$table} USING TIMEOUT 5s SET v = ? WHERE pk = ? AND ck = ?",
            ['updated', $id, 1]
        );
        $this->assertSame('updated', $this->fetchValue($table, $id));

        $this->connection->query(
            "DELETE FROM {$table} USING TIMEOUT 5s WHERE pk = ? AND ck = ?",
            [$id, 1]
        );
        $this->assertNull($this->fetchValue($table, $id));
    }

    public function testUsingTimeoutWithTimestamp(): void {
        if (!self::isScyllaDb()) {
            $this->markTestSkipped('USING TIMEOUT is ScyllaDB-only');
        }

        $table = "{$this->keyspace}.scylla_ext";
        $timestamp = 1700000000000000;

        // USING TIMEOUT composes with the ordinary USING TIMESTAMP clause.
        $this->connection->query(
            "INSERT INTO {$table} (pk, ck, v) VALUES (?, ?, ?) USING TIMEOUT 5s AND TIMESTAMP ?",
            [91, 1, 'stamped', $timestamp]
        );

        $row = $this->connection->query(
            "SELECT WRITETIME(v) AS w FROM {$table} WHERE pk = ? AND ck = ?",
            [91, 1]
        )->asRowsResult()->fetch();

        $this->assertIsArray($row);
        $this->assertSame($timestamp, $row['w']);
    }

    protected static function setupTable(): void {
        $conn = self::newConnection(self::$defaultKeyspace);
        $conn->query(
            'CREATE TABLE IF NOT EXISTS scylla_ext (pk int, ck int, v text, n int, PRIMARY KEY (pk, ck))'
        );
        $conn->query("INSERT INTO scylla_ext (pk, ck, v, n) VALUES (1, 1, 'alpha', 10)");
        $conn->query("INSERT INTO scylla_ext (pk, ck, v, n) VALUES (1, 2, 'beta', 20)");
        $conn->query("INSERT INTO scylla_ext (pk, ck, v, n) VALUES (2, 1, 'gamma', 30)");
        $conn->disconnect();
    }

    private function fetchValue(string $table, int $pk): mixed {
        $row = $this->connection->query(
            "SELECT v FROM {$table} WHERE pk = ? AND ck = ?",
            [$pk, 1]
        )->asRowsResult()->fetch();

        return is_array($row) ? $row['v'] : null;
    }
}
