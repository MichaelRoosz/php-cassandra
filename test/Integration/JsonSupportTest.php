<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

/**
 * Integration tests for CQL JSON support.
 *
 * See https://docs.datastax.com/en/cql-oss/3.3/cql/cql_using/useInsertJSON.html
 *
 * JSON is handled entirely server side: the driver only transports the JSON
 * document as a `varchar` bind value, so no dedicated API is required.
 */
final class JsonSupportTest extends AbstractIntegrationTestCase {
    public function testFromJsonAndToJsonFunctions(): void {
        $table = "{$this->keyspace}.json_category";

        $this->connection->query(
            "INSERT INTO {$table} (id, lastname, points, tags) VALUES (?, ?, fromJson(?), fromJson(?))",
            [7, 'FRAME', '640', '["k1","k2"]']
        );

        $row = $this->fetchRow($table, 7);
        $this->assertSame(640, $row['points'], 'fromJson() parses a scalar JSON document');
        $this->assertSame(['k1', 'k2'], $row['tags'], 'fromJson() parses a collection literal');

        $encoded = $this->connection->query(
            "SELECT toJson(tags) AS tags_json, toJson(points) AS points_json FROM {$table} WHERE id = ?",
            [7]
        )->asRowsResult()->fetch();

        $this->assertIsArray($encoded);
        $this->assertIsString($encoded['tags_json']);
        $this->assertIsString($encoded['points_json']);
        $this->assertSame(['k1', 'k2'], json_decode($encoded['tags_json'], true));
        $this->assertSame(640, json_decode($encoded['points_json'], true));
    }
    public function testInsertJsonWithBoundParameter(): void {
        $table = "{$this->keyspace}.json_category";

        $this->connection->query(
            "INSERT INTO {$table} JSON ?",
            ['{"id": 2, "lastname": "VOS", "category": "Sprint", "points": 700, "tags": ["b"]}']
        );

        $row = $this->fetchRow($table, 2);

        $this->assertSame('VOS', $row['lastname']);
        $this->assertSame('Sprint', $row['category']);
        $this->assertSame(700, $row['points']);
        $this->assertSame(['b'], $row['tags']);
    }

    public function testInsertJsonWithDefaultNullOverwritesOmittedColumns(): void {
        $table = "{$this->keyspace}.json_category";

        $this->connection->query(
            "INSERT INTO {$table} JSON ?",
            ['{"id": 4, "lastname": "SUTHERLAND", "category": "GC", "points": 780}']
        );

        $this->connection->query(
            "INSERT INTO {$table} JSON ? DEFAULT NULL",
            ['{"id": 4, "points": 900}']
        );

        $row = $this->fetchRow($table, 4);

        $this->assertSame(900, $row['points']);
        $this->assertNull($row['lastname'], 'DEFAULT NULL nulls out columns missing from the document');
        $this->assertNull($row['category']);
    }

    public function testInsertJsonWithDefaultUnsetKeepsOmittedColumns(): void {
        $table = "{$this->keyspace}.json_category";

        $this->connection->query(
            "INSERT INTO {$table} JSON ?",
            ['{"id": 3, "lastname": "SUTHERLAND", "category": "GC", "points": 780}']
        );

        $this->connection->query(
            "INSERT INTO {$table} JSON ? DEFAULT UNSET",
            ['{"id": 3, "points": 900}']
        );

        $row = $this->fetchRow($table, 3);

        $this->assertSame(900, $row['points']);
        $this->assertSame('SUTHERLAND', $row['lastname'], 'DEFAULT UNSET leaves omitted columns untouched');
        $this->assertSame('GC', $row['category']);
    }

    public function testInsertJsonWithLiteralDocument(): void {
        $table = "{$this->keyspace}.json_category";

        $this->connection->query(
            "INSERT INTO {$table} JSON '{"
            . '"id": 1, "lastname": "SUTHERLAND", "category": "GC", '
            . '"points": 780, "tags": ["a", "b"]}\''
        );

        $row = $this->fetchRow($table, 1);

        $this->assertSame('SUTHERLAND', $row['lastname']);
        $this->assertSame('GC', $row['category']);
        $this->assertSame(780, $row['points']);
        $this->assertSame(['a', 'b'], $row['tags']);
    }

    public function testInsertJsonWithOmittedColumnDefaultsToNull(): void {
        $table = "{$this->keyspace}.json_category";

        $this->connection->query(
            "INSERT INTO {$table} JSON ?",
            ['{"id": 5, "lastname": "BRAND"}']
        );

        $row = $this->fetchRow($table, 5);

        $this->assertSame('BRAND', $row['lastname']);
        $this->assertNull($row['category'], 'A column absent from the document is written as null');
        $this->assertNull($row['points']);
        $this->assertNull($row['tags']);
    }

    public function testSelectJsonReturnsSingleJsonColumn(): void {
        $table = "{$this->keyspace}.json_category";

        $this->connection->query(
            "INSERT INTO {$table} JSON ?",
            ['{"id": 6, "lastname": "VOS", "category": "GC", "points": 500, "tags": ["x"]}']
        );

        $row = $this->connection->query("SELECT JSON * FROM {$table} WHERE id = ?", [6])
            ->asRowsResult()->fetch();

        $this->assertIsArray($row);
        $this->assertSame(['[json]'], array_keys($row), 'SELECT JSON returns one column named [json]');
        $this->assertIsString($row['[json]']);

        /** @var array<string, mixed> $decoded */
        $decoded = json_decode($row['[json]'], true);

        $this->assertSame(6, $decoded['id']);
        $this->assertSame('VOS', $decoded['lastname']);
        $this->assertSame('GC', $decoded['category']);
        $this->assertSame(500, $decoded['points']);
        $this->assertSame(['x'], $decoded['tags']);
    }

    protected static function setupTable(): void {
        $conn = self::newConnection(self::$defaultKeyspace);
        $conn->query(
            'CREATE TABLE IF NOT EXISTS json_category ('
            . 'id int PRIMARY KEY, lastname varchar, category varchar, points int, tags set<varchar>)'
        );
        $conn->disconnect();
    }

    /**
     * @return array<int|string, mixed>
     */
    private function fetchRow(string $table, int $id): array {
        $row = $this->connection->query("SELECT * FROM {$table} WHERE id = ?", [$id])
            ->asRowsResult()->fetch();

        $this->assertIsArray($row);

        return $row;
    }
}
