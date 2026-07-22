<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Request\Request;
use Cassandra\Response\Response;
use Cassandra\Response\Result\SchemaChangeResult;

/**
 * Integration tests for materialized views.
 *
 * See https://docs.datastax.com/en/cql-oss/3.3/cql/cql_using/useCreateMV.html
 *
 * A view is maintained by the server from its base table; the driver simply
 * queries it like any other table. Cassandra needs materialized views enabled
 * in cassandra.yaml (see docker-compose.yml); ScyllaDB supports them by default.
 */
final class MaterializedViewTest extends AbstractIntegrationTestCase {
    /**
     * Cassandra warns on every CREATE MATERIALIZED VIEW that the feature is
     * experimental. That is expected here, so let it through while still
     * failing on anything else.
     */
    #[\Override]
    public function onWarnings(array $warnings, Request $request, Response $response): void {
        $unexpected = array_filter(
            $warnings,
            static fn(string $warning): bool => !str_contains($warning, 'Materialized views are experimental')
        );

        if ($unexpected !== []) {
            parent::onWarnings($unexpected, $request, $response);
        }
    }

    public function testCreateAndDropView(): void {
        $created = $this->connection->query(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS {$this->keyspace}.mv_temp AS "
            . "SELECT pk, ck, n FROM {$this->keyspace}.mv_scores "
            . 'WHERE pk IS NOT NULL AND ck IS NOT NULL AND n IS NOT NULL '
            . 'PRIMARY KEY (n, pk, ck)'
        );
        $this->assertInstanceOf(SchemaChangeResult::class, $created);

        $dropped = $this->connection->query(
            "DROP MATERIALIZED VIEW IF EXISTS {$this->keyspace}.mv_temp"
        );
        $this->assertInstanceOf(SchemaChangeResult::class, $dropped);
    }

    public function testViewIsQueryableByItsOwnPartitionKey(): void {
        $view = "{$this->keyspace}.mv_scores_by_score";

        $this->insertScore(40, 1, 'dave', 400);
        $this->insertScore(41, 1, 'erin', 400);

        $this->assertEventually($view, 400, 40, ['pk' => 40, 'ck' => 1, 'name' => 'dave']);

        $rows = $this->connection->query("SELECT pk, name FROM {$view} WHERE n = ?", [400])
            ->asRowsResult()->fetchAll();

        $this->assertCount(2, $rows, 'Both base rows share one view partition');

        $names = array_column($rows, 'name');
        sort($names);
        $this->assertSame(['dave', 'erin'], $names);
    }

    public function testViewReflectsDeletes(): void {
        $view = "{$this->keyspace}.mv_scores_by_score";

        $this->insertScore(30, 1, 'carol', 300);
        $this->assertEventually($view, 300, 30, ['pk' => 30, 'ck' => 1, 'name' => 'carol']);

        $this->connection->query(
            "DELETE FROM {$this->keyspace}.mv_scores WHERE pk = ? AND ck = ?",
            [30, 1]
        );

        $this->assertEventually($view, 300, 30, null, 'Deleting the base row must remove it from the view');
    }

    public function testViewReflectsInserts(): void {
        $view = "{$this->keyspace}.mv_scores_by_score";

        $this->insertScore(10, 1, 'alice', 100);

        $this->assertEventually($view, 100, 10, ['pk' => 10, 'ck' => 1, 'name' => 'alice']);
    }

    public function testViewReflectsUpdates(): void {
        $view = "{$this->keyspace}.mv_scores_by_score";

        $this->insertScore(20, 1, 'bob', 200);
        $this->assertEventually($view, 200, 20, ['pk' => 20, 'ck' => 1, 'name' => 'bob']);

        // The score is the view's partition key, so changing it moves the row.
        $this->connection->query(
            "UPDATE {$this->keyspace}.mv_scores SET n = ? WHERE pk = ? AND ck = ?",
            [250, 20, 1]
        );

        $this->assertEventually($view, 200, 20, null, 'The old view partition must no longer hold the row');
        $this->assertEventually($view, 250, 20, ['pk' => 20, 'ck' => 1, 'name' => 'bob']);
    }

    protected static function setupTable(): void {
        $conn = self::newConnection(self::$defaultKeyspace);
        $conn->query(
            'CREATE TABLE IF NOT EXISTS mv_scores (pk int, ck int, name varchar, n int, PRIMARY KEY (pk, ck))'
        );
        $conn->query(
            'CREATE MATERIALIZED VIEW IF NOT EXISTS mv_scores_by_score AS '
            . 'SELECT pk, ck, name, n FROM mv_scores '
            . 'WHERE pk IS NOT NULL AND ck IS NOT NULL AND n IS NOT NULL '
            . 'PRIMARY KEY (n, pk, ck)'
        );
        $conn->disconnect();
    }

    /**
     * View updates are applied asynchronously, so poll until the expected state
     * shows up rather than sleeping for a fixed amount of time.
     *
     * `n` is the view's partition key and `pk` its first clustering column, so
     * this reads exactly one view row without filtering.
     *
     * @param array<string, mixed>|null $expected the awaited row, or null if it must be absent
     */
    private function assertEventually(
        string $view,
        int $score,
        int $pk,
        ?array $expected,
        string $message = ''
    ): void {
        $actual = null;

        for ($attempt = 0; $attempt < 50; $attempt++) {
            $row = $this->connection->query(
                "SELECT pk, ck, name FROM {$view} WHERE n = ? AND pk = ?",
                [$score, $pk]
            )->asRowsResult()->fetch();

            $actual = is_array($row) ? $row : null;

            if ($actual == $expected) {
                break;
            }

            usleep(100000);
        }

        $this->assertEquals($expected, $actual, $message);
    }

    private function insertScore(int $pk, int $ck, string $name, int $n): void {
        $this->connection->query(
            "INSERT INTO {$this->keyspace}.mv_scores (pk, ck, name, n) VALUES (?, ?, ?, ?)",
            [$pk, $ck, $name, $n]
        );
    }
}
