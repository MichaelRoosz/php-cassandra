<?php

declare(strict_types=1);

namespace Cassandra\Test\Integration;

use Cassandra\Consistency;
use Cassandra\Exception\ServerException\AlreadyExistsException;
use Cassandra\Exception\ServerException\InvalidException;
use Cassandra\Exception\ServerException\SyntaxErrorException;
use Cassandra\Exception\ServerException\UnavailableException;

/**
 * The server errors a single test node can really produce, with the details an
 * application reads from them.
 */
final class ServerErrorIntegrationTest extends AbstractIntegrationTestCase {
    public function testAnExistingTableReportsItsName(): void {
        $this->connection->query("CREATE TABLE IF NOT EXISTS {$this->keyspace}.error_exists (id int PRIMARY KEY)");

        try {
            $this->connection->query("CREATE TABLE {$this->keyspace}.error_exists (id int PRIMARY KEY)");
            $this->fail('expected AlreadyExistsException');
        } catch (AlreadyExistsException $e) {
            $this->assertSame($this->keyspace, $e->getErrorContext()->keyspace);
            $this->assertSame('error_exists', $e->getErrorContext()->table);
        }
    }

    public function testAnUnknownTableIsInvalidAndLeavesTheConnectionUsable(): void {
        try {
            $this->connection->query("SELECT * FROM {$this->keyspace}.no_such_table");
            $this->fail('expected InvalidException');
        } catch (InvalidException) {
            $this->assertTrue($this->connection->isConnected());
        }

        $this->assertSame(1, $this->connection->query('SELECT key FROM system.local')->asRowsResult()->getRowCount());
    }

    public function testBadSyntaxIsReported(): void {
        $this->expectException(SyntaxErrorException::class);
        $this->connection->query('SELEC nonsense');
    }

    public function testCounterAndAsciiColumnsRoundTrip(): void {
        $this->connection->query("CREATE TABLE IF NOT EXISTS {$this->keyspace}.error_counter (id ascii PRIMARY KEY, hits counter)");
        $this->connection->query("TRUNCATE {$this->keyspace}.error_counter");
        $this->connection->query("UPDATE {$this->keyspace}.error_counter SET hits = hits + ? WHERE id = ?", [5, 'a']);
        $this->connection->query("UPDATE {$this->keyspace}.error_counter SET hits = hits + ? WHERE id = ?", [-2, 'a']);

        $row = $this->connection->query("SELECT id, hits FROM {$this->keyspace}.error_counter WHERE id = ?", ['a'])->asRowsResult()->fetch();

        $this->assertSame(['id' => 'a', 'hits' => 3], $row);
    }

    public function testTooFewReplicasReportsWhatWasNeeded(): void {
        // A replication factor no single node can meet, which is the point and
        // which the server warns about.
        $this->connection->unregisterWarningsListener($this);
        $keyspace = $this->keyspace . '_rf3';
        $this->connection->query("CREATE KEYSPACE IF NOT EXISTS {$keyspace} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");

        try {
            $this->connection->query("CREATE TABLE IF NOT EXISTS {$keyspace}.t (id int PRIMARY KEY)");
            $this->connection->query("SELECT id FROM {$keyspace}.t WHERE id = ?", [1], Consistency::QUORUM);
            $this->fail('expected UnavailableException');
        } catch (UnavailableException $e) {
            $context = $e->getErrorContext();
            $this->assertSame(Consistency::QUORUM, $context->consistency);
            $this->assertSame(2, $context->nodesRequired);
            $this->assertSame(1, $context->nodesAlive);
        } finally {
            $this->connection->query("DROP KEYSPACE IF EXISTS {$keyspace}");
        }
    }
}
