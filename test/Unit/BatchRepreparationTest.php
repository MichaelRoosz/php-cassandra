<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Response\Result\VoidResult;

/**
 * What happens when a node answers a BATCH with UNPREPARED.
 *
 * A batch encodes each prepared entry down to a statement id and a value list
 * the moment it is appended, so — unlike an EXECUTE, which keeps the prepared
 * result it was built from — it used to have nothing left to prepare again and
 * nowhere to put a new statement id. The whole batch failed on a statement the
 * node had merely forgotten.
 *
 * Driven against the fake server rather than mocks, because what matters is the
 * real chain of requests: the batch has to go out again carrying the id the
 * repreparation produced, not the one the node refused.
 */
final class BatchRepreparationTest extends AbstractUnitTestCase {
    /** @var ?resource $serverProcess */
    private $serverProcess = null;

    /** @var ?resource $serverStdout */
    private $serverStdout = null;

    protected function tearDown(): void {
        $this->stopServer();
    }

    public function testAnAsyncBatchIsRepreparedAndSentAgain(): void {
        $connection = $this->connect('unprepared-batch-once');

        $prepared = $connection->prepare('INSERT INTO ks.t (id) VALUES (?)');

        $batch = $connection->createBatchRequest();
        $batch->appendPreparedStatement($prepared, [1]);

        $statement = $connection->batchAsync($batch);
        $result = $statement->getResult();

        $this->assertInstanceOf(VoidResult::class, $result);

        $this->assertSame(
            ['pid1', 'pid2'],
            $this->batchIdsSeenByServer(),
            'the batch must be sent again carrying the id the repreparation produced'
        );
    }

    public function testEveryEntryOfARepeatedStatementIsReplaced(): void {
        // The same prepared statement appended more than once is several entries
        // sharing one id. Replacing only the entry the node happened to trip on
        // would leave the others carrying an id it has already refused.
        $connection = $this->connect('unprepared-batch-once');

        $prepared = $connection->prepare('INSERT INTO ks.t (id) VALUES (?)');

        $batch = $connection->createBatchRequest();
        $batch->appendPreparedStatement($prepared, [1]);
        $batch->appendPreparedStatement($prepared, [2]);
        $batch->appendPreparedStatement($prepared, [3]);

        $this->assertInstanceOf(VoidResult::class, $connection->batch($batch));

        $seen = $this->batchIdsSeenByServer();

        $this->assertSame(['pid1', 'pid1', 'pid1'], array_slice($seen, 0, 3));
        $this->assertSame(['pid2', 'pid2', 'pid2'], array_slice($seen, 3, 3), 'every entry must carry the new id');
    }

    public function testMixedBatchesKeepTheirPlainQueries(): void {
        $connection = $this->connect('unprepared-batch-once');

        $prepared = $connection->prepare('INSERT INTO ks.t (id) VALUES (?)');

        $batch = $connection->createBatchRequest();
        $batch->appendQuery('INSERT INTO ks.t (id) VALUES (10)');
        $batch->appendPreparedStatement($prepared, [1]);
        $batch->appendQuery('INSERT INTO ks.t (id) VALUES (11)', [12]);

        $this->assertInstanceOf(VoidResult::class, $connection->batch($batch));

        // The plain queries are not prepared statements, so they contribute no
        // ids — but the batch still has to be walked past them to find the one
        // that was replaced, on the server side as well as the client's.
        $this->assertSame(['pid1', 'pid2'], $this->batchIdsSeenByServer());
    }

    public function testTheBudgetScalesWithTheStatementsTheBatchCarries(): void {
        // A node answers UNPREPARED one statement at a time, so a batch of five
        // distinct statements the node has forgotten needs five rounds. Held to
        // the flat limit of three it would fail for having done exactly what it
        // was asked to.
        $connection = $this->connect('always-unprepared');

        $batch = $connection->createBatchRequest();
        for ($i = 0; $i < 5; $i++) {
            $batch->appendPreparedStatement($connection->prepare('INSERT INTO ks.t' . $i . ' (id) VALUES (?)'), [$i]);
        }

        try {
            $connection->batch($batch);
            $this->fail('expected the driver to stop repreparing eventually');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_REPREPARATION_LIMIT_REACHED->value, $e->getCode());
            $this->assertSame(7, $e->getContext()['max_repreparations'] ?? null, 'MAX_REPREPARATIONS + 5 distinct statements - 1');
        }

        $this->assertTrue($connection->isConnected(), 'nothing here should have cost the connection');
    }

    public function testTheSyncPathStopsRepreparingABatchTheNodeNeverKeeps(): void {
        $connection = $this->connect('always-unprepared');

        $prepared = $connection->prepare('INSERT INTO ks.t (id) VALUES (?)');

        $batch = $connection->createBatchRequest();
        $batch->appendPreparedStatement($prepared, [1]);

        try {
            $connection->batch($batch);
            $this->fail('expected the driver to stop repreparing');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_REPREPARATION_LIMIT_REACHED->value, $e->getCode());
        }

        $this->assertTrue($connection->isConnected(), 'nothing here should have cost the connection');
    }

    public function testUnpreparedNamingAStatementTheBatchDoesNotHoldIsRefused(): void {
        // The node can only be answered about a statement the batch actually
        // carries. One it does not is not something to prepare again — it is a
        // node and a client that disagree about what was sent, and guessing
        // which entry was meant would re-send a batch the node has already
        // refused.
        $connection = $this->connect('unprepared-batch-unknown-id');

        $prepared = $connection->prepare('INSERT INTO ks.t (id) VALUES (?)');

        $batch = $connection->createBatchRequest();
        $batch->appendPreparedStatement($prepared, [1]);

        try {
            $connection->batch($batch);
            $this->fail('expected the driver to refuse an UNPREPARED it cannot match');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_UNPREPARED_BATCH_STATEMENT_NOT_FOUND->value, $e->getCode());
        }

        $this->assertTrue($connection->isConnected(), 'nothing here should have cost the connection');
    }

    /**
     * The prepared statement ids of every BATCH the server has been sent so far,
     * flattened in the order they arrived.
     *
     * @return array<string>
     */
    private function batchIdsSeenByServer(): array {
        if ($this->serverStdout === null) {
            $this->fail('the fake Cassandra server is not running');
        }

        $ids = [];

        while (($line = fgets($this->serverStdout)) !== false) {
            $line = trim($line);
            if (!str_starts_with($line, 'batch ')) {
                continue;
            }

            $reported = substr($line, strlen('batch '));
            if ($reported === '') {
                continue;
            }

            foreach (explode(',', $reported) as $id) {
                $ids[] = $id;
            }
        }

        return $ids;
    }

    private function connect(string $mode): Connection {
        $port = $this->startServer($mode);

        $node = new SocketNodeConfig(
            host: '127.0.0.1',
            port: $port,
            socketOptions: [
                SO_RCVTIMEO => ['sec' => 2, 'usec' => 0],
                SO_SNDTIMEO => ['sec' => 2, 'usec' => 0],
            ],
        );

        $connection = new Connection(
            nodes: [$node],
            options: new ConnectionOptions(
                requestTimeoutInSeconds: 5.0,
                heartbeatIntervalInSeconds: null,
            ),
        );

        $connection->connect();

        return $connection;
    }

    private function startServer(string $mode): int {
        $descriptors = [1 => ['pipe', 'w'], 2 => ['pipe', 'w']];
        $process = proc_open(
            ['php', __DIR__ . '/Support/fake-cassandra-server.php', $mode, '0'],
            $descriptors,
            $pipes
        );

        if ($process === false) {
            $this->fail('could not start the fake Cassandra server');
        }

        $this->serverProcess = $process;

        $ready = fgets($pipes[1]);
        if ($ready === false || !preg_match('/^ready (\d+)$/', trim($ready), $matches)) {
            $this->fail('fake Cassandra server did not start listening');
        }

        stream_set_blocking($pipes[1], false);
        $this->serverStdout = $pipes[1];

        return (int) $matches[1];
    }

    private function stopServer(): void {
        if ($this->serverProcess === null) {
            return;
        }

        proc_terminate($this->serverProcess);
        proc_close($this->serverProcess);
        $this->serverProcess = null;
        $this->serverStdout = null;
    }
}
