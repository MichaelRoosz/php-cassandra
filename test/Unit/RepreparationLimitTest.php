<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\StatementException;
use Cassandra\Statement;
use Cassandra\Connection\StreamIdPool;
use ReflectionProperty;

/**
 * What happens when a node answers a prepared statement with UNPREPARED for
 * good.
 *
 * The driver answers UNPREPARED by preparing the statement again and
 * re-executing it, and a node that never keeps the prepared statement answers
 * that the same way — so something has to stop the exchange, and both paths
 * carry a count of the rounds behind them to do it. Driven against the fake
 * server's "always-unprepared" mode rather than mocks, because the whole point
 * is what the two paths do with a real chain of requests.
 */
final class RepreparationLimitTest extends AbstractUnitTestCase {
    /** @var ?resource $serverProcess */
    private $serverProcess = null;

    protected function tearDown(): void {
        $this->stopServer();
    }

    public function testAnAsyncStatementIsGivenUpOnAndReleasesItsStreamId(): void {
        $connection = $this->connect();

        $prepared = $connection->prepare('SELECT * FROM ks.t WHERE id = ?');
        $statement = $connection->executeAsync($prepared, [1]);
        $streamId = $statement->getStreamId();

        try {
            $statement->getResult();
            $this->fail('expected the driver to stop repreparing');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_REPREPARATION_LIMIT_REACHED->value, $e->getCode());
        }

        // The statement is taken out of the pending map before its answer is
        // handled, so a failure while handling it has to finish the statement
        // as well. Left as it was, its owner would wait on it for good — and be
        // told it belongs to another connection.
        $this->assertTrue($statement->isAbandoned(), 'a statement the driver gave up on must not stay pending');

        try {
            $statement->getResult();
            $this->fail('expected an abandoned statement to be refused');
        } catch (StatementException $e) {
            $this->assertSame(ExceptionCode::STATEMENT_ABANDONED->value, $e->getCode());
        }

        // The answer that ended the exchange was read off the wire, so the node
        // is done with the id: it belongs back in circulation rather than being
        // parked, or burned for the lifetime of the connection.
        $this->assertSame([], $this->orphanedStreams($connection), 'giving up here should park no stream id');
        $this->assertContains($streamId, $this->recycledStreams($connection), 'the stream id must go back into circulation');

        $connection->disconnect();
    }

    public function testEveryStatementGivenUpOnPutsItsStreamIdBack(): void {
        // The point of releasing the id above: a client that keeps hitting this
        // must not lose one of the 32768 ids every time. They are not handed
        // straight back out — getNewStreamId() works through the counter before
        // it touches the pool — so what matters is that they are in the pool at
        // all rather than lost between the two.
        $connection = $this->connect();

        $prepared = $connection->prepare('SELECT * FROM ks.t WHERE id = ?');

        $streamIds = [];

        for ($i = 0; $i < 5; $i++) {
            $statement = $connection->executeAsync($prepared, [$i]);
            $streamIds[] = $statement->getStreamId();

            try {
                $statement->getResult();
                $this->fail('expected the driver to stop repreparing');
            } catch (ConnectionException $e) {
                $this->assertSame(ExceptionCode::CONNECTION_REPREPARATION_LIMIT_REACHED->value, $e->getCode());
            }
        }

        $this->assertSame([], $this->orphanedStreams($connection), 'no stream id should have been parked');

        $recycled = $this->recycledStreams($connection);
        foreach ($streamIds as $streamId) {
            $this->assertContains($streamId, $recycled, 'every id given up on must be back in the pool');
        }

        $connection->disconnect();
    }

    public function testTheSyncPathStopsRepreparingToo(): void {
        $connection = $this->connect();

        $prepared = $connection->prepare('SELECT * FROM ks.t WHERE id = ?');

        try {
            $connection->execute($prepared, [1]);
            $this->fail('expected the driver to stop repreparing');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_REPREPARATION_LIMIT_REACHED->value, $e->getCode());
        }

        // The sync path recurses for each round and unwinds in order, so the
        // depth it counts is its own rather than the connection's: a second
        // call has to get the full allowance again.
        try {
            $connection->execute($prepared, [1]);
            $this->fail('expected the driver to stop repreparing');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_REPREPARATION_LIMIT_REACHED->value, $e->getCode());
        }

        $this->assertTrue($connection->isConnected(), 'nothing here should have cost the connection');

        $connection->disconnect();
    }

    private function connect(): Connection {
        $port = $this->startServer();

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

    /**
     * @return array<int, float>
     */
    private function orphanedStreams(Connection $connection): array {
        /** @var array<int, float> $orphaned */
        $orphaned = (new ReflectionProperty(StreamIdPool::class, 'orphanedStreams'))->getValue($this->streamIdPoolOf($connection));

        return $orphaned;
    }

    /**
     * @return array<int>
     */
    private function recycledStreams(Connection $connection): array {
        /** @var \SplQueue<int> $recycled */
        $recycled = (new ReflectionProperty(StreamIdPool::class, 'recycledStreams'))->getValue($this->streamIdPoolOf($connection));

        return array_values(iterator_to_array($recycled));
    }

    private function startServer(): int {
        $descriptors = [1 => ['pipe', 'w'], 2 => ['pipe', 'w']];
        $process = proc_open(
            ['php', __DIR__ . '/Support/fake-cassandra-server.php', 'always-unprepared', '0'],
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

        return (int) $matches[1];
    }

    private function stopServer(): void {
        if ($this->serverProcess === null) {
            return;
        }

        proc_terminate($this->serverProcess);
        proc_close($this->serverProcess);
        $this->serverProcess = null;
    }

    private function streamIdPoolOf(Connection $connection): StreamIdPool {
        /** @var StreamIdPool $pool */
        $pool = (new ReflectionProperty(Connection::class, 'streamIds'))->getValue($connection);

        return $pool;
    }
}
