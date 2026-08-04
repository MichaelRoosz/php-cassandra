<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Request\Prepare;
use Cassandra\Response\Result\CachedPreparedResult;

/**
 * What a cache hit on the asynchronous path does to the cache entry.
 *
 * {@see \Cassandra\Connection\PreparedResultCache::store()} keeps a copy of the
 * PREPARE rather than the caller's own object, because a request is addressed on
 * its way to the wire and keeps what it was given: one that is sent again after
 * a {@see \Cassandra\Connection::setKeyspace()} would otherwise leave the entry
 * naming a keyspace other than the one it is filed under, and the repreparation
 * path rebuilds its PREPARE out of exactly that request — so an UNPREPARED for
 * the statement id would prepare and execute the query against the wrong
 * keyspace, with nothing to show for it.
 *
 * A cache hit on the async path is handled like any other answer, and handling a
 * PREPARE answer records the request that produced it. That must not put the
 * caller's live object back into the entry, which would undo the copy.
 *
 * Driven against the fake server rather than mocks, because the hit only happens
 * on the way through {@see \Cassandra\Connection\RequestExecutor}.
 */
final class PreparedResultCacheAsyncHitTest extends AbstractUnitTestCase {
    /** @var ?resource $serverProcess */
    private $serverProcess = null;

    /** @var ?resource $serverStdout */
    private $serverStdout = null;

    protected function tearDown(): void {
        $this->stopServer();
    }

    public function testTheEntryDoesNotFollowTheCallersRequestAfterAnAsyncHit(): void {
        $connection = $this->connect();

        // Populates the cache, which stores a copy of this request.
        $connection->syncRequest(new Prepare('SELECT * FROM ks.t'));

        // The same query as a second, distinct object: a cache hit, served
        // without a round trip.
        $callersRequest = new Prepare('SELECT * FROM ks.t');
        $statement = $connection->asyncRequest($callersRequest);

        $entry = $statement->getPreparedResult();
        $this->assertInstanceOf(CachedPreparedResult::class, $entry, 'the second prepare must be served from the cache');
        $this->assertSame(1, $this->prepareCountSeenByServer(), 'a cache hit must cost no round trip');

        $storedRequest = $entry->getRequest();
        $this->assertInstanceOf(Prepare::class, $storedRequest);
        $this->assertNotSame(
            $callersRequest,
            $storedRequest,
            'the cache entry must keep its own copy of the PREPARE, not the caller\'s object'
        );

        // What the copy exists for: the caller's request is re-addressed by a
        // later send, and the entry must not move with it.
        $callersRequest->applyDefaultKeyspace('somewhere-else');

        $this->assertNull(
            $storedRequest->getOptions()->keyspace,
            'the cache entry must still name the keyspace it is filed under'
        );
    }

    private function connect(): Connection {
        $port = $this->startServer('idle');

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
     * How many PREPAREs the server has been sent so far, which is what the
     * cache is meant to keep down.
     */
    private function prepareCountSeenByServer(): int {
        if ($this->serverStdout === null) {
            $this->fail('the fake Cassandra server is not running');
        }

        $count = 0;

        while (($line = fgets($this->serverStdout)) !== false) {
            if (str_starts_with(trim($line), 'prepared ')) {
                $count++;
            }
        }

        return $count;
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
