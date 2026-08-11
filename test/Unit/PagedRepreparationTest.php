<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Request\Options\ExecuteOptions;

/**
 * What happens when a node forgets a prepared statement half way through a
 * result set.
 *
 * The first page of a paged EXECUTE is built from the PREPARE's own result, but
 * every page after it is built from the page before —
 * {@see \Cassandra\Request\Execute::__construct()} takes a
 * {@see \Cassandra\Response\Result\RowsResult} for exactly that reason, and it
 * is what {@see \Cassandra\Connection::executeAll()} hands it. A page is not the
 * prepared statement, so an UNPREPARED for one used to leave the driver with
 * nothing to prepare again and be reported as an internal error against the
 * previous result's type, rather than being recovered from the way an
 * UNPREPARED for the first page is.
 *
 * Driven against the fake server's "unprepared-second-page" mode rather than
 * mocks, because what has to hold is a property of the whole chain: the page is
 * prepared again, and asked for again with the new statement id and the same
 * paging state.
 */
final class PagedRepreparationTest extends AbstractUnitTestCase {
    /** @var ?resource $serverProcess */
    private $serverProcess = null;

    /** @var ?resource $serverStdout */
    private $serverStdout = null;

    protected function tearDown(): void {
        $this->stopServer();
    }

    public function testAsyncPagingRepreparesAStatementForgottenBetweenPages(): void {
        $connection = $this->connect();

        $prepared = $connection->prepare('SELECT * FROM ks.t WHERE id = ?');

        $firstPage = $connection->executeAsync($prepared, [1])->getRowsResult();
        $pagingState = $firstPage->getRowsMetadata()->pagingState;
        $this->assertSame('page2', $pagingState);

        // The async path chains the repreparation onto the statement the caller
        // is holding rather than recursing, so it is worth its own pass: the
        // statement outlives the EXECUTE it was created for, and the request it
        // is waiting for is replaced twice on the way to the answer.
        $secondPage = $connection->executeAsync(
            $firstPage,
            [1],
            options: (new ExecuteOptions())->withPagingState($pagingState),
        )->getRowsResult();

        $this->assertSame([['id' => 2]], $secondPage->fetchAll());
        $this->assertNull($secondPage->getRowsMetadata()->pagingState);

        $connection->disconnect();

        $this->assertSame(
            [
                'prepared 1',
                'execute pid1 -',
                'execute pid1 page2',
                'prepared 2',
                'execute pid2 page2',
            ],
            $this->serverLog(),
        );
    }

    public function testPagingRepreparesAStatementForgottenBetweenPages(): void {
        $connection = $this->connect();

        $prepared = $connection->prepare('SELECT * FROM ks.t WHERE id = ?');

        $pages = $connection->executeAll($prepared, [1]);

        // Both pages arrive: the second one only after the driver has prepared
        // the statement again and asked for it a second time.
        $this->assertCount(2, $pages);
        $this->assertSame([['id' => 1]], $pages[0]->fetchAll());
        $this->assertSame([['id' => 2]], $pages[1]->fetchAll());

        $connection->disconnect();

        // What the node was actually sent, which is the whole of what this is
        // about: the page that was refused is asked for again with the id of
        // the statement that has just been prepared, and with the same paging
        // state — a repreparation that started the result set over would show
        // as "execute pid2 -" here.
        $this->assertSame(
            [
                'prepared 1',
                'execute pid1 -',
                'execute pid1 page2',
                'prepared 2',
                'execute pid2 page2',
            ],
            $this->serverLog(),
        );
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
     * Everything the fake server has reported so far, one line per request it
     * was sent.
     *
     * @return list<string>
     */
    private function serverLog(): array {
        if ($this->serverStdout === null) {
            return [];
        }

        $lines = [];
        while (($line = fgets($this->serverStdout)) !== false) {
            $line = trim($line);
            if ($line !== '') {
                $lines[] = $line;
            }
        }

        return $lines;
    }

    private function startServer(): int {
        $descriptors = [1 => ['pipe', 'w'], 2 => ['pipe', 'w']];
        $process = proc_open(
            ['php', __DIR__ . '/Support/fake-cassandra-server.php', 'unprepared-second-page', '0'],
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
