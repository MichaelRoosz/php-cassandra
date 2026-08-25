<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ResponseException;

/**
 * A repeated paging state must stop the all-page helpers instead of making
 * them retain and request the same page without end.
 */
final class PagingStateCycleTest extends AbstractUnitTestCase {
    /** @var ?resource $serverProcess */
    private $serverProcess = null;

    protected function tearDown(): void {
        $this->stopServer();
    }

    public function testExecuteAllRejectsARepeatedPagingState(): void {
        $connection = $this->connect();

        try {
            $prepared = $connection->prepare('SELECT * FROM ks.t WHERE id = ?');

            $connection->executeAll($prepared, [1]);
            $this->fail('Expected executeAll to reject the repeated paging state');
        } catch (ResponseException $e) {
            $this->assertPagingCycleException($e, 'executeAll');
        } finally {
            $connection->disconnect();
        }
    }

    public function testQueryAllRejectsARepeatedPagingState(): void {
        $connection = $this->connect();

        try {
            $connection->queryAll('SELECT * FROM ks.t');
            $this->fail('Expected queryAll to reject the repeated paging state');
        } catch (ResponseException $e) {
            $this->assertPagingCycleException($e, 'queryAll');
        } finally {
            $connection->disconnect();
        }
    }

    private function assertPagingCycleException(ResponseException $exception, string $operation): void {
        $this->assertSame(ExceptionCode::RESPONSE_ROWS_PAGING_STATE_CYCLE->value, $exception->getCode());
        $this->assertSame(
            [
                'operation' => $operation,
                'pages_received' => 2,
                'paging_state_length' => strlen('same-page'),
            ],
            $exception->getContext()
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

    private function startServer(): int {
        $descriptors = [1 => ['pipe', 'w'], 2 => ['pipe', 'w']];
        $process = proc_open(
            ['php', __DIR__ . '/Support/fake-cassandra-server.php', 'repeat-paging-state', '0'],
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
}
