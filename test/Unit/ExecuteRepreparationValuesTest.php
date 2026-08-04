<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Response\Result\VoidResult;

/**
 * What an EXECUTE is re-sent with after a node answers it with UNPREPARED.
 *
 * A repreparation is exactly the case where the bind marker types may have
 * moved: an ALTER, or a table dropped and created again, is one of the reasons
 * a node stops recognising a statement id. The refused EXECUTE holds its values
 * already encoded against the types of the statement the node has just
 * forgotten, and {@see \Cassandra\Request\Request::encodeQueryValuesForBindMarkerTypes()}
 * passes an encoded value straight through — so rebuilding from those sent the
 * new statement id with the old statement's encoding. Where the widths differ
 * the node rejects it; where they do not, it writes the wrong bytes with
 * nothing to show for it.
 *
 * Driven against the fake server rather than mocks, because what matters is the
 * bytes that actually go out on the second attempt.
 */
final class ExecuteRepreparationValuesTest extends AbstractUnitTestCase {
    /** @var ?resource $serverProcess */
    private $serverProcess = null;

    /** @var ?resource $serverStdout */
    private $serverStdout = null;

    protected function tearDown(): void {
        $this->stopServer();
    }

    public function testAnAsyncExecuteIsAlsoReEncoded(): void {
        // The async path chains the follow-up request onto the statement the
        // caller is holding rather than recursing, so it reaches the rebuild by
        // a different route and is worth pinning separately.
        $connection = $this->connect();

        $prepared = $connection->prepare('INSERT INTO ks.t (id) VALUES (?)');

        $statement = $connection->executeAsync($prepared, [42]);

        $this->assertInstanceOf(VoidResult::class, $statement->getResult());

        $this->assertSame(
            [
                ['id' => 'pid1', 'values' => ['0000002a']],
                ['id' => 'pid2', 'values' => ['000000000000002a']],
            ],
            $this->executesSeenByServer()
        );
    }

    public function testTheReExecuteEncodesTheValuesAgainstTheNewBindMarkerTypes(): void {
        $connection = $this->connect();

        $prepared = $connection->prepare('INSERT INTO ks.t (id) VALUES (?)');

        // Bound against an int marker, so it goes out as four bytes. The node
        // then forgets the statement and hands back one whose marker is a
        // bigint, so the re-execute has to carry eight.
        $this->assertInstanceOf(VoidResult::class, $connection->execute($prepared, [42]));

        $seen = $this->executesSeenByServer();

        $this->assertCount(2, $seen, 'the EXECUTE must be sent again after the repreparation');

        $this->assertSame(['id' => 'pid1', 'values' => ['0000002a']], $seen[0]);

        $this->assertSame(
            ['id' => 'pid2', 'values' => ['000000000000002a']],
            $seen[1],
            'the re-executed statement must carry the value encoded for the new marker type, not the refused statement\'s'
        );
    }

    private function connect(): Connection {
        $port = $this->startServer('unprepared-execute-retyped');

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
     * Every EXECUTE the server has been sent so far, in order: the statement id
     * it named and the hex of each value it carried.
     *
     * @return list<array{id: string, values: list<string>}>
     */
    private function executesSeenByServer(): array {
        if ($this->serverStdout === null) {
            $this->fail('the fake Cassandra server is not running');
        }

        $executes = [];

        while (($line = fgets($this->serverStdout)) !== false) {
            $line = trim($line);
            if (!str_starts_with($line, 'execute ')) {
                continue;
            }

            $reported = explode(' ', substr($line, strlen('execute ')), 2);

            $executes[] = [
                'id' => $reported[0],
                'values' => ($reported[1] ?? '') === '' ? [] : explode(',', $reported[1]),
            ];
        }

        return $executes;
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
