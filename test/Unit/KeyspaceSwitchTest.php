<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ServerException;

/**
 * Switching the keyspace on a live connection, up to protocol v4.
 *
 * There the keyspace is a property of the node's session rather than of a
 * request, so {@see \Cassandra\Connection::setKeyspace()} has to send a USE and
 * live with what the node makes of it: how it spells the keyspace on the way
 * out, what it does with a USE that came back refused, that it cannot move the
 * session off a keyspace at all, and that the statements prepared in the one it
 * is leaving do not follow it into the next.
 *
 * Driven against the fake server rather than mocks — "refuse-use" for the calls
 * the node turns down, "idle" for the ones it accepts — because every one of
 * those is a question about what actually went over the wire.
 */
final class KeyspaceSwitchTest extends AbstractUnitTestCase {
    /** @var ?array<int, resource> $serverPipes */
    private ?array $serverPipes = null;
    /** @var ?resource $serverProcess */
    private $serverProcess = null;

    protected function tearDown(): void {
        $this->stopServer();
    }

    public function testAConnectedConnectionCannotBeMovedOffItsKeyspace(): void {
        $connection = $this->connect('idle');

        $connection->setKeyspace('ks_a');

        // Up to v4 the keyspace belongs to the node's session and no CQL un-sets
        // one, so recording '' would leave every request running against ks_a
        // while getKeyspace() named none.
        try {
            $connection->setKeyspace('');
            $this->fail('expected clearing the keyspace to be refused up to v4');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_KEYSPACE_CANNOT_BE_CLEARED->value, $e->getCode());
        }

        $this->assertSame('ks_a', $connection->getKeyspace(), 'a refused clear must not change the recorded keyspace');
        $this->assertTrue($connection->isConnected(), 'refusing the call is not a reason to drop the connection');

        $connection->disconnect();
    }

    public function testAKeyspaceCannotCarryCqlOfItsOwn(): void {
        $connection = $this->connect();

        $this->expectRefusal(static fn () => $connection->setKeyspace('ks"; DROP KEYSPACE other; --'));

        // One statement, with the whole name inside the quotes and the closing
        // quote it tried to smuggle in doubled rather than ending them.
        $this->assertContains('USE "ks""; DROP KEYSPACE other; --";', $this->queriesSeenByServer());

        $connection->disconnect();
    }

    public function testARefusedSwitchLeavesTheConnectionOnTheKeyspaceItWasOn(): void {
        $connection = $this->connect();

        // The node's session stayed where it was, so the connection has to as
        // well. Recording the new keyspace anyway would make every following
        // request run against '' while getKeyspace() named the other one.
        $this->expectRefusal(static fn () => $connection->setKeyspace('nosuchkeyspace'));

        $this->assertSame('', $connection->getKeyspace(), 'a refused USE must not change the recorded keyspace');
        $this->assertTrue($connection->isConnected(), 'a refused USE is the node\'s answer, not a broken connection');

        // And the connection is still usable, on the keyspace it never left.
        $connection->query('SELECT * FROM ks.t');

        $connection->disconnect();
    }

    public function testClearingAKeyspaceThatWasNeverSetIsANoOp(): void {
        $connection = $this->connect('idle');

        // Nothing to move off, so there is nothing this cannot honour.
        $connection->setKeyspace('');

        $this->assertSame('', $connection->getKeyspace());

        $connection->disconnect();
    }

    public function testSwitchingKeyspaceEmptiesThePreparedStatementCache(): void {
        $connection = $this->connect('idle');

        $query = 'SELECT * FROM t WHERE id = ?';

        $connection->setKeyspace('ks_a');
        $connection->prepare($query);

        // Up to v4 a PREPARE cannot carry a keyspace, so the cache cannot tell
        // ks_a's prepared statement from ks_b's. Kept, it would answer this
        // second prepare with ks_a's statement id and every EXECUTE on it would
        // silently hit ks_a's table.
        $connection->setKeyspace('ks_b');
        $connection->prepare($query);

        $this->assertSame(2, $this->preparesSeenByServer(), 'the second keyspace must prepare its own statement');

        $connection->disconnect();
    }

    public function testTheCacheSurvivesAKeyspaceThatDidNotChange(): void {
        $connection = $this->connect('idle');

        $query = 'SELECT * FROM t WHERE id = ?';

        $connection->setKeyspace('ks_a');
        $connection->prepare($query);

        // Same keyspace, so the statements in the cache are still the right
        // ones: throwing them away would cost a round trip per prepare for
        // nothing.
        $connection->setKeyspace('ks_a');
        $connection->prepare($query);

        $this->assertSame(1, $this->preparesSeenByServer(), 'a switch that changed nothing must not empty the cache');

        $connection->disconnect();
    }

    public function testTheKeyspaceIsSentAsAQuotedIdentifier(): void {
        $connection = $this->connect();

        // Quoting is what keeps a keyspace name from being read as CQL of its
        // own, and what makes v4 address the same keyspace v5 does: unquoted,
        // the server folds the name to lower case, so "MyKs" would reach a
        // different keyspace here than it does through the v5 request option.
        $this->expectRefusal(static fn () => $connection->setKeyspace('MyKs'));

        $this->assertContains('USE "MyKs";', $this->queriesSeenByServer());

        $connection->disconnect();
    }

    private function connect(string $mode = 'refuse-use'): Connection {
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

    /**
     * Run a keyspace switch the node is going to refuse, and assert that it was.
     *
     * @param callable(): void $switch
     */
    private function expectRefusal(callable $switch): void {
        try {
            $switch();
            $this->fail('expected the node to refuse the keyspace');
        } catch (ServerException $e) {
            $this->assertStringContainsString('Keyspace does not exist', $e->getMessage());
        }
    }

    /**
     * How many PREPAREs the server has been sent so far, which it reports as
     * "prepared <n>" on stdout — the count the prepared-result cache is there to
     * hold down, and the only way to tell a cache hit from a round trip.
     */
    private function preparesSeenByServer(): int {
        if ($this->serverPipes === null) {
            $this->fail('the fake Cassandra server is not running');
        }

        $prepares = 0;

        while (($line = fgets($this->serverPipes[1])) !== false) {
            if (str_starts_with(trim($line), 'prepared ')) {
                $prepares++;
            }
        }

        return $prepares;
    }

    /**
     * The CQL of every QUERY the server has seen so far, which it reports as
     * "query <cql>" on stdout.
     *
     * @return array<string>
     */
    private function queriesSeenByServer(): array {
        if ($this->serverPipes === null) {
            $this->fail('the fake Cassandra server is not running');
        }

        $queries = [];

        while (($line = fgets($this->serverPipes[1])) !== false) {
            $line = trim($line);
            if (str_starts_with($line, 'query ')) {
                $queries[] = substr($line, strlen('query '));
            }
        }

        return $queries;
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
        $this->serverPipes = $pipes;

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
        $this->serverPipes = null;
    }
}
