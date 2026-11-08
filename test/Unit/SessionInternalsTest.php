<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\Session;
use Cassandra\Connection\StatementRegistry;
use Cassandra\Connection\StreamIdPool;
use Cassandra\Exception\CassandraException;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\RequestTimeoutException;
use Cassandra\Exception\ServerException;
use Cassandra\Exception\StreamException;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Request\Query;
use Cassandra\Request\Request;
use Cassandra\Response\Response;
use Cassandra\Response\Result\PreparedResult;
use Cassandra\Statement;
use Cassandra\Test\Support\ScriptedNode;
use Cassandra\Test\Support\ScriptedNodeConfig;
use Cassandra\WarningsListener;
use Closure;
use ReflectionProperty;

/**
 * The session's bookkeeping when the node misbehaves in ways that are hard to
 * arrange on a real server: writes and reads that fail, an exhausted stream id
 * space, a listener closing the connection, and timeouts reached in every wait.
 */
final class SessionInternalsTest extends AbstractUnitTestCase {
    private const NO_ANSWER = 'SLOW';

    protected function tearDown(): void {
        ScriptedNode::reset();
        parent::tearDown();
    }

    public function testABatchStillRefusedAfterRepreparationIsNotRetriedForever(): void {
        // Reprepared under a new id, the batch no longer holds the id the node keeps naming.
        $calls = 0;
        $connection = $this->connect(responder: static function (Request $r) use (&$calls) {
            return match ($r->getOpcode()) {
                Opcode::REQUEST_PREPARE => $calls++ === 0 ? self::prepared('id') : self::prepared('zz'),
                Opcode::REQUEST_BATCH => [Opcode::RESPONSE_ERROR, pack('N', 0x2500) . ScriptedNode::string('unprepared') . pack('n', 2) . 'id'],
                default => null,
            };
        });
        $batch = $connection->createBatchRequest();
        $batch->appendPreparedStatement($connection->prepare('SELECT 1'));

        $this->assertRaises(ConnectionException::class, static fn () => $connection->batch($batch));
    }

    public function testACachedPreparationThatCannotBeExecutedFreesItsStreamId(): void {
        $connection = $this->connect(version: ProtocolVersion::V4, responder: static fn (Request $r) => $r->getOpcode() === Opcode::REQUEST_PREPARE
            ? [Opcode::RESPONSE_RESULT, self::preparedWithOneMarker()]
            : null);
        $connection->queryAsync('SELECT ?', [1])->getResult();
        $inFlight = $this->registry($connection)->getCount();

        // The PREPARE is answered from the cache, and its EXECUTE refuses the values.
        $this->assertRaises(\Cassandra\Exception\RequestException::class, static fn () => $connection->queryAsync('SELECT ?', [1, 2]));
        $this->assertSame($inFlight, $this->registry($connection)->getCount());
        $this->assertInstanceOf(\Cassandra\Response\Result\VoidResult::class, $connection->query('SELECT 1'));
    }

    public function testADeadlineOfAnotherStatementFiringDuringAWait(): void {
        $connection = $this->connect();
        $waitedOn = $connection->queryAsync(self::NO_ANSWER, options: new QueryOptions(requestTimeoutInSeconds: 0.3));
        $sooner = $connection->queryAsync(self::NO_ANSWER, options: new QueryOptions(requestTimeoutInSeconds: 0.05));

        $this->assertRaises(RequestTimeoutException::class, static fn () => $connection->getResponseForStatement($waitedOn));
        $this->assertTrue($sooner->isTimedOut());
        $this->assertTrue($waitedOn->isTimedOut());
    }

    public function testAListenerClosingTheConnectionMidRepreparation(): void {
        $connection = $this->connect(responder: static fn (Request $r) => $r->getOpcode() === Opcode::REQUEST_EXECUTE
            ? ScriptedNode::frame(Opcode::RESPONSE_ERROR, self::warnings() . pack('N', 0x2500) . ScriptedNode::string('unprepared') . pack('n', 2) . 'id', (int) $r->getStream(), 0x08)
            : null);
        $prepared = $connection->prepare('SELECT 1');
        $connection->registerWarningsListener(new class($connection) implements WarningsListener {
            public function __construct(private readonly Connection $connection) {
            }

            public function onWarnings(array $warnings, Request $request, Response $response): void {
                $this->connection->disconnect();
            }
        });

        $statement = $connection->executeAsync($prepared);
        $this->assertRaises(CassandraException::class, static fn () => $statement->getResult());
    }

    public function testAMalformedLateAnswerReleasesItsParkedStreamId(): void {
        $late = new class() {
            public ?int $stream = null;
        };
        $connection = $this->connect(requestTimeout: 0.05, responder: static function (Request $r) use ($late) {
            if ($late->stream === null || !($r instanceof Query) || $r->getQuery() !== 'SELECT 1') {
                return null;
            }

            return ScriptedNode::frame(Opcode::RESPONSE_RESULT, pack('N', 99), $late->stream)
                . ScriptedNode::frame(Opcode::RESPONSE_RESULT, pack('N', 1), (int) $r->getStream());
        });
        $timedOut = $connection->queryAsync(self::NO_ANSWER);
        $this->assertRaises(RequestTimeoutException::class, static fn () => $connection->waitForAllPendingStatements());
        $this->assertTrue($this->streamIds($connection)->isOrphaned($timedOut->getStreamId()));

        $late->stream = $timedOut->getStreamId();
        $this->assertRaises(CassandraException::class, static fn () => $connection->query('SELECT 1'));
        $this->assertFalse($this->streamIds($connection)->isOrphaned($timedOut->getStreamId()));
    }

    public function testAnExhaustedStreamIdSpaceTimesOutTheWait(): void {
        $connection = $this->connect(requestTimeout: 0.1);
        (new ReflectionProperty(StreamIdPool::class, 'nextStreamId'))->setValue($this->streamIds($connection), StreamIdPool::MAX_STREAM_ID + 1);
        (new ReflectionProperty(StreamIdPool::class, 'recycledStreams'))->setValue($this->streamIds($connection), new \SplQueue());

        try {
            $connection->queryAsync('SELECT 1');
            $this->fail('expected the wait for a stream id to time out');
        } catch (RequestTimeoutException $e) {
            $this->assertSame(ExceptionCode::REQUESTTIMEOUT_STREAM_IDS_EXHAUSTED->value, $e->getCode());
        }
    }

    public function testAStreamIdAlreadyHeldIsNotReused(): void {
        $connection = $this->connect();
        $squatter = new Statement($connection, 0, $this->streamIds($connection)->getGeneration(), new Query('SELECT 1'));
        $next = (new ReflectionProperty(StreamIdPool::class, 'nextStreamId'))->getValue($this->streamIds($connection));
        $this->assertIsInt($next);
        $this->registry($connection)->register($next, $squatter);

        $this->assertRaises(ConnectionException::class, static fn () => $connection->queryAsync('SELECT 1'));
    }

    public function testConnectionHousekeeping(): void {
        $connection = $this->connect(keyspace: 'ks', version: ProtocolVersion::V4);
        $this->assertContains('USE "ks";', array_map(static fn (Request $r): string => $r instanceof Query ? $r->getQuery() : '', ScriptedNode::$received));
        $connection->connect();

        // From v5 a keyspace travels with each request, so switching only records it.
        (new ReflectionProperty(Session::class, 'version'))->setValue($this->session($connection), ProtocolVersion::V5);
        $connection->setKeyspace('other');
        $this->assertSame('other', $connection->getKeyspace());

    }

    public function testFailedReadsAreTransportFailures(): void {
        $connection = $this->connect();
        $statement = $connection->queryAsync(self::NO_ANSWER);

        ScriptedNode::$failRead = self::broken();
        $this->assertRaises(StreamException::class, static fn () => $connection->drainAvailableResponses());
        $this->assertTrue($statement->isAbandoned());

        $connection = $this->connect();
        ScriptedNode::$failRead = self::broken();
        $this->assertRaises(StreamException::class, static fn () => $connection->query('SELECT 1'));
    }

    public function testFailedWritesReleaseTheirStreamIds(): void {
        $connection = $this->connect();

        ScriptedNode::$failWrite = self::broken();
        $this->assertRaises(StreamException::class, static fn () => $connection->queryAsync('SELECT 1'));

        $connection = $this->connect();
        ScriptedNode::$failWrite = self::broken();
        $this->assertRaises(StreamException::class, static fn () => $connection->query('SELECT 1'));

        // The connection that failed is replaced on the next call.
        $this->assertInstanceOf(\Cassandra\Response\Result\VoidResult::class, $this->connect()->query('SELECT 1'));
    }

    public function testFollowUpRequestsThatCannotBeSent(): void {
        // A listener runs between reading the PREPARE answer and sending the EXECUTE it leads to.
        $interferences = [
            ExceptionCode::CONNECTION_STREAM_ID_ALREADY_IN_USE->value => fn (Connection $connection, Request $prepare) => $this->registry($connection)->register((int) $prepare->getStream(), new Statement($connection, (int) $prepare->getStream(), $this->streamIds($connection)->getGeneration(), new Query('SELECT 1'))),
            ExceptionCode::CONNECTION_CHAINED_REQUEST_CONNECTION_GONE->value => static fn (Connection $connection) => $connection->disconnect(),
            ExceptionCode::STREAM_WRITE_FAILED->value => static function (): void {
                ScriptedNode::$failWrite = self::broken();
            },
        ];

        foreach ($interferences as $code => $interfere) {
            $connection = $this->connect(version: ProtocolVersion::V4, responder: static fn (Request $r) => $r->getOpcode() === Opcode::REQUEST_PREPARE
                ? ScriptedNode::frame(Opcode::RESPONSE_RESULT, self::warnings() . self::preparedWithOneMarker(), (int) $r->getStream(), 0x08)
                : null);
            $connection->registerWarningsListener(new class($connection, $interfere) implements WarningsListener {
                public function __construct(private readonly Connection $connection, private readonly Closure $interfere) {
                }

                public function onWarnings(array $warnings, Request $request, Response $response): void {
                    ($this->interfere)($this->connection, $request);
                }
            });

            $statement = $connection->queryAsync('SELECT ?', [1]);

            try {
                $statement->getResult();
                $this->fail("expected the follow-up to fail with $code");
            } catch (CassandraException $e) {
                $this->assertSame($code, $e->getCode(), $e->getMessage());
            }
        }
    }

    public function testNonBlockingResolution(): void {
        $connection = $this->connect();
        $pending = $connection->queryAsync(self::NO_ANSWER);

        // Another statement's answer is read first, and the pass goes round again.
        $other = $connection->queryAsync('SELECT 1');
        $this->assertFalse($connection->tryResolveStatement($pending));
        $this->assertTrue($other->isResultReady());

        // A read timeout on a read that was not to wait leaves the connection standing.
        ScriptedNode::$failRead = new StreamException('quiet', ExceptionCode::STREAM_TIMEOUT_DURING_READ->value);
        $this->assertFalse($connection->tryResolveStatement($pending));
        $this->assertTrue($connection->isConnected());
    }

    public function testQueryAllFollowsEveryPage(): void {
        $page = static function (int $value, ?string $pagingState): array {
            $flags = 0x0001 | ($pagingState !== null ? 0x0002 : 0);
            $state = $pagingState !== null ? pack('N', strlen($pagingState)) . $pagingState : '';

            return [Opcode::RESPONSE_RESULT, pack('N', 2) . pack('NN', $flags, 1) . $state
                . ScriptedNode::string('ks') . ScriptedNode::string('t') . ScriptedNode::string('c') . pack('n', 9)
                . pack('N', 1) . pack('N', 4) . pack('N', $value)];
        };
        $connection = $this->connect(responder: static fn (Request $r) => $r instanceof Query && $r->getQuery() === 'SELECT c'
            ? ($r->getOptions()->pagingState === null ? $page(1, 'next') : $page(2, null))
            : null);

        $pages = $connection->queryAll('SELECT c');
        $this->assertCount(2, $pages);
        $this->assertSame([['c' => 2]], $pages[1]->fetchAll());
    }

    public function testRepreparationAnsweredWithTheWrongKind(): void {
        $void = [Opcode::RESPONSE_RESULT, pack('N', 1)];
        $unprepared = [Opcode::RESPONSE_ERROR, pack('N', 0x2500) . ScriptedNode::string('unprepared') . pack('n', 2) . 'id'];

        // An auto-prepared async query whose PREPARE is answered with a void result.
        $connection = $this->connect(responder: static fn (Request $r) => $r->getOpcode() === Opcode::REQUEST_PREPARE ? $void : null);
        $statement = $connection->queryAsync('SELECT ?', [1]);
        $this->assertRaises(ConnectionException::class, static fn () => $statement->getResult());

        // An async execute reprepared and answered with a void result.
        $calls = 0;
        $connection = $this->connect(responder: static function (Request $r) use (&$calls, $void, $unprepared) {
            return match ($r->getOpcode()) {
                Opcode::REQUEST_PREPARE => $calls++ === 0 ? self::prepared() : $void,
                Opcode::REQUEST_EXECUTE => $unprepared,
                default => null,
            };
        });
        $prepared = $connection->prepare('SELECT 1');
        $statement = $connection->executeAsync($prepared, [], options: new ExecuteOptions());
        $this->assertRaises(ConnectionException::class, static fn () => $statement->getResult());
    }

    public function testSyncAutoPreparation(): void {
        $executeAnswer = [Opcode::RESPONSE_RESULT, pack('N', 1)];
        $connection = $this->connect(version: ProtocolVersion::V4, responder: static function (Request $r) use (&$executeAnswer) {
            return match ($r->getOpcode()) {
                Opcode::REQUEST_PREPARE => [Opcode::RESPONSE_RESULT, self::preparedWithOneMarker()],
                Opcode::REQUEST_EXECUTE => $executeAnswer,
                default => null,
            };
        });
        $this->assertInstanceOf(\Cassandra\Response\Result\VoidResult::class, $connection->query('SELECT ?', [1]));

        // An EXECUTE answered with something that is not a result.
        $executeAnswer = [Opcode::RESPONSE_READY, ''];

        try {
            $connection->query('SELECT ?', [2]);
            $this->fail('expected the non-result answer to be refused');
        } catch (ConnectionException $e) {
            $this->assertSame(ExceptionCode::CONNECTION_AUTO_PREPARE_UNEXPECTED_RESPONSE_REEXECUTE->value, $e->getCode());
        }
    }

    public function testUnpreparedForRequestsThatCannotBeReprepared(): void {
        $unprepared = [Opcode::RESPONSE_ERROR, pack('N', 0x2500) . ScriptedNode::string('unprepared') . pack('n', 2) . 'id'];
        $connection = $this->connect(responder: static fn (Request $r) => in_array($r->getOpcode(), [Opcode::REQUEST_QUERY, Opcode::REQUEST_EXECUTE], true) ? $unprepared : null);

        // A plain query has nothing to prepare again: the error is the answer.
        $this->assertRaises(ServerException::class, static fn () => $connection->query('SELECT 1', options: new QueryOptions(autoPrepare: false)));

        // A prepared result that never recorded its PREPARE, or recorded something else.
        $orphan = self::preparedResult();
        $this->assertRaises(ConnectionException::class, static fn () => $connection->execute($orphan));
        $foreign = self::preparedResult();
        $foreign->setRequest(new Query('SELECT 1'));
        $this->assertRaises(ConnectionException::class, static fn () => $connection->execute($foreign));
    }

    public function testWaitingForAStatementThatIsAlreadyAnswered(): void {
        $connection = $this->connect();
        $statement = $connection->queryAsync('SELECT 1');
        $connection->waitForStatements([$statement]);

        $response = $this->session($connection)->getNextResponseForStream($statement->getStreamId(), $statement->getStreamGeneration(), null, $statement);
        $this->assertSame($statement->getResponse(), $response);
    }

    public function testWaitsThatFindTheirAnswerReady(): void {
        $connection = $this->connect();

        $ready = $connection->queryAsync('SELECT 1');
        $connection->waitForStatements([$ready]);
        $this->assertSame($ready, $connection->waitForAnyStatement([$ready]));

        $failed = $connection->queryAsync('FAIL', options: new QueryOptions(autoPrepare: false));
        $this->assertRaises(ServerException::class, static fn () => $connection->getResponseForStatement($failed));

        $pending = $connection->queryAsync(self::NO_ANSWER);
        $answered = $connection->queryAsync('SELECT 1');
        $this->assertSame(1, $connection->tryResolveStatements([$pending, $answered]));
        $this->assertSame(0, $connection->tryResolveStatements([$pending]));

        // Reading an event finds a response first and puts it on its statement.
        $answer = $connection->queryAsync('SELECT 1');
        $this->assertNull($connection->tryReadNextEvent());
        $this->assertTrue($answer->isResultReady());
    }

    public function testWaitsThatRunOutOfTime(): void {
        $connection = $this->connect(requestTimeout: 0.05);

        $connection->queryAsync(self::NO_ANSWER, options: new QueryOptions(requestTimeoutInSeconds: 30.0));
        $connection->waitForAllPendingStatements(0.05);

        $expiring = $connection->queryAsync(self::NO_ANSWER);
        $this->assertRaises(RequestTimeoutException::class, static fn () => $connection->waitForAllPendingStatements());
        $this->assertTrue($expiring->isTimedOut());

        $expiring = $connection->queryAsync(self::NO_ANSWER);
        $this->assertRaises(RequestTimeoutException::class, static fn () => $connection->waitForAnyStatement([$expiring]));

        $late = $connection->queryAsync(self::NO_ANSWER);
        usleep(100_000);
        $this->assertRaises(RequestTimeoutException::class, static fn () => $connection->getResponseForStatement($late));
    }

    /**
     * @param class-string<CassandraException> $expected
     * @param Closure(): mixed $call
     */
    private function assertRaises(string $expected, Closure $call): void {
        try {
            $call();
            $this->fail("expected $expected");
        } catch (CassandraException $e) {
            $this->assertInstanceOf($expected, $e, get_class($e) . ': ' . $e->getMessage());
        }
    }

    private static function broken(): StreamException {
        return new StreamException('injected', ExceptionCode::STREAM_WRITE_FAILED->value);
    }

    /**
     * Answers queries at once, leaves SLOW unanswered and FAIL with an error.
     */
    private function connect(float $requestTimeout = 2.0, string $keyspace = '', ?Closure $responder = null, ?ProtocolVersion $version = null): Connection {
        ScriptedNode::reset(static function (Request $request) use ($responder) {
            $answer = $responder !== null ? $responder($request) : null;
            if ($answer !== null) {
                return $answer;
            }

            return match (true) {
                $request instanceof Query && $request->getQuery() === self::NO_ANSWER => '',
                $request instanceof Query && $request->getQuery() === 'FAIL' => [Opcode::RESPONSE_ERROR, pack('N', 0x2200) . ScriptedNode::string('bad')],
                $request instanceof Query && str_starts_with($request->getQuery(), 'USE') => [Opcode::RESPONSE_RESULT, pack('N', 3) . ScriptedNode::string('ks')],
                $request->getOpcode() === Opcode::REQUEST_PREPARE => self::prepared(),
                in_array($request->getOpcode(), [Opcode::REQUEST_QUERY, Opcode::REQUEST_EXECUTE], true) => [Opcode::RESPONSE_RESULT, pack('N', 1)],
                default => null,
            };
        });
        $connection = new Connection([new ScriptedNodeConfig()], $keyspace, new ConnectionOptions(heartbeatIntervalInSeconds: null, requestTimeoutInSeconds: $requestTimeout, allowedProtocolVersions: $version !== null ? [$version] : ProtocolVersion::PREFERRED_ORDER));
        $connection->connect();

        return $connection;
    }

    /**
     * @return array{Opcode, string}
     */
    private static function prepared(string $id = 'id'): array {
        return [Opcode::RESPONSE_RESULT, pack('N', 4) . pack('n', strlen($id)) . $id . pack('NNN', 0, 0, 0) . pack('NN', 0x0004, 0)];
    }

    private static function preparedResult(): PreparedResult {
        $body = self::prepared()[1];

        return new PreparedResult(new \Cassandra\Protocol\Header(ProtocolVersion::V4, 0, 1, Opcode::RESPONSE_RESULT, strlen($body)), new \Cassandra\Response\StreamReader($body));
    }

    private static function preparedWithOneMarker(): string {
        return pack('N', 4) . pack('n', 2) . 'id' . pack('NNN', 0x0001, 1, 0) . ScriptedNode::string('ks') . ScriptedNode::string('t')
            . ScriptedNode::string('a') . pack('n', 0x0009) . pack('NN', 0x0004, 0);
    }

    private function registry(Connection $connection): StatementRegistry {
        /** @var StatementRegistry */
        return (new ReflectionProperty(Session::class, 'statements'))->getValue($this->session($connection));
    }

    private function session(Connection $connection): Session {
        return self::sessionOf($connection);
    }

    private function streamIds(Connection $connection): StreamIdPool {
        /** @var StreamIdPool */
        return (new ReflectionProperty(Session::class, 'streamIds'))->getValue($this->session($connection));
    }

    private static function warnings(): string {
        return pack('n', 1) . ScriptedNode::string('a warning');
    }
}
