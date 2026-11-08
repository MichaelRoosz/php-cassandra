<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\Authenticator;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\Deadline;
use Cassandra\Connection\NodeConfig;
use Cassandra\Connection\NodeHealth;
use Cassandra\Connection\NodeImplementation;
use Cassandra\Connection\PreparedResultCache;
use Cassandra\Connection\RequestCompressor;
use Cassandra\Connection\StreamIdPool;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Consistency;
use Cassandra\EventType;
use Cassandra\Exception\CassandraException;
use Cassandra\Exception\CompressionException;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\RequestException;
use Cassandra\Exception\ResponseException;
use Cassandra\Exception\ServerException;
use Cassandra\Exception\StreamException;
use Cassandra\Exception\TypeInfoException;
use Cassandra\Exception\ValueException;
use Cassandra\Exception\ValueFactoryException;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Batch;
use Cassandra\Request\Execute;
use Cassandra\Request\Options\BatchOptions;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Request\Options\PrepareOptions;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Request\Prepare;
use Cassandra\Request\Query;
use Cassandra\Request\Register;
use Cassandra\Request\Request;
use Cassandra\Response\Event;
use Cassandra\Response\Result\PreparedResult;
use Cassandra\Response\Result\RowsResult;
use Cassandra\Response\Result\VoidResult;
use Cassandra\Test\Support\ScriptedNode;
use Cassandra\Test\Support\ScriptedNodeConfig;
use Cassandra\Value\ValueEncodeConfig;
use Closure;
use ReflectionProperty;
use RuntimeException;
use stdClass;

/**
 * What the connection makes of answers a healthy node does not give, driven by a
 * {@see ScriptedNode}: unexpected response types at every stage, failing
 * handshakes, and the bookkeeping helpers at their limits.
 */
final class ConnectionLayerErrorPathsTest extends AbstractUnitTestCase {
    protected function tearDown(): void {
        ScriptedNode::reset();
        parent::tearDown();
    }

    public function testBookkeepingAtItsLimits(): void {
        $deadlines = new Deadline(30.0);
        $deadlines->setRequestTimeout(30.0);
        $this->assertSame(30.0, $deadlines->getRequestTimeout());

        $health = new NodeHealth();
        $node = new StreamNodeConfig();
        for ($i = 0; $i < 20; $i++) {
            $health->recordFailure($node);
        }
        $this->assertFalse($health->isAvailable($node));

        $cache = new PreparedResultCache(0);
        $cache->store(new Prepare('a'), $this->preparedResult(), ProtocolVersion::V4);
        $this->assertNull($cache->get(new Prepare('a')));
        $small = new PreparedResultCache(1);
        $small->store(new Prepare('a'), $this->preparedResult(), ProtocolVersion::V4);
        $small->store(new Prepare('b'), $this->preparedResult(), ProtocolVersion::V4);
        $this->assertNull($small->get(new Prepare('a')));
        $this->assertNotNull($small->get(new Prepare('b')));

        $pool = new StreamIdPool();
        (new ReflectionProperty(StreamIdPool::class, 'generation'))->setValue($pool, PHP_INT_MAX);
        $pool->reset();
        $this->assertSame(0, $pool->getGeneration());

        $inner = new ScriptedNode(new ScriptedNodeConfig());
        $compressor = new RequestCompressor($inner, 'lz4');
        $compressor->write('x');
        $this->assertSame('', $compressor->readAvailableDataFromSource(1, 1, -1.0));
    }

    public function testConnectionAccessors(): void {
        $connection = $this->connect([
            Opcode::REQUEST_QUERY->value => static fn (Request $r) => str_starts_with((string) ($r instanceof Query ? $r->getQuery() : ''), 'USE') ? [Opcode::RESPONSE_RESULT, pack('N', 3) . ScriptedNode::string('ks')] : self::rows(7),
            Opcode::REQUEST_PREPARE->value => self::prepared(),
        ]);

        $this->assertSame(Consistency::ONE, $connection->getConsistency());
        $this->assertSame($connection, $connection->withConsistency(Consistency::QUORUM));
        $this->assertSame(Consistency::QUORUM, $connection->getConsistency());
        $connection->configureValueEncoding(new ValueEncodeConfig());
        $this->assertSame(4, $connection->getVersion());
        $this->assertNotNull($connection->getNode());
        $this->assertInstanceOf(PreparedResult::class, $connection->prepareAsync('SELECT 1', new PrepareOptions())->getPreparedResult());
        $this->assertCount(1, $connection->queryAll('SELECT 1'));
        $this->assertSame($connection, $connection->withKeyspace('ks'));
        $this->assertSame('ks', $connection->getKeyspace());
        $connection->setRequestTimeout(30.0);
        $connection->setRequestTimeout(30.0);
    }

    public function testExecuteNeedsAPreparedStatement(): void {
        $void = new VoidResult(new \Cassandra\Protocol\Header(ProtocolVersion::V4, 0, 1, Opcode::RESPONSE_RESULT, 4), new \Cassandra\Response\StreamReader(pack('N', 1)));
        $this->assertRaises(RequestException::class, static fn () => new Execute($void, []));

        $rows = self::rowsResult();
        $this->assertRaises(RequestException::class, static fn () => new Execute($rows, []));

        $execute = new Execute($this->preparedResult(), [], Consistency::ONE, new ExecuteOptions(skipMetadata: true));
        $this->assertSame([], $execute->getValues());
        $execute->setVersion(ProtocolVersion::V5);
        $execute->setStream(1);
        $this->assertRaises(RequestException::class, static fn () => $execute->getBody());

        $batch = new Batch();
        $this->assertSame(0, $batch->replacePreparedStatement($this->preparedResult()));
    }

    public function testHandshakeFailures(): void {
        $this->assertConnectFails([Opcode::REQUEST_OPTIONS->value => [Opcode::RESPONSE_READY, '']]);
        $this->assertConnectFails([Opcode::REQUEST_STARTUP->value => [Opcode::RESPONSE_SUPPORTED, pack('n', 0)]]);
        $this->assertConnectFails([Opcode::REQUEST_STARTUP->value => [Opcode::RESPONSE_AUTHENTICATE, ScriptedNode::string('X')]]);
        $this->assertConnectFails([
            Opcode::REQUEST_STARTUP->value => [Opcode::RESPONSE_AUTHENTICATE, ScriptedNode::string('X')],
            Opcode::REQUEST_AUTH_RESPONSE->value => [Opcode::RESPONSE_READY, ''],
        ], new ScriptedNodeConfig(username: 'u', password: 'p'));

        $versions = pack('n', 1) . ScriptedNode::string('PROTOCOL_VERSIONS') . pack('n', 1) . ScriptedNode::string('3/v3');
        $this->assertConnectFails(
            [Opcode::REQUEST_OPTIONS->value => [Opcode::RESPONSE_SUPPORTED, $versions]],
            options: new ConnectionOptions(initialProtocolVersion: ProtocolVersion::V5, allowedProtocolVersions: [ProtocolVersion::V5], heartbeatIntervalInSeconds: null),
        );
    }

    public function testNodeImplementationsThatFail(): void {
        $this->assertConnectFails([], new ScriptedNodeConfig(stdClass::class));
        $this->assertConnectFails([], new ScriptedNodeConfig(ExplodingNode::class));

        // Whatever an authenticator raises is reported as its own kind, so it can be handled as one.
        $authenticate = [Opcode::REQUEST_STARTUP->value => [Opcode::RESPONSE_AUTHENTICATE, ScriptedNode::string('X')]];
        foreach ([
            ConnectionException::class => new RuntimeException('native'),
            CompressionException::class => new CompressionException('c', 0),
            StreamException::class => new StreamException('n', 0),
            RequestException::class => new RequestException('r', 0),
            ResponseException::class => new ResponseException('s', 0),
            ValueException::class => new ValueException('v', 0),
            ValueFactoryException::class => new ValueFactoryException('f', 0),
            TypeInfoException::class => new TypeInfoException('t', 0),
        ] as $expected => $failure) {
            $config = new ScriptedNodeConfig(authenticator: new ThrowingAuthenticator($failure));
            $exception = $this->assertConnectFails($authenticate, $config);
            $this->assertTrue(is_a($exception, $expected) || is_a($exception, ConnectionException::class), get_class($exception));
        }
    }

    public function testNonBlockingReadsReturnWhatHasArrived(): void {
        $event = ScriptedNode::frame(Opcode::RESPONSE_EVENT, ScriptedNode::string('STATUS_CHANGE') . ScriptedNode::string('UP') . "\4\x7F\0\0\1" . pack('N', 9042), -1);
        $connection = $this->connect([
            Opcode::REQUEST_REGISTER->value => static fn (Request $r) => ScriptedNode::frame(Opcode::RESPONSE_READY, '', (int) $r->getStream()) . $event,
            Opcode::REQUEST_QUERY->value => static fn (Request $r) => ($r instanceof Query && $r->getQuery() === 'FAIL') ? [Opcode::RESPONSE_ERROR, pack('N', 0x2200) . ScriptedNode::string('bad')] : self::rows(1),
        ]);

        $connection->syncRequest(new Register([EventType::STATUS_CHANGE]));
        $this->assertInstanceOf(Event::class, $connection->tryReadNextEvent());

        $connection->asyncRequest(new Query('SELECT 1'));
        $this->assertNotNull($connection->tryReadNextResponse());

        $statement = $connection->queryAsync('SELECT 1');
        $this->assertTrue($connection->tryResolveStatement($statement));
        $this->assertTrue($connection->tryResolveStatement($statement));
        $this->assertInstanceOf(RowsResult::class, $connection->getResponseForStatement($statement));

        $connection->asyncRequest(new Query('SELECT 1'));
        $this->assertNotNull($connection->waitForNextResponse(1.0));

        $failed = $connection->queryAsync('FAIL', options: new QueryOptions(autoPrepare: false));
        $connection->waitForStatements([$failed]);
        $this->assertRaises(ServerException::class, static fn () => $connection->getResponseForStatement($failed));
    }

    public function testOptionsThatCannotBeHonoured(): void {
        $this->assertRaises(ConnectionException::class, static fn () => new ConnectionOptions(requestTimeoutInSeconds: NAN));
        $this->assertRaises(ConnectionException::class, static fn () => new ConnectionOptions(heartbeatIntervalInSeconds: INF));
        $this->assertRaises(ConnectionException::class, static fn () => new ConnectionOptions(heartbeatTimeoutInSeconds: NAN));
        $this->assertArrayHasKey('CQL_VERSION', (new ConnectionOptions())->toArray());
    }

    public function testQuotedBindMarkersAreFoundAroundComments(): void {
        $values = ['Quoted' => 1, 'b"c' => 2];
        $query = new Query("SELECT -- :\"No\"\n :\"Quoted\" // :\"Nope\"\n, :\"b\"\"c\" FROM t", $values, Consistency::ONE, new QueryOptions(autoPrepare: false));
        $query->setVersion(ProtocolVersion::V4);
        $query->setStream(1);
        $this->assertStringContainsString('Quoted', $query->getBody());
        $this->assertStringContainsString('b"c', $query->getBody());
    }

    public function testRepreparationGoneWrong(): void {
        $unprepared = [Opcode::RESPONSE_ERROR, pack('N', 0x2500) . ScriptedNode::string('unprepared') . pack('n', 2) . 'id'];
        $void = [Opcode::RESPONSE_RESULT, pack('N', 1)];
        $supported = [Opcode::RESPONSE_SUPPORTED, pack('n', 0)];

        // Auto-prepare answered with something other than a prepared result, or the re-execution with something other than a result.
        $this->assertRaises(CassandraException::class, fn () => $this->connect([Opcode::REQUEST_PREPARE->value => $void])->query('SELECT ?', [1]));
        $this->assertRaises(CassandraException::class, fn () => $this->connect([Opcode::REQUEST_PREPARE->value => self::prepared(), Opcode::REQUEST_EXECUTE->value => $supported])->query('SELECT ?', [1]));

        // Repreparation after UNPREPARED answered badly, or the re-sent execute answered badly.
        foreach ([$void, $supported] as $prepareAnswer) {
            $connection = $this->connect([Opcode::REQUEST_PREPARE->value => self::prepared()]);
            $prepared = $connection->prepare('SELECT 1');
            ScriptedNode::reset(self::answers([Opcode::REQUEST_EXECUTE->value => $unprepared, Opcode::REQUEST_PREPARE->value => $prepareAnswer]));
            $this->assertRaises(CassandraException::class, static fn () => $connection->execute($prepared));
        }

        $connection = $this->connect([Opcode::REQUEST_PREPARE->value => self::prepared()]);
        $prepared = $connection->prepare('SELECT 1');
        $calls = 0;
        ScriptedNode::reset(self::answers([
            Opcode::REQUEST_EXECUTE->value => static function () use (&$calls, $unprepared, $supported) {
                return $calls++ === 0 ? $unprepared : $supported;
            },
            Opcode::REQUEST_PREPARE->value => self::prepared(),
        ]));
        $this->assertRaises(CassandraException::class, static fn () => $connection->execute($prepared));

        // A batch holding the statement, re-sent and answered badly.
        $connection = $this->connect([Opcode::REQUEST_PREPARE->value => self::prepared()]);
        $prepared = $connection->prepare('SELECT 1');
        $batch = $connection->createBatchRequest();
        $batch->appendPreparedStatement($prepared);
        $calls = 0;
        ScriptedNode::reset(self::answers([
            Opcode::REQUEST_BATCH->value => static function () use (&$calls, $unprepared, $supported) {
                return $calls++ === 0 ? $unprepared : $supported;
            },
            Opcode::REQUEST_PREPARE->value => self::prepared(),
        ]));
        $this->assertRaises(CassandraException::class, static fn () => $connection->batch($batch));
    }

    public function testRequestsRefuseOptionsTheirProtocolLacks(): void {
        $v4 = static function (Request $request): string {
            $request->setVersion(ProtocolVersion::V4);
            $request->setStream(1);

            return $request->getBody();
        };

        $this->assertRaises(RequestException::class, static fn () => $v4(new Query('SELECT 1', [], Consistency::ONE, new QueryOptions(keyspace: 'ks'))));
        $this->assertRaises(RequestException::class, static fn () => $v4(new Query('SELECT 1', [], Consistency::ONE, new QueryOptions(nowInSeconds: 1))));
        $this->assertRaises(RequestException::class, static fn () => $v4(new Batch(options: new BatchOptions(keyspace: 'ks'))));
        $this->assertRaises(RequestException::class, static fn () => $v4(new Batch(options: new BatchOptions(nowInSeconds: 1))));
        $this->assertRaises(RequestException::class, static fn () => $v4(new Prepare('SELECT 1', new PrepareOptions(keyspace: 'ks'))));

        $query = new Query('SELECT ?', [true, false], Consistency::ONE, new QueryOptions(autoPrepare: false));
        $this->assertStringContainsString("\1", $v4($query));
        $this->assertSame(Opcode::REQUEST_QUERY, $query->getOpcode());
        $this->assertSame(ProtocolVersion::V4, $query->getProtocolVersion());
        $this->assertSame(4, $query->getVersion());
        $this->assertNull($query->getPayload());
        $query->setFlags(0);
        $this->assertSame(0, $query->getFlags());
        $this->assertStringStartsWith("\x04", (string) $query);
    }

    public function testUnexpectedResponseTypesAreRefused(): void {
        $supported = [Opcode::RESPONSE_SUPPORTED, pack('n', 0)];
        $connection = $this->connect([Opcode::REQUEST_PREPARE->value => self::prepared()]);
        $prepared = $connection->prepare('SELECT 1');

        ScriptedNode::reset(self::answers([
            Opcode::REQUEST_QUERY->value => $supported,
            Opcode::REQUEST_EXECUTE->value => $supported,
            Opcode::REQUEST_BATCH->value => $supported,
            Opcode::REQUEST_PREPARE->value => $supported,
        ]));

        $this->assertRaises(ConnectionException::class, static fn () => $connection->query('SELECT 1'));
        $this->assertRaises(ConnectionException::class, static fn () => $connection->execute($prepared));
        $this->assertRaises(ConnectionException::class, static fn () => $connection->batch($connection->createBatchRequest()));
        $this->assertRaises(ConnectionException::class, static fn () => $connection->prepare('SELECT 2'));
        $this->assertRaises(ConnectionException::class, static fn () => $connection->setKeyspace('ks'));
    }

    /**
     * @param array<int, mixed> $answers opcode value => answer, or a closure of the request
     */
    private static function answers(array $answers): Closure {
        return static function (Request $request) use ($answers): mixed {
            $answer = $answers[$request->getOpcode()->value] ?? null;

            return $answer instanceof Closure ? $answer($request) : $answer;
        };
    }

    /**
     * @param array<int, mixed> $answers
     */
    private function assertConnectFails(array $answers, ?NodeConfig $config = null, ?ConnectionOptions $options = null): CassandraException {
        ScriptedNode::reset(self::answers($answers));
        $connection = new Connection([$config ?? new ScriptedNodeConfig()], options: $options ?? new ConnectionOptions(heartbeatIntervalInSeconds: null, requestTimeoutInSeconds: 2.0));

        try {
            $connection->connect();
            $this->fail('expected the connection to fail');
        } catch (CassandraException $e) {
            $this->addToAssertionCount(1);

            return $e;
        }
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

    /**
     * @param array<int, mixed> $answers
     */
    private function connect(array $answers): Connection {
        ScriptedNode::reset(self::answers($answers));
        $connection = new Connection([new ScriptedNodeConfig()], options: new ConnectionOptions(heartbeatIntervalInSeconds: null, requestTimeoutInSeconds: 2.0));
        $connection->connect();

        return $connection;
    }

    /**
     * @return array{Opcode, string}
     */
    private static function prepared(): array {
        return [Opcode::RESPONSE_RESULT, pack('N', 4) . pack('n', 2) . 'id' . pack('NNN', 0, 0, 0) . pack('NN', 0x0004, 0)];
    }

    private function preparedResult(): PreparedResult {
        $body = self::prepared()[1];
        $result = new PreparedResult(new \Cassandra\Protocol\Header(ProtocolVersion::V4, 0, 1, Opcode::RESPONSE_RESULT, strlen($body)), new \Cassandra\Response\StreamReader($body));
        $result->setRequest(new Prepare('SELECT 1'));

        return $result;
    }

    /**
     * @return array{Opcode, string}
     */
    private static function rows(int $value): array {
        return [Opcode::RESPONSE_RESULT, pack('N', 2) . pack('NN', 1, 1) . ScriptedNode::string('ks') . ScriptedNode::string('t') . ScriptedNode::string('c') . pack('n', 9) . pack('N', 1) . pack('N', 4) . pack('N', $value)];
    }

    private static function rowsResult(): RowsResult {
        $body = self::rows(1)[1];
        $result = new RowsResult(new \Cassandra\Protocol\Header(ProtocolVersion::V4, 0, 1, Opcode::RESPONSE_RESULT, strlen($body)), new \Cassandra\Response\StreamReader($body));

        return $result;
    }
}

/**
 * A transport whose connect fails with an error that is not the library's.
 */
final class ExplodingNode extends NodeImplementation implements \Cassandra\Connection\IoNode {
    public function __construct(private readonly NodeConfig $config) {
    }

    public function close(): void {
    }

    public function connect(): void {
        throw new RuntimeException('a native failure inside a custom transport');
    }

    public function getConfig(): NodeConfig {
        return $this->config;
    }

    public function readAvailableDataFromSource(int $expectedLength, int $upperBoundaryLength, ?float $readDeadline): string {
        return '';
    }

    public function write(string $data): void {
    }

    public function writeRequest(Request $request): void {
    }
}

final class ThrowingAuthenticator implements Authenticator {
    public function __construct(private readonly \Throwable $failure) {
    }

    public function authenticationSucceeded(?string $token): void {
    }

    public function evaluateChallenge(?string $challenge): string {
        return '';
    }

    public function initialResponse(string $serverAuthenticator): string {
        throw $this->failure;
    }
}
