<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Compression\Lz4Compressor;
use Cassandra\Connection;
use Cassandra\Connection\ConnectionOptions;
use Cassandra\Connection\RandomSelector;
use Cassandra\Connection\ResponseReader;
use Cassandra\Connection\SocketNodeConfig;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Consistency;
use Cassandra\Exception\CassandraException;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ResponseException;
use Cassandra\Exception\ServerException;
use Cassandra\Exception\SocketException;
use Cassandra\Exception\StringMathException;
use Cassandra\Exception\ValueException;
use Cassandra\Protocol\Header;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Batch;
use Cassandra\Request\Execute;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Request\Query;
use Cassandra\Response\Error\Context\ErrorContext;
use Cassandra\Response\Result\CachedPreparedResult;
use Cassandra\Response\Result\PreparedResult;
use Cassandra\Response\Result\RowsResult;
use Cassandra\Response\ResultIterator;
use Cassandra\Response\StreamReader;
use Cassandra\StringMath\DecimalCalculator;
use Cassandra\Test\Support\Faults;
use Cassandra\Test\Support\ScriptedNode;
use Cassandra\Test\Support\ScriptedNodeConfig;
use Cassandra\Type;
use Cassandra\TypeInfo\MapCollectionInfo;
use Cassandra\TypeInfo\SimpleTypeInfo;
use Cassandra\TypeInfo\VectorInfo;
use Cassandra\Value;
use Closure;
use DateTimeImmutable;
use ReflectionMethod;
use ReflectionProperty;

/**
 * The small branches left over: wrong error contexts, protocol helpers, and
 * guards that a broken PHP or a 32-bit build would reach.
 */
final class RemainingErrorPathsTest extends AbstractUnitTestCase {
    protected function setUp(): void {
        parent::setUp();

        if (PHP_INT_SIZE < 8 && $this->name() !== 'test32BitBigintRange') {
            $this->markTestSkipped('needs 64-bit integers');
        }
    }

    protected function tearDown(): void {
        Faults::reset();
        ScriptedNode::reset();
        putenv('APP_CASSANDRA_DEBUG');
        (new ReflectionProperty(DecimalCalculator::class, 'calculator'))->setValue(null, null);
        parent::tearDown();
    }

    public function test32BitBigintRange(): void {
        if (PHP_INT_SIZE !== 4) {
            $this->markTestSkipped('only a 32-bit build reaches these');
        }

        $this->assertRaises(ValueException::class, static fn () => Value\Bigint::fromBinary(pack('NN', 1, 0)));
        $this->assertRaises(ValueException::class, static fn () => Value\Bigint::fromBinary("\x00\x00\x00\x00\x80\x00\x00\x00"));
        $this->assertRaises(ValueException::class, static fn () => Value\Bigint::fromBinary("\xFF\xFF\xFF\xFF\x7F\xFF\xFF\xFF"));
        Faults::inject('unpack', false);
        $this->assertRaises(ValueException::class, static fn () => Value\Bigint::fromBinary(pack('NN', 0, 1)));
    }

    public function testDebugModeAppendsTheContextToTheMessage(): void {
        putenv('APP_CASSANDRA_DEBUG=1');
        $this->assertStringContainsString('"a":1', (new ValueException('m', 1, ['a' => 1]))->getMessage());
    }

    public function testNodeHelpers(): void {
        $nodes = [new StreamNodeConfig(host: 'a'), new StreamNodeConfig(host: 'b'), new StreamNodeConfig(host: 'c')];
        $this->assertCount(3, (new RandomSelector())->order($nodes));

        Faults::inject('extension_loaded', false);
        $this->assertRaises(SocketException::class, static fn () => new SocketNodeConfig());

        $node = new ScriptedNode(new ScriptedNodeConfig());
        $describe = new ReflectionMethod($node, 'describeTimeout');
        $this->assertSame('INF', $describe->invoke($node, INF));
        $this->assertSame('-INF', $describe->invoke($node, -INF));

        // The byte count starts over rather than overflowing.
        ScriptedNode::reset(static fn () => ScriptedNode::frame(Opcode::RESPONSE_READY, '', 0));
        (new ReflectionProperty(Connection\NodeImplementation::class, 'receivedByteCount'))->setValue($node, PHP_INT_MAX);
        $request = new Query('SELECT 1');
        $request->setStream(0);
        $node->writeRequest($request);
        $node->read(9, null);
        $this->assertSame(9, $node->getReceivedByteCount());

        Faults::injectWhere('unpack', static fn (mixed $format): bool => is_string($format) && str_starts_with($format, 'Cflags'), false);
        ScriptedNode::reset(static fn () => ScriptedNode::frame(Opcode::RESPONSE_READY, '', 0));
        $node->writeRequest($request);
        $this->assertRaises(ConnectionException::class, static fn () => (new ResponseReader())->readResponse($node, ProtocolVersion::V4, null));
    }

    public function testProtocolVersionHelpers(): void {
        $this->assertSame(0x84, ProtocolVersion::V4->asIncomingVersion());
        $this->assertNull(ProtocolVersion::getHighestSupportedVersion([ProtocolVersion::V3], [ProtocolVersion::V5]));
        $this->assertNull(ProtocolVersion::getHighestSupportedVersion([], []));

        $versions = pack('n', 1) . ScriptedNode::string('PROTOCOL_VERSIONS') . pack('n', 1) . ScriptedNode::string('3/v3');
        ScriptedNode::reset(static fn (\Cassandra\Request\Request $request) => $request->getOpcode() === Opcode::REQUEST_OPTIONS ? [Opcode::RESPONSE_SUPPORTED, $versions] : null);
        $connection = new Connection([new ScriptedNodeConfig()], options: new ConnectionOptions(allowedProtocolVersions: [ProtocolVersion::V4], heartbeatIntervalInSeconds: null));
        $this->assertRaises(ConnectionException::class, static fn () => $connection->connect());
    }

    public function testResultHelpers(): void {
        $header = new Header(ProtocolVersion::V4, 0, 1, Opcode::RESPONSE_RESULT, 4);
        $this->assertRaises(ResponseException::class, static fn () => new CachedPreparedResult($header, new StreamReader(pack('N', 4))));

        $prepared = $this->preparedResult();
        $this->assertSame(0, (new Batch())->replacePreparedStatement($prepared));

        // Iterating without asking for the current row first.
        $rows = $this->rows(3);
        $iterator = new ResultIterator($rows);
        $iterator->next();
        $this->assertSame(['c' => 2], $iterator->current());
        $iterator->rewind();
        $this->assertSame(['c' => 1], $iterator->current());

        // Continuing from a page that carries a paging state, and a keyspace of its own.
        $page = $this->rows(1, pagingState: 'next');
        $page->setPreviousResult($prepared);
        $execute = new Execute($page, [], Consistency::ONE, new ExecuteOptions(keyspace: 'own'));
        $this->assertSame('next', $execute->getOptions()->pagingState);
        $execute->applyDefaultKeyspace('other');
        $this->assertSame('own', $execute->getOptions()->keyspace);
    }

    public function testServerExceptionsRefuseAContextOfAnotherKind(): void {
        foreach (glob(__DIR__ . '/../../src/Exception/ServerException/*.php') ?: [] as $file) {
            $class = 'Cassandra\\Exception\\ServerException\\' . basename($file, '.php');
            if (!method_exists($class, 'getErrorContext') || (new ReflectionMethod($class, 'getErrorContext'))->getDeclaringClass()->getName() !== $class) {
                continue;
            }
            /** @var ServerException $exception */
            $exception = new $class(new ErrorContext(), 'm', 1, []);
            $this->assertRaises(ResponseException::class, static fn () => $exception->getErrorContext());
        }

        $context = ['error_code' => 1, 'error_type' => 'X', 'protocol_version' => '4/v4', 'stream_id' => 1, 'tracing_uuid' => null, 'warnings' => [], 'payload' => null];
        $base = (new \ReflectionClass(ServerException::class))->newInstance(new ErrorContext(), 'm', 1, $context);
        $this->assertInstanceOf(ServerException::class, $base);
        $this->assertSame($context, $base->getContext());
        $this->assertSame($context, $base->context());
        $this->assertSame([], $base->getErrorContext()->toArray());
    }

    public function testStatementsNotYetAnswered(): void {
        ScriptedNode::reset(static fn (\Cassandra\Request\Request $request) => match ($request->getOpcode()) {
            Opcode::REQUEST_OPTIONS => [Opcode::RESPONSE_SUPPORTED, pack('n', 0)],
            Opcode::REQUEST_STARTUP => [Opcode::RESPONSE_READY, ''],
            default => null,
        });
        $connection = new Connection([new ScriptedNodeConfig()], options: new ConnectionOptions(heartbeatIntervalInSeconds: null, requestTimeoutInSeconds: 5.0));
        $statement = $connection->queryAsync('SELECT 1', options: new \Cassandra\Request\Options\QueryOptions(autoPrepare: false));

        $this->assertNull($statement->tryGetPreparedResult());
        $this->assertNull($statement->tryGetRowsResult());
        $this->assertNull($statement->tryGetSchemaChangeResult());
        $this->assertNull($statement->tryGetSetKeyspaceResult());
    }

    public function testTypeCompatibility(): void {
        $int = new SimpleTypeInfo(Type::INT);
        $map = new MapCollectionInfo($int, $int, false);
        $this->assertTrue($map->isBinaryCompatibleWith(new MapCollectionInfo($int, $int, false)));
        $this->assertFalse((new VectorInfo($int, 2))->isBinaryCompatibleWith(new VectorInfo($int, 3)));

        $vector = Value\Vector::fromValue([1.0], Type::FLOAT, 1);
        $this->assertInstanceOf(VectorInfo::class, (new ReflectionMethod($vector, 'binaryTypeInfo'))->invoke($vector));
    }

    public function testUnknownWriteTypesAreRefused(): void {
        foreach ([0x1100 => pack('nNN', 1, 1, 1), 0x1500 => pack('nNNN', 1, 1, 1, 1)] as $code => $prefix) {
            $body = pack('N', $code) . ScriptedNode::string('m') . $prefix . ScriptedNode::string('SIDEWAYS');
            $this->assertRaises(ResponseException::class, static fn () => (new ReflectionMethod(ResponseReader::class, 'createResponse'))->invoke(new ResponseReader(), new Header(ProtocolVersion::V4, 0, 1, Opcode::RESPONSE_ERROR, strlen($body)), $body));
        }
    }

    public function testValueFailuresUnderneath(): void {
        // A decimal whose unscaled part is empty.
        $this->assertRaises(ValueException::class, static fn () => Value\Decimal::fromBinary(pack('N', 0)));

        // The calculator failing.
        DecimalCalculator::set(new FailingCalculator());
        $this->assertRaises(ValueException::class, static fn () => Value\Varint::fromBinary(str_repeat("\x7F", 20)));
        $this->assertRaises(ValueException::class, static fn () => Value\Varint::fromValue('123456789012345678901234567890')->getBinary());

        // Timestamps that do not fit in a signed 64-bit count of milliseconds.
        $this->assertRaises(ValueException::class, static fn () => new Value\Timestamp((new DateTimeImmutable('@0'))->setTimestamp(intdiv(PHP_INT_MIN, 1000) - 1)));
        $this->assertRaises(ValueException::class, static fn () => new Value\Timestamp((new DateTimeImmutable('@0'))->setTimestamp(intdiv(PHP_INT_MAX, 1000))->modify('+999 milliseconds')));

        Faults::injectWhere('unpack', static fn (mixed $format): bool => $format === 'v2', false);
        $this->assertNotSame('', (new Lz4Compressor(preferExtension: false))->compressBlock(str_repeat('abcdefgh', 64) . 'tail-of-the-input'));
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
            $this->assertInstanceOf($expected, $e, $e->getMessage());
        }
    }

    private function preparedResult(): PreparedResult {
        $body = pack('N', 4) . pack('n', 2) . 'id' . pack('NNN', 0, 0, 0) . pack('NN', 0x0004, 0);

        return new PreparedResult(new Header(ProtocolVersion::V4, 0, 1, Opcode::RESPONSE_RESULT, strlen($body)), new StreamReader($body));
    }

    private function rows(int $count, ?string $pagingState = null): RowsResult {
        $flags = 0x0001 | ($pagingState === null ? 0 : 0x0002);
        $body = pack('N', 2) . pack('NN', $flags, 1) . ($pagingState === null ? '' : pack('N', strlen($pagingState)) . $pagingState)
            . ScriptedNode::string('ks') . ScriptedNode::string('t') . ScriptedNode::string('c') . pack('n', 9) . pack('N', $count);
        for ($i = 1; $i <= $count; $i++) {
            $body .= pack('N', 4) . pack('N', $i);
        }

        return new RowsResult(new Header(ProtocolVersion::V4, 0, 1, Opcode::RESPONSE_RESULT, strlen($body)), new StreamReader($body));
    }
}

/**
 * A calculator that fails, as the native one does on a value it cannot handle.
 */
final class FailingCalculator extends DecimalCalculator {
    public function add1(string $decimal): string {
        throw new StringMathException('injected', 0);
    }

    public function addUnsignedInt8(string $decimal, int $addend): string {
        throw new StringMathException('injected', 0);
    }

    public function divideBy256(string $decimal): array {
        throw new StringMathException('injected', 0);
    }

    public function multiplyBy256(string $decimal): string {
        throw new StringMathException('injected', 0);
    }

    public function sub1(string $decimal): string {
        throw new StringMathException('injected', 0);
    }
}
