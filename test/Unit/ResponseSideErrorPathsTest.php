<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Connection;
use Cassandra\Connection\ResponseReader;
use Cassandra\Connection\StreamNodeConfig;
use Cassandra\Exception\CassandraException;
use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ResponseException;
use Cassandra\Exception\ServerException;
use Cassandra\Exception\StatementException;
use Cassandra\Protocol\Header;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Query;
use Cassandra\Response\Error;
use Cassandra\Response\Event;
use Cassandra\Response\Event\Data\SchemaChangeTarget;
use Cassandra\Response\Event\SchemaChangeEvent;
use Cassandra\Response\Event\StatusChangeEvent;
use Cassandra\Response\Event\TopologyChangeEvent;
use Cassandra\Response\Response;
use Cassandra\Response\Result;
use Cassandra\Response\Result\Data\RowsData;
use Cassandra\Response\Result\Data\SetKeyspaceData;
use Cassandra\Response\Result\Data\VoidData;
use Cassandra\Response\Result\PreparedResult;
use Cassandra\Response\Result\RowsResult;
use Cassandra\Response\Result\SchemaChangeResult;
use Cassandra\Response\Result\SetKeyspaceResult;
use Cassandra\Response\Result\VoidResult;
use Cassandra\Response\StreamReader;
use Cassandra\Statement;
use Cassandra\StatementStatus;
use Cassandra\Test\Support\Faults;
use Cassandra\Type;
use Cassandra\TypeInfo\SimpleTypeInfo;
use Cassandra\Value\ValueEncodeConfig;
use Closure;
use ReflectionMethod;
use ReflectionProperty;
use stdClass;

/**
 * Responses that are malformed, of an unexpected kind, or read in a way the
 * result does not support.
 */
final class ResponseSideErrorPathsTest extends AbstractUnitTestCase {
    private const NO_METADATA = 0x0004;

    protected function tearDown(): void {
        Faults::reset();
        parent::tearDown();
    }

    /**
     * @return array<string, array{class-string<CassandraException>, Closure(self): mixed}>
     */
    public static function malformed(): array {
        $event = static fn (string $body): Closure => static fn (self $t): mixed => $t->response(Opcode::RESPONSE_EVENT, $body);
        $result = static fn (string $body): Closure => static fn (self $t): mixed => $t->response(Opcode::RESPONSE_RESULT, $body);
        $s = self::string(...);

        return [
            'unknown error code' => [ResponseException::class, static fn (self $t) => $t->response(Opcode::RESPONSE_ERROR, pack('N', 0x7777) . $s('x'))],
            'unknown error type, read directly' => [ResponseException::class, static fn () => new Error(new Header(ProtocolVersion::V4, 0, 1, Opcode::RESPONSE_ERROR, 0), new StreamReader(pack('N', 0x7777) . $s('x')))],
            'unknown event type' => [ResponseException::class, $event($s('NOPE'))],
            'unknown event type, read directly' => [ResponseException::class, static fn () => (new Event(new Header(ProtocolVersion::V4, 0, -1, Opcode::RESPONSE_EVENT, 0), new StreamReader($s('NOPE'))))->getType()],
            'unknown schema change type' => [ResponseException::class, $event($s('SCHEMA_CHANGE') . $s('MOVED') . $s('TABLE') . $s('ks') . $s('t'))],
            'unknown schema change target' => [ResponseException::class, $event($s('SCHEMA_CHANGE') . $s('CREATED') . $s('INDEX') . $s('ks'))],
            'unknown status change' => [ResponseException::class, $event($s('STATUS_CHANGE') . $s('SIDEWAYS') . "\4\x7F\0\0\1" . pack('N', 1))],
            'unknown topology change' => [ResponseException::class, $event($s('TOPOLOGY_CHANGE') . $s('SIDEWAYS') . "\4\x7F\0\0\1" . pack('N', 1))],
            'unknown result kind' => [ResponseException::class, $result(pack('N', 9))],
            'unknown schema change type in a result' => [ResponseException::class, static fn (self $t) => $t->resultOf(pack('N', 5) . $s('MOVED') . $s('TABLE') . $s('ks') . $s('t'))->asSchemaChangeResult()->getSchemaChangeData()],
            'unknown schema change target in a result' => [ResponseException::class, static fn (self $t) => $t->resultOf(pack('N', 5) . $s('CREATED') . $s('INDEX') . $s('ks'))->asSchemaChangeResult()->getSchemaChangeData()],
            'too many result columns' => [ResponseException::class, $result(pack('N', 2) . pack('NN', 0x0001, 65536) . $s('ks') . $s('t'))],
            'negative result columns' => [ResponseException::class, $result(pack('N', 2) . pack('NN', self::NO_METADATA, -1))],
            'unknown opcode in the header' => [ConnectionException::class, static fn (self $t) => $t->readFrame("\x84\0\0\1\x77" . pack('N', 0))],
        ];
    }

    public function readFrame(string $bytes): ?Response {
        $node = new class($bytes) extends \Cassandra\Connection\NodeImplementation {
            private int $offset = 0;

            public function __construct(private readonly string $bytes) {
            }

            public function close(): void {
            }

            public function getConfig(): \Cassandra\Connection\NodeConfig {
                return new StreamNodeConfig();
            }

            public function readAvailableDataFromSource(int $expectedLength, int $upperBoundaryLength, ?float $readDeadline): string {
                $chunk = substr($this->bytes, $this->offset, $upperBoundaryLength);
                $this->offset += strlen($chunk);

                return $chunk;
            }

            public function write(string $data): void {
            }

            public function writeRequest(\Cassandra\Request\Request $request): void {
            }
        };

        return (new ResponseReader())->readResponse($node, ProtocolVersion::V4, null);
    }

    public function response(Opcode $opcode, string $body, ProtocolVersion $version = ProtocolVersion::V4): Response {
        $header = new Header($version, 0, 1, $opcode, strlen($body));
        $response = (new ReflectionMethod(ResponseReader::class, 'createResponse'))->invoke(new ResponseReader(), $header, $body);
        $this->assertInstanceOf(Response::class, $response);

        return $response;
    }

    public function resultOf(string $body): Result {
        $result = $this->response(Opcode::RESPONSE_RESULT, $body);
        $this->assertInstanceOf(Result::class, $result);

        return $result;
    }

    public function testAccessorsOfResponses(): void {
        $error = $this->response(Opcode::RESPONSE_ERROR, pack('N', 0x2200) . self::string('bad'));
        $this->assertInstanceOf(Error::class, $error);
        $this->assertSame('bad', $error->getMessage());
        $this->assertSame(0x2200, $error->getType()->value);
        $this->assertSame(0, $error->getFlags());
        $this->assertSame(4, $error->getVersion());
        $this->assertInstanceOf(StreamReader::class, $error->getBodyStreamReader());

        $status = $this->response(Opcode::RESPONSE_EVENT, self::string('STATUS_CHANGE') . self::string('UP') . "\4\x7F\0\0\1" . pack('N', 9042));
        $this->assertInstanceOf(StatusChangeEvent::class, $status);
        $this->assertSame($status->getData(), $status->getStatusChangeData());

        $topology = $this->response(Opcode::RESPONSE_EVENT, self::string('TOPOLOGY_CHANGE') . self::string('NEW_NODE') . "\4\x7F\0\0\1" . pack('N', 9042));
        $this->assertInstanceOf(TopologyChangeEvent::class, $topology);
        $topologyData = $topology->getData();
        $this->assertInstanceOf(\Cassandra\Response\Event\Data\TopologyChangeData::class, $topologyData);
        $this->assertSame('NEW_NODE', $topologyData->changeType->value);

        $aggregate = $this->response(Opcode::RESPONSE_EVENT, self::string('SCHEMA_CHANGE') . self::string('CREATED') . self::string('AGGREGATE') . self::string('ks') . self::string('agg') . pack('n', 1) . self::string('int'));
        $this->assertInstanceOf(SchemaChangeEvent::class, $aggregate);
        $aggregateData = $aggregate->getData();
        $this->assertInstanceOf(\Cassandra\Response\Event\Data\SchemaChangeData::class, $aggregateData);
        $this->assertSame(SchemaChangeTarget::AGGREGATE, $aggregateData->target);
        $this->assertSame(['int'], $aggregateData->argumentTypes);

        $plain = new Event(new Header(ProtocolVersion::V4, 0, -1, Opcode::RESPONSE_EVENT, 0), new StreamReader(self::string('STATUS_CHANGE')));
        $this->assertInstanceOf(\Cassandra\Response\Event\Data\EventData::class, $plain->getData());
    }

    public function testCompressedFramesWithABadLengthPrefix(): void {
        Faults::inject('unpack', static fn (string $format, string $data, int $offset = 0): array|false => \unpack($format, $data, $offset), false);
        $this->assertRaises(ConnectionException::class, fn () => $this->readFrame("\x84\x01\0\1\x08" . pack('N', 8) . pack('N', 4) . "\0\0\0\0"));

        Faults::reset();
        $body = (new \Cassandra\Compression\Lz4Compressor())->compressBlock(pack('N', 1));
        $this->assertRaises(CassandraException::class, fn () => $this->readFrame("\x84\x01\0\1\x08" . pack('N', strlen($body) + 4) . pack('N', 5) . $body));
    }

    /**
     * @param class-string<CassandraException> $expected
     * @param Closure(self): mixed $call
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('malformed')]
    public function testMalformedResponse(string $expected, Closure $call): void {
        $this->assertRaises($expected, fn () => $call($this));
    }

    public function testMetadataChangedOnV5(): void {
        $body = pack('N', 2) . pack('NN', 0x0001 | 0x0008, 1) . pack('n', 2) . 'm2' . self::string('ks') . self::string('t') . self::string('c') . pack('n', 0x0009) . pack('N', 0);
        $rows = $this->response(Opcode::RESPONSE_RESULT, $body, ProtocolVersion::V5);
        $this->assertInstanceOf(RowsResult::class, $rows);
        $this->assertTrue($rows->hasMetadataChanged());
        $this->assertSame('m2', $rows->getRowsMetadata()->metadataId);
    }

    public function testReadingRowsInEveryShape(): void {
        $rows = $this->keyValueRows([['a', 1], ['b', 2], ['a', 3], [null, 4]]);

        $this->assertSame(4, $rows->rowCount());
        $this->assertSame(['a' => 1], $rows->fetchKeyPair());
        $this->assertSame(['b' => 2], $rows->fetchKeyPair());
        $rows->rewindOneRow();
        $this->assertSame(['b' => 2], $rows->fetchKeyPair());
        $this->assertSame(['a' => 3], $rows->fetchKeyPair());
        $this->assertRaises(ResponseException::class, static fn () => $rows->fetchKeyPair());

        $data = $rows->getData();
        $this->assertInstanceOf(RowsData::class, $data);
        $this->assertCount(4, $data->rows);
        $this->assertSame($data, $data);

        $merged = $this->keyValueRows([['a', 1], ['a', 2], ['b', 3], ['a', 4]]);
        $this->assertSame(['a' => [1, 2, 4], 'b' => 3], $merged->fetchAllKeyPairs(0, 1, mergeDuplicates: true));

        $nullKey = $this->keyValueRows([[null, 1]]);
        $this->assertRaises(ResponseException::class, static fn () => $nullKey->fetchAllKeyPairs());

        $empty = $this->keyValueRows([]);
        $this->assertFalse($empty->fetchKeyPair());
        $empty->rewindOneRow();

        $this->assertRaises(ResponseException::class, static fn () => (new ReflectionMethod($rows, 'configureFetchObject'))->invoke($rows, stdClass::class));
        (new ReflectionProperty(RowsResult::class, 'fetchObjectConfiguration'))->setValue($rows, ['rowClass' => stdClass::class, 'constructorArgs' => [], 'fetchType' => \Cassandra\Response\Result\FetchType::ASSOC]);
        $rows->rewind();
        $this->assertRaises(ResponseException::class, static fn () => $rows->fetchObject());
    }

    public function testReadingRowsWithoutMetadata(): void {
        $rows = $this->rows(self::NO_METADATA, 2, pack('N', 1) . pack('N', 1) . 'a' . pack('N', 1) . 'b');
        $this->assertTrue($rows->hasNoMetadata());
        $this->assertFalse($rows->hasMorePages());
        $this->assertSame(2, $rows->columnCount());
        foreach (['fetchAllColumns', 'fetchAllKeyPairs', 'fetchColumn', 'fetchKeyPair', 'fetch', 'getRowsData'] as $method) {
            $this->assertRaises(ResponseException::class, static fn () => $rows->{$method}());
        }
    }

    public function testResultKindsAndConversions(): void {
        $void = $this->response(Opcode::RESPONSE_RESULT, pack('N', 1));
        $this->assertInstanceOf(VoidResult::class, $void);
        $this->assertInstanceOf(VoidData::class, $void->getData());
        $this->assertSame([], iterator_to_array($void->getIterator()));
        $this->assertSame($void, $void->asVoidResult());
        foreach (['asPreparedResult', 'asRowsResult', 'asSchemaChangeResult', 'asSetKeyspaceResult'] as $method) {
            $this->assertRaises(ResponseException::class, static fn () => $void->{$method}());
        }

        $keyspace = $this->response(Opcode::RESPONSE_RESULT, pack('N', 3) . self::string('ks'));
        $this->assertInstanceOf(SetKeyspaceResult::class, $keyspace);
        $this->assertInstanceOf(SetKeyspaceData::class, $keyspace->getData());
        $this->assertSame('ks', $keyspace->getSetKeyspaceData()->keyspace);
        $this->assertNotEmpty(iterator_to_array($keyspace->getIterator()));
        $this->assertSame($keyspace, $keyspace->asSetKeyspaceResult());
        $this->assertRaises(ResponseException::class, static fn () => $keyspace->asVoidResult());

        $schema = $this->response(Opcode::RESPONSE_RESULT, pack('N', 5) . self::string('CREATED') . self::string('AGGREGATE') . self::string('ks') . self::string('agg') . pack('n', 1) . self::string('int'));
        $this->assertInstanceOf(SchemaChangeResult::class, $schema);
        $this->assertSame($schema, $schema->asSchemaChangeResult());
        $this->assertSame(['int'], $schema->getSchemaChangeData()->argumentTypes);
        $this->assertEquals($schema->getSchemaChangeData(), $schema->getData());
        $this->assertNotEmpty(iterator_to_array($schema->getIterator()));

        $prepared = $this->response(Opcode::RESPONSE_RESULT, pack('N', 4) . pack('n', 2) . 'id' . pack('NNN', 0, 0, 0) . pack('NN', 0x0004, 0));
        $this->assertInstanceOf(PreparedResult::class, $prepared);
        $this->assertSame($prepared, $prepared->asPreparedResult());
        $this->assertEquals($prepared->getPreparedData(), $prepared->getData());
        $this->assertSame('id', $prepared->getPreparedData()->id);
        $this->assertNotEmpty(iterator_to_array($prepared->getIterator()));

        $base = new Result(new Header(ProtocolVersion::V4, 0, 1, Opcode::RESPONSE_RESULT, 4), new StreamReader(pack('N', 1)));
        $this->assertSame([], iterator_to_array($base->getIterator()));
        $this->assertSame(0, $base->getRowCount());

        // Each typed result refuses a body of another kind.
        $header = new Header(ProtocolVersion::V4, 0, 1, Opcode::RESPONSE_RESULT, 4);
        $this->assertRaises(ResponseException::class, static fn () => (new VoidResult($header, new StreamReader(pack('N', 3) . self::string('ks'))))->getVoidData());
        $this->assertRaises(ResponseException::class, static fn () => (new SetKeyspaceResult($header, new StreamReader(pack('N', 1))))->getSetKeyspaceData());
        $this->assertRaises(ResponseException::class, static fn () => (new SchemaChangeResult($header, new StreamReader(pack('N', 1))))->getSchemaChangeData());
        $this->assertRaises(ResponseException::class, static fn () => new PreparedResult($header, new StreamReader(pack('N', 1))));
    }

    public function testRowsResultContinuesFromAPreviousResult(): void {
        $rows = $this->keyValueRows([['a', 1]]);
        $previous = $this->keyValueRows([]);
        $this->assertRaises(ResponseException::class, static fn () => $rows->setPreviousResult($previous));
    }

    public function testStatementResultAccessors(): void {
        $connection = new Connection([new StreamNodeConfig()]);
        $statement = static function (?Response $response) use ($connection): Statement {
            $statement = new Statement($connection, 1, 1, new Query('SELECT 1'));
            $statement->setResponse($response);
            $statement->setStatus($response === null ? StatementStatus::WAITING_FOR_RESULT : StatementStatus::RESULT_READY);

            return $statement;
        };

        $void = $statement($this->response(Opcode::RESPONSE_RESULT, pack('N', 1)));
        foreach (['getPreparedResult', 'getRowsResult', 'getSchemaChangeResult', 'getSetKeyspaceResult', 'tryGetPreparedResult', 'tryGetRowsResult', 'tryGetSchemaChangeResult', 'tryGetSetKeyspaceResult'] as $method) {
            $this->assertRaises(StatementException::class, static fn () => $void->{$method}());
        }
        $this->assertFalse($void->isWaitingForResult());

        $supported = $statement($this->response(Opcode::RESPONSE_SUPPORTED, pack('n', 0)));
        $this->assertRaises(StatementException::class, static fn () => $supported->getResult());
        $this->assertRaises(StatementException::class, static fn () => $supported->tryGetResult());

        $error = $statement($this->response(Opcode::RESPONSE_ERROR, pack('N', 0x2200) . self::string('bad')));
        $this->assertRaises(ServerException::class, static fn () => $error->getResponse());
        $this->assertRaises(ServerException::class, static fn () => $error->tryGetResponse());

        $keyspace = $statement($this->response(Opcode::RESPONSE_RESULT, pack('N', 3) . self::string('ks')));
        $this->assertInstanceOf(SetKeyspaceResult::class, $keyspace->getSetKeyspaceResult());
        $this->assertInstanceOf(SetKeyspaceResult::class, $keyspace->tryGetSetKeyspaceResult());

        $schema = $statement($this->response(Opcode::RESPONSE_RESULT, pack('N', 5) . self::string('CREATED') . self::string('KEYSPACE') . self::string('ks')));
        $this->assertInstanceOf(SchemaChangeResult::class, $schema->getSchemaChangeResult());
        $this->assertInstanceOf(SchemaChangeResult::class, $schema->tryGetSchemaChangeResult());

        $prepared = $statement($this->response(Opcode::RESPONSE_RESULT, pack('N', 4) . pack('n', 2) . 'id' . pack('NNN', 0, 0, 0) . pack('NN', 0x0004, 0)));
        $this->assertInstanceOf(PreparedResult::class, $prepared->tryGetPreparedResult());

        $rows = $statement($this->keyValueRows([]));
        $this->assertInstanceOf(RowsResult::class, $rows->tryGetRowsResult());
        $this->assertInstanceOf(Result::class, $rows->tryGetResult());
    }

    public function testStreamReaderRefusals(): void {
        $this->assertRaises(ResponseException::class, static fn () => (new StreamReader('')) ->readDouble());
        $this->assertRaises(ResponseException::class, static fn () => (new StreamReader(''))->readFloat());
        $this->assertRaises(ResponseException::class, static fn () => (new StreamReader(''))->readLong());
        $this->assertRaises(ResponseException::class, static fn () => (new StreamReader(''))->readShort());
        $this->assertRaises(ResponseException::class, static fn () => (new StreamReader("\xFF\xFF\xFF\xFE"))->readLongString());

        foreach (['readDouble' => 8, 'readFloat' => 4, 'readInt' => 4, 'readLong' => 8, 'readShort' => 2] as $method => $width) {
            Faults::inject('unpack', false);
            $this->assertRaises(ResponseException::class, static fn () => (new StreamReader(str_repeat("\0", $width)))->{$method}());
        }

        Faults::inject('inet_ntop', false);
        $this->assertRaises(ResponseException::class, static fn () => (new StreamReader("\4\1\2\3\4"))->readInetAddr());

        $config = new ValueEncodeConfig();
        $this->assertRaises(ResponseException::class, static fn () => (new StreamReader(pack('N', 15) . str_repeat("\0", 15)))->readValue(new SimpleTypeInfo(Type::UUID), $config));
        $this->assertRaises(ResponseException::class, static fn () => (new StreamReader(pack('N', 4) . pack('N', -1)))->readValue(new \Cassandra\TypeInfo\ListCollectionInfo(new SimpleTypeInfo(Type::INT), false), $config));
        $this->assertRaises(ResponseException::class, static fn () => (new StreamReader(pack('n', 0x7777)))->readTypeInfo());
        $this->assertRaises(ResponseException::class, static fn () => (new StreamReader(pack('n', Type::VECTOR->value)))->readTypeInfo());
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

    /**
     * @param list<array{?string, int}> $pairs
     */
    private function keyValueRows(array $pairs): RowsResult {
        $body = pack('N', count($pairs));
        foreach ($pairs as [$key, $value]) {
            $body .= ($key === null ? "\xFF\xFF\xFF\xFF" : pack('N', strlen($key)) . $key) . pack('N', 4) . pack('N', $value);
        }

        $metadata = pack('NN', 0x0001, 2) . self::string('ks') . self::string('t') . self::string('k') . pack('n', 0x000D) . self::string('v') . pack('n', 0x0009);
        $result = $this->response(Opcode::RESPONSE_RESULT, pack('N', 2) . $metadata . $body);
        $this->assertInstanceOf(RowsResult::class, $result);

        return $result;
    }

    private function rows(int $flags, int $columns, string $rowsBody): RowsResult {
        $result = $this->response(Opcode::RESPONSE_RESULT, pack('N', 2) . pack('NN', $flags, $columns) . $rowsBody);
        $this->assertInstanceOf(RowsResult::class, $result);

        return $result;
    }

    private static function string(string $value): string {
        return pack('n', strlen($value)) . $value;
    }
}
