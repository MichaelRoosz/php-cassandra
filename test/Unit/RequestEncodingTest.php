<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Consistency;
use Cassandra\EventType;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\RequestException;
use Cassandra\Exception\ValueException;
use Cassandra\Exception\ValueFactoryException;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Batch;
use Cassandra\Request\BatchType;
use Cassandra\Request\Execute;
use Cassandra\Request\Options\BatchOptions;
use Cassandra\Request\Options\ExecuteOptions;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Request\Prepare;
use Cassandra\Request\Query;
use Cassandra\Request\QueryFlag;
use Cassandra\Request\Register;
use Cassandra\Request\Startup;
use Cassandra\SerialConsistency;
use Cassandra\Protocol\Header;
use Cassandra\Protocol\Opcode;
use Cassandra\Response\Result\CachedPreparedResult;
use Cassandra\Response\Result\ColumnInfo;
use Cassandra\Response\Result\Data\PreparedData;
use Cassandra\Response\Result\PrepareMetadata;
use Cassandra\Response\Result\RowsMetadata;
use Cassandra\Response\StreamReader;
use Cassandra\Value\NotSet;
use Cassandra\Value\Float32;
use Cassandra\Type;
use Cassandra\TypeInfo\SimpleTypeInfo;
use DateTimeImmutable;
use ReflectionClass;
use ReflectionMethod;

/**
 * Unit tests for the binary encoding of request options.
 */
final class RequestEncodingTest extends AbstractUnitTestCase {
    /**
     * @return array<string, array{int}>
     */
    public static function outOfInt32RangeProvider(): array {
        // On 32-bit PHP the out-of-range literals below overflow to float, which
        // would TypeError against the int parameter under strict_types. A PHP int
        // can never exceed int32 there anyway, so provide a benign value that the
        // test skips.
        if (PHP_INT_SIZE < 8) {
            return ['n/a on 32-bit' => [0]];
        }

        return [
            'just above max' => [2147483648],
            'just below min' => [-2147483649],
            'php int max' => [PHP_INT_MAX],
        ];
    }
    /**
     * @return array<string, array{SerialConsistency}>
     */
    public static function serialConsistencyProvider(): array {
        return [
            'SERIAL' => [SerialConsistency::SERIAL],
            'LOCAL_SERIAL' => [SerialConsistency::LOCAL_SERIAL],
        ];
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('serialConsistencyProvider')]
    public function testBatchEncodesSerialConsistencyValue(SerialConsistency $serial): void {
        $batch = new Batch(
            type: BatchType::LOGGED,
            consistency: Consistency::ONE,
            options: new BatchOptions(serialConsistency: $serial)
        );
        $batch->setVersion(ProtocolVersion::V4);
        $batch->appendQuery('UPDATE t SET v = 1 WHERE id = 1 IF v = 0');

        $body = $batch->getBody();
        $tail = substr($body, -2);

        $this->assertSame(
            pack('n', $serial->value),
            $tail,
            'Batch must encode the serial consistency code, not the enum instance'
        );
    }

    public function testBatchRejectsMoreStatementsThanTheCountCanExpress(): void {
        // The statement count goes out as a [short]. pack('n', …) takes the low
        // two bytes without complaint, so one statement past the limit used to
        // announce a count of zero and leave the coordinator misparsing the body
        // rather than rejecting it.
        $batch = new Batch(BatchType::UNLOGGED, Consistency::ONE, new BatchOptions());
        for ($i = 0; $i <= 65535; $i++) {
            $batch->appendQuery('INSERT INTO t (id) VALUES (1)');
        }
        $batch->setVersion(ProtocolVersion::V5);

        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_BATCH_TOO_MANY_STATEMENTS->value);

        $batch->getBody();
    }

    public function testBatchRejectsNegativeDefaultTimestampFromProtocolV4(): void {
        $batch = new Batch(options: new BatchOptions(defaultTimestamp: -1));
        $batch->appendQuery('SELECT * FROM t');
        $batch->setVersion(ProtocolVersion::V4);

        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_INVALID_DEFAULT_TIMESTAMP->value);

        $batch->getBody();
    }

    public function testBatchRejectsOversizedPreparedStatementId(): void {
        $batch = new Batch();

        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_FIELD_TOO_LONG->value);

        $batch->appendPreparedStatement(self::preparedResult(str_repeat('i', 65536)));
    }

    public function testBindValuesMatchMarkerNamesCaseInsensitively(): void {
        $markers = [self::columnInfo('userid'), self::columnInfo('name')];

        $encoded = self::testableRequest()->encodeValuesForBindMarkers(
            ['userId' => 5, 'Name' => 7],
            $markers,
            namesForValues: true
        );

        $this->assertSame(['userid', 'name'], array_keys($encoded));
        $this->assertInstanceOf(\Cassandra\Value\Int32::class, $encoded['userid']);
        $this->assertSame(5, $encoded['userid']->getValue());
    }

    public function testCaseDistinctQuotedBindMarkersUseExactNames(): void {
        $encoded = self::testableRequest()->encodeValuesForBindMarkers(
            ['Foo' => 5, 'foo' => 6],
            [self::columnInfo('Foo'), self::columnInfo('foo')],
            namesForValues: true
        );

        $this->assertSame(['Foo', 'foo'], array_keys($encoded));
        $this->assertInstanceOf(\Cassandra\Value\Int32::class, $encoded['Foo']);
        $this->assertInstanceOf(\Cassandra\Value\Int32::class, $encoded['foo']);
        $this->assertSame(5, $encoded['Foo']->getValue());
        $this->assertSame(6, $encoded['foo']->getValue());
    }

    public function testCustomPayloadRejectsIntegerKeysAtThePublicBoundary(): void {
        $request = new Query('SELECT 1');

        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_INVALID_CUSTOM_PAYLOAD->value);

        (new ReflectionMethod($request, 'setPayload'))->invoke($request, [0 => 'value']);
    }
    public function testCustomPayloadRejectsNonStringValuesAtThePublicBoundary(): void {
        $request = new Query('SELECT 1');

        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_INVALID_CUSTOM_PAYLOAD->value);

        (new ReflectionMethod($request, 'setPayload'))->invoke($request, ['key' => []]);
    }

    public function testCustomPayloadRemainsSupportedForV4Queries(): void {
        $request = new Query('SELECT 1');
        $request->setPayload(['key' => 'value']);
        $request->setStream(0);
        $request->setVersion(ProtocolVersion::V4);

        $frame = (string) $request;

        $this->assertSame(\Cassandra\Protocol\Flag::CUSTOM_PAYLOAD, ord($frame[1]));
    }

    public function testCustomPayloadRequiresProtocolV4(): void {
        $request = new Query('SELECT 1');
        $request->setPayload(['key' => 'value']);
        $request->setStream(0);
        $request->setVersion(ProtocolVersion::V3);

        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_CUSTOM_PAYLOAD_UNSUPPORTED_PROTOCOL->value);

        (string) $request;
    }

    public function testCustomPayloadRequiresSupportedOpcode(): void {
        $request = new Startup();
        $request->setPayload(['key' => 'value']);
        $request->setStream(0);
        $request->setVersion(ProtocolVersion::V4);

        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_CUSTOM_PAYLOAD_UNSUPPORTED_OPCODE->value);

        (string) $request;
    }

    public function testDirectQueryKeepsQuotedBindMarkerCase(): void {
        $query = 'SELECT * FROM t WHERE "col" = :"CaseSensitive"';
        $request = new Query(
            query: $query,
            values: ['CaseSensitive' => new \Cassandra\Value\Int32(1)],
        );
        $request->setVersion(ProtocolVersion::V5);

        $valueSection = substr($request->getBody(), 4 + strlen($query) + 2 + 4);

        $this->assertSame(
            pack('n', 1)
                . pack('n', strlen('CaseSensitive')) . 'CaseSensitive'
                . pack('N', 4) . pack('N', 1),
            $valueSection,
        );
    }

    public function testDuplicateNamedBindMarkerThrows(): void {
        // Two markers reported under the same name would otherwise collapse into
        // a single value entry, silently sending fewer values than the statement
        // has markers.
        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_VALUES_DUPLICATE_BIND_MARKER->value);

        self::testableRequest()->encodeValuesForBindMarkers(
            ['a' => 5],
            [self::columnInfo('a'), self::columnInfo('a')],
            namesForValues: true
        );
    }

    public function testEncodedValueNamesFromBindMarkersKeepExactCase(): void {
        // A quoted identifier like "userId" is case-sensitive; the name reported
        // by the server must be sent back unchanged.
        $binary = self::testableRequest()->encodeValuesAsBinary(
            ['userId' => new \Cassandra\Value\Int32(1)],
            namesForValues: true,
            namesAreExact: true
        );

        $this->assertStringContainsString("\x00\x06userId", $binary);
    }

    public function testExecuteRejectsNegativeDefaultTimestampFromProtocolV4(): void {
        $request = new Execute(
            self::preparedResult('id'),
            [],
            options: new ExecuteOptions(defaultTimestamp: -1),
        );
        $request->setVersion(ProtocolVersion::V4);

        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_INVALID_DEFAULT_TIMESTAMP->value);

        $request->getBody();
    }

    public function testExecuteRejectsOversizedPreparedStatementId(): void {
        $request = new Execute(self::preparedResult(str_repeat('i', 65536)), []);
        $request->setVersion(ProtocolVersion::V4);

        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_FIELD_TOO_LONG->value);

        $request->getBody();
    }

    public function testExecuteRejectsOversizedResultMetadataId(): void {
        $request = new Execute(self::preparedResult('id', str_repeat('m', 65536)), []);
        $request->setVersion(ProtocolVersion::V5);

        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_FIELD_TOO_LONG->value);

        $request->getBody();
    }

    public function testExplicitNullBindValueIsAccepted(): void {
        $encoded = self::testableRequest()->encodeValuesForBindMarkers(
            ['userid' => null],
            [self::columnInfo('userid')],
            namesForValues: true
        );

        $this->assertSame(['userid' => null], $encoded);
    }

    public function testExtraPositionalBindValueThrows(): void {
        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_VALUES_EXTRA_BIND_VALUE->value);

        self::testableRequest()->encodeValuesForBindMarkers(
            [5, 6],
            [self::columnInfo('a')],
            namesForValues: false
        );
    }

    public function testFailedBatchAppendDoesNotRetainNotSetState(): void {
        $batch = new Batch();

        try {
            $batch->appendQuery('invalid', [new NotSet(), new \stdClass()]);
            $this->fail('Expected the unsupported value after NotSet to fail');
        } catch (RequestException $e) {
            $this->assertSame(ExceptionCode::REQUEST_VALUES_UNSUPPORTED_VALUE_TYPE->value, $e->getCode());
        }

        $batch->appendQuery('valid', [1]);
        $batch->setVersion(ProtocolVersion::V3);

        $this->assertNotSame('', $batch->getBody());
    }

    public function testMissingNamedBindValueThrows(): void {
        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_VALUES_MISSING_BIND_VALUE->value);

        self::testableRequest()->encodeValuesForBindMarkers(
            ['wrong_name' => 5],
            [self::columnInfo('userid')],
            namesForValues: true
        );
    }

    public function testMissingPositionalBindValueThrows(): void {
        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_VALUES_MISSING_BIND_VALUE->value);

        self::testableRequest()->encodeValuesForBindMarkers(
            [5],
            [self::columnInfo('a'), self::columnInfo('b')],
            namesForValues: false
        );
    }

    public function testNotSetIsRejectedForProtocolV3BatchesAfterAppend(): void {
        $batch = new Batch();
        $batch->appendQuery('UPDATE t SET v = ? WHERE id = 1', [new NotSet()]);
        $batch->setVersion(ProtocolVersion::V3);

        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_VALUES_NOT_SET_UNSUPPORTED_PROTOCOL->value);

        $batch->getBody();
    }

    public function testNotSetIsRejectedForProtocolV3Queries(): void {
        $request = new Query('UPDATE t SET v = ? WHERE id = 1', [new NotSet()]);
        $request->setVersion(ProtocolVersion::V3);

        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_VALUES_NOT_SET_UNSUPPORTED_PROTOCOL->value);

        $request->getBody();
    }

    public function testNotSetKeepsItsDistinctSentinelFromProtocolV4(): void {
        $request = new Query('UPDATE t SET v = ? WHERE id = 1', [new NotSet()]);
        $request->setVersion(ProtocolVersion::V4);

        $this->assertStringEndsWith("\xff\xff\xff\xfe", $request->getBody());
    }

    public function testPreparedValueObjectMustMatchBindMarkerType(): void {
        $this->expectException(ValueFactoryException::class);
        $this->expectExceptionCode(ExceptionCode::VALUEFACTORY_VALUE_OBJECT_TYPE_MISMATCH->value);

        self::testableRequest()->encodeValuesForBindMarkers(
            [Float32::fromValue(1.0)],
            [self::columnInfo('value')],
            namesForValues: false,
        );
    }

    public function testProtocolV3AllowsNegativeDefaultTimestampExceptMinimum(): void {
        $request = new Query('SELECT * FROM t', options: new QueryOptions(defaultTimestamp: -1));
        $request->setVersion(ProtocolVersion::V3);

        $this->assertStringEndsWith("\xff\xff\xff\xff\xff\xff\xff\xff", $request->getBody());

        if (PHP_INT_SIZE < 8) {
            return;
        }

        $request = new Query('SELECT * FROM t', options: new QueryOptions(defaultTimestamp: PHP_INT_MIN));
        $request->setVersion(ProtocolVersion::V3);

        try {
            $request->getBody();
            $this->fail('Expected the minimum signed 64-bit timestamp to be rejected');
        } catch (RequestException $e) {
            $this->assertSame(ExceptionCode::REQUEST_INVALID_DEFAULT_TIMESTAMP->value, $e->getCode());
        }
    }

    public function testQueryAcceptsTheLargestExpressibleValueCount(): void {
        // The boundary that testQueryRejectsMoreValuesThanTheCountCanExpress is
        // drawn at: exactly 65535 values still encodes, and announces itself as
        // 0xffff.
        $request = new Query(
            query: 'INSERT INTO t (id) VALUES (?)',
            values: array_fill(0, 65535, 1),
            consistency: Consistency::ONE,
            options: new QueryOptions()
        );
        $request->setVersion(ProtocolVersion::V5);

        $body = $request->getBody();

        $this->assertSame("\xff\xff", substr($body, -65535 * 8 - 2, 2));
    }

    public function testQueryEncodesInInt32RangeIntAsFourBytes(): void {
        $request = new Query(
            query: 'INSERT INTO t (id) VALUES (?)',
            values: [2147483647],
            consistency: Consistency::ONE,
            options: new QueryOptions()
        );
        $request->setVersion(ProtocolVersion::V5);

        // The single bound value's [bytes] frame is the tail of the body:
        // 4-byte length prefix followed by the 4-byte int32.
        $body = $request->getBody();

        $this->assertSame(pack('N', 4) . pack('N', 2147483647), substr($body, -8));
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('serialConsistencyProvider')]
    public function testQueryEncodesSerialConsistencyValue(SerialConsistency $serial): void {
        $request = new Query(
            query: 'UPDATE t SET v = ? WHERE id = ? IF v = ?',
            values: [],
            consistency: Consistency::ONE,
            options: new QueryOptions(serialConsistency: $serial)
        );
        $request->setVersion(ProtocolVersion::V5);

        $body = $request->getBody();

        // body = [int query length][query][short consistency][int flags][short serial consistency]
        $offset = 4 + strlen('UPDATE t SET v = ? WHERE id = ? IF v = ?');

        $this->assertSame(
            Consistency::ONE->value,
            $this->unpackInt('n', substr($body, $offset, 2))
        );
        $this->assertSame(
            QueryFlag::WITH_SERIAL_CONSISTENCY,
            $this->unpackInt('N', substr($body, $offset + 2, 4)) & QueryFlag::WITH_SERIAL_CONSISTENCY,
            'The serial consistency flag must be set'
        );
        $this->assertSame(
            $serial->value,
            $this->unpackInt('n', substr($body, $offset + 6, 2)),
            'Query must encode the serial consistency code, not the enum instance'
        );
    }

    public function testQueryEncodesSmallPositivePageSizeUnchanged(): void {
        $request = new Query(
            query: 'SELECT * FROM t',
            consistency: Consistency::ONE,
            options: new QueryOptions(pageSize: 1)
        );
        $request->setVersion(ProtocolVersion::V5);

        $this->assertSame(1, $this->unpackInt('N', substr($request->getBody(), -4)));
    }

    public function testQueryOmitsSerialConsistencyWhenNotRequested(): void {
        $request = new Query(
            query: 'SELECT * FROM t',
            values: [],
            consistency: Consistency::ONE,
            options: new QueryOptions()
        );
        $request->setVersion(ProtocolVersion::V5);

        $body = $request->getBody();
        $offset = 4 + strlen('SELECT * FROM t');

        $this->assertSame(
            0,
            $this->unpackInt('N', substr($body, $offset + 2, 4)) & QueryFlag::WITH_SERIAL_CONSISTENCY,
            'The serial consistency flag must not be set when the option is unused'
        );
    }

    public function testQueryOptionsRejectNonPositivePageSize(): void {
        foreach ([0, -1] as $pageSize) {
            try {
                new QueryOptions(pageSize: $pageSize);
                $this->fail('expected page size ' . $pageSize . ' to be refused');
            } catch (RequestException $e) {
                $this->assertSame(ExceptionCode::REQUEST_INVALID_PAGE_SIZE->value, $e->getCode());
            }
        }
    }

    public function testQueryRejectsDateTimeInUntypedPath(): void {
        $request = new Query(
            query: 'INSERT INTO t (ts) VALUES (?)',
            values: [new DateTimeImmutable('2024-01-01 00:00:00')],
            consistency: Consistency::ONE,
            options: new QueryOptions()
        );
        $request->setVersion(ProtocolVersion::V5);

        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_VALUES_AMBIGUOUS_DATETIME->value);

        $request->getBody();
    }

    public function testQueryRejectsMoreValuesThanTheCountCanExpress(): void {
        $request = new Query(
            query: 'INSERT INTO t (id) VALUES (?)',
            values: array_fill(0, 65536, 1),
            consistency: Consistency::ONE,
            options: new QueryOptions()
        );
        $request->setVersion(ProtocolVersion::V5);

        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_VALUES_TOO_MANY_VALUES->value);

        $request->getBody();
    }

    public function testQueryRejectsNegativeDefaultTimestampFromProtocolV4(): void {
        foreach ([ProtocolVersion::V4, ProtocolVersion::V5] as $version) {
            $request = new Query('SELECT * FROM t', options: new QueryOptions(defaultTimestamp: -1));
            $request->setVersion($version);

            try {
                $request->getBody();
                $this->fail('Expected a negative default timestamp to be rejected for ' . $version->name);
            } catch (RequestException $e) {
                $this->assertSame(ExceptionCode::REQUEST_INVALID_DEFAULT_TIMESTAMP->value, $e->getCode());
            }
        }
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('outOfInt32RangeProvider')]
    public function testQueryRejectsOutOfInt32RangeIntInUntypedPath(int $value): void {
        if (PHP_INT_SIZE < 8) {
            $this->markTestSkipped('a PHP int is always within int32 range on 32-bit');
        }

        $request = new Query(
            query: 'INSERT INTO t (id) VALUES (?)',
            values: [$value],
            consistency: Consistency::ONE,
            options: new QueryOptions()
        );
        $request->setVersion(ProtocolVersion::V5);

        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_VALUES_INT_OUT_OF_INT32_RANGE->value);

        $request->getBody();
    }

    public function testQueryRejectsOversizedKeyspace(): void {
        $request = new Query(
            query: 'SELECT * FROM t',
            consistency: Consistency::ONE,
            options: new QueryOptions(keyspace: str_repeat('k', 65536))
        );
        $request->setVersion(ProtocolVersion::V5);

        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_FIELD_TOO_LONG->value);

        $request->getBody();
    }

    public function testQuotedMarkerTextInsideStringsAndCommentsIsIgnored(): void {
        $query = "SELECT ':\"Fake\"' FROM t /* :\"AlsoFake\" */ WHERE id = :fake";
        $request = new Query(
            query: $query,
            values: ['Fake' => new \Cassandra\Value\Int32(1)],
        );
        $request->setVersion(ProtocolVersion::V5);

        $valueSection = substr($request->getBody(), 4 + strlen($query) + 2 + 4);

        $this->assertStringStartsWith(pack('n', 1) . pack('n', 4) . 'fake', $valueSection);
    }

    public function testRegisterRejectsNonEventTypeEntries(): void {
        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_INVALID_REGISTER_EVENT->value);

        (new ReflectionClass(Register::class))->newInstanceArgs([[EventType::SCHEMA_CHANGE, 'STATUS_CHANGE']]);
    }

    public function testRegisterUsesADedicatedCodeWhenItsEventCountOverflows(): void {
        $request = new Register(array_fill(0, 65536, EventType::SCHEMA_CHANGE));

        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_TOO_MANY_REGISTER_EVENTS->value);

        $request->getBody();
    }

    public function testRepeatedPreparedStatementReplacementIsAtomic(): void {
        $query = 'INSERT INTO ks.t (v) VALUES (?)';
        $old = self::preparedResultWithMarker('old', Type::VARINT, $query);
        $new = self::preparedResultWithMarker('new', Type::INT, $query);

        $batch = new Batch();
        $batch->appendPreparedStatement($old, [1]);
        $batch->appendPreparedStatement($old, ['2']);

        try {
            $batch->replacePreparedStatement($new);
            $this->fail('Expected the second value to be incompatible with the new int marker');
        } catch (ValueException $e) {
            $this->assertSame(ExceptionCode::VALUE_INTEGER_INVALID_VALUE_TYPE->value, $e->getCode());
        }

        $this->assertSame($old, $batch->findPreparedStatement('old'));
        $this->assertNull($batch->findPreparedStatement('new'));
    }

    public function testRequestRejectsFrameBodyPastProtocolMaximum(): void {
        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_FRAME_BODY_TOO_LARGE->value);

        self::testableRequest()->assertBodyLength((256 * 1024 * 1024) + 1);
    }

    public function testStartupRejectsNonStringNames(): void {
        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_INVALID_STARTUP_OPTION->value);

        (new ReflectionClass(Startup::class))->newInstanceArgs([[0 => 'value']]);
    }

    public function testStartupRejectsNonStringValues(): void {
        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_INVALID_STARTUP_OPTION->value);

        (new ReflectionClass(Startup::class))->newInstanceArgs([['name' => 1]]);
    }

    public function testUnknownNamedBindValueThrows(): void {
        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_VALUES_EXTRA_BIND_VALUE->value);

        self::testableRequest()->encodeValuesForBindMarkers(
            ['a' => 5, 'unused' => 6],
            [self::columnInfo('a')],
            namesForValues: true
        );
    }

    public function testUnusedCaseVariantIsReportedAsAnExtraValue(): void {
        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_VALUES_EXTRA_BIND_VALUE->value);

        self::testableRequest()->encodeValuesForBindMarkers(
            ['UserId' => 5, 'userid' => 6],
            [self::columnInfo('userid')],
            namesForValues: true
        );
    }

    public function testWrongCaseDoesNotBindMixedCaseQuotedMarker(): void {
        $this->expectException(RequestException::class);
        $this->expectExceptionCode(ExceptionCode::REQUEST_VALUES_MISSING_BIND_VALUE->value);

        self::testableRequest()->encodeValuesForBindMarkers(
            ['foo' => 5],
            [self::columnInfo('Foo')],
            namesForValues: true
        );
    }

    private static function columnInfo(string $name): \Cassandra\Response\Result\ColumnInfo {
        return new \Cassandra\Response\Result\ColumnInfo(
            keyspace: 'ks',
            tableName: 't',
            name: $name,
            type: new \Cassandra\TypeInfo\SimpleTypeInfo(\Cassandra\Type::INT),
        );
    }

    private static function preparedResult(string $id, ?string $rowsMetadataId = null): CachedPreparedResult {
        return new CachedPreparedResult(
            new Header(version: ProtocolVersion::V5, flags: 0, stream: 0, opcode: Opcode::RESPONSE_RESULT, length: 0),
            new StreamReader(''),
            new PreparedData(
                id: $id,
                prepareMetadata: new PrepareMetadata(flags: 0, bindMarkersCount: 0, bindMarkers: [], pkCount: null, pkIndex: null),
                rowsMetadata: new RowsMetadata(flags: 0, columnsCount: 0, pagingState: null, metadataId: null, columns: []),
                rowsMetadataId: $rowsMetadataId,
            ),
        );
    }

    private static function preparedResultWithMarker(string $id, Type $type, string $query): CachedPreparedResult {
        $result = new CachedPreparedResult(
            new Header(version: ProtocolVersion::V4, flags: 0, stream: 0, opcode: Opcode::RESPONSE_RESULT, length: 0),
            new StreamReader(''),
            new PreparedData(
                id: $id,
                prepareMetadata: new PrepareMetadata(
                    flags: 0,
                    bindMarkersCount: 1,
                    bindMarkers: [new ColumnInfo('ks', 't', 'v', new SimpleTypeInfo($type))],
                    pkCount: 0,
                    pkIndex: [],
                ),
                rowsMetadata: new RowsMetadata(flags: 0, columnsCount: 0, pagingState: null, metadataId: null, columns: []),
            ),
        );
        $result->setRequest(new Prepare($query));

        return $result;
    }

    private static function testableRequest(): TestableBindMarkerRequest {
        return new TestableBindMarkerRequest();
    }

    private function unpackInt(string $format, string $bytes): int {
        $unpacked = unpack($format, $bytes);

        $this->assertIsArray($unpacked);
        $this->assertIsInt($unpacked[1]);

        return $unpacked[1];
    }
}

/**
 * Exposes the protected value-encoding helpers of {@see \Cassandra\Request\Request}
 * for direct unit testing.
 */
final class TestableBindMarkerRequest extends \Cassandra\Request\Request {
    public function __construct() {
        parent::__construct(\Cassandra\Protocol\Opcode::REQUEST_QUERY);
    }

    public function assertBodyLength(int $length): void {
        self::assertFrameBodyLength($length);
    }

    /**
     * @param array<mixed> $values
     */
    public function encodeValuesAsBinary(array $values, bool $namesForValues, bool $namesAreExact): string {
        return $this->encodeQueryValuesAsBinary($values, $namesForValues, $namesAreExact);
    }

    /**
     * @param array<mixed> $values
     * @param array<\Cassandra\Response\Result\ColumnInfo> $bindMarkers
     * @return array<mixed>
     */
    public function encodeValuesForBindMarkers(array $values, array $bindMarkers, bool $namesForValues): array {
        return $this->encodeQueryValuesForBindMarkerTypes($values, $bindMarkers, $namesForValues);
    }
}
