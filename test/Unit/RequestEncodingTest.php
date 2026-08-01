<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Consistency;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\RequestException;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Batch;
use Cassandra\Request\BatchType;
use Cassandra\Request\Options\BatchOptions;
use Cassandra\Request\Options\QueryOptions;
use Cassandra\Request\Query;
use Cassandra\Request\QueryFlag;
use Cassandra\SerialConsistency;
use DateTimeImmutable;

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

    public function testExplicitNullBindValueIsAccepted(): void {
        $encoded = self::testableRequest()->encodeValuesForBindMarkers(
            ['userid' => null],
            [self::columnInfo('userid')],
            namesForValues: true
        );

        $this->assertSame(['userid' => null], $encoded);
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

    private static function columnInfo(string $name): \Cassandra\Response\Result\ColumnInfo {
        return new \Cassandra\Response\Result\ColumnInfo(
            keyspace: 'ks',
            tableName: 't',
            name: $name,
            type: new \Cassandra\TypeInfo\SimpleTypeInfo(\Cassandra\Type::INT),
        );
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
