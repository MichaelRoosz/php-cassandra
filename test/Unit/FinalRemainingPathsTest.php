<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Protocol\Header;
use Cassandra\Protocol\Opcode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\Request\Options\PrepareOptions;
use Cassandra\Request\Prepare;
use Cassandra\Response\Result\RowsMetadata;
use Cassandra\Response\Result\RowsResult;
use Cassandra\Response\StreamReader;
use Cassandra\Test\Support\ScriptedNode;
use Cassandra\Type;
use Cassandra\TypeInfo\CustomInfo;
use Cassandra\TypeInfo\MapCollectionInfo;
use Cassandra\TypeInfo\SimpleTypeInfo;
use Cassandra\TypeInfo\TupleInfo;
use Cassandra\Value;
use Cassandra\Value\EncodeOption\MapEncodeOption;
use Cassandra\Value\MapCollection;
use Cassandra\Value\MapEntry;
use Cassandra\Value\ValueComparator;
use Cassandra\Value\ValueEncodeConfig;
use Cassandra\VIntCodec;
use Closure;
use DateInterval;
use DateTimeImmutable;

/**
 * The last branches of the value and response layers that no larger scenario
 * passes through.
 */
final class FinalRemainingPathsTest extends AbstractUnitTestCase {
    public function testANegativeRowCountIsRefused(): void {
        $body = pack('N', 2) . pack('NN', 0x0001, 1) . ScriptedNode::string('ks') . ScriptedNode::string('t') . ScriptedNode::string('c') . pack('n', 9)
            . "\xFF\xFF\xFF\xFF";

        try {
            new RowsResult(new Header(ProtocolVersion::V4, 0, 1, Opcode::RESPONSE_RESULT, strlen($body)), new StreamReader($body));
            $this->fail('expected the row count to be refused');
        } catch (\Cassandra\Exception\ResponseException $e) {
            $this->assertSame(ExceptionCode::RESPONSE_ROWS_ROW_COUNT_OUT_OF_RANGE->value, $e->getCode());
        }
    }
    public function testAnInvertedIntervalWithNegativeFieldsIsPositive(): void {
        if (PHP_INT_SIZE < 8) {
            $this->markTestSkipped('needs 64-bit integers');
        }

        // Regression: "--5" from format('%r%d') was read as nothing, turning the interval into 0s.
        $interval = DateInterval::createFromDateString('-5 days -3 hours');
        $this->assertInstanceOf(DateInterval::class, $interval);
        $interval->invert = 1;

        $base = new DateTimeImmutable('2020-01-01 00:00:00');
        $this->assertSame('2020-01-06 03:00:00', $base->add($interval)->format('Y-m-d H:i:s'));
        $this->assertSame('5d3h', Value\Duration::fromValue($interval)->asString());

        $interval->invert = 0;
        $this->assertSame('-5d3h', Value\Duration::fromValue($interval)->asString());
    }

    public function testAPrepareWithItsOwnKeyspaceKeepsIt(): void {
        $prepare = new Prepare('SELECT 1', new PrepareOptions(keyspace: 'mine'));
        $prepare->applyDefaultKeyspace('default');
        $this->assertSame('mine', $prepare->getOptions()->keyspace);
    }

    public function testAskingTheIteratorForTheCurrentRowTwice(): void {
        $body = pack('N', 2) . pack('NN', 0x0001, 1) . ScriptedNode::string('ks') . ScriptedNode::string('t') . ScriptedNode::string('c') . pack('n', 9)
            . pack('N', 2) . pack('N', 4) . pack('N', 1) . pack('N', 4) . pack('N', 2);
        $rows = new RowsResult(new Header(ProtocolVersion::V4, 0, 1, Opcode::RESPONSE_RESULT, strlen($body)), new StreamReader($body));

        $iterator = $rows->getIterator();
        $this->assertSame(['c' => 1], $iterator->current());
        $this->assertSame(['c' => 1], $iterator->current());
        $iterator->next();
        $this->assertSame(['c' => 2], $iterator->current());
    }

    public function testATupleWithMoreElementsThanItsTypeIsRefused(): void {
        $int = pack('N', 4);
        $left = $int . pack('N', 1) . $int . pack('N', 1);
        $right = $int . pack('N', 1) . $int . pack('N', 2);

        $this->expectException(ValueException::class);
        ValueComparator::compare(new TupleInfo([new SimpleTypeInfo(Type::INT)]), $left, $right);
    }

    public function testBinaryCompatibilityOfDefinedTypes(): void {
        $int = new SimpleTypeInfo(Type::INT);
        $this->assertTrue((new MapCollectionInfo($int, $int, false))->isBinaryCompatibleWith(new MapCollectionInfo($int, new SimpleTypeInfo(Type::INT), true)));

        $custom = Value\Custom::fromValue('x', 'org.example.T');
        $this->assertTrue($custom->isBinaryCompatibleWith(new CustomInfo('org.example.T')));
        $this->assertFalse($custom->isBinaryCompatibleWith(new CustomInfo('org.example.Other')));

        $tuple = Value\Tuple::fromValue([1], [Type::INT]);
        $this->assertTrue($tuple->isBinaryCompatibleWith(new TupleInfo([new SimpleTypeInfo(Type::INT)])));
    }

    public function testColumnSpecsGivenPerColumn(): void {
        $column = ScriptedNode::string('ks') . ScriptedNode::string('t') . ScriptedNode::string('c') . pack('n', 9);
        $body = pack('N', 2) . pack('NN', 0, 1) . $column . pack('N', 1) . pack('N', 4) . pack('N', 7);
        $rows = new RowsResult(new Header(ProtocolVersion::V4, 0, 1, Opcode::RESPONSE_RESULT, strlen($body)), new StreamReader($body));
        $this->assertSame([['c' => 7]], $rows->fetchAll());

        $body = pack('N', 4) . pack('n', 2) . 'id' . pack('NNN', 0, 1, 0) . $column . pack('NN', 0x0004, 0);
        $prepared = new \Cassandra\Response\Result\PreparedResult(new Header(ProtocolVersion::V4, 0, 1, Opcode::RESPONSE_RESULT, strlen($body)), new StreamReader($body));
        $this->assertSame('t', $prepared->getPreparedData()->prepareMetadata->bindMarkers[0]->tableName);
    }

    public function testComparatorTiesOnNullsAndEqualTimeUuids(): void {
        $tuple = new TupleInfo([new SimpleTypeInfo(Type::INT), new SimpleTypeInfo(Type::INT)]);
        $left = Value\Tuple::fromValue([null, 1], [Type::INT, Type::INT])->getBinary();
        $right = Value\Tuple::fromValue([null, 2], [Type::INT, Type::INT])->getBinary();
        $this->assertSame(-1, ValueComparator::compare($tuple, $left, $right) <=> 0);

        // The version nibble is not part of the timestamp, so these tie.
        $version1 = (string) hex2bin('8db964108dba11f0b0eb325096b39f47');
        $version2 = (string) hex2bin('8db964108dba21f0b0eb325096b39f47');
        $this->assertSame(0, ValueComparator::compare(new SimpleTypeInfo(Type::TIMEUUID), $version1, $version2));
    }

    public function testDateAndTimestampStringsPhpRefuses(): void {
        if (PHP_INT_SIZE < 8) {
            $this->markTestSkipped('needs 64-bit integers');
        }

        $this->assertCode(ExceptionCode::VALUE_DATE_INVALID_STRING_FORMAT, static fn () => Value\Date::fromValue('2020-13-01'));

        $this->assertCode(ExceptionCode::VALUE_TIMESTAMP_INVALID_VALUE_TYPE, static fn () => Value\Timestamp::fromValue('2020-01-01 25:99'));
    }

    public function testDurationComponentsThatOverflowOnlyTogether(): void {
        if (PHP_INT_SIZE < 8) {
            $this->markTestSkipped('needs 64-bit integers');
        }

        // Each component fits on its own; their nanosecond total does not.
        $this->assertCode(ExceptionCode::VALUE_DURATION_NANOSECONDS_OUT_OF_RANGE, static fn () => Value\Duration::fromValue('2562047h48m'));
    }

    public function testMapKeysThatCannotBecomeArrayKeys(): void {
        $auto = new ValueEncodeConfig(mapEncodeOption: MapEncodeOption::AUTO);
        $asArray = new ValueEncodeConfig(mapEncodeOption: MapEncodeOption::AS_ARRAY);

        $objectKey = MapCollection::fromEntries([new MapEntry(Value\Int32::fromValue(1), 2)], Type::INT, Type::INT);
        $this->assertInstanceOf(MapCollection::class, $objectKey->asConfigured($auto));
        $this->assertCode(ExceptionCode::VALUE_MAP_CANNOT_CONVERT_TO_ARRAY, static fn () => $objectKey->asConfigured($asArray));

        // Two spellings of one text key meet as the same PHP array key.
        $collapsing = MapCollection::fromEntries([new MapEntry('1', 1), new MapEntry(1, 2)], Type::VARCHAR, Type::INT);
        $this->assertInstanceOf(MapCollection::class, $collapsing->asConfigured($auto));
        $this->assertCode(ExceptionCode::VALUE_MAP_CANNOT_CONVERT_TO_ARRAY, static fn () => $collapsing->asConfigured($asArray));
    }

    public function testNewerRowsMetadataIsKept(): void {
        $newer = new RowsMetadata(0, 1, null, 'new-id', null);
        $this->assertSame($newer, $newer->mergeWithPreviousMetadata(new RowsMetadata(0, 1, null, 'old-id', null)));
    }

    public function testVIntCodecEdges(): void {
        $codec = new VIntCodec();
        $this->assertSame(5, $codec->decodeUnsignedVint64("\x05"));
        $this->assertSame(5, $codec->decodeUnsignedVint32($codec->encodeUnsignedVint32(5)));
        $this->assertSame(-3, $codec->decodeSignedVint32($codec->encodeSignedVint32(-3)));

        if (PHP_INT_SIZE === 8) {
            // The widest value takes all eight extra bytes.
            $this->assertSame(PHP_INT_MAX, $codec->decodeUnsignedVint64($codec->encodeUnsignedVint64(PHP_INT_MAX)));
            $this->assertSame(-1, $codec->decodeUnsignedVint64($codec->encodeUnsignedVint64(-1)));
        }
    }

    /**
     * @param Closure(): mixed $call
     */
    private function assertCode(ExceptionCode $expected, Closure $call): void {
        try {
            $call();
            $this->fail('expected ' . $expected->name);
        } catch (ValueException $e) {
            $this->assertSame($expected->value, $e->getCode(), $e->getMessage());
        }
    }
}
