<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Consistency;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ResponseException as ResponseException;
use Cassandra\Exception\VIntCodecException;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\Value\EncodeOption\UuidEncodeOption;
use Cassandra\Value\ValueEncodeConfig;
use Cassandra\ValueFactory;
use Cassandra\VIntCodec;

final class StreamReaderTest extends AbstractUnitTestCase {
    public function testBasicReadAndOffset(): void {
        $reader = new StreamReader('abcdef');

        $this->assertSame(0, $reader->pos());
        $this->assertSame('ab', $reader->read(2));
        $this->assertSame(2, $reader->pos());

        $reader->offset(1);
        $this->assertSame(1, $reader->pos());
        $this->assertSame('bc', $reader->read(2));

        $reader->reset();
        $this->assertSame(0, $reader->pos());
        $this->assertSame('ab', $reader->read(2));
    }

    /**
     * Truncated vint data must be rejected rather than decoded as if the missing
     * bytes were zero.
     */
    public function testDecodeTruncatedVintThrows(): void {
        $codec = new VIntCodec();

        $encoded = $codec->encodeUnsignedVint64(1 << 40);
        $this->assertGreaterThan(2, strlen($encoded));

        $this->expectException(VIntCodecException::class);
        $codec->decodeUnsignedVint64(substr($encoded, 0, -1));
    }

    public function testOffsetsOutsideAvailableDataAreRefused(): void {
        foreach (
            [
                static fn (StreamReader $reader) => $reader->offset(-1),
                static fn (StreamReader $reader) => $reader->offset(4),
                static fn (StreamReader $reader) => $reader->extraDataOffset(-1),
                static fn (StreamReader $reader) => $reader->extraDataOffset(4),
            ] as $setInvalidOffset
        ) {
            $reader = new StreamReader('abc');

            try {
                $setInvalidOffset($reader);
                $this->fail('expected an invalid stream offset to be refused');
            } catch (ResponseException $e) {
                $this->assertSame(ExceptionCode::RESPONSE_SR_INVALID_OFFSET->value, $e->getCode());
            }
        }

        $reader = new StreamReader('abc');
        $reader->offset(2);
        $reader->extraDataOffset(2);

        $this->expectException(ResponseException::class);
        $this->expectExceptionCode(ExceptionCode::RESPONSE_SR_INVALID_OFFSET->value);
        $reader->offset(2);
    }

    public function testReadBeyondAvailableThrows(): void {
        $reader = new StreamReader('abc');
        $this->expectException(ResponseException::class);
        $this->expectExceptionCode(ExceptionCode::RESPONSE_SR_READ_BEYOND_AVAILABLE->value);
        $reader->read(10);
    }

    public function testReadByteShortIntLong(): void {
        // byte
        $reader = new StreamReader(pack('C', 0xAB));
        $this->assertSame(0xAB, $reader->readByte());

        // short
        $reader = new StreamReader(pack('n', 0x1234));
        $this->assertSame(0x1234, $reader->readShort());

        // int
        $reader = new StreamReader(pack('N', 2147483647) . pack('N', -1));
        $this->assertSame(2147483647, $reader->readInt());
        $this->assertSame(-1, $reader->readInt());

        if ($this->integerHasAtLeast64Bits()) {
            // long: StreamReader uses unpack('J'), so test simple positive values within range
            $valPos = 1;
            $valPos2 = 2;
            $bin = pack('J', $valPos) . pack('J', $valPos2);
            $reader = new StreamReader($bin);
            $this->assertSame($valPos, $reader->readLong());
            $this->assertSame($valPos2, $reader->readLong());
        }
    }

    public function testReadBytesTreatsEveryNegativeLengthAsNull(): void {
        $reader = new StreamReader(pack('N', -2));

        $this->assertNull($reader->readBytes());
    }

    public function testReadConsistency(): void {
        // short representing Consistency::ONE (0x0001)
        $reader = new StreamReader(pack('n', Consistency::ONE->value));
        $this->assertSame(Consistency::ONE, $reader->readConsistency());

        // invalid consistency short triggers error
        $reader = new StreamReader(pack('n', 0x00FF));
        $this->expectException(ResponseException::class);
        $this->expectExceptionCode(ExceptionCode::RESPONSE_SR_INVALID_CONSISTENCY->value);
        $reader->readConsistency();
    }

    public function testReadInetAddrErrors(): void {
        // invalid length
        $reader = new StreamReader(pack('C', 5) . 'abcde');
        $this->expectException(ResponseException::class);
        $this->expectExceptionCode(ExceptionCode::RESPONSE_SR_INVALID_INET_LENGTH->value);
        $reader->readInetAddr();
    }

    public function testReadInetAndInetAddr(): void {
        // IPv4 address
        $addr = inet_pton('10.0.0.7');
        $bin = pack('C', 4) . $addr;
        $reader = new StreamReader($bin);
        $this->assertSame('10.0.0.7', $reader->readInetAddr());

        // IPv6 address
        $addr6 = inet_pton('2001:db8::1');
        $bin = pack('C', 16) . $addr6;
        $reader = new StreamReader($bin);
        $this->assertSame('2001:db8::1', $reader->readInetAddr());

        // readInet combines address length + address + int port
        $addr = inet_pton('127.0.0.1');
        $bin = pack('C', 4) . $addr . pack('N', 9042);
        $reader = new StreamReader($bin);
        $this->assertSame(['ip' => '127.0.0.1', 'port' => 9042], $reader->readInet());
    }

    public function testReadStringCollectionsAndMaps(): void {
        // readStringList: count=3, values a,b,""
        $list = ['a', 'b', ''];
        $bin = pack('n', count($list));
        foreach ($list as $v) {
            $bin .= pack('n', strlen($v)) . $v;
        }
        $reader = new StreamReader($bin);
        $this->assertSame($list, $reader->readStringList());

        // readStringMap: 2 entries
        $entries = ['k1' => 'v1', 'k2' => ''];
        $bin = pack('n', 2);
        foreach ($entries as $k => $v) {
            $bin .= pack('n', strlen($k)) . $k;
            $bin .= pack('n', strlen($v)) . $v;
        }
        $reader = new StreamReader($bin);
        $this->assertSame($entries, $reader->readStringMap());

        // readStringMultimap: 1 key -> 2 values
        $bin = pack('n', 1);
        $bin .= pack('n', 3) . 'key';
        $bin .= pack('n', 2); // list count
        $bin .= pack('n', 1) . 'a';
        $bin .= pack('n', 1) . 'b';
        $reader = new StreamReader($bin);
        $this->assertSame(['key' => ['a', 'b']], $reader->readStringMultimap());

        // readBytesMap: 3 entries with null, empty and non-empty
        $bin = pack('n', 3);
        $bin .= pack('n', 4) . 'key1' . pack('N', -1); // null
        $bin .= pack('n', 4) . 'key2' . pack('N', 0); // empty string
        $bin .= pack('n', 4) . 'key3' . pack('N', strlen('value')) . 'value'; // non-empty
        $reader = new StreamReader($bin);
        $this->assertSame(
            ['key1' => null, 'key2' => '', 'key3' => 'value'],
            $reader->readBytesMap()
        );

        // readReasonMap: count=1, ip=127.0.0.1, value=123
        $ip = inet_pton('127.0.0.1');
        $bin = pack('N', 1); // count
        $bin .= pack('C', 4) . $ip; // inet addr
        $bin .= pack('n', 123); // short
        $reader = new StreamReader($bin);
        $this->assertSame(['127.0.0.1' => 123], $reader->readReasonMap());
    }

    public function testReadStringVariants(): void {
        // readString: length as short
        $s1 = 'hello';
        $bin = pack('n', strlen($s1)) . $s1 . pack('n', 0);
        $reader = new StreamReader($bin);
        $this->assertSame('hello', $reader->readString());
        $this->assertSame('', $reader->readString());

        // readLongString: length as int
        $s2 = 'world!';
        $bin = pack('N', strlen($s2)) . $s2 . pack('N', 0);
        $reader = new StreamReader($bin);
        $this->assertSame('world!', $reader->readLongString());
        $this->assertSame('', $reader->readLongString());

        // readShortBytes
        $b1 = "\x01\x02\x03";
        $bin = pack('n', strlen($b1)) . $b1 . pack('n', 0);
        $reader = new StreamReader($bin);
        $this->assertSame($b1, $reader->readShortBytes());
        $this->assertSame('', $reader->readShortBytes());

        // readBytes: int length with null(-1), empty(0), non-empty
        $data = 'xyz';
        $bin = pack('N', -1) . pack('N', 0) . pack('N', strlen($data)) . $data;
        $reader = new StreamReader($bin);
        $this->assertNull($reader->readBytes());
        $this->assertSame('', $reader->readBytes());
        $this->assertSame($data, $reader->readBytes());
    }

    /**
     * A collection carries its element type inline, so a type read off the wire
     * descends once per level of nesting — as deep as the peer asks for. PHP has
     * no catchable stack overflow, so a type deeper than this client will read
     * has to be refused rather than followed.
     */
    public function testReadTypeInfoRefusesUnboundedNesting(): void {
        $bin = str_repeat(pack('n', Type::LIST->value), 512) . pack('n', Type::INT->value);

        $reader = new StreamReader($bin);

        $this->expectException(ResponseException::class);
        $this->expectExceptionCode(ExceptionCode::RESPONSE_SR_TYPE_NESTING_TOO_DEEP->value);

        $reader->readTypeInfo();
    }

    public function testReadTypeInfoSimpleAndCollections(): void {
        // simple type: INT
        $bin = pack('n', Type::INT->value);
        $reader = new StreamReader($bin);
        $typeInfo = $reader->readTypeInfo();
        $this->assertSame(Type::INT, $typeInfo->type);

        // list<varchar>
        $bin = pack('n', Type::LIST->value) . pack('n', Type::VARCHAR->value);
        $reader = new StreamReader($bin);
        $typeInfo = $reader->readTypeInfo();
        $this->assertSame(Type::LIST, $typeInfo->type);

        // map<inet,int>
        $bin = pack('n', Type::MAP->value) . pack('n', Type::INET->value) . pack('n', Type::INT->value);
        $reader = new StreamReader($bin);
        $typeInfo = $reader->readTypeInfo();
        $this->assertSame(Type::MAP, $typeInfo->type);
    }

    public function testReadTypeInfoStillAcceptsRealisticNesting(): void {
        // list<list<list<map<text,int>>>> — deeper than any schema needs, and
        // well inside what the limit allows.
        $bin = str_repeat(pack('n', Type::LIST->value), 3)
            . pack('n', Type::MAP->value)
            . pack('n', Type::VARCHAR->value)
            . pack('n', Type::INT->value);

        $reader = new StreamReader($bin);

        $this->assertSame(Type::LIST, $reader->readTypeInfo()->type);
    }

    public function testReadUuid(): void {
        $uuid = '00112233-4455-6677-8899-aabbccddeeff';
        $hex = str_replace('-', '', $uuid);
        $bin = pack('H*', $hex);
        $reader = new StreamReader($bin);
        $this->assertSame($uuid, $reader->readUuid());
    }

    /**
     * Cassandra distinguishes null (length -1) from empty (length 0), and every
     * type below allows an empty value (AbstractType::allowsEmpty()). A
     * fixed-length decoder must not read its fixed size anyway — that consumes
     * the following cell and desyncs the rest of the row.
     */
    public function testReadValueEmptyFixedLengthValueDoesNotConsumeNextValue(): void {
        $cfg = new ValueEncodeConfig();

        $fixedLengthTypes = [
            Type::BIGINT,
            Type::BOOLEAN,
            Type::DOUBLE,
            Type::FLOAT,
            Type::INT,
            Type::SMALLINT,
            Type::TIMESTAMP,
            Type::TIMEUUID,
            Type::TINYINT,
            Type::UUID,
        ];

        foreach ($fixedLengthTypes as $type) {
            // an empty cell, followed by a varchar cell holding "next"
            $bin = pack('N', 0) . pack('N', 4) . 'next';
            $reader = new StreamReader($bin);

            $this->assertNull(
                $reader->readValue(ValueFactory::getTypeInfoFromType($type), $cfg),
                'empty ' . $type->name . ' should decode to null'
            );
            $this->assertSame(4, $reader->pos(), 'empty ' . $type->name . ' must consume exactly 0 bytes of body');
            $this->assertSame(
                'next',
                $reader->readValue(ValueFactory::getTypeInfoFromType(Type::VARCHAR), $cfg),
                'the value after an empty ' . $type->name . ' must still decode'
            );
        }
    }

    /**
     * A tuple and a UDT are the one family whose empty value is both allowed and
     * meaningful: Cassandra's TupleType overrides allowsEmpty() to true and
     * leaves isEmptyValueMeaningless() at false, so an empty cell denotes a
     * tuple whose components are all absent — an all-null tuple, not a null
     * tuple. UserType extends TupleType and inherits the same reading. The
     * DataStax Java driver's TupleCodec agrees ("empty byte buffers will result
     * in empty values"; null only for a null buffer).
     *
     * The null cell is asserted beside it because the whole point is that the
     * two stay distinct, and the trailing int because an empty cell must still
     * consume exactly zero bytes of body.
     */
    public function testReadValueEmptyTupleAndUdtDecodeToAllNullFields(): void {
        $cfg = new ValueEncodeConfig();

        $tupleInfo = ValueFactory::getTypeInfoFromTypeDefinition([
            'type' => Type::TUPLE,
            'valueTypes' => [Type::INT, Type::VARCHAR],
        ]);
        $udtInfo = ValueFactory::getTypeInfoFromTypeDefinition([
            'type' => Type::UDT,
            'valueTypes' => ['a' => Type::INT, 'b' => Type::VARCHAR],
        ]);

        foreach ([
            'tuple' => [$tupleInfo, [0 => null, 1 => null]],
            'UDT' => [$udtInfo, ['a' => null, 'b' => null]],
        ] as $label => [$typeInfo, $expected]) {
            // empty cell, then a null cell, then an int cell holding 4242
            $bin = pack('N', 0)
                . "\xff\xff\xff\xff"
                . pack('N', 4) . pack('N', 4242);

            $reader = new StreamReader($bin);

            $this->assertSame($expected, $reader->readValue($typeInfo, $cfg), 'empty ' . $label);
            $this->assertSame(4, $reader->pos(), 'empty ' . $label . ' must consume exactly 0 bytes of body');

            $this->assertNull($reader->readValue($typeInfo, $cfg), 'null ' . $label);

            $this->assertSame(
                4242,
                $reader->readValue(ValueFactory::getTypeInfoFromType(Type::INT), $cfg),
                'the value after an empty ' . $label . ' must still decode'
            );
        }
    }

    public function testReadValueEmptyValuePerTypeRepresentation(): void {
        $cfg = new ValueEncodeConfig();

        // Raw-byte-string types represent an empty cell as an empty string.
        foreach ([Type::ASCII, Type::BLOB, Type::TEXT, Type::VARCHAR] as $type) {
            $reader = new StreamReader(pack('N', 0));
            $this->assertSame('', $reader->readValue(ValueFactory::getTypeInfoFromType($type), $cfg), $type->name);
        }

        // Collections represent it as an empty collection.
        $reader = new StreamReader(pack('N', 0));
        $this->assertSame([], $reader->readValue(
            ValueFactory::getTypeInfoFromTypeDefinition(['type' => Type::LIST, 'valueType' => Type::INT]),
            $cfg
        ));

        $reader = new StreamReader(pack('N', 0));
        $this->assertSame([], $reader->readValue(
            ValueFactory::getTypeInfoFromTypeDefinition(['type' => Type::MAP, 'keyType' => Type::VARCHAR, 'valueType' => Type::INT]),
            $cfg
        ));
    }

    public function testReadValueHappyPaths(): void {
        // Value reading relies on length(int) + binary value according to TypeInfo
        // Example: int value 123
        $val = 123;
        $valueBinary = pack('N', $val);
        $bin = pack('N', strlen($valueBinary)) . $valueBinary; // length + content
        $reader = new StreamReader($bin);
        $cfg = new ValueEncodeConfig();
        $typeInfo = ValueFactory::getTypeInfoFromType(Type::INT);
        $this->assertSame($val, $reader->readValue($typeInfo, $cfg));

        // null (-1)
        $reader = new StreamReader(pack('N', -1));
        $this->assertNull($reader->readValue($typeInfo, $cfg));

        // empty (0) for varchar
        $typeInfo = ValueFactory::getTypeInfoFromType(Type::VARCHAR);
        $reader = new StreamReader(pack('N', 0));
        $this->assertSame('', $reader->readValue($typeInfo, $cfg));
    }

    /**
     * A cell whose declared length disagrees with what the decoder consumed must
     * fail loudly instead of silently shifting every following value.
     */
    public function testReadValueLengthMismatchThrows(): void {
        // an int cell claiming 8 bytes; the decoder only consumes 4
        $bin = pack('N', 8) . pack('N', 1) . pack('N', 2);
        $reader = new StreamReader($bin);

        $this->expectException(ResponseException::class);
        $this->expectExceptionCode(ExceptionCode::RESPONSE_SR_VALUE_LENGTH_MISMATCH->value);

        $reader->readValue(ValueFactory::getTypeInfoFromType(Type::INT), new ValueEncodeConfig());
    }

    public function testReadValueTreatsEveryNegativeBytesLengthAsNull(): void {
        $reader = new StreamReader(pack('N', -2) . pack('N', -3));
        $typeInfo = ValueFactory::getTypeInfoFromType(Type::INT);

        $this->assertNull($reader->readValue($typeInfo, new ValueEncodeConfig()));
        $this->assertNull($reader->readValue($typeInfo, new ValueEncodeConfig()));
    }

    public function testReadVIntVariants(): void {
        $codec = new VIntCodec();

        $cases = [0, 1, -1, 127, 128, 255, 256, -256, 2147483647, -2147483647 -1, PHP_INT_MAX, PHP_INT_MIN];
        foreach ($cases as $n) {
            $reader = new StreamReader($codec->encodeSignedVint64($n));
            $this->assertSame($n, $reader->readSignedVint64());
        }

        $uCases = [0, 1, 127, 128, 255, 256, 65535, 2147483647, PHP_INT_MAX];
        foreach ($uCases as $n) {
            $reader = new StreamReader($codec->encodeUnsignedVint64($n));
            $this->assertSame($n, $reader->readUnsignedVInt64());
        }
    }

    public function testReasonMapRejectsCountThatDoesNotFitBody(): void {
        $reader = new StreamReader(pack('N', 1));

        $this->expectException(ResponseException::class);
        $this->expectExceptionCode(ExceptionCode::RESPONSE_SR_INVALID_REASON_MAP_COUNT->value);

        $reader->readReasonMap();
    }

    public function testReasonMapRejectsExcessiveEagerAllocation(): void {
        $entry = chr(4) . inet_pton('127.0.0.1') . pack('n', 1);
        $reader = new StreamReader(pack('N', 65536) . str_repeat($entry, 65536));

        $this->expectException(ResponseException::class);
        $this->expectExceptionCode(ExceptionCode::RESPONSE_SR_INVALID_REASON_MAP_COUNT->value);

        $reader->readReasonMap();
    }
    public function testReasonMapRejectsNegativeCount(): void {
        $reader = new StreamReader(pack('N', -1));

        try {
            $reader->readReasonMap();
            $this->fail('expected negative reason-map count to be refused');
        } catch (ResponseException $e) {
            $this->assertSame(ExceptionCode::RESPONSE_SR_INVALID_REASON_MAP_COUNT->value, $e->getCode());
        }
    }

    public function testUuidEncodeOptions(): void {
        $uuid = '346c9059-7d07-47e6-91c8-092b50e8306f';
        $raw = pack('H*', str_replace('-', '', $uuid));
        $cell = pack('N', strlen($raw)) . $raw;

        foreach ([Type::UUID, Type::TIMEUUID] as $type) {
            $typeInfo = ValueFactory::getTypeInfoFromType($type);

            // default: canonical string form
            $reader = new StreamReader($cell);
            $this->assertSame(
                $uuid,
                $reader->readValue($typeInfo, new ValueEncodeConfig()),
                $type->name . ' default should decode to the canonical string'
            );
            $this->assertSame(20, $reader->pos());

            // AS_STRING explicit
            $reader = new StreamReader($cell);
            $cfg = new ValueEncodeConfig(uuidEncodeOption: UuidEncodeOption::AS_STRING);
            $this->assertSame($uuid, $reader->readValue($typeInfo, $cfg));

            // AS_BINARY: the raw 16 bytes, no formatting
            $reader = new StreamReader($cell);
            $cfg = new ValueEncodeConfig(uuidEncodeOption: UuidEncodeOption::AS_BINARY);
            $decoded = $reader->readValue($typeInfo, $cfg);
            $this->assertSame($raw, $decoded, $type->name . ' AS_BINARY should decode to raw bytes');
            $this->assertSame(16, strlen((string) $decoded));
            $this->assertSame(20, $reader->pos(), 'AS_BINARY must consume the whole cell body');
        }
    }

    public function testUuidEncodeOptionsInsideCollection(): void {
        // A list<uuid> element recurses through readValue, so the config must
        // reach nested values too.
        $uuid = '346c9059-7d07-47e6-91c8-092b50e8306f';
        $raw = pack('H*', str_replace('-', '', $uuid));

        $listBody = pack('N', 1) . pack('N', strlen($raw)) . $raw; // count=1, one element
        $cell = pack('N', strlen($listBody)) . $listBody;
        $typeInfo = ValueFactory::getTypeInfoFromTypeDefinition([
            'type' => Type::LIST,
            'valueType' => Type::UUID,
        ]);

        $reader = new StreamReader($cell);
        $this->assertSame([$uuid], $reader->readValue($typeInfo, new ValueEncodeConfig()));

        $reader = new StreamReader($cell);
        $cfg = new ValueEncodeConfig(uuidEncodeOption: UuidEncodeOption::AS_BINARY);
        $this->assertSame([$raw], $reader->readValue($typeInfo, $cfg));
    }
}
