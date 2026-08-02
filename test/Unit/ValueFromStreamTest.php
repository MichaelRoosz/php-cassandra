<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Type;
use Cassandra\ValueFactory;
use Cassandra\Response\StreamReader;
use Cassandra\Value;
use Cassandra\Value\ValueBase;

final class ValueFromStreamTest extends AbstractUnitTestCase {
    public function testCollectionsFromStream(): void {
        // list<varchar>
        $listVals = [[], ['a'], ['hello', 'world']];
        foreach ($listVals as $v) {
            $obj = $this->decodeViaFromStream(
                Type::LIST,
                $v,
                ['valueType' => Type::VARCHAR]
            );
            $this->assertSame($v, $obj->getValue());
        }

        // set<int>
        $setVals = [[], [1], [1, 2, -3, 4]];
        foreach ($setVals as $v) {
            $obj = $this->decodeViaFromStream(
                Type::SET,
                $v,
                ['valueType' => Type::INT]
            );
            $this->assertSame($v, $obj->getValue());
        }

        // map<varchar,int>
        $mapVals = [
            [],
            ['a' => 1],
            ['hello' => 1, 'world' => -2],
        ];
        foreach ($mapVals as $v) {
            $obj = $this->decodeViaFromStream(
                Type::MAP,
                $v,
                ['keyType' => Type::VARCHAR, 'valueType' => Type::INT]
            );
            $this->assertSame($v, $obj->getValue());
        }
    }

    public function testDurationFromStream(): void {

        if (!$this->integerHasAtLeast64Bits()) {
            $this->markTestSkipped('Duration requires 64-bit integer');
        }

        $vals = [
            '0s',
            '1y2mo3d4h5m6s7ms8us9ns',
            '-1h2m3s',
            'PT0S',
        ];
        foreach ($vals as $v) {
            $obj = $this->decodeViaFromStream(Type::DURATION, $v);
            $this->assertIsString($obj->getValue());
            // Re-encode the decoded string via Value\Duration to normalize, then compare
            $normalized = (string) Value\Duration::fromValue($obj->getValue());
            $this->assertSame($normalized, $obj->getValue());
        }
    }

    public function testInetFromStream(): void {
        foreach ([
            '127.0.0.1',
            '0.0.0.0',
            '::1',
            '2001:db8::1',
        ] as $ip) {
            $obj = $this->decodeViaFromStream(Type::INET, $ip);
            $this->assertSame($ip, $obj->getValue());
        }
    }

    public function testShortenedTupleAndUdtFromBinaryNullFillTrailingValues(): void {
        $tupleType = ValueFactory::getTypeInfoFromTypeDefinition([
            'type' => Type::TUPLE,
            'valueTypes' => [Type::INT, Type::VARCHAR],
        ]);
        $tuple = Value\Tuple::fromBinary(pack('N', 4) . pack('N', 7), $tupleType);
        $this->assertSame([7, null], $tuple->getValue());

        $udtType = ValueFactory::getTypeInfoFromTypeDefinition([
            'type' => Type::UDT,
            'valueTypes' => [
                'existing' => Type::INT,
                'added_later' => Type::VARCHAR,
            ],
        ]);
        $udt = Value\UDT::fromBinary(pack('N', 4) . pack('N', 7), $udtType);
        $this->assertSame(['existing' => 7, 'added_later' => null], $udt->getValue());
    }

    public function testSimpleScalarsFromStream(): void {
        // int
        foreach ([0, 1, -1, Value\Int32::VALUE_MAX, Value\Int32::VALUE_MIN] as $v) {
            $obj = $this->decodeViaFromStream(Type::INT, $v);
            $this->assertSame($v, $obj->getValue());
        }

        // boolean
        foreach ([true, false] as $v) {
            $obj = $this->decodeViaFromStream(Type::BOOLEAN, $v);
            $this->assertSame($v, $obj->getValue());
        }

        // varchar
        foreach ([
            '',
            'hello',
            'Unicode: 🚀 中文 العربية русский',
        ] as $v) {
            $obj = $this->decodeViaFromStream(Type::VARCHAR, $v);
            $this->assertSame($v, $obj->getValue());
        }

        // bigint
        foreach ([0, 1, -1, PHP_INT_MAX, PHP_INT_MIN] as $v) {
            $obj = $this->decodeViaFromStream(Type::BIGINT, $v);
            $this->assertSame($v, $obj->getValue());
        }
    }

    public function testTupleAndUdtFromStream(): void {
        // tuple<varchar, int, boolean>
        $tupleVals = [
            ['hello', 1, true],
            ['', 0, false],
            ['x', -42, true],
        ];
        foreach ($tupleVals as $v) {
            $obj = $this->decodeViaFromStream(
                Type::TUPLE,
                $v,
                ['valueTypes' => [Type::VARCHAR, Type::INT, Type::BOOLEAN]]
            );
            $this->assertIsArray($obj->getValue());
            $this->assertSame($v, array_values($obj->getValue()));
        }

        // udt {street: varchar, zip: int, active: boolean}
        $udtVals = [
            ['street' => 'Main', 'zip' => 12345, 'active' => true],
            ['street' => '', 'zip' => 0, 'active' => false],
            ['street' => 'Unicode 🏠', 'zip' => Value\Int32::VALUE_MAX, 'active' => true],
        ];
        foreach ($udtVals as $v) {
            $obj = $this->decodeViaFromStream(
                Type::UDT,
                $v,
                ['valueTypes' => [
                    'street' => Type::VARCHAR,
                    'zip' => Type::INT,
                    'active' => Type::BOOLEAN,
                ]]
            );
            $this->assertSame($v, $obj->getValue());
        }

        // udt with null fields should decode as is (nulls preserved)
        $v = ['street' => null, 'zip' => null, 'active' => null];
        $obj = $this->decodeViaFromStream(
            Type::UDT,
            $v,
            ['valueTypes' => [
                'street' => Type::VARCHAR,
                'zip' => Type::INT,
                'active' => Type::BOOLEAN,
            ]]
        );
        $this->assertSame($v, $obj->getValue());
    }

    public function testVectorFromStreamFloatAndVarint(): void {
        // vector<float,3>
        $floatVec = [1.1, -2.2, 3.3];
        $obj = $this->decodeViaFromStream(
            Type::VECTOR,
            $floatVec,
            ['valueType' => Type::FLOAT, 'dimensions' => 3]
        );
        $decoded = $obj->getValue();
        $this->assertIsArray($decoded);
        $this->assertCount(3, $decoded);
        foreach ([0, 1, 2] as $i) {
            $this->assertIsFloat($decoded[$i]);
            $this->assertEqualsWithDelta((float) $floatVec[$i], (float) $decoded[$i], max(abs($floatVec[$i]) * 0.01, 0.0001));
        }

        // vector<varint,4>
        $varintVec = ['0', '1', '-2', '170141183460469231731687303715884105727'];
        $obj = $this->decodeViaFromStream(
            Type::VECTOR,
            $varintVec,
            ['valueType' => Type::VARINT, 'dimensions' => 4]
        );
        $decoded = $obj->getValue();
        $this->assertIsArray($decoded);
        $this->assertCount(4, $decoded);
        foreach ([0, 1, 2, 3] as $i) {
            $this->assertIsString($decoded[$i]);
            $this->assertEquals((string) $varintVec[$i], (string) $decoded[$i]);
        }
    }

    /**
     * smallint/tinyint have a positive fixedLength() but Cassandra still
     * serializes them as variable-length inside vectors (CASSANDRA-14476 is
     * unmerged in cassandra-5.0/trunk: ShortType/ByteType do not override
     * valueLengthIfFixed()). So each element must carry an unsigned-VInt length
     * prefix on the wire, and the read and write paths must agree on that.
     */
    public function testVectorFromStreamSmallintTinyintUseVariableLengthFraming(): void {
        // vector<smallint,4>
        $smallintVec = [1, -2, 300, 32767];
        $typeInfo = ValueFactory::getTypeInfoFromTypeDefinition(
            ['type' => Type::VECTOR, 'valueType' => Type::SMALLINT, 'dimensions' => 4]
        );
        $binary = ValueFactory::getBinaryByTypeInfo($typeInfo, $smallintVec);

        // Each of the 4 elements: 1-byte unsigned-VInt length (0x02) + 2 bytes.
        $this->assertSame(4 * 3, strlen($binary));
        foreach ([0, 3, 6, 9] as $elementOffset) {
            $this->assertSame(0x02, ord($binary[$elementOffset]), 'missing unsigned-VInt length prefix');
        }

        $decoded = $this->decodeViaFromStream(
            Type::VECTOR,
            $smallintVec,
            ['valueType' => Type::SMALLINT, 'dimensions' => 4]
        )->getValue();
        $this->assertSame($smallintVec, $decoded);

        // vector<tinyint,3>
        $tinyintVec = [1, -2, 127];
        $typeInfo = ValueFactory::getTypeInfoFromTypeDefinition(
            ['type' => Type::VECTOR, 'valueType' => Type::TINYINT, 'dimensions' => 3]
        );
        $binary = ValueFactory::getBinaryByTypeInfo($typeInfo, $tinyintVec);

        // Each of the 3 elements: 1-byte unsigned-VInt length (0x01) + 1 byte.
        $this->assertSame(3 * 2, strlen($binary));
        foreach ([0, 2, 4] as $elementOffset) {
            $this->assertSame(0x01, ord($binary[$elementOffset]), 'missing unsigned-VInt length prefix');
        }

        $decoded = $this->decodeViaFromStream(
            Type::VECTOR,
            $tinyintVec,
            ['valueType' => Type::TINYINT, 'dimensions' => 3]
        )->getValue();
        $this->assertSame($tinyintVec, $decoded);
    }

    /**
     * A variable-length element whose serialized size is >= 128 bytes must have
     * its length prefix written as an unsigned VInt (Cassandra's
     * putUnsignedVInt32), not a two's-complement varint. The two encodings only
     * diverge at length >= 128, so this needs a large element to catch it.
     */
    public function testVectorFromStreamVariableLengthElementOver127Bytes(): void {
        $longText = str_repeat('x', 200);
        $textVec = [$longText, 'short'];

        $typeInfo = ValueFactory::getTypeInfoFromTypeDefinition(
            ['type' => Type::VECTOR, 'valueType' => Type::VARCHAR, 'dimensions' => 2]
        );
        $binary = ValueFactory::getBinaryByTypeInfo($typeInfo, $textVec);

        // 200 as an unsigned VInt is 0x80 0xC8 (a two's-complement varint would
        // instead be 0x00 0xC8, which the reader would misinterpret).
        $this->assertSame("\x80\xC8", substr($binary, 0, 2), 'length prefix must be an unsigned VInt');

        $decoded = $this->decodeViaFromStream(
            Type::VECTOR,
            $textVec,
            ['valueType' => Type::VARCHAR, 'dimensions' => 2]
        )->getValue();
        $this->assertSame($textVec, $decoded);
    }
    /**
     * @template T
     * @param T $phpValue
     * @param array<string, mixed> $typeDefinition
     * @return ValueBase
     */
    private function decodeViaFromStream(Type $type, mixed $phpValue, array $typeDefinition = []): ValueBase {
        $typeInfo = $typeDefinition
            ? ValueFactory::getTypeInfoFromTypeDefinition($typeDefinition + ['type' => $type])
            : ValueFactory::getTypeInfoFromType($type);

        $binary = $phpValue instanceof ValueBase
            ? $phpValue->getBinary()
            : ValueFactory::getBinaryByTypeInfo($typeInfo, $phpValue);

        $stream = new StreamReader($binary);

        // Pass the content length. Classes that don't need it will ignore it.
        return ValueFactory::getValueObjectFromStream($typeInfo, strlen($binary), $stream);
    }
}
