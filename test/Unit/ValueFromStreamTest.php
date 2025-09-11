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
        foreach ([0, 1, -1, 9223372036854775807, -9223372036854775807 - 1] as $v) {
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
