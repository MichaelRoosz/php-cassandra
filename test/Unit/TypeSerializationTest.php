<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Type;
use Cassandra\Value;
use Cassandra\ValueFactory;

final class TypeSerializationTest extends AbstractUnitTestCase {
    public function testAscii(): void {
        $ascii = 'abcABC123!#_';
        $this->assertSame($ascii, Value\Ascii::fromBinary((Value\Ascii::fromValue($ascii))->getBinary())->getValue());
    }

    public function testBigint(): void {

        if (!$this->integerHasAtLeast64Bits()) {
            $this->markTestSkipped('Bigint requires 64-bit integer');
        }

        $int64Max = 9223372036854775807;
        $this->assertSame($int64Max, Value\Bigint::fromBinary((Value\Bigint::fromValue($int64Max))->getBinary())->getValue());
    }

    public function testBlob(): void {
        $blob = 'abcABC123!#_' . hex2bin('FFAA22');
        $this->assertSame($blob, Value\Blob::fromBinary((Value\Blob::fromValue($blob))->getBinary())->getValue());
    }

    public function testBoolean(): void {
        $this->assertSame(false, Value\Boolean::fromBinary((Value\Boolean::fromValue(false))->getBinary())->getValue());
        $this->assertSame(true, Value\Boolean::fromBinary((Value\Boolean::fromValue(true))->getBinary())->getValue());
    }

    public function testCounter(): void {

        if (!$this->integerHasAtLeast64Bits()) {
            $this->markTestSkipped('Counter requires 64-bit integer');
        }

        $counter = 12345678901234;
        $this->assertSame($counter, Value\Counter::fromBinary((Value\Counter::fromValue($counter))->getBinary())->getValue());
    }

    public function testCustom(): void {
        $customValue = 'abcABC123!#_' . hex2bin('FFAA22');
        $javaClassName = 'java.lang.String';
        $this->assertSame(
            $customValue,
            Value\Custom::fromBinary(
                (Value\Custom::fromValue($customValue, $javaClassName))->getBinary(),
                ValueFactory::getTypeInfoFromTypeDefinition([
                    'type' => Type::CUSTOM,
                    'javaClassName' => $javaClassName,
                ])
            )->getValue()
        );
    }

    public function testDate(): void {
        $days = 19434;
        $this->assertSame($days, Value\Date::fromBinary((Value\Date::fromValue($days))->getBinary())->asInteger());

        $date = '2025-08-11';
        $this->assertSame($date, (Value\Date::fromValue($date))->asString());
        $this->assertSame($date, Value\Date::fromBinary((Value\Date::fromValue($date))->getBinary())->asString());
    }

    public function testDecimal(): void {
        $decimal = '34345454545.120';
        $this->assertSame($decimal, Value\Decimal::fromBinary((Value\Decimal::fromValue($decimal))->getBinary())->getValue());

        $decimal = '34345454545';
        $this->assertSame($decimal, Value\Decimal::fromBinary((Value\Decimal::fromValue($decimal))->getBinary())->getValue());
    }

    public function testDouble(): void {
        $double = 12345678901234.4545435;
        $this->assertSame($double, Value\Double::fromBinary((Value\Double::fromValue($double))->getBinary())->getValue());
    }

    public function testDuration(): void {
        if (!$this->integerHasAtLeast64Bits()) {
            $this->markTestSkipped('Duration requires 64-bit integer');
        }

        $minDuration = [
            'months' => -2147483647 - 1,
            'days' => -2147483647 - 1,
            'nanoseconds' => PHP_INT_MIN,
        ];

        $minDurationAsString = '-178956970y8mo2147483648d2562047h47m16s854ms775us808ns';

        $maxDuration = [
            'months' => 2147483647,
            'days' => 2147483647,
            'nanoseconds' => PHP_INT_MAX,
        ];

        $maxDurationAsString = '178956970y7mo2147483647d2562047h47m16s854ms775us807ns';

        $exampleDuration = [
            'months' => 1,
            'days' => 2,
            'nanoseconds' => 3000,
        ];

        $saneDurationString = '3000y11mo20d23h59m59s123ms456us789ns';

        $this->assertSame(
            $maxDuration,
            Value\Duration::fromBinary((Value\Duration::fromValue($maxDuration))->getBinary())->asNativeValue()
        );

        $this->assertSame(
            $minDurationAsString,
            (string) (Value\Duration::fromValue($minDuration))
        );

        $this->assertSame(
            $maxDurationAsString,
            (string) (Value\Duration::fromValue($maxDuration))
        );

        $this->assertSame(
            $minDuration,
            (Value\Duration::fromValue($minDurationAsString))->asNativeValue()
        );

        $this->assertSame(
            $maxDuration,
            (Value\Duration::fromValue($maxDurationAsString))->asNativeValue()
        );

        $this->assertSame(
            $saneDurationString,
            (string) (Value\Duration::fromValue($saneDurationString))
        );

        $this->assertSame(
            '+ 0Y 1M 2D 0H 0M 0S 3F',
            (Value\Duration::fromValue($exampleDuration))->asDateInterval()->format('%R %yY %mM %dD %hH %iM %sS %fF')
        );

        $this->assertSame(
            '+ 3000Y 11M 20D 23H 59M 59S 123456F',
            (Value\Duration::fromValue($saneDurationString))->asDateInterval()->format('%R %yY %mM %dD %hH %iM %sS %fF')
        );

        $this->assertSame(
            '+ -3000Y -11M -20D -23H -59M -59S -123456F',
            (Value\Duration::fromValue('-' . $saneDurationString))->asDateInterval()->format('%R %yY %mM %dD %hH %iM %sS %fF')
        );

        $this->assertSame(
            '+ -178956970Y -8M -2147483648D -2562047H -47M -16S -854775F',
            (Value\Duration::fromValue($minDuration))->asDateInterval()->format('%R %yY %mM %dD %hH %iM %sS %fF')
        );

        $this->assertSame(
            '+ 178956970Y 7M 2147483647D 2562047H 47M 16S 854775F',
            (Value\Duration::fromValue($maxDuration))->asDateInterval()->format('%R %yY %mM %dD %hH %iM %sS %fF')
        );

        $this->assertSame(
            $exampleDuration,
            (Value\Duration::fromValue(
                (Value\Duration::fromValue($exampleDuration))->asDateInterval()
            ))->asNativeValue()
        );

        $this->assertSame(
            '3000y11mo20d23h59m59s123ms456us',
            (string) (
                Value\Duration::fromValue(
                    (Value\Duration::fromValue(
                        (Value\Duration::fromValue($saneDurationString))->asNativeValue()
                    ))->asDateInterval()
                )
            )
        );

        $this->assertSame(
            '-3000y11mo20d23h59m59s123ms456us',
            (string) (
                Value\Duration::fromValue(
                    (Value\Duration::fromValue(
                        (Value\Duration::fromValue('-' . $saneDurationString))->asNativeValue()
                    ))->asDateInterval()
                )
            )
        );

        $this->assertSame(
            [
                'months' => -2147483647 - 1,
                'days' => -2147483647 - 1,
                'nanoseconds' => PHP_INT_MIN + 808,
            ],
            (Value\Duration::fromValue((Value\Duration::fromValue($minDuration))->asDateInterval()))->asNativeValue()
        );

        $this->assertSame(
            [
                'months' => 2147483647,
                'days' => 2147483647,
                'nanoseconds' => PHP_INT_MAX - 807,
            ],
            (Value\Duration::fromValue((Value\Duration::fromValue($maxDuration))->asDateInterval()))->asNativeValue()
        );

        $this->assertSame(
            '+ 0Y 0M -1D -2H -10M 0S 0F',
            (Value\Duration::fromValue('-1d2h10m'))->asDateInterval()->format('%R %yY %mM %dD %hH %iM %sS %fF')
        );

        $this->assertSame(
            '+ 0Y 0M 1D 2H 10M 0S 0F',
            (Value\Duration::fromValue('1d2h10m'))->asDateInterval()->format('%R %yY %mM %dD %hH %iM %sS %fF')
        );

        $this->assertSame(
            '-1d2562047h47m16s854ms775us808ns',
            (string) (Value\Duration::fromValue('-1d' . substr((string) PHP_INT_MIN, 1) . 'ns'))
        );

        $this->assertSame(
            '+1d2562047h47m16s854ms775us807ns',
            '+' . (string) (Value\Duration::fromValue('1d' . PHP_INT_MAX . 'ns'))
        );

        $this->assertSame(
            '+ 0Y 0M -1D -2562047H -47M -16S -854775F',
            (Value\Duration::fromValue('-1d' . substr((string) PHP_INT_MIN, 1) . 'ns'))
                ->asDateInterval()->format('%R %yY %mM %dD %hH %iM %sS %fF')
        );

        $this->assertSame(
            '+ 0Y 0M 1D 2562047H 47M 16S 854775F',
            (Value\Duration::fromValue('1d' . PHP_INT_MAX . 'ns'))
                ->asDateInterval()->format('%R %yY %mM %dD %hH %iM %sS %fF')
        );
    }

    public function testFloat32(): void {
        $float = 1024.5;
        $this->assertSame($float, Value\Float32::fromBinary((new Value\Float32($float))->getBinary())->getValue());
    }

    public function testInet(): void {
        $ipv4 = '192.168.22.1';
        $this->assertSame($ipv4, Value\Inet::fromBinary((Value\Inet::fromValue($ipv4))->getBinary())->getValue());

        $ipv6 = '2001:db8:3333:4444:5555:6666:7777:8888';
        $this->assertSame($ipv6, Value\Inet::fromBinary((Value\Inet::fromValue($ipv6))->getBinary())->getValue());
    }

    public function testInteger(): void {
        $int1 = 234355434;
        $this->assertSame($int1, Value\Int32::fromBinary((Value\Int32::fromValue($int1))->getBinary())->getValue());

        $int2 = -234355434;
        $this->assertSame($int2, Value\Int32::fromBinary((Value\Int32::fromValue($int2))->getBinary())->getValue());
    }

    public function testListCollection(): void {
        $value = [
            1,
            1,
            2,
            2,
        ];

        $definition = Type::INT;

        $this->assertSame(
            $value,
            Value\ListCollection::fromBinary(
                (Value\ListCollection::fromValue($value, $definition))->getBinary(),
                ValueFactory::getTypeInfoFromTypeDefinition([
                    'type' => Type::LIST,
                    'valueType' => $definition,
                ])
            )->getValue()
        );
    }

    public function testMapCollection(): void {
        $value = [
            'a' => 1,
            'b' => 2,
        ];

        $keyDefinition = Type::ASCII;
        $valueDefinition = Type::INT;

        $this->assertSame(
            $value,
            Value\MapCollection::fromBinary(
                (Value\MapCollection::fromValue($value, $keyDefinition, $valueDefinition))->getBinary(),
                ValueFactory::getTypeInfoFromTypeDefinition([
                    'type' => Type::MAP,
                    'keyType' => $keyDefinition,
                    'valueType' => $valueDefinition,
                ])
            )->getValue()
        );
    }

    public function testNested(): void {
        $value = [
            [
                'id' => 1,
                'name' => 'string',
                'active' => true,
                'friends' => [
                    'string1',
                    'string2',
                    'string3',
                ],
                'drinks' => [
                    [
                        'qty' => 5,
                        'brand' => 'Pepsi',
                    ],
                    [
                        'qty' => 3,
                        'brand' => 'Coke',
                    ],
                ],
            ], [
                'id' => 2,
                'name' => 'string',
                'active' => false,
                'friends' => [
                    'string4',
                    'string5',
                    'string6',
                ],
                'drinks' => [],
            ],
        ];

        $definition = [
            'type' => Type::UDT,
            'valueTypes' => [
                'id' => Type::INT,
                'name' => Type::VARCHAR,
                'active' => Type::BOOLEAN,
                'friends' => [
                    'type' => Type::LIST,
                    'valueType' => Type::VARCHAR,
                ],
                'drinks' => [
                    'type' => Type::LIST,
                    'valueType' => [
                        'type' => Type::UDT,
                        'valueTypes' => [
                            'qty' => Type::INT,
                            'brand' => Type::VARCHAR,
                        ],
                    ],
                ],
            ],
        ];

        $this->assertSame(
            $value,
            Value\SetCollection::fromBinary(
                (Value\SetCollection::fromValue($value, $definition))->getBinary(),
                ValueFactory::getTypeInfoFromTypeDefinition([
                    'type' => Type::SET,
                    'valueType' => $definition,
                ])
            )->getValue()
        );
    }

    public function testSetCollection(): void {
        $value = [
            1,
            2,
            3,
        ];

        $definition = Type::INT;

        $this->assertSame(
            $value,
            Value\SetCollection::fromBinary(
                (Value\SetCollection::fromValue($value, $definition))->getBinary(),
                ValueFactory::getTypeInfoFromTypeDefinition([
                    'type' => Type::SET,
                    'valueType' => $definition,
                ])
            )->getValue()
        );
    }

    public function testSmallint(): void {
        $int1 = 32123;
        $this->assertSame($int1, Value\Smallint::fromBinary((Value\Smallint::fromValue($int1))->getBinary())->getValue());

        $int2 = -32124;
        $this->assertSame($int2, Value\Smallint::fromBinary((Value\Smallint::fromValue($int2))->getBinary())->getValue());
    }

    public function testTime(): void {
        if (!$this->integerHasAtLeast64Bits()) {
            $this->markTestSkipped('Time requires 64-bit integer');
        }

        $timeInNs = 86399999999999;
        $this->assertSame($timeInNs, Value\Time::fromBinary((Value\Time::fromValue($timeInNs))->getBinary())->asInteger());
    }

    public function testTimestamp(): void {
        if (!$this->integerHasAtLeast64Bits()) {
            $this->markTestSkipped('Timestamp requires 64-bit integer');
        }

        $timeInMs = 1674341495053;
        $this->assertSame($timeInMs, Value\Timestamp::fromBinary((Value\Timestamp::fromValue($timeInMs))->getBinary())->asInteger());
    }

    public function testTimeuuid(): void {
        $timeUuid = 'bd23b48a-99de-11ed-a8fc-0242ac120002';
        $this->assertSame($timeUuid, Value\Timeuuid::fromBinary((Value\Timeuuid::fromValue($timeUuid))->getBinary())->getValue());
    }

    public function testTinyint(): void {
        $int1 = 127;
        $this->assertSame($int1, Value\Tinyint::fromBinary((Value\Tinyint::fromValue($int1))->getBinary())->getValue());

        $int2 = -127;
        $this->assertSame($int2, Value\Tinyint::fromBinary((Value\Tinyint::fromValue($int2))->getBinary())->getValue());
    }

    public function testTuple(): void {
        $value = [
            1,
            '2',
        ];

        $definition = [
            Type::INT,
            Type::VARCHAR,
        ];

        $this->assertSame(
            $value,
            Value\Tuple::fromBinary(
                (Value\Tuple::fromValue($value, $definition))->getBinary(),
                ValueFactory::getTypeInfoFromTypeDefinition([
                    'type' => Type::TUPLE,
                    'valueTypes' => $definition,
                ])
            )->getValue()
        );
    }

    public function testUDT(): void {
        $value = [
            'intField' => 1,
            'textField' => '2',
        ];

        $definition =[
            'intField' => Type::INT,
            'textField' => Type::VARCHAR,
        ];

        $this->assertSame(
            $value,
            Value\UDT::fromBinary(
                (Value\UDT::fromValue($value, $definition))->getBinary(),
                ValueFactory::getTypeInfoFromTypeDefinition([
                    'type' => Type::UDT,
                    'valueTypes' => $definition,
                ])
            )->getValue()
        );
    }

    public function testUuid(): void {
        $uuid = '346c9059-7d07-47e6-91c8-092b50e8306f';
        $this->assertSame($uuid, Value\Uuid::fromBinary((Value\Uuid::fromValue($uuid))->getBinary())->getValue());
    }

    public function testVarchar(): void {
        $varchar = 'abcABC123!#_';
        $this->assertSame($varchar, Value\Varchar::fromBinary((Value\Varchar::fromValue($varchar))->getBinary())->getValue());
    }

    public function testVarint(): void {
        $varint = 922337203685477580;
        $this->assertSame((string) $varint, (Value\Varint::fromValue($varint))->getValue());

        $varint = -922337203685477580;
        $this->assertSame((string) $varint, (Value\Varint::fromValue($varint))->getValue());

        $varint = 922337203685477580;
        $this->assertSame($varint, (Value\Varint::fromValue($varint))->asInt());

        $varint = -922337203685477580;
        $this->assertSame($varint, (Value\Varint::fromValue($varint))->asInt());

        $varint = 922337203685477580;
        $this->assertSame((string) $varint, Value\Varint::fromBinary((Value\Varint::fromValue($varint))->getBinary())->getValue());

        $varint = -922337203685477580;
        $this->assertSame((string) $varint, Value\Varint::fromBinary((Value\Varint::fromValue($varint))->getBinary())->getValue());

        $varint = 922337203685477580;
        $this->assertSame($varint, Value\Varint::fromBinary((Value\Varint::fromValue($varint))->getBinary())->asInt());

        $varint = -922337203685477580;
        $this->assertSame($varint, Value\Varint::fromBinary((Value\Varint::fromValue($varint))->getBinary())->asInt());

        $this->assertSame('0', Value\Varint::fromBinary("\x00")->getValue());
        $this->assertSame('1', Value\Varint::fromBinary("\x01")->getValue());
        $this->assertSame('127', Value\Varint::fromBinary("\x7F")->getValue());
        $this->assertSame('128', Value\Varint::fromBinary("\x00\x80")->getValue());
        $this->assertSame('129', Value\Varint::fromBinary("\x00\x81")->getValue());
        $this->assertSame('-1', Value\Varint::fromBinary("\xFF")->getValue());
        $this->assertSame('-128', Value\Varint::fromBinary("\x80")->getValue());
        $this->assertSame('-129', Value\Varint::fromBinary("\xFF\x7F")->getValue());

        $this->assertSame(0, Value\Varint::fromBinary("\x00")->asInt());
        $this->assertSame(1, Value\Varint::fromBinary("\x01")->asInt());
        $this->assertSame(127, Value\Varint::fromBinary("\x7F")->asInt());
        $this->assertSame(128, Value\Varint::fromBinary("\x00\x80")->asInt());
        $this->assertSame(129, Value\Varint::fromBinary("\x00\x81")->asInt());
        $this->assertSame(-1, Value\Varint::fromBinary("\xFF")->asInt());
        $this->assertSame(-128, Value\Varint::fromBinary("\x80")->asInt());
        $this->assertSame(-129, Value\Varint::fromBinary("\xFF\x7F")->asInt());

        $this->assertSame("\x00", (Value\Varint::fromValue(0))->getBinary());
        $this->assertSame("\x01", (Value\Varint::fromValue(1))->getBinary());
        $this->assertSame("\x7F", (Value\Varint::fromValue(127))->getBinary());
        $this->assertSame("\x00\x80", (Value\Varint::fromValue(128))->getBinary());
        $this->assertSame("\x00\x81", (Value\Varint::fromValue(129))->getBinary());
        $this->assertSame("\xFF", (Value\Varint::fromValue(-1))->getBinary());
        $this->assertSame("\x80", (Value\Varint::fromValue(-128))->getBinary());
        $this->assertSame("\xFF\x7F", (Value\Varint::fromValue(-129))->getBinary());
    }
}
