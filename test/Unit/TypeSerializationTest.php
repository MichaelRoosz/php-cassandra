<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use PHPUnit\Framework\TestCase;
use Cassandra\Type;
use Cassandra\TypeFactory;

final class TypeSerializationTest extends TestCase {
    public function testAscii(): void {
        $ascii = 'abcABC123!#_';
        $this->assertSame($ascii, Type\Ascii::fromBinary((Type\Ascii::fromValue($ascii))->getBinary())->getValue());
    }

    public function testBigint(): void {
        $int64Max = 9223372036854775807;
        $this->assertSame($int64Max, Type\Bigint::fromBinary((Type\Bigint::fromValue($int64Max))->getBinary())->getValue());
    }

    public function testBlob(): void {
        $blob = 'abcABC123!#_' . hex2bin('FFAA22');
        $this->assertSame($blob, Type\Blob::fromBinary((Type\Blob::fromValue($blob))->getBinary())->getValue());
    }

    public function testBoolean(): void {
        $this->assertSame(false, Type\Boolean::fromBinary((Type\Boolean::fromValue(false))->getBinary())->getValue());
        $this->assertSame(true, Type\Boolean::fromBinary((Type\Boolean::fromValue(true))->getBinary())->getValue());
    }

    public function testCounter(): void {
        $counter = 12345678901234;
        $this->assertSame($counter, Type\Counter::fromBinary((Type\Counter::fromValue($counter))->getBinary())->getValue());
    }

    public function testCustom(): void {
        $customValue = 'abcABC123!#_' . hex2bin('FFAA22');
        $javaClassName = 'java.lang.String';
        $this->assertSame(
            $customValue,
            Type\Custom::fromBinary(
                (Type\Custom::fromValue($customValue, $javaClassName))->getBinary(),
                TypeFactory::getTypeInfoFromTypeDefinition([
                    'type' => Type::CUSTOM,
                    'javaClassName' => $javaClassName,
                ])
            )->getValue()
        );
    }

    public function testDate(): void {
        $days = 19434;
        $this->assertSame($days, Type\Date::fromBinary((Type\Date::fromValue($days))->getBinary())->asInteger());

        $date = '2025-08-11';
        $this->assertSame($date, (Type\Date::fromValue($date))->asString());
        $this->assertSame($date, Type\Date::fromBinary((Type\Date::fromValue($date))->getBinary())->asString());
    }

    public function testDecimal(): void {
        $decimal = '34345454545.120';
        $this->assertSame($decimal, Type\Decimal::fromBinary((Type\Decimal::fromValue($decimal))->getBinary())->getValue());

        $decimal = '34345454545';
        $this->assertSame($decimal, Type\Decimal::fromBinary((Type\Decimal::fromValue($decimal))->getBinary())->getValue());
    }

    public function testDouble(): void {
        $double = 12345678901234.4545435;
        $this->assertSame($double, Type\Double::fromBinary((Type\Double::fromValue($double))->getBinary())->getValue());
    }

    public function testDuration(): void {
        $minDuration = [
            'months' => -2147483648,
            'days' => -2147483648,
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
            Type\Duration::fromBinary((Type\Duration::fromValue($maxDuration))->getBinary())->asNativeValue()
        );

        $this->assertSame(
            $minDurationAsString,
            (string) (Type\Duration::fromValue($minDuration))
        );

        $this->assertSame(
            $maxDurationAsString,
            (string) (Type\Duration::fromValue($maxDuration))
        );

        $this->assertSame(
            $minDuration,
            (Type\Duration::fromValue($minDurationAsString))->asNativeValue()
        );

        $this->assertSame(
            $maxDuration,
            (Type\Duration::fromValue($maxDurationAsString))->asNativeValue()
        );

        $this->assertSame(
            $saneDurationString,
            (string) (Type\Duration::fromValue($saneDurationString))
        );

        $this->assertSame(
            '+ 0Y 1M 2D 0H 0M 0S 3F',
            (Type\Duration::fromValue($exampleDuration))->asDateInterval()->format('%R %yY %mM %dD %hH %iM %sS %fF')
        );

        $this->assertSame(
            '+ 3000Y 11M 20D 23H 59M 59S 123456F',
            (Type\Duration::fromValue($saneDurationString))->asDateInterval()->format('%R %yY %mM %dD %hH %iM %sS %fF')
        );

        $this->assertSame(
            '+ -3000Y -11M -20D -23H -59M -59S -123456F',
            (Type\Duration::fromValue('-' . $saneDurationString))->asDateInterval()->format('%R %yY %mM %dD %hH %iM %sS %fF')
        );

        $this->assertSame(
            '+ -178956970Y -8M -2147483648D -2562047H -47M -16S -854775F',
            (Type\Duration::fromValue($minDuration))->asDateInterval()->format('%R %yY %mM %dD %hH %iM %sS %fF')
        );

        $this->assertSame(
            '+ 178956970Y 7M 2147483647D 2562047H 47M 16S 854775F',
            (Type\Duration::fromValue($maxDuration))->asDateInterval()->format('%R %yY %mM %dD %hH %iM %sS %fF')
        );

        $this->assertSame(
            $exampleDuration,
            (Type\Duration::fromValue(
                (Type\Duration::fromValue($exampleDuration))->asDateInterval()
            ))->asNativeValue()
        );

        $this->assertSame(
            '3000y11mo20d23h59m59s123ms456us',
            (string) (
                Type\Duration::fromValue(
                    (Type\Duration::fromValue(
                        (Type\Duration::fromValue($saneDurationString))->asNativeValue()
                    ))->asDateInterval()
                )
            )
        );

        $this->assertSame(
            '-3000y11mo20d23h59m59s123ms456us',
            (string) (
                Type\Duration::fromValue(
                    (Type\Duration::fromValue(
                        (Type\Duration::fromValue('-' . $saneDurationString))->asNativeValue()
                    ))->asDateInterval()
                )
            )
        );

        $this->assertSame(
            [
                'months' => -2147483648,
                'days' => -2147483648,
                'nanoseconds' => PHP_INT_MIN + 808,
            ],
            (Type\Duration::fromValue((Type\Duration::fromValue($minDuration))->asDateInterval()))->asNativeValue()
        );

        $this->assertSame(
            [
                'months' => 2147483647,
                'days' => 2147483647,
                'nanoseconds' => PHP_INT_MAX - 807,
            ],
            (Type\Duration::fromValue((Type\Duration::fromValue($maxDuration))->asDateInterval()))->asNativeValue()
        );

        $this->assertSame(
            '+ 0Y 0M -1D -2H -10M 0S 0F',
            (Type\Duration::fromValue('-1d2h10m'))->asDateInterval()->format('%R %yY %mM %dD %hH %iM %sS %fF')
        );

        $this->assertSame(
            '+ 0Y 0M 1D 2H 10M 0S 0F',
            (Type\Duration::fromValue('1d2h10m'))->asDateInterval()->format('%R %yY %mM %dD %hH %iM %sS %fF')
        );

        $this->assertSame(
            '-1d2562047h47m16s854ms775us808ns',
            (string) (Type\Duration::fromValue('-1d' . substr((string) PHP_INT_MIN, 1) . 'ns'))
        );

        $this->assertSame(
            '+1d2562047h47m16s854ms775us807ns',
            '+' . (string) (Type\Duration::fromValue('1d' . PHP_INT_MAX . 'ns'))
        );

        $this->assertSame(
            '+ 0Y 0M -1D -2562047H -47M -16S -854775F',
            (Type\Duration::fromValue('-1d' . substr((string) PHP_INT_MIN, 1) . 'ns'))
                ->asDateInterval()->format('%R %yY %mM %dD %hH %iM %sS %fF')
        );

        $this->assertSame(
            '+ 0Y 0M 1D 2562047H 47M 16S 854775F',
            (Type\Duration::fromValue('1d' . PHP_INT_MAX . 'ns'))
                ->asDateInterval()->format('%R %yY %mM %dD %hH %iM %sS %fF')
        );
    }

    public function testFloat32(): void {
        $float = 1024.5;
        $this->assertSame($float, Type\Float32::fromBinary((new Type\Float32($float))->getBinary())->getValue());
    }

    public function testInet(): void {
        $ipv4 = '192.168.22.1';
        $this->assertSame($ipv4, Type\Inet::fromBinary((Type\Inet::fromValue($ipv4))->getBinary())->getValue());

        $ipv6 = '2001:db8:3333:4444:5555:6666:7777:8888';
        $this->assertSame($ipv6, Type\Inet::fromBinary((Type\Inet::fromValue($ipv6))->getBinary())->getValue());
    }

    public function testInteger(): void {
        $int1 = 234355434;
        $this->assertSame($int1, Type\Integer::fromBinary((Type\Integer::fromValue($int1))->getBinary())->getValue());

        $int2 = -234355434;
        $this->assertSame($int2, Type\Integer::fromBinary((Type\Integer::fromValue($int2))->getBinary())->getValue());
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
            Type\ListCollection::fromBinary(
                (Type\ListCollection::fromValue($value, $definition))->getBinary(),
                TypeFactory::getTypeInfoFromTypeDefinition([
                    'type' => Type::LIST_COLLECTION,
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
            Type\MapCollection::fromBinary(
                (Type\MapCollection::fromValue($value, $keyDefinition, $valueDefinition))->getBinary(),
                TypeFactory::getTypeInfoFromTypeDefinition([
                    'type' => Type::MAP_COLLECTION,
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
                    'type' => Type::LIST_COLLECTION,
                    'valueType' => Type::VARCHAR,
                ],
                'drinks' => [
                    'type' => Type::LIST_COLLECTION,
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
            Type\SetCollection::fromBinary(
                (Type\SetCollection::fromValue($value, $definition))->getBinary(),
                TypeFactory::getTypeInfoFromTypeDefinition([
                    'type' => Type::SET_COLLECTION,
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
            Type\SetCollection::fromBinary(
                (Type\SetCollection::fromValue($value, $definition))->getBinary(),
                TypeFactory::getTypeInfoFromTypeDefinition([
                    'type' => Type::SET_COLLECTION,
                    'valueType' => $definition,
                ])
            )->getValue()
        );
    }

    public function testSmallint(): void {
        $int1 = 32123;
        $this->assertSame($int1, Type\Smallint::fromBinary((Type\Smallint::fromValue($int1))->getBinary())->getValue());

        $int2 = -32124;
        $this->assertSame($int2, Type\Smallint::fromBinary((Type\Smallint::fromValue($int2))->getBinary())->getValue());
    }

    public function testTime(): void {
        $timeInNs = 86399999999999;
        $this->assertSame($timeInNs, Type\Time::fromBinary((Type\Time::fromValue($timeInNs))->getBinary())->asInteger());
    }

    public function testTimestamp(): void {
        $timeInMs = 1674341495053;
        $this->assertSame($timeInMs, Type\Timestamp::fromBinary((Type\Timestamp::fromValue($timeInMs))->getBinary())->asInteger());
    }

    public function testTimeuuid(): void {
        $timeUuid = 'bd23b48a-99de-11ed-a8fc-0242ac120002';
        $this->assertSame($timeUuid, Type\Timeuuid::fromBinary((Type\Timeuuid::fromValue($timeUuid))->getBinary())->getValue());
    }

    public function testTinyint(): void {
        $int1 = 127;
        $this->assertSame($int1, Type\Tinyint::fromBinary((Type\Tinyint::fromValue($int1))->getBinary())->getValue());

        $int2 = -127;
        $this->assertSame($int2, Type\Tinyint::fromBinary((Type\Tinyint::fromValue($int2))->getBinary())->getValue());
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
            Type\Tuple::fromBinary(
                (Type\Tuple::fromValue($value, $definition))->getBinary(),
                TypeFactory::getTypeInfoFromTypeDefinition([
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
            Type\UDT::fromBinary(
                (Type\UDT::fromValue($value, $definition))->getBinary(),
                TypeFactory::getTypeInfoFromTypeDefinition([
                    'type' => Type::UDT,
                    'valueTypes' => $definition,
                ])
            )->getValue()
        );
    }

    public function testUuid(): void {
        $uuid = '346c9059-7d07-47e6-91c8-092b50e8306f';
        $this->assertSame($uuid, Type\Uuid::fromBinary((Type\Uuid::fromValue($uuid))->getBinary())->getValue());
    }

    public function testVarchar(): void {
        $varchar = 'abcABC123!#_';
        $this->assertSame($varchar, Type\Varchar::fromBinary((Type\Varchar::fromValue($varchar))->getBinary())->getValue());
    }

    public function testVarint(): void {
        $varint = 922337203685477580;
        $this->assertSame((string) $varint, (Type\Varint::fromValue($varint))->getValue());

        $varint = -922337203685477580;
        $this->assertSame((string) $varint, (Type\Varint::fromValue($varint))->getValue());

        $varint = 922337203685477580;
        $this->assertSame($varint, (Type\Varint::fromValue($varint))->getValueAsInt());

        $varint = -922337203685477580;
        $this->assertSame($varint, (Type\Varint::fromValue($varint))->getValueAsInt());

        $varint = 922337203685477580;
        $this->assertSame((string) $varint, Type\Varint::fromBinary((Type\Varint::fromValue($varint))->getBinary())->getValue());

        $varint = -922337203685477580;
        $this->assertSame((string) $varint, Type\Varint::fromBinary((Type\Varint::fromValue($varint))->getBinary())->getValue());

        $varint = 922337203685477580;
        $this->assertSame($varint, Type\Varint::fromBinary((Type\Varint::fromValue($varint))->getBinary())->getValueAsInt());

        $varint = -922337203685477580;
        $this->assertSame($varint, Type\Varint::fromBinary((Type\Varint::fromValue($varint))->getBinary())->getValueAsInt());

        $this->assertSame('0', Type\Varint::fromBinary("\x00")->getValue());
        $this->assertSame('1', Type\Varint::fromBinary("\x01")->getValue());
        $this->assertSame('127', Type\Varint::fromBinary("\x7F")->getValue());
        $this->assertSame('128', Type\Varint::fromBinary("\x00\x80")->getValue());
        $this->assertSame('129', Type\Varint::fromBinary("\x00\x81")->getValue());
        $this->assertSame('-1', Type\Varint::fromBinary("\xFF")->getValue());
        $this->assertSame('-128', Type\Varint::fromBinary("\x80")->getValue());
        $this->assertSame('-129', Type\Varint::fromBinary("\xFF\x7F")->getValue());

        $this->assertSame(0, Type\Varint::fromBinary("\x00")->getValueAsInt());
        $this->assertSame(1, Type\Varint::fromBinary("\x01")->getValueAsInt());
        $this->assertSame(127, Type\Varint::fromBinary("\x7F")->getValueAsInt());
        $this->assertSame(128, Type\Varint::fromBinary("\x00\x80")->getValueAsInt());
        $this->assertSame(129, Type\Varint::fromBinary("\x00\x81")->getValueAsInt());
        $this->assertSame(-1, Type\Varint::fromBinary("\xFF")->getValueAsInt());
        $this->assertSame(-128, Type\Varint::fromBinary("\x80")->getValueAsInt());
        $this->assertSame(-129, Type\Varint::fromBinary("\xFF\x7F")->getValueAsInt());

        $this->assertSame("\x00", (Type\Varint::fromValue(0))->getBinary());
        $this->assertSame("\x01", (Type\Varint::fromValue(1))->getBinary());
        $this->assertSame("\x7F", (Type\Varint::fromValue(127))->getBinary());
        $this->assertSame("\x00\x80", (Type\Varint::fromValue(128))->getBinary());
        $this->assertSame("\x00\x81", (Type\Varint::fromValue(129))->getBinary());
        $this->assertSame("\xFF", (Type\Varint::fromValue(-1))->getBinary());
        $this->assertSame("\x80", (Type\Varint::fromValue(-128))->getBinary());
        $this->assertSame("\xFF\x7F", (Type\Varint::fromValue(-129))->getBinary());
    }
}
