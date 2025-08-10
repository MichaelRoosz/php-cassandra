<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use PHPUnit\Framework\TestCase;
use Cassandra\Type;
use Cassandra\TypeFactory;

final class TypeSerializationTest extends TestCase {
    public function testAscii(): void {
        $ascii = 'abcABC123!#_';
        $this->assertSame($ascii, Type\Ascii::fromBinary((new Type\Ascii($ascii))->getBinary())->getValue());
    }

    public function testBigint(): void {
        $int64Max = 9223372036854775807;
        $this->assertSame($int64Max, Type\Bigint::fromBinary((new Type\Bigint($int64Max))->getBinary())->getValue());
    }

    public function testBlob(): void {
        $blob = 'abcABC123!#_' . hex2bin('FFAA22');
        $this->assertSame($blob, Type\Blob::fromBinary((new Type\Blob($blob))->getBinary())->getValue());
    }

    public function testBoolean(): void {
        $this->assertSame(false, Type\Boolean::fromBinary((new Type\Boolean(false))->getBinary())->getValue());
        $this->assertSame(true, Type\Boolean::fromBinary((new Type\Boolean(true))->getBinary())->getValue());
    }

    public function testCollectionList(): void {
        $value = [
            1,
            1,
            2,
            2,
        ];

        $definition = Type::INT;

        $this->assertSame(
            $value,
            Type\CollectionList::fromBinary(
                (new Type\CollectionList($value, $definition))->getBinary(),
                TypeFactory::getTypeInfoFromTypeDefinition([
                    'type' => Type::COLLECTION_LIST,
                    'valueType' => $definition,
                ])
            )->getValue()
        );
    }

    public function testCollectionMap(): void {
        $value = [
            'a' => 1,
            'b' => 2,
        ];

        $keyDefinition = Type::ASCII;
        $valueDefinition = Type::INT;

        $this->assertSame(
            $value,
            Type\CollectionMap::fromBinary(
                (new Type\CollectionMap($value, $keyDefinition, $valueDefinition))->getBinary(),
                TypeFactory::getTypeInfoFromTypeDefinition([
                    'type' => Type::COLLECTION_MAP,
                    'keyType' => $keyDefinition,
                    'valueType' => $valueDefinition,
                ])
            )->getValue()
        );
    }

    public function testCollectionSet(): void {
        $value = [
            1,
            2,
            3,
        ];

        $definition = Type::INT;

        $this->assertSame(
            $value,
            Type\CollectionSet::fromBinary(
                (new Type\CollectionSet($value, $definition))->getBinary(),
                TypeFactory::getTypeInfoFromTypeDefinition([
                    'type' => Type::COLLECTION_SET,
                    'valueType' => $definition,
                ])
            )->getValue()
        );
    }

    public function testCounter(): void {
        $counter = 12345678901234;
        $this->assertSame($counter, Type\Counter::fromBinary((new Type\Counter($counter))->getBinary())->getValue());
    }

    public function testCustom(): void {
        $customValue = 'abcABC123!#_' . hex2bin('FFAA22');
        $javaClassName = 'java.lang.String';
        $this->assertSame(
            $customValue,
            Type\Custom::fromBinary(
                (new Type\Custom($customValue, $javaClassName))->getBinary(),
                TypeFactory::getTypeInfoFromTypeDefinition([
                    'type' => Type::CUSTOM,
                    'javaClassName' => $javaClassName,
                ])
            )->getValue()
        );
    }

    public function testDate(): void {
        $days = 19434;
        $this->assertSame($days, Type\Date::fromBinary((new Type\Date($days))->getBinary())->getValue());
    }

    public function testDecimal(): void {
        $decimal = '34345454545.120';
        $this->assertSame($decimal, Type\Decimal::fromBinary((new Type\Decimal($decimal))->getBinary())->getValue());

        $decimal = '34345454545';
        $this->assertSame($decimal, Type\Decimal::fromBinary((new Type\Decimal($decimal))->getBinary())->getValue());
    }

    public function testDouble(): void {
        $double = 12345678901234.4545435;
        $this->assertSame($double, Type\Double::fromBinary((new Type\Double($double))->getBinary())->getValue());
    }

    public function testDuration(): void {
        $minDuration = [
            'months' => -2147483648,
            'days' => -2147483648,
            'nanoseconds' => PHP_INT_MIN,
        ];

        $minDurationAsString = '-178956970y8mo306783378w2d2562047h47m16s854ms775us808ns';

        $maxDuration = [
            'months' => 2147483647,
            'days' => 2147483647,
            'nanoseconds' => PHP_INT_MAX,
        ];

        $maxDurationAsString = '178956970y7mo306783378w1d2562047h47m16s854ms775us807ns';

        $exampleDuration = [
            'months' => 1,
            'days' => 2,
            'nanoseconds' => 3000,
        ];

        $saneDurationString = '3000y11mo2w6d23h59m59s123ms456us789ns';

        $this->assertSame(
            $maxDuration,
            Type\Duration::fromBinary((new Type\Duration($maxDuration))->getBinary())->getValue()
        );

        $this->assertSame(
            $minDurationAsString,
            (string) new Type\Duration($minDuration)
        );

        $this->assertSame(
            $maxDurationAsString,
            (string) new Type\Duration($maxDuration)
        );

        $this->assertSame(
            $minDuration,
            Type\Duration::fromString($minDurationAsString)->getValue()
        );

        $this->assertSame(
            $maxDuration,
            Type\Duration::fromString($maxDurationAsString)->getValue()
        );

        $this->assertSame(
            $saneDurationString,
            (string) Type\Duration::fromString($saneDurationString)
        );

        $this->assertSame(
            '+ 0Y 1M 2D 0H 0M 0S 3F',
            (new Type\Duration($exampleDuration))->toDateInterval()->format('%R %yY %mM %dD %hH %iM %sS %fF')
        );

        $this->assertSame(
            '+ 3000Y 11M 20D 23H 59M 59S 123456F',
            (new Type\Duration(Type\Duration::fromString($saneDurationString)->getValue()))->toDateInterval()->format('%R %yY %mM %dD %hH %iM %sS %fF')
        );

        $this->assertSame(
            '+ -3000Y -11M -20D -23H -59M -59S -123456F',
            (new Type\Duration(Type\Duration::fromString('-' . $saneDurationString)->getValue()))->toDateInterval()->format('%R %yY %mM %dD %hH %iM %sS %fF')
        );

        $this->assertSame(
            '+ -178956970Y -8M -2147483648D -2562047H -47M -16S -854775F',
            (new Type\Duration($minDuration))->toDateInterval()->format('%R %yY %mM %dD %hH %iM %sS %fF')
        );

        $this->assertSame(
            '+ 178956970Y 7M 2147483647D 2562047H 47M 16S 854775F',
            (new Type\Duration($maxDuration))->toDateInterval()->format('%R %yY %mM %dD %hH %iM %sS %fF')
        );

        $this->assertSame(
            $exampleDuration,
            Type\Duration::fromDateInterval((new Type\Duration($exampleDuration))->toDateInterval())->getValue()
        );

        $this->assertSame(
            '3000y11mo2w6d23h59m59s123ms456us',
            (string) Type\Duration::fromDateInterval((new Type\Duration(Type\Duration::fromString($saneDurationString)->getValue()))->toDateInterval())
        );

        $this->assertSame(
            '-3000y11mo2w6d23h59m59s123ms456us',
            (string) Type\Duration::fromDateInterval((new Type\Duration(Type\Duration::fromString('-' . $saneDurationString)->getValue()))->toDateInterval())
        );

        $this->assertSame(
            [
                'months' => -2147483648,
                'days' => -2147483648,
                'nanoseconds' => PHP_INT_MIN + 808,
            ],
            Type\Duration::fromDateInterval((new Type\Duration($minDuration))->toDateInterval())->getValue()
        );

        $this->assertSame(
            [
                'months' => 2147483647,
                'days' => 2147483647,
                'nanoseconds' => PHP_INT_MAX - 807,
            ],
            Type\Duration::fromDateInterval((new Type\Duration($maxDuration))->toDateInterval())->getValue()
        );

        /*
                $this->assertSame(
                    '- 0Y 0M 1D 2H 10M 0S 0F',
                    (new Type\Duration(Type\Duration::fromString('-1d2h10m')->getValue())->format('%R %yY %mM %dD %hH %iM %sS %fF')
                );

                $this->assertSame(
                    '+ 0Y 0M 1D 2H 10M 0S 0F',
                    (new Type\Duration(Type\Duration::fromString('1d2h10m')->getValue())->format('%R %yY %mM %dD %hH %iM %sS %fF')
                );

                $this->assertSame(
                    '-1d2562047h47m16s854ms775us808ns',
                    (string)(Type\Duration::fromString('-1d' . substr((string)PHP_INT_MIN, 1) . 'ns'))
                );

                $this->assertSame(
                    '+1d2562047h47m16s854ms775us807ns',
                    '+' . (string)(Type\Duration::fromString('1d' . PHP_INT_MAX . 'ns'))
                );

                $this->assertSame(
                    '- 292Y 3M 11D 0H 47M 16S 854775F',
                    (new Type\Duration(Type\Duration::fromString('-1d' . substr((string)PHP_INT_MIN, 1) . 'ns')->getValue())->format('%R %yY %mM %dD %hH %iM %sS %fF')
                );

                $this->assertSame(
                    '+ 292Y 3M 11D 0H 47M 16S 854775F',
                    (new Type\Duration(Type\Duration::fromString('1d' . PHP_INT_MAX . 'ns')->getValue())->format('%R %yY %mM %dD %hH %iM %sS %fF')
                );
        */
    }

    public function testFloat32(): void {
        $float = 1024.5;
        $this->assertSame($float, Type\Float32::fromBinary((new Type\Float32($float))->getBinary())->getValue());
    }

    public function testInet(): void {
        $ipv4 = '192.168.22.1';
        $this->assertSame($ipv4, Type\Inet::fromBinary((new Type\Inet($ipv4))->getBinary())->getValue());

        $ipv6 = '2001:db8:3333:4444:5555:6666:7777:8888';
        $this->assertSame($ipv6, Type\Inet::fromBinary((new Type\Inet($ipv6))->getBinary())->getValue());
    }

    public function testInteger(): void {
        $int1 = 234355434;
        $this->assertSame($int1, Type\Integer::fromBinary((new Type\Integer($int1))->getBinary())->getValue());

        $int2 = -234355434;
        $this->assertSame($int2, Type\Integer::fromBinary((new Type\Integer($int2))->getBinary())->getValue());
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
                    'type' => Type::COLLECTION_LIST,
                    'valueType' => Type::VARCHAR,
                ],
                'drinks' => [
                    'type' => Type::COLLECTION_LIST,
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
            Type\CollectionSet::fromBinary(
                (new Type\CollectionSet($value, $definition))->getBinary(),
                TypeFactory::getTypeInfoFromTypeDefinition([
                    'type' => Type::COLLECTION_SET,
                    'valueType' => $definition,
                ])
            )->getValue()
        );
    }

    public function testSmallint(): void {
        $int1 = 32123;
        $this->assertSame($int1, Type\Smallint::fromBinary((new Type\Smallint($int1))->getBinary())->getValue());

        $int2 = -32124;
        $this->assertSame($int2, Type\Smallint::fromBinary((new Type\Smallint($int2))->getBinary())->getValue());
    }

    public function testTime(): void {
        $timeInNs = 86399999999999;
        $this->assertSame($timeInNs, Type\Time::fromBinary((new Type\Time($timeInNs))->getBinary())->getValue());
    }

    public function testTimestamp(): void {
        $timeInMs = 1674341495053;
        $this->assertSame($timeInMs, Type\Timestamp::fromBinary((new Type\Timestamp($timeInMs))->getBinary())->getValue());
    }

    public function testTimeuuid(): void {
        $timeUuid = 'bd23b48a-99de-11ed-a8fc-0242ac120002';
        $this->assertSame($timeUuid, Type\Timeuuid::fromBinary((new Type\Timeuuid($timeUuid))->getBinary())->getValue());
    }

    public function testTinyint(): void {
        $int1 = 127;
        $this->assertSame($int1, Type\Tinyint::fromBinary((new Type\Tinyint($int1))->getBinary())->getValue());

        $int2 = -127;
        $this->assertSame($int2, Type\Tinyint::fromBinary((new Type\Tinyint($int2))->getBinary())->getValue());
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
                (new Type\Tuple($value, $definition))->getBinary(),
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
                (new Type\UDT($value, $definition))->getBinary(),
                TypeFactory::getTypeInfoFromTypeDefinition([
                    'type' => Type::UDT,
                    'valueTypes' => $definition,
                ])
            )->getValue()
        );
    }

    public function testUuid(): void {
        $uuid = '346c9059-7d07-47e6-91c8-092b50e8306f';
        $this->assertSame($uuid, Type\Uuid::fromBinary((new Type\Uuid($uuid))->getBinary())->getValue());
    }

    public function testVarchar(): void {
        $varchar = 'abcABC123!#_';
        $this->assertSame($varchar, Type\Varchar::fromBinary((new Type\Varchar($varchar))->getBinary())->getValue());
    }

    public function testVarint(): void {
        $varint = 922337203685477580;
        $this->assertSame((string) $varint, (new Type\Varint($varint))->getValue());

        $varint = -922337203685477580;
        $this->assertSame((string) $varint, (new Type\Varint($varint))->getValue());

        $varint = 922337203685477580;
        $this->assertSame($varint, (new Type\Varint($varint))->getValueAsInt());

        $varint = -922337203685477580;
        $this->assertSame($varint, (new Type\Varint($varint))->getValueAsInt());

        $varint = 922337203685477580;
        $this->assertSame((string) $varint, Type\Varint::fromBinary((new Type\Varint($varint))->getBinary())->getValue());

        $varint = -922337203685477580;
        $this->assertSame((string) $varint, Type\Varint::fromBinary((new Type\Varint($varint))->getBinary())->getValue());

        $varint = 922337203685477580;
        $this->assertSame($varint, Type\Varint::fromBinary((new Type\Varint($varint))->getBinary())->getValueAsInt());

        $varint = -922337203685477580;
        $this->assertSame($varint, Type\Varint::fromBinary((new Type\Varint($varint))->getBinary())->getValueAsInt());

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

        $this->assertSame("\x00", (new Type\Varint(0))->getBinary());
        $this->assertSame("\x01", (new Type\Varint(1))->getBinary());
        $this->assertSame("\x7F", (new Type\Varint(127))->getBinary());
        $this->assertSame("\x00\x80", (new Type\Varint(128))->getBinary());
        $this->assertSame("\x00\x81", (new Type\Varint(129))->getBinary());
        $this->assertSame("\xFF", (new Type\Varint(-1))->getBinary());
        $this->assertSame("\x80", (new Type\Varint(-128))->getBinary());
        $this->assertSame("\xFF\x7F", (new Type\Varint(-129))->getBinary());
    }
}
