<?php

declare(strict_types=1);

namespace Cassandra\Test\Phpunit;

use PHPUnit\Framework\TestCase;
use Cassandra\Type;

final class TypeSerializationTest extends TestCase
{
    public function testAscii(): void
    {
        $ascii = 'abcABC123!#_';
        $this->assertSame($ascii, Type\Ascii::parse(Type\Ascii::binary($ascii)));
    }

    public function testBigint(): void
    {
        $int64Max = 9223372036854775807;
        $this->assertSame($int64Max, Type\Bigint::parse(Type\Bigint::binary($int64Max)));
    }

    public function testBlob(): void
    {
        $blob = 'abcABC123!#_' . hex2bin('FFAA22');
        $this->assertSame($blob, Type\Blob::parse(Type\Blob::binary($blob)));
    }

    public function testBoolean(): void
    {
        $this->assertSame(false, Type\Boolean::parse(Type\Boolean::binary(false)));
        $this->assertSame(true, Type\Boolean::parse(Type\Boolean::binary(true)));
    }

    public function testCollectionList(): void
    {
        $value = [
            1,
            1,
            2,
            2
        ];

        $definition = [
            Type\Base::INT
        ];

        $this->assertSame($value, Type\CollectionList::parse(Type\CollectionList::binary($value, $definition), $definition));
    }

    public function testCollectionMap(): void
    {
        $value = [
            'a' => 1,
            'b' => 2
        ];

        $definition = [
            Type\Base::ASCII,
            Type\Base::INT
        ];

        $this->assertSame($value, Type\CollectionMap::parse(Type\CollectionMap::binary($value, $definition), $definition));
    }

    public function testCollectionSet(): void
    {
        $value = [
            1,
            2,
            3
        ];

        $definition =[
            Type\Base::INT
        ];

        $this->assertSame($value, Type\CollectionSet::parse(Type\CollectionSet::binary($value, $definition), $definition));
    }

    public function testCounter(): void
    {
        $counter = 12345678901234;
        $this->assertSame($counter, Type\Counter::parse(Type\Counter::binary($counter)));
    }

    public function testCustom(): void
    {
        $custom = 'abcABC123!#_' . hex2bin('FFAA22');
        $this->assertSame($custom, Type\Custom::parse(Type\Custom::binary($custom, []), []));
    }

    public function testDate(): void
    {
        $days = 19434;
        $this->assertSame($days, Type\Date::parse(Type\Date::binary($days)));
    }

    public function testDecimal(): void
    {
        $decimal = '34345454545.120';
        $this->assertSame($decimal, Type\Decimal::parse(Type\Decimal::binary($decimal)));

        $decimal = '34345454545';
        $this->assertSame($decimal, Type\Decimal::parse(Type\Decimal::binary($decimal)));
    }

    public function testDouble(): void
    {
        $double = 12345678901234.4545435;
        $this->assertSame($double, Type\Double::parse(Type\Double::binary($double)));
    }

    public function testDuration(): void
    {
        $duration = [
            'months' => 1234567890,
            'days' => 2034567890,
            'nanoseconds' => 223372036854775807,
        ];
        $this->assertSame($duration, Type\Duration::parse(Type\Duration::binary($duration)));
    }

    public function testInet(): void
    {
        $ipv4 = '192.168.22.1';
        $this->assertSame($ipv4, Type\Inet::parse(Type\Inet::binary($ipv4)));

        $ipv6 = '2001:db8:3333:4444:5555:6666:7777:8888';
        $this->assertSame($ipv6, Type\Inet::parse(Type\Inet::binary($ipv6)));
    }

    public function testPhpFloat(): void
    {
        $float = 1024.5;
        $this->assertSame($float, Type\PhpFloat::parse(Type\PhpFloat::binary($float)));
    }

    public function testPhpInt(): void
    {
        $int1 = 234355434;
        $this->assertSame($int1, Type\PhpInt::parse(Type\PhpInt::binary($int1)));

        $int2 = -234355434;
        $this->assertSame($int2, Type\PhpInt::parse(Type\PhpInt::binary($int2)));
    }

    public function testSmallint(): void
    {
        $int1 = 32123;
        $this->assertSame($int1, Type\Smallint::parse(Type\Smallint::binary($int1)));

        $int2 = -32124;
        $this->assertSame($int2, Type\Smallint::parse(Type\Smallint::binary($int2)));
    }

    public function testTime(): void
    {
        $timeInNs = 1674341495053123456;
        $this->assertSame($timeInNs, Type\Time::parse(Type\Time::binary($timeInNs)));
    }

    public function testTimestamp(): void
    {
        $timeInMs = 1674341495053;
        $this->assertSame($timeInMs, Type\Timestamp::parse(Type\Timestamp::binary($timeInMs)));
    }

    public function testTimeuuid(): void
    {
        $timeUuid = 'bd23b48a-99de-11ed-a8fc-0242ac120002';
        $this->assertSame($timeUuid, Type\Timeuuid::parse(Type\Timeuuid::binary($timeUuid)));
    }

    public function testTinyint(): void
    {
        $int1 = 127;
        $this->assertSame($int1, Type\Tinyint::parse(Type\Tinyint::binary($int1)));

        $int2 = -127;
        $this->assertSame($int2, Type\Tinyint::parse(Type\Tinyint::binary($int2)));
    }

    public function testTuple(): void
    {
        $value = [
            1,
            '2'
        ];

        $definition = [
            Type\Base::INT,
            Type\Base::VARCHAR
        ];

        $this->assertSame($value, Type\Tuple::parse(Type\Tuple::binary($value, $definition), $definition));
    }

    public function testUDT(): void
    {
        $value = [
            'intField' => 1,
            'textField' => '2'
        ];

        $definition =[
            'intField' => Type\Base::INT,
            'textField' => Type\Base::VARCHAR
        ];

        $this->assertSame($value, Type\UDT::parse(Type\UDT::binary($value, $definition), $definition));
    }

    public function testUuid(): void
    {
        $uuid = '346c9059-7d07-47e6-91c8-092b50e8306f';
        $this->assertSame($uuid, Type\Uuid::parse(Type\Uuid::binary($uuid)));
    }

    public function testVarchar(): void
    {
        $varchar = 'abcABC123!#_';
        $this->assertSame($varchar, Type\Varchar::parse(Type\Varchar::binary($varchar)));
    }

    public function testVarint(): void
    {
        $varint = 922337203685477580;
        $this->assertSame($varint, Type\Varint::parse(Type\Varint::binary($varint)));

        $varint = -922337203685477580;
        $this->assertSame($varint, Type\Varint::parse(Type\Varint::binary($varint)));

        $this->assertSame(0, Type\Varint::parse("\x00"));
        $this->assertSame(1, Type\Varint::parse("\x01"));
        $this->assertSame(127, Type\Varint::parse("\x7F"));
        $this->assertSame(128, Type\Varint::parse("\x00\x80"));
        $this->assertSame(129, Type\Varint::parse("\x00\x81"));
        $this->assertSame(-1, Type\Varint::parse("\xFF"));
        $this->assertSame(-128, Type\Varint::parse("\x80"));
        $this->assertSame(-129, Type\Varint::parse("\xFF\x7F"));

        $this->assertSame("\x00", Type\Varint::binary(0));
        $this->assertSame("\x01", Type\Varint::binary(1));
        $this->assertSame("\x7F", Type\Varint::binary(127));
        $this->assertSame("\x00\x80", Type\Varint::binary(128));
        $this->assertSame("\x00\x81", Type\Varint::binary(129));
        $this->assertSame("\xFF", Type\Varint::binary(-1));
        $this->assertSame("\x80", Type\Varint::binary(-128));
        $this->assertSame("\xFF\x7F", Type\Varint::binary(-129));
    }

    public function testNested(): void
    {
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
                    ]
                ],
            ],[
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
            [
                'type' => Type\Base::UDT,
                'definition' => [
                    'id' => Type\Base::INT,
                    'name' => Type\Base::VARCHAR,
                    'active' => Type\Base::BOOLEAN,
                    'friends' => [
                        'type' => Type\Base::COLLECTION_LIST,
                        'value' => Type\Base::VARCHAR
                    ],
                    'drinks' => [
                        'type' => Type\Base::COLLECTION_LIST,
                        'value' => [
                            'type' => Type\Base::UDT,
                            'typeMap' => [
                                'qty' => Type\Base::INT,
                                'brand' => Type\Base::VARCHAR
                            ],
                        ],
                    ],
                ],
            ]
        ];
        $this->assertSame($value, Type\CollectionSet::parse(Type\CollectionSet::binary($value, $definition), $definition));
    }
}
