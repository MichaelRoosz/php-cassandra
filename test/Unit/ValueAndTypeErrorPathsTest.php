<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Exception\CassandraException;
use Cassandra\Exception\StringMathException;
use Cassandra\Exception\TypeInfoException;
use Cassandra\Exception\ValueException;
use Cassandra\Exception\ValueFactoryException;
use Cassandra\Exception\VIntCodecException;
use Cassandra\Response\StreamReader;
use Cassandra\StringMath\DecimalCalculator;
use Cassandra\StringMath\DecimalCalculator\BCMath;
use Cassandra\StringMath\DecimalCalculator\GMP;
use Cassandra\StringMath\DecimalCalculator\Native;
use Cassandra\Test\Support\Faults;
use Cassandra\Type;
use Cassandra\TypeInfo\CustomInfo;
use Cassandra\TypeInfo\ListCollectionInfo;
use Cassandra\TypeInfo\MapCollectionInfo;
use Cassandra\TypeInfo\SetCollectionInfo;
use Cassandra\TypeInfo\SimpleTypeInfo;
use Cassandra\TypeInfo\TupleInfo;
use Cassandra\TypeInfo\UDTInfo;
use Cassandra\TypeInfo\VectorInfo;
use Cassandra\Value;
use Cassandra\ValueFactory;
use Cassandra\VIntCodec;
use Closure;
use DateInterval;
use DateTimeImmutable;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use ReflectionMethod;
use ReflectionProperty;
use stdClass;

/**
 * The refusals of the value and type classes: wrong PHP types, missing or
 * mismatched definitions, out-of-range numbers, and failures of the PHP
 * functions underneath, injected with {@see Faults}.
 */
final class ValueAndTypeErrorPathsTest extends AbstractUnitTestCase {
    protected function setUp(): void {
        parent::setUp();

        if (PHP_INT_SIZE < 8) {
            $this->markTestSkipped('needs 64-bit integers');
        }
    }

    protected function tearDown(): void {
        Faults::reset();
        (new ReflectionProperty(DecimalCalculator::class, 'calculator'))->setValue(null, null);
        parent::tearDown();
    }

    /**
     * @return array<string, array{class-string<CassandraException>, Closure(): mixed}>
     */
    public static function refusals(): array {
        $int = new SimpleTypeInfo(Type::INT);
        $text = Type::VARCHAR;
        $bytes = static fn (string $binary): StreamReader => new StreamReader($binary);

        $cases = [
            // Definitions that are not what the type needs.
            'custom: no type' => [TypeInfoException::class, static fn () => self::unchecked(CustomInfo::class, 'fromTypeDefinition', [])],
            'custom: wrong type' => [TypeInfoException::class, static fn () => self::unchecked(CustomInfo::class, 'fromTypeDefinition', ['type' => Type::INT])],
            'custom: no class' => [TypeInfoException::class, static fn () => self::unchecked(CustomInfo::class, 'fromTypeDefinition', ['type' => Type::CUSTOM])],
            'custom: class not a string' => [TypeInfoException::class, static fn () => self::unchecked(CustomInfo::class, 'fromTypeDefinition', ['type' => Type::CUSTOM, 'javaClassName' => 1])],
            'list: no type' => [TypeInfoException::class, static fn () => self::unchecked(ListCollectionInfo::class, 'fromTypeDefinition', [])],
            'list: wrong type' => [TypeInfoException::class, static fn () => self::unchecked(ListCollectionInfo::class, 'fromTypeDefinition', ['type' => Type::SET, 'valueType' => $text])],
            'list: no value type' => [TypeInfoException::class, static fn () => self::unchecked(ListCollectionInfo::class, 'fromTypeDefinition', ['type' => Type::LIST])],
            'set: no type' => [TypeInfoException::class, static fn () => self::unchecked(SetCollectionInfo::class, 'fromTypeDefinition', [])],
            'set: wrong type' => [TypeInfoException::class, static fn () => self::unchecked(SetCollectionInfo::class, 'fromTypeDefinition', ['type' => Type::LIST, 'valueType' => $text])],
            'set: no value type' => [TypeInfoException::class, static fn () => self::unchecked(SetCollectionInfo::class, 'fromTypeDefinition', ['type' => Type::SET])],
            'map: no type' => [TypeInfoException::class, static fn () => self::unchecked(MapCollectionInfo::class, 'fromTypeDefinition', [])],
            'map: wrong type' => [TypeInfoException::class, static fn () => self::unchecked(MapCollectionInfo::class, 'fromTypeDefinition', ['type' => Type::LIST, 'keyType' => $text, 'valueType' => $text])],
            'map: no key type' => [TypeInfoException::class, static fn () => self::unchecked(MapCollectionInfo::class, 'fromTypeDefinition', ['type' => Type::MAP, 'valueType' => $text])],
            'map: no value type' => [TypeInfoException::class, static fn () => self::unchecked(MapCollectionInfo::class, 'fromTypeDefinition', ['type' => Type::MAP, 'keyType' => $text])],
            'simple: no type' => [TypeInfoException::class, static fn () => self::unchecked(SimpleTypeInfo::class, 'fromTypeDefinition', [])],
            'simple: complex type' => [TypeInfoException::class, static fn () => self::unchecked(SimpleTypeInfo::class, 'fromTypeDefinition', ['type' => Type::LIST])],
            'tuple: no type' => [TypeInfoException::class, static fn () => self::unchecked(TupleInfo::class, 'fromTypeDefinition', [])],
            'tuple: wrong type' => [TypeInfoException::class, static fn () => self::unchecked(TupleInfo::class, 'fromTypeDefinition', ['type' => Type::UDT, 'valueTypes' => [$text]])],
            'tuple: no value types' => [TypeInfoException::class, static fn () => self::unchecked(TupleInfo::class, 'fromTypeDefinition', ['type' => Type::TUPLE])],
            'tuple: value types not a list' => [TypeInfoException::class, static fn () => self::unchecked(TupleInfo::class, 'fromTypeDefinition', ['type' => Type::TUPLE, 'valueTypes' => 'x'])],
            'udt: no type' => [TypeInfoException::class, static fn () => self::unchecked(UDTInfo::class, 'fromTypeDefinition', [])],
            'udt: wrong type' => [TypeInfoException::class, static fn () => self::unchecked(UDTInfo::class, 'fromTypeDefinition', ['type' => Type::TUPLE, 'valueTypes' => ['a' => $text]])],
            'udt: no value types' => [TypeInfoException::class, static fn () => self::unchecked(UDTInfo::class, 'fromTypeDefinition', ['type' => Type::UDT])],
            'udt: value types not an array' => [TypeInfoException::class, static fn () => self::unchecked(UDTInfo::class, 'fromTypeDefinition', ['type' => Type::UDT, 'valueTypes' => 'x'])],
            'vector: no dimensions' => [TypeInfoException::class, static fn () => new VectorInfo($int, 0)],
            'vector: no type' => [TypeInfoException::class, static fn () => self::unchecked(VectorInfo::class, 'fromTypeDefinition', [])],
            'vector: wrong type' => [TypeInfoException::class, static fn () => self::unchecked(VectorInfo::class, 'fromTypeDefinition', ['type' => Type::LIST, 'valueType' => $text, 'dimensions' => 1])],
            'vector: no value type' => [TypeInfoException::class, static fn () => self::unchecked(VectorInfo::class, 'fromTypeDefinition', ['type' => Type::VECTOR, 'dimensions' => 1])],
            'vector: no dimensions given' => [TypeInfoException::class, static fn () => self::unchecked(VectorInfo::class, 'fromTypeDefinition', ['type' => Type::VECTOR, 'valueType' => $text])],
            'vector: dimensions not an int' => [TypeInfoException::class, static fn () => self::unchecked(VectorInfo::class, 'fromTypeDefinition', ['type' => Type::VECTOR, 'valueType' => $text, 'dimensions' => '3'])],
            'factory: complex type without a definition' => [ValueFactoryException::class, static fn () => ValueFactory::getTypeInfoFromType(Type::LIST)],
            'factory: null value' => [ValueFactoryException::class, static fn () => ValueFactory::getBinaryByTypeInfo($int, null)],

            // Values a type cannot hold.
            'bigint: not an int' => [ValueException::class, static fn () => Value\Bigint::fromMixedValue('1')],
            'blob: not a string' => [ValueException::class, static fn () => Value\Blob::fromMixedValue(1)],
            'boolean: not a bool' => [ValueException::class, static fn () => Value\Boolean::fromMixedValue(1)],
            'custom: no type info' => [ValueException::class, static fn () => Value\Custom::fromMixedValue('x')],
            'custom: wrong type info' => [ValueException::class, static fn () => Value\Custom::fromMixedValue('x', $int)],
            'custom: not a string' => [ValueException::class, static fn () => Value\Custom::fromMixedValue(1, new CustomInfo('a.B'))],
            'custom: binary without type info' => [ValueException::class, static fn () => Value\Custom::fromBinary('x')],
            'custom: binary with wrong type info' => [ValueException::class, static fn () => Value\Custom::fromBinary('x', $int)],
            'date: out of range' => [ValueException::class, static fn () => new Value\Date(4_294_967_296)],
            'date: bad string' => [ValueException::class, static fn () => new Value\Date('not a date')],
            'date: string past the range' => [ValueException::class, static fn () => new Value\Date('5881580-07-12')],
            'date: string before the range' => [ValueException::class, static fn () => new Value\Date('-5877641-06-22')],
            'date: object past the range' => [ValueException::class, static fn () => new Value\Date(new DateTimeImmutable('-5877641-06-22'))],
            'date: wrong PHP type' => [ValueException::class, static fn () => Value\Date::fromMixedValue(1.5)],
            'decimal: not numeric' => [ValueException::class, static fn () => new Value\Decimal('abc')],
            'decimal: mixed not numeric' => [ValueException::class, static fn () => Value\Decimal::fromMixedValue([])],
            'decimal: exponent too large' => [ValueException::class, static fn () => new Value\Decimal('1e99999999')],
            'double: not numeric' => [ValueException::class, static fn () => Value\Double::fromMixedValue('x')],
            'duration: wrong PHP type' => [ValueException::class, static fn () => Value\Duration::fromMixedValue(1)],
            'duration: no months' => [ValueException::class, static fn () => self::unchecked(Value\Duration::class, 'fromValue', ['days' => 0, 'nanoseconds' => 0])],
            'duration: no days' => [ValueException::class, static fn () => self::unchecked(Value\Duration::class, 'fromValue', ['months' => 0, 'nanoseconds' => 0])],
            'duration: days out of range' => [ValueException::class, static fn () => self::unchecked(Value\Duration::class, 'fromValue', ['months' => 0, 'days' => 2_147_483_648, 'nanoseconds' => 0])],
            'duration: no nanoseconds' => [ValueException::class, static fn () => self::unchecked(Value\Duration::class, 'fromValue', ['months' => 0, 'days' => 0])],
            'duration: component wider than an int' => [ValueException::class, static fn () => self::unchecked(Value\Duration::class, 'fromValue', '99999999999999999999999h')],
            'float: not numeric' => [ValueException::class, static fn () => Value\Float32::fromMixedValue('x')],
            'inet: wrong binary length' => [ValueException::class, static fn () => new Value\Inet("\1\2\3", isInBinaryForm: true)],
            'inet: not a string' => [ValueException::class, static fn () => Value\Inet::fromMixedValue(1)],
            'inet: wrong stream length' => [ValueException::class, static fn () => Value\Inet::fromStream($bytes("\1\2\3"), 3)],
            'int: out of range' => [ValueException::class, static fn () => new Value\Int32(2_147_483_648)],
            'list: not an array' => [ValueException::class, static fn () => Value\ListCollection::fromMixedValue(1)],
            'list: no type info' => [ValueException::class, static fn () => Value\ListCollection::fromMixedValue([])],
            'list: wrong type info' => [ValueException::class, static fn () => Value\ListCollection::fromMixedValue([], $int)],
            'list: stream without type info' => [ValueException::class, static fn () => Value\ListCollection::fromStream($bytes(''), 0)],
            'list: stream with wrong type info' => [ValueException::class, static fn () => Value\ListCollection::fromStream($bytes(''), 0, $int)],
            'map: entries not a list' => [ValueException::class, static fn () => self::unchecked(Value\MapCollection::class, 'fromEntries', ['a' => new Value\MapEntry(1, 1)], Type::INT, Type::INT)],
            'map: not an entry' => [ValueException::class, static fn () => self::unchecked(Value\MapCollection::class, 'fromEntries', [1], Type::INT, Type::INT)],
            'map: null key' => [ValueException::class, static fn () => self::unchecked(Value\MapCollection::class, 'fromEntries', [new Value\MapEntry(null, 1)], Type::INT, Type::INT)],
            'map: not an array' => [ValueException::class, static fn () => Value\MapCollection::fromMixedValue(1)],
            'map: no type info' => [ValueException::class, static fn () => Value\MapCollection::fromMixedValue([])],
            'map: wrong type info' => [ValueException::class, static fn () => Value\MapCollection::fromMixedValue([], $int)],
            'map: stream without type info' => [ValueException::class, static fn () => Value\MapCollection::fromStream($bytes(''), 0)],
            'map: stream with wrong type info' => [ValueException::class, static fn () => Value\MapCollection::fromStream($bytes(''), 0, $int)],
            'map: null key on the wire' => [ValueException::class, static fn () => Value\MapCollection::fromBinary(pack('NN', 1, 0xFFFFFFFF) . pack('N', 4) . pack('N', 1), new MapCollectionInfo($int, $int, false))],
            'set: not an array' => [ValueException::class, static fn () => Value\SetCollection::fromMixedValue(1)],
            'set: no type info' => [ValueException::class, static fn () => Value\SetCollection::fromMixedValue([])],
            'set: wrong type info' => [ValueException::class, static fn () => Value\SetCollection::fromMixedValue([], $int)],
            'set: stream without type info' => [ValueException::class, static fn () => Value\SetCollection::fromStream($bytes(''), 0)],
            'set: stream with wrong type info' => [ValueException::class, static fn () => Value\SetCollection::fromStream($bytes(''), 0, $int)],
            'smallint: out of range' => [ValueException::class, static fn () => new Value\Smallint(32768)],
            'smallint: not an int' => [ValueException::class, static fn () => Value\Smallint::fromMixedValue('1')],
            'time: out of range' => [ValueException::class, static fn () => new Value\Time(-1)],
            'time: bad string' => [ValueException::class, static fn () => new Value\Time('noon')],
            'time: impossible time' => [ValueException::class, static fn () => new Value\Time('25:00:00')],
            'time: wrong PHP type' => [ValueException::class, static fn () => Value\Time::fromMixedValue(1.5)],
            'timestamp: wrong PHP type' => [ValueException::class, static fn () => Value\Timestamp::fromMixedValue(1.5)],
            'timeuuid: wrong binary length' => [ValueException::class, static fn () => Value\Timeuuid::fromBinary('short')],
            'timeuuid: not a string' => [ValueException::class, static fn () => Value\Timeuuid::fromMixedValue(1)],
            'tinyint: out of range' => [ValueException::class, static fn () => new Value\Tinyint(128)],
            'tinyint: not an int' => [ValueException::class, static fn () => Value\Tinyint::fromMixedValue('1')],
            'tuple: not an array' => [ValueException::class, static fn () => Value\Tuple::fromMixedValue(1)],
            'tuple: no type info' => [ValueException::class, static fn () => Value\Tuple::fromMixedValue([])],
            'tuple: wrong type info' => [ValueException::class, static fn () => Value\Tuple::fromMixedValue([], $int)],
            'tuple: stream without type info' => [ValueException::class, static fn () => Value\Tuple::fromStream($bytes(''), 0)],
            'tuple: stream with wrong type info' => [ValueException::class, static fn () => Value\Tuple::fromStream($bytes(''), 0, $int)],
            'udt: not an array' => [ValueException::class, static fn () => Value\UDT::fromMixedValue(1)],
            'udt: no type info' => [ValueException::class, static fn () => Value\UDT::fromMixedValue([])],
            'udt: wrong type info' => [ValueException::class, static fn () => Value\UDT::fromMixedValue([], $int)],
            'udt: stream without type info' => [ValueException::class, static fn () => Value\UDT::fromStream($bytes(''), 0)],
            'udt: stream with wrong type info' => [ValueException::class, static fn () => Value\UDT::fromStream($bytes(''), 0, $int)],
            'uuid: wrong binary length' => [ValueException::class, static fn () => Value\Uuid::fromBinary('short')],
            'uuid: not a string' => [ValueException::class, static fn () => Value\Uuid::fromMixedValue(1)],
            'varchar: not a string' => [ValueException::class, static fn () => Value\Varchar::fromMixedValue(1)],
            'varint: not an integer' => [ValueException::class, static fn () => new Value\Varint('1.5')],
            'varint: mixed float' => [ValueException::class, static fn () => Value\Varint::fromMixedValue(1.5)],
            'vector: not an array' => [ValueException::class, static fn () => Value\Vector::fromMixedValue(1)],
            'vector: no type info' => [ValueException::class, static fn () => Value\Vector::fromMixedValue([])],
            'vector: wrong type info' => [ValueException::class, static fn () => Value\Vector::fromMixedValue([], $int)],
            'vector: stream without type info' => [ValueException::class, static fn () => Value\Vector::fromStream($bytes(''), 0)],
            'vector: stream with wrong type info' => [ValueException::class, static fn () => Value\Vector::fromStream($bytes(''), 0, $int)],
            'length-prefixed: no length' => [ValueException::class, static fn () => Value\Varchar::fromStream($bytes('x'))],
            'length-prefixed: empty meaningless value' => [ValueException::class, static fn () => Value\Varint::fromStream($bytes(''), 0)],
            'fixed-width: wrong length' => [ValueException::class, static fn () => Value\Int32::fromStream($bytes("\0\0\0\0\0"), 5)],
            'fixed-width: empty meaningless value' => [ValueException::class, static fn () => Value\Int32::fromStream($bytes(''), 0)],

            // Varints out of the range a 32-bit read can carry.
            'vint: signed 32 too large' => [VIntCodecException::class, static fn () => (new VIntCodec())->decodeSignedVint32((new VIntCodec())->encodeSignedVint64(PHP_INT_MAX))],
            'vint: unsigned 32 too large' => [VIntCodecException::class, static fn () => (new VIntCodec())->decodeUnsignedVint32((new VIntCodec())->encodeUnsignedVint64(PHP_INT_MAX))],
            'vint: empty' => [VIntCodecException::class, static fn () => (new VIntCodec())->decodeUnsignedVint64('')],
            'vint: encode signed 32 too large' => [VIntCodecException::class, static fn () => (new VIntCodec())->encodeSignedVint32(PHP_INT_MAX)],
            'vint: encode unsigned 32 negative' => [VIntCodecException::class, static fn () => (new VIntCodec())->encodeUnsignedVint32(-1)],
            'vint: read signed 32 too large' => [VIntCodecException::class, static fn () => (new VIntCodec())->readSignedVint32($bytes((new VIntCodec())->encodeSignedVint64(PHP_INT_MAX)))],
            'vint: read unsigned 32 too large' => [VIntCodecException::class, static fn () => (new VIntCodec())->readUnsignedVint32($bytes((new VIntCodec())->encodeUnsignedVint64(PHP_INT_MAX)))],

            // The pure-PHP calculator refuses what is not a digit string.
            'native: add1' => [StringMathException::class, static fn () => (new Native())->add1('1a')],
            'native: addUnsignedInt8 digits' => [StringMathException::class, static fn () => (new Native())->addUnsignedInt8('1a', 1)],
            'native: addUnsignedInt8 addend' => [StringMathException::class, static fn () => (new Native())->addUnsignedInt8('1', 256)],
            'native: multiplyBy256' => [StringMathException::class, static fn () => (new Native())->multiplyBy256('1a')],
            'native: sub1' => [StringMathException::class, static fn () => (new Native())->sub1('1a')],
        ];

        // A failed unpack(), which only a broken PHP would produce.
        $unpackFailures = [
            'bigint' => static fn () => Value\Bigint::fromBinary(str_repeat("\0", 8)),
            'date' => static fn () => Value\Date::fromBinary(str_repeat("\0", 4)),
            'decimal scale' => static fn () => Value\Decimal::fromBinary(str_repeat("\0", 5)),
            'double' => static fn () => Value\Double::fromBinary(str_repeat("\0", 8)),
            'float' => static fn () => Value\Float32::fromBinary(str_repeat("\0", 4)),
            'int' => static fn () => Value\Int32::fromBinary(str_repeat("\0", 4)),
            'smallint' => static fn () => Value\Smallint::fromBinary(str_repeat("\0", 2)),
            'time' => static fn () => Value\Time::fromBinary(str_repeat("\0", 8)),
            'timestamp' => static fn () => Value\Timestamp::fromBinary(str_repeat("\0", 8)),
            'tinyint' => static fn () => Value\Tinyint::fromBinary("\0"),
            'varint' => static fn () => Value\Varint::fromBinary("\1"),
        ];
        foreach ($unpackFailures as $name => $call) {
            $cases["$name: unpack fails"] = [ValueException::class, static function () use ($call): mixed {
                Faults::inject('unpack', false);

                return $call();
            }];
        }
        $cases['vint: unpack fails'] = [VIntCodecException::class, static function (): mixed {
            Faults::inject('unpack', false);

            return (new VIntCodec())->decodeUnsignedVint64("\1");
        }];
        $cases['inet: inet_ntop fails'] = [ValueException::class, static function (): mixed {
            Faults::inject('inet_ntop', false);

            return (new Value\Inet("\1\2\3\4", isInBinaryForm: true))->getValue();
        }];

        return $cases;
    }

    public function testAccessorsAndConstants(): void {
        $this->assertSame(Type::ASCII, Value\Ascii::fromValue('a')->getType());
        $this->assertSame(Type::COUNTER, Value\Counter::fromValue(1)->getType());
        $this->assertSame(Type::BIGINT, Value\Bigint::fromValue(1)->getType());
        $this->assertSame(Type::BOOLEAN, Value\Boolean::fromValue(true)->getType());
        $this->assertSame(Type::DATE, Value\Date::fromValue('2020-01-01')->getType());
        $this->assertSame(Type::DECIMAL, Value\Decimal::fromValue('1')->getType());
        $this->assertSame(Type::DOUBLE, Value\Double::fromValue(1.0)->getType());
        $this->assertSame(Type::DURATION, Value\Duration::fromValue('1h')->getType());
        $this->assertSame(Type::INET, Value\Inet::fromValue('::1')->getType());
        $this->assertSame(Type::SMALLINT, Value\Smallint::fromValue(1)->getType());
        $this->assertSame(Type::TIME, Value\Time::fromValue(1)->getType());
        $this->assertSame(Type::TIMEUUID, Value\Timeuuid::fromValue('8db96410-8dba-11f0-b0eb-325096b39f47')->getType());
        $this->assertSame(Type::TINYINT, Value\Tinyint::fromValue(1)->getType());
        $this->assertSame(Type::TUPLE, Value\Tuple::fromValue([1], [Type::INT])->getType());
        $this->assertSame(Type::VARINT, Value\Varint::fromValue(1)->getType());
        $this->assertSame(Type::VECTOR, Value\Vector::fromValue([1.0], Type::FLOAT, 1)->getType());
        $this->assertSame(Type::CUSTOM, Value\Custom::fromValue('x', 'a.B')->getType());
        $this->assertSame('a.B', Value\Custom::fromValue('x', 'a.B')->getJavaClassName());
        $this->assertSame("\0x", Value\Custom::fromValue("\0x", 'a.B')->getBinary());
        $this->assertInstanceOf(Value\Custom::class, Value\Custom::fromMixedValue('x', new CustomInfo('a.B')));

        foreach ([Value\Bigint::class, Value\Boolean::class, Value\Date::class, Value\Double::class, Value\Time::class, Value\Timestamp::class, Value\Uuid::class] as $class) {
            $this->assertTrue($class::isSerializedAsFixedLength(), $class);
        }
        foreach ([Value\Custom::class, Value\ListCollection::class, Value\MapCollection::class, Value\SetCollection::class, Value\Tuple::class, Value\UDT::class, Value\Vector::class] as $class) {
            $this->assertTrue($class::requiresDefinition(), $class);
        }
        $this->assertSame(16, Value\Uuid::fixedLength());
        $this->assertSame(-1, Value\Varchar::fixedLength());
        $this->assertFalse(Value\Varchar::hasFixedLength());
        $this->assertFalse(Value\Varchar::isReadableWithoutLength());
        $this->assertTrue(Value\Int32::hasFixedLength());
        $this->assertTrue(Value\Int32::isReadableWithoutLength());
        $this->assertSame(-1, Value\Duration::fixedLength());
        $this->assertFalse(Value\Duration::hasFixedLength());
        $this->assertTrue(Value\Duration::isReadableWithoutLength());
    }

    #[RequiresPhpExtension('bcmath')]
    public function testBcmathRefusals(): void {
        $calculator = new BCMath();
        foreach (['addUnsignedInt8' => ['1a', 1], 'divideBy256' => ['1a'], 'multiplyBy256' => ['1a'], 'sub1' => ['1a']] as $method => $arguments) {
            $this->assertRaises(StringMathException::class, static fn () => $calculator->{$method}(...$arguments));
        }
        $this->assertRaises(StringMathException::class, static fn () => $calculator->addUnsignedInt8('1', 256));

        Faults::inject('bcmod', static fn () => throw new \DivisionByZeroError('injected'));
        $this->assertRaises(StringMathException::class, static fn () => $calculator->divideBy256('512'));

        Faults::inject('extension_loaded', false);
        $this->assertRaises(StringMathException::class, static fn () => new BCMath());
    }

    public function testCurrentTimeConstructors(): void {
        $this->assertInstanceOf(Value\Date::class, Value\Date::today());
        $this->assertInstanceOf(Value\Time::class, Value\Time::now());
        $this->assertInstanceOf(Value\Timestamp::class, Value\Timestamp::now());
        $this->assertSame(16, strlen(Value\Uuid::random()->getBinary()));
    }

    public function testDecimalCalculatorChoosesWhatIsInstalled(): void {
        $property = new ReflectionProperty(DecimalCalculator::class, 'calculator');

        $property->setValue(null, null);
        Faults::inject('extension_loaded', false, false);
        $this->assertInstanceOf(Native::class, DecimalCalculator::get());

        if (extension_loaded('bcmath')) {
            $property->setValue(null, null);
            Faults::inject('extension_loaded', false, true, true);
            $this->assertInstanceOf(BCMath::class, DecimalCalculator::get());
        }

        $this->assertSame("\0", (new Native())->toBinary('-0'));
    }

    public function testDurationsConvertToDateIntervals(): void {
        $this->assertSame('0', Value\Duration::fromValue('0s')->asDateInterval()->format('%s'));

        Faults::reset();
        $duration = Value\Duration::fromValue(new DateInterval('P1D'));
        $this->assertSame(1, $duration->asNativeValue()['days']);
        $this->assertSame(['months' => 0, 'days' => 0, 'nanoseconds' => 0], Value\Duration::fromValue('000000000000000000000000h')->asNativeValue());
    }

    #[RequiresPhpExtension('gmp')]
    public function testGmpRefusals(): void {
        $calculator = new GMP();
        foreach (['add1' => ['1a'], 'addUnsignedInt8' => ['1a', 1], 'divideBy256' => ['1a'], 'multiplyBy256' => ['1a'], 'sub1' => ['1a']] as $method => $arguments) {
            $this->assertRaises(StringMathException::class, static fn () => $calculator->{$method}(...$arguments));
        }
        $this->assertRaises(StringMathException::class, static fn () => $calculator->addUnsignedInt8('1', 256));

        Faults::inject('extension_loaded', false);
        $this->assertRaises(StringMathException::class, static fn () => new GMP());
    }

    public function testObjectsAreNotPassedWhereAStringIsDue(): void {
        $this->assertRaises(ValueException::class, static fn () => Value\Inet::fromMixedValue(new stdClass()));
    }

    /**
     * @param class-string<CassandraException> $expected
     * @param Closure(): mixed $call
     */
    #[DataProvider('refusals')]
    public function testRefusal(string $expected, Closure $call): void {
        $this->assertRaises($expected, $call);
    }

    public function testStringFormsOfValues(): void {
        $this->assertSame('2020-01-01', (string) Value\Date::fromValue('2020-01-01'));
        $this->assertSame('00:00:00.000000001', (string) Value\Time::fromValue(1));
        $this->assertSame('00:00:00.000000001', Value\Time::fromValue(1)->getValue());
        $this->assertSame('1970-01-01 00:00:00.000+0000', (string) Value\Timestamp::fromValue(0));
        $this->assertSame('hello', (string) Value\Varchar::fromValue('hello'));
        $this->assertSame('1', (string) Value\Int32::fromValue(1));
        $this->assertSame([1, 2], json_decode((string) Value\ListCollection::fromValue([1, 2], Type::INT)));
        $this->assertSame(2147483648 + 7, Value\Date::fromValue(2147483648 + 7)->asConfigured(new Value\ValueEncodeConfig(dateEncodeOption: Value\EncodeOption\DateEncodeOption::AS_INT)));
    }

    public function testTypeDefinitionsThatSucceed(): void {
        $this->assertSame(Type::INT, SimpleTypeInfo::fromTypeDefinition(['type' => Type::INT])->type);
        $udt = UDTInfo::fromTypeDefinition(['type' => Type::UDT, 'valueTypes' => ['a' => Type::INT], 'isFrozen' => false, 'keyspace' => 'ks', 'name' => 'u']);
        $this->assertSame('ks', $udt->keyspace);

        $tuple = new TupleInfo([new SimpleTypeInfo(Type::INT)]);
        $this->assertFalse($tuple->isBinaryCompatibleWith(new TupleInfo([new SimpleTypeInfo(Type::INT), new SimpleTypeInfo(Type::INT)])));
        $this->assertFalse($tuple->isBinaryCompatibleWith(new TupleInfo([new SimpleTypeInfo(Type::BLOB)])));
    }

    public function testUuidsCanBeReadAsBinary(): void {
        $binary = new Value\ValueEncodeConfig(uuidEncodeOption: Value\EncodeOption\UuidEncodeOption::AS_BINARY);
        $string = new Value\ValueEncodeConfig();
        $uuid = Value\Uuid::fromValue('550e8400-e29b-41d4-a716-446655440000');
        $timeuuid = Value\Timeuuid::fromValue('8db96410-8dba-11f0-b0eb-325096b39f47');

        $this->assertSame($uuid->getBinary(), $uuid->asConfigured($binary));
        $this->assertSame('550e8400-e29b-41d4-a716-446655440000', $uuid->asConfigured($string));
        $this->assertSame($timeuuid->getBinary(), $timeuuid->asConfigured($binary));
        $this->assertSame('8db96410-8dba-11f0-b0eb-325096b39f47', $timeuuid->asConfigured($string));
    }

    /**
     * @param class-string<CassandraException> $expected
     * @param Closure(): mixed $call
     */
    private function assertRaises(string $expected, Closure $call): void {
        try {
            $call();
            $this->fail("expected $expected");
        } catch (CassandraException $e) {
            $this->assertInstanceOf($expected, $e, $e->getMessage());
        }
    }

    /**
     * Calls a method with arguments its signature rules out, as unvalidated
     * runtime data would arrive, without a static analyser objecting.
     */
    private static function unchecked(string $class, string $method, mixed ...$arguments): mixed {
        return (new ReflectionMethod($class, $method))->invoke(null, ...$arguments);
    }
}
