<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\TypeInfoException;
use Cassandra\Exception\ValueException;
use Cassandra\Exception\ValueFactoryException;
use Cassandra\StringMath\DecimalCalculator;
use Cassandra\Type;
use Cassandra\Value;
use Cassandra\ValueFactory;
use PHPUnit\Framework\Attributes\PreserveGlobalState;
use PHPUnit\Framework\Attributes\RunInSeparateProcess;

final class TypeSerializationTest extends AbstractUnitTestCase {
    /**
     * @return array<string, array{class-string<Value\ValueWithFixedLength>, int}>
     */
    public static function fixedLengthValueProvider(): array {
        return [
            'bigint' => [Value\Bigint::class, 8],
            'date' => [Value\Date::class, 4],
            'double' => [Value\Double::class, 8],
            'float' => [Value\Float32::class, 4],
            'int' => [Value\Int32::class, 4],
            'smallint' => [Value\Smallint::class, 2],
            'time' => [Value\Time::class, 8],
            'timestamp' => [Value\Timestamp::class, 8],
            'tinyint' => [Value\Tinyint::class, 1],
        ];
    }
    /**
     * @return array<string, array{array<mixed>}>
     */
    public static function malformedNestedTypeDefinitionProvider(): array {
        return [
            'list value' => [['type' => Type::LIST, 'valueType' => 'bad']],
            'set value' => [['type' => Type::SET, 'valueType' => 'bad']],
            'map key' => [['type' => Type::MAP, 'keyType' => 'bad', 'valueType' => Type::INT]],
            'map value' => [['type' => Type::MAP, 'keyType' => Type::INT, 'valueType' => 'bad']],
            'tuple value' => [['type' => Type::TUPLE, 'valueTypes' => ['bad']]],
            'UDT field' => [['type' => Type::UDT, 'valueTypes' => ['field' => 'bad']]],
            'vector value' => [['type' => Type::VECTOR, 'valueType' => 'bad', 'dimensions' => 3]],
        ];
    }

    public function testAscii(): void {
        $ascii = 'abcABC123!#_';
        $this->assertSame($ascii, Value\Ascii::fromBinary((Value\Ascii::fromValue($ascii))->getBinary())->getValue());
    }

    public function testBigint(): void {

        foreach ([0, 1, -1, 1000, -1000, PHP_INT_MAX, PHP_INT_MIN] as $v) {
            $this->assertSame($v, Value\Bigint::fromBinary((Value\Bigint::fromValue($v))->getBinary())?->getValue());
        }
    }

    public function testBlob(): void {
        $blob = 'abcABC123!#_' . hex2bin('FFAA22');
        $this->assertSame($blob, Value\Blob::fromBinary((Value\Blob::fromValue($blob))->getBinary())->getValue());
    }

    public function testBoolean(): void {
        $this->assertSame(false, Value\Boolean::fromBinary((Value\Boolean::fromValue(false))->getBinary())?->getValue());
        $this->assertSame(true, Value\Boolean::fromBinary((Value\Boolean::fromValue(true))->getBinary())?->getValue());
    }

    public function testBooleanEmptyBinaryDecodesToNull(): void {
        // An empty value is legal for boolean and denotes null, not false:
        // BooleanSerializer deserializes one to null and BooleanType reports
        // isEmptyValueMeaningless(). false is a different value with an encoding
        // of its own ("\0"). See EmptyValueConformanceTest for the same rule
        // across every type, and EmptyValueIntegrationTest for a real node.
        //
        // A value wider than one byte is the other half of BooleanSerializer's
        // "Expected 1 or 0 byte value" and stays a length error; see
        // testBooleanRejectsTrailingBinaryData().
        $this->assertNull(Value\Boolean::fromBinary(''));
    }

    public function testBooleanRejectsTrailingBinaryData(): void {
        try {
            Value\Boolean::fromBinary("\1\0");
            $this->fail('Expected a boolean value wider than one byte to be rejected');
        } catch (ValueException $e) {
            $this->assertSame(ExceptionCode::VALUE_INVALID_DATA_LENGTH->value, $e->getCode());
        }
    }

    public function testCollectionsRejectCountsThatDoNotFitTheirBodies(): void {
        $cases = [
            [Value\ListCollection::class, ['type' => Type::LIST, 'valueType' => Type::INT]],
            [Value\SetCollection::class, ['type' => Type::SET, 'valueType' => Type::INT]],
            [Value\MapCollection::class, ['type' => Type::MAP, 'keyType' => Type::INT, 'valueType' => Type::INT]],
        ];

        foreach ($cases as [$class, $definition]) {
            $typeInfo = ValueFactory::getTypeInfoFromTypeDefinition($definition);

            foreach ([pack('N', -1), pack('N', 2)] as $binary) {
                try {
                    $class::fromBinary($binary, $typeInfo);
                    $this->fail('expected malformed ' . $definition['type']->name . ' count to be refused');
                } catch (ValueException) {
                    $this->addToAssertionCount(1);
                }
            }
        }
    }

    public function testCounter(): void {

        foreach ([0, 1, -1, 1000, -1000, PHP_INT_MAX, PHP_INT_MIN] as $v) {
            $this->assertSame($v, Value\Counter::fromBinary((Value\Counter::fromValue($v))->getBinary())?->getValue());
        }
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
        if (!$this->integerHasAtLeast64Bits()) {
            $this->markTestSkipped('Date requires 64-bit integer');
        }

        $days = 19434;
        $this->assertSame($days, Value\Date::fromBinary((Value\Date::fromValue($days))->getBinary())->asInteger());

        $date = '2025-08-11';
        $this->assertSame($date, (Value\Date::fromValue($date))->asString());
        $this->assertSame($date, Value\Date::fromBinary((Value\Date::fromValue($date))->getBinary())->asString());
    }

    public function testDateAcceptsOnlyTwoDigitMonthAndDay(): void {
        // The pattern used to allow one digit for either, but the sign this
        // class prepends before parsing makes DateTimeImmutable read the next
        // "-" as a second timezone, so such a date was refused a step later
        // anyway. Zero-padded forms, and the signed years the padding is there
        // for, are what actually work.
        if (!$this->integerHasAtLeast64Bits()) {
            $this->markTestSkipped('Date requires 64-bit integer');
        }

        foreach (['2024-1-1', '2024-01-1', '2024-1-01'] as $date) {
            try {
                Value\Date::fromValue($date);
                $this->fail('expected ' . $date . ' to be refused');
            } catch (ValueException $e) {
                $this->assertSame(ExceptionCode::VALUE_DATE_INVALID_STRING_FORMAT->value, $e->getCode(), $date);
            }
        }

        $this->assertSame('2024-01-01', Value\Date::fromValue('2024-01-01')->asString());
        $this->assertSame('+10000-07-10', Value\Date::fromValue('+10000-07-10')->asString());
        $this->assertSame('-0001-01-01', Value\Date::fromValue('-0001-01-01')->asString());
    }

    public function testDateRejectsInvalidCalendarFields(): void {
        foreach (['2023-02-30', '2023-00-01'] as $date) {
            try {
                Value\Date::fromValue($date);
                $this->fail('expected ' . $date . ' to be refused');
            } catch (ValueException $e) {
                $this->assertSame(ExceptionCode::VALUE_DATE_INVALID_STRING_FORMAT->value, $e->getCode());
            }
        }
    }

    public function testDecimal(): void {
        $decimal = '34345454545.120';
        $this->assertSame($decimal, Value\Decimal::fromBinary((Value\Decimal::fromValue($decimal))->getBinary())?->getValue());

        $decimal = '34345454545';
        $this->assertSame($decimal, Value\Decimal::fromBinary((Value\Decimal::fromValue($decimal))->getBinary())?->getValue());
    }

    /**
     * Expanding an exponent must leave the integer part canonical.
     *
     * scientificToDecimalString() shifts the decimal point through the mantissa's
     * digits, which leaves whatever leading zeros the mantissa had: "0.5e1"
     * became "05" and "0e5" became "000000". The plain-decimal path drops those,
     * so the same number reached getValue() spelled two different ways depending
     * on which form it arrived in — and the wire encoding has no way to show a
     * leading zero, so the value read back from a node did not match the one
     * that was written.
     */
    public function testDecimalDropsLeadingZerosWhenExpandingAnExponent(): void {
        foreach ([
            ['0e5', '0'],
            ['0.5e1', '5'],
            ['0.05e1', '0.5'],
            ['0.10e1', '1'],
            ['-0.5e1', '-5'],
            ['000.5e1', '5'],
            ['0.001e3', '1'],
        ] as [$input, $expected]) {
            $decimal = new Value\Decimal($input);

            $this->assertSame($expected, $decimal->getValue(), "value for input '{$input}'");
            $this->assertSame(
                $expected,
                Value\Decimal::fromBinary($decimal->getBinary())?->getValue(),
                "roundtrip for input '{$input}'"
            );
        }
    }

    /**
     * A decimal whose digits are all zero has an unscaled varint of zero, and
     * zero has no sign — so "-0" and "-0.0" came back from a node as "0" and
     * "0.0", which is not what they went out as. The sign is dropped at
     * construction instead, and only for an all-zero magnitude: "-0.100" has an
     * unscaled value of -100 and keeps both its sign and its scale.
     */
    public function testDecimalDropsTheSignOfAZeroValue(): void {
        foreach ([
            ['-0', '0'],
            ['-0.0', '0.0'],
            ['-0.00', '0.00'],
            ['-0e5', '0'],
            ['-0.000e2', '0'],
        ] as [$input, $expected]) {
            $decimal = new Value\Decimal($input);

            $this->assertSame($expected, $decimal->getValue(), "value for input '{$input}'");
            $this->assertSame(
                $expected,
                Value\Decimal::fromBinary($decimal->getBinary())?->getValue(),
                "roundtrip for input '{$input}'"
            );
        }

        $this->assertSame('0', (new Value\Decimal(-0.0))->getValue());

        // A negative value that merely looks like zero at its integer digit
        // keeps its sign: its unscaled value really is negative.
        $this->assertSame('-0.100', (new Value\Decimal('-0.100'))->getValue());
        $this->assertSame('-0.100', Value\Decimal::fromBinary((new Value\Decimal('-0.100'))->getBinary())?->getValue());
    }

    public function testDecimalFromBinaryAcceptsWhatConstructionAllows(): void {
        // The two bounds meet exactly: a value constructed at the limit encodes
        // to a scale fromBinary() still reads, so the largest decimal this class
        // will take is one it can also read back.
        $atLimit = '0.' . str_repeat('0', 99_999) . '1';

        $decimal = Value\Decimal::fromValue($atLimit);
        $this->assertSame($atLimit, Value\Decimal::fromBinary($decimal->getBinary())?->getValue());
    }

    public function testDecimalFromBinaryRejectsOutOfRangeScale(): void {
        // Decoding expands the value into a plain string whose length grows with
        // the scale, so an absurd (positive or negative) scale — cheap to send,
        // expensive to expand — must be rejected rather than allocating gigabytes.
        foreach ([200000000, -200000000] as $scale) {
            $binary = pack('N', $scale & 0xFFFFFFFF) . (new Value\Varint(1))->getBinary();

            try {
                Value\Decimal::fromBinary($binary);
                $this->fail('Expected ValueException for decimal scale ' . $scale);
            } catch (\Cassandra\Exception\ValueException $e) {
                $this->assertSame(ExceptionCode::VALUE_DECIMAL_SCALE_OUT_OF_RANGE->value, $e->getCode());
            }
        }
    }

    public function testDecimalFromBinaryWithNegativeScale(): void {
        // scale is a signed int32; -3 with unscaled 12 means 12 * 10^3.
        $binary = pack('N', 0xFFFFFFFD) . (new Value\Varint(12))->getBinary();
        $this->assertSame('12000', Value\Decimal::fromBinary($binary)?->getValue());
    }

    public function testDecimalFromFloatKeepsFractionalValue(): void {
        foreach ([
            [1.5, '1.5'],
            [0.1, '0.1'],
            [-0.5, '-0.5'],
            [100.0, '100'],
            [1.0E-5, '0.00001'],
        ] as [$float, $expected]) {
            $this->assertSame($expected, (new Value\Decimal($float))->getValue());
            $this->assertSame($expected, Value\Decimal::fromBinary((new Value\Decimal($float))->getBinary())?->getValue());
        }

        $this->expectException(\Cassandra\Exception\ValueException::class);
        new Value\Decimal(INF);
    }

    public function testDecimalNormalizesNumericStrings(): void {
        // is_numeric() accepts these string forms, but the varint wire encoding
        // cannot express an exponent or a leading "+"/whitespace. They must be
        // normalized at construction so the value is encodable by getBinary().
        foreach ([
            ['1e5', '100000'],
            ['1.5e3', '1500'],
            ['1.5E3', '1500'],
            ['2e-3', '0.002'],
            ['+1.5', '1.5'],
            ['-2E2', '-200'],
            ['.5', '0.5'],
            ['5.', '5'],
            ['  1.5  ', '1.5'],
        ] as [$input, $expected]) {
            $decimal = new Value\Decimal($input);
            $this->assertSame($expected, $decimal->getValue(), "value for input '{$input}'");
            $this->assertSame(
                $expected,
                Value\Decimal::fromBinary($decimal->getBinary())?->getValue(),
                "roundtrip for input '{$input}'"
            );
        }

        // Plain decimal strings keep their explicit scale (trailing zeros) verbatim.
        foreach (['1.50', '34345454545.120', '-0.100', '42'] as $plain) {
            $this->assertSame($plain, (new Value\Decimal($plain))->getValue(), "verbatim for '{$plain}'");
        }
    }

    /**
     * A decimal is a four-byte scale followed by an unscaled varint, and a
     * varint is at least one byte — so four bytes is a truncated decimal rather
     * than one with an unscaled value of zero, and must not be read as though
     * Varint::fromBinary() had returned something.
     */
    public function testDecimalRejectsABodyWithNoUnscaledVarint(): void {
        $this->assertNull(Value\Decimal::fromBinary(''));

        foreach ([1, 2, 3, 4] as $length) {
            try {
                Value\Decimal::fromBinary(str_repeat("\x00", $length));
                $this->fail('Expected ValueException for a ' . $length . '-byte decimal');
            } catch (ValueException $e) {
                $this->assertSame(ExceptionCode::VALUE_DECIMAL_UNPACK_FAILED->value, $e->getCode());
            }
        }

        // Five bytes is the shortest well-formed decimal: scale 0, unscaled 0.
        $this->assertSame('0', Value\Decimal::fromBinary(pack('N', 0) . "\x00")?->getValue());
    }

    public function testDecimalRejectsScaleItCouldNotReadBack(): void {
        // getBinary() takes the scale straight from the fraction it was given,
        // so without the same bound fromBinary() applies, this class would encode
        // values it then refuses to decode — a decimal that can be written to a
        // node and never read back from one.
        $tooManyFractionDigits = '0.' . str_repeat('0', 150_000) . '1';

        try {
            Value\Decimal::fromValue($tooManyFractionDigits);
            $this->fail('Expected ValueException for an out-of-range decimal scale');
        } catch (\Cassandra\Exception\ValueException $e) {
            $this->assertSame(ExceptionCode::VALUE_DECIMAL_SCALE_OUT_OF_RANGE->value, $e->getCode());
        }
    }

    public function testDouble(): void {
        $double = 12345678901234.4545435;
        $this->assertSame($double, Value\Double::fromBinary((Value\Double::fromValue($double))->getBinary())?->getValue());
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

    public function testDurationRejectsInvalidStrings(): void {
        if (!$this->integerHasAtLeast64Bits()) {
            $this->markTestSkipped('Duration requires 64-bit integer');
        }

        foreach (['hello world', '5x2d', 'P', '-', '', '1d nonsense'] as $invalid) {
            try {
                new Value\Duration($invalid);
                $this->fail('Expected ValueException for duration string: "' . $invalid . '"');
            } catch (\Cassandra\Exception\ValueException $e) {
                $this->addToAssertionCount(1);
            }
        }
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('fixedLengthValueProvider')]
    public function testFixedLengthValuesRejectShortAndTrailingData(string $class, int $length): void {
        foreach ([str_repeat("\0", $length - 1), str_repeat("\0", $length + 1)] as $binary) {
            try {
                $class::fromBinary($binary);
                $this->fail($class . ' accepted a malformed binary length');
            } catch (ValueException $e) {
                $this->assertSame(ExceptionCode::VALUE_INVALID_DATA_LENGTH->value, $e->getCode());
            }
        }
    }

    public function testFloat32(): void {
        $float = 1024.5;
        $this->assertSame($float, Value\Float32::fromBinary((new Value\Float32($float))->getBinary())?->getValue());
    }

    public function testInet(): void {
        $ipv4 = '192.168.22.1';
        $this->assertSame($ipv4, Value\Inet::fromBinary((Value\Inet::fromValue($ipv4))->getBinary())?->getValue());

        $ipv6 = '2001:db8:3333:4444:5555:6666:7777:8888';
        $this->assertSame($ipv6, Value\Inet::fromBinary((Value\Inet::fromValue($ipv6))->getBinary())?->getValue());
    }

    public function testInetWrapsNativeEncodingErrors(): void {
        try {
            Value\Inet::fromValue("bad\0address")->getBinary();
            $this->fail('Expected a ValueException');
        } catch (ValueException $e) {
            $this->assertSame(ExceptionCode::VALUE_INET_TO_BINARY_FAILED->value, $e->getCode());
            $this->assertNull($e->getPrevious());
        }
    }

    public function testInteger(): void {
        $int1 = 234355434;
        $this->assertSame($int1, Value\Int32::fromBinary((Value\Int32::fromValue($int1))->getBinary())?->getValue());

        $int2 = -234355434;
        $this->assertSame($int2, Value\Int32::fromBinary((Value\Int32::fromValue($int2))->getBinary())?->getValue());
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

    public function testListCollectionWithValueObjects(): void {
        // Elements provided as pre-built Value objects must encode identically
        // to the same elements provided as raw PHP values (regression: getBinary()
        // previously rejected ValueBase elements).
        $definition = Type::INT;

        $rawBinary = (Value\ListCollection::fromValue([1, 2, 3], $definition))->getBinary();

        $objectBinary = (Value\ListCollection::fromValue([
            Value\Int32::fromValue(1),
            Value\Int32::fromValue(2),
            Value\Int32::fromValue(3),
        ], $definition))->getBinary();

        $this->assertSame(bin2hex($rawBinary), bin2hex($objectBinary));

        $this->assertSame(
            [1, 2, 3],
            Value\ListCollection::fromBinary(
                $objectBinary,
                ValueFactory::getTypeInfoFromTypeDefinition([
                    'type' => Type::LIST,
                    'valueType' => $definition,
                ])
            )->getValue()
        );
    }

    /**
     * @param array<mixed> $definition
     */
    #[\PHPUnit\Framework\Attributes\DataProvider('malformedNestedTypeDefinitionProvider')]
    public function testMalformedNestedTypeDefinitionThrowsProjectException(array $definition): void {
        $this->expectException(ValueFactoryException::class);
        $this->expectExceptionCode(ExceptionCode::VALUEFACTORY_TYPEDEF_INVALID_NESTED_DEFINITION->value);

        ValueFactory::getTypeInfoFromTypeDefinition($definition);
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

    public function testMapCollectionPreservesDistinctFloatingPointKeys(): void {
        $typeInfo = ValueFactory::getTypeInfoFromTypeDefinition([
            'type' => Type::MAP,
            'keyType' => Type::DOUBLE,
            'valueType' => Type::INT,
        ]);

        $binary = pack('N', 2)
            . pack('N', 8) . pack('E', 1.234567890123456) . pack('N', 4) . pack('N', 1)
            . pack('N', 8) . pack('E', 1.234567890123457) . pack('N', 4) . pack('N', 2);

        $decoded = Value\MapCollection::fromBinary($binary, $typeInfo);

        $this->assertCount(2, $decoded->getValue());
        $this->assertSame($binary, $decoded->getBinary());
    }

    public function testMapCollectionRejectsComparatorEqualKeys(): void {
        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_MAP_DUPLICATE_KEY->value);

        Value\MapCollection::fromValue(
            ['1.0' => 'first', '1.00' => 'second'],
            Type::DECIMAL,
            Type::VARCHAR,
            true,
        )->getBinary();
    }

    public function testMapCollectionRoundTripsSpecialFloatingPointKeys(): void {
        foreach ([Type::FLOAT, Type::DOUBLE] as $keyType) {
            $typeInfo = ValueFactory::getTypeInfoFromTypeDefinition([
                'type' => Type::MAP,
                'keyType' => $keyType,
                'valueType' => Type::INT,
            ]);
            $format = $keyType === Type::FLOAT ? 'G' : 'E';
            $length = $keyType === Type::FLOAT ? 4 : 8;

            foreach ([NAN, INF, -INF, 0.0, -0.0] as $index => $key) {
                $binary = pack('N', 1)
                    . pack('N', $length) . pack($format, $key)
                    . pack('N', 4) . pack('N', $index);

                $decoded = Value\MapCollection::fromBinary($binary, $typeInfo);
                $this->assertSame($binary, $decoded->getBinary(), $keyType->name . ' key at index ' . $index);
            }
        }
    }

    public function testMapCollectionSerializesKeysInCqlOrder(): void {
        $ascending = Value\MapCollection::fromValue(
            [-2 => 'a', -1 => 'b', 0 => 'c', 1 => 'd'],
            Type::INT,
            Type::VARCHAR,
            true,
        )->getBinary();
        $descending = Value\MapCollection::fromValue(
            [1 => 'd', 0 => 'c', -1 => 'b', -2 => 'a'],
            Type::INT,
            Type::VARCHAR,
            true,
        )->getBinary();

        $this->assertSame($ascending, $descending);
        $this->assertSame(
            [-2, -1, 0, 1],
            array_keys(Value\MapCollection::fromBinary(
                $descending,
                ValueFactory::getTypeInfoFromTypeDefinition([
                    'type' => Type::MAP,
                    'keyType' => Type::INT,
                    'valueType' => Type::VARCHAR,
                    'isFrozen' => true,
                ])
            )->getValue())
        );
    }

    public function testMapCollectionWithBooleanKeys(): void {
        // A PHP array key is an int or a string and nothing else, so a boolean
        // key is folded to 1/0 by PHP itself on the way in, and by
        // MapCollection::fromStream() for the keys it decodes. Encoding has to
        // spell it back out, or a map<boolean, …> could not be sent at all —
        // neither one built here nor one read off the wire a moment earlier.
        $keyDefinition = Type::BOOLEAN;
        $valueDefinition = Type::VARCHAR;

        $typeInfo = ValueFactory::getTypeInfoFromTypeDefinition([
            'type' => Type::MAP,
            'keyType' => $keyDefinition,
            'valueType' => $valueDefinition,
        ]);

        $binary = (Value\MapCollection::fromValue([true => 'yes', false => 'no'], $keyDefinition, $valueDefinition))->getBinary();

        // count, then (len, key, len, value) per entry, ordered false before
        // true regardless of the PHP array's insertion order.
        $this->assertSame(
            '00000002' . '00000001' . '00' . '00000002' . bin2hex('no')
                      . '00000001' . '01' . '00000003' . bin2hex('yes'),
            bin2hex($binary)
        );

        $decoded = Value\MapCollection::fromBinary($binary, $typeInfo);

        $this->assertSame([0 => 'no', 1 => 'yes'], $decoded->getValue());

        // And a decoded map goes back out unchanged, which is what a read-modify-write
        // of a map<boolean, …> column comes down to.
        $this->assertSame(bin2hex($binary), bin2hex($decoded->getBinary()));
    }

    public function testMapCollectionWithIntegerLikeStringKeys(): void {
        // PHP folds an array key that is the canonical decimal spelling of an
        // integer into an int, so a map<text, …> key such as "123" is an int by
        // the time anything sees it — both in a literal written here and in the
        // key MapCollection::fromStream() decoded a moment earlier. Encoding has
        // to spell it back out, or such a map could neither be sent nor written
        // back after being read, and the caller has no way to avoid the fold.
        $keyDefinition = Type::VARCHAR;
        $valueDefinition = Type::INT;

        $typeInfo = ValueFactory::getTypeInfoFromTypeDefinition([
            'type' => Type::MAP,
            'keyType' => $keyDefinition,
            'valueType' => $valueDefinition,
        ]);

        $binary = (Value\MapCollection::fromValue(['123' => 5], $keyDefinition, $valueDefinition))->getBinary();

        // count, then (len, key, len, value): the key goes out as the three text
        // bytes it was written as, not as an int.
        $this->assertSame(
            '00000001' . '00000003' . bin2hex('123') . '00000004' . '00000005',
            bin2hex($binary)
        );

        $decoded = Value\MapCollection::fromBinary($binary, $typeInfo);

        $this->assertSame([123 => 5], $decoded->getValue());

        // And a decoded map goes back out unchanged, which is what a
        // read-modify-write of a map<text, …> column comes down to.
        $this->assertSame(bin2hex($binary), bin2hex($decoded->getBinary()));

        // The same for the other string-valued key types, and for a key that is
        // not an integer spelling and so was never folded.
        foreach ([Type::ASCII, Type::BLOB, Type::TEXT, Type::VARCHAR] as $stringKeyType) {
            foreach ([['0' => 1], ['-7' => 1], ['1.5' => 1], ['01' => 1], ['abc' => 1]] as $map) {
                $encoded = (Value\MapCollection::fromValue($map, $stringKeyType, Type::INT))->getBinary();

                $this->assertSame(
                    array_map('strval', array_keys($map)),
                    array_map('strval', array_keys(
                        Value\MapCollection::fromBinary($encoded, ValueFactory::getTypeInfoFromTypeDefinition([
                            'type' => Type::MAP,
                            'keyType' => $stringKeyType,
                            'valueType' => Type::INT,
                        ]))->getValue()
                    )),
                    $stringKeyType->name . ' key ' . (string) array_key_first($map)
                );
            }
        }
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

    public function testSetCollectionRejectsDuplicateCqlValues(): void {
        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_SET_DUPLICATE_ELEMENT->value);

        Value\SetCollection::fromValue(['1.0', '1.00'], Type::DECIMAL, true)->getBinary();
    }

    public function testSetCollectionSerializesUniqueValuesInCqlOrder(): void {
        $ascending = Value\SetCollection::fromValue([-2, -1, 0, 1], Type::INT, true)->getBinary();
        $descending = Value\SetCollection::fromValue([1, 0, -1, -2], Type::INT, true)->getBinary();

        $this->assertSame($ascending, $descending);
        $this->assertSame(
            [-2, -1, 0, 1],
            Value\SetCollection::fromBinary(
                $descending,
                ValueFactory::getTypeInfoFromTypeDefinition([
                    'type' => Type::SET,
                    'valueType' => Type::INT,
                    'isFrozen' => true,
                ])
            )->getValue()
        );
    }

    public function testSetCollectionUsesTimeuuidSignedTailOrdering(): void {
        $signedNegativeTail = '00000000-0000-1000-8000-000000000000';
        $signedPositiveTail = '00000000-0000-1000-7f00-000000000000';
        $typeInfo = ValueFactory::getTypeInfoFromTypeDefinition([
            'type' => Type::SET,
            'valueType' => Type::TIMEUUID,
            'isFrozen' => true,
        ]);

        $binary = Value\SetCollection::fromValue(
            [$signedPositiveTail, $signedNegativeTail],
            Type::TIMEUUID,
            true,
        )->getBinary();

        $this->assertSame(
            [$signedNegativeTail, $signedPositiveTail],
            Value\SetCollection::fromBinary($binary, $typeInfo)->getValue(),
        );
    }

    public function testSetCollectionWithValueObjects(): void {
        // Elements provided as pre-built Value objects must encode identically
        // to the same elements provided as raw PHP values (regression: getBinary()
        // previously rejected ValueBase elements).
        $definition = Type::INT;

        $rawBinary = (Value\SetCollection::fromValue([1, 2, 3], $definition))->getBinary();

        $objectBinary = (Value\SetCollection::fromValue([
            Value\Int32::fromValue(1),
            Value\Int32::fromValue(2),
            Value\Int32::fromValue(3),
        ], $definition))->getBinary();

        $this->assertSame(bin2hex($rawBinary), bin2hex($objectBinary));

        $this->assertSame(
            [1, 2, 3],
            Value\SetCollection::fromBinary(
                $objectBinary,
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
        $this->assertSame($timeInMs, Value\Timestamp::fromBinary((Value\Timestamp::fromValue($timeInMs))->getBinary())?->asInteger());
    }

    public function testTimestampRejectsDateTimeOutsideIntegerMillisecondRange(): void {
        if (!$this->integerHasAtLeast64Bits()) {
            $this->markTestSkipped('Timestamp requires 64-bit integer');
        }

        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_TIMESTAMP_OUT_OF_RANGE->value);

        Value\Timestamp::fromValue(new \DateTimeImmutable('@9223372036854776'));
    }

    public function testTimestampRejectsInvalidOrNonIsoStrings(): void {
        foreach (['2023-02-30', 'next Thursday'] as $timestamp) {
            try {
                Value\Timestamp::fromValue($timestamp);
                $this->fail('expected ' . $timestamp . ' to be refused');
            } catch (ValueException $e) {
                $this->assertSame(ExceptionCode::VALUE_TIMESTAMP_INVALID_VALUE_TYPE->value, $e->getCode());
            }
        }

        $this->assertSame(1296705900000, Value\Timestamp::fromValue('2011-02-03T04:05:00.000+0000')->asInteger());
        $this->assertSame(1296705900000, Value\Timestamp::fromValue('2011-02-03 04:05')->asInteger());
    }

    public function testTimeuuid(): void {
        $timeUuid = 'bd23b48a-99de-11ed-a8fc-0242ac120002';
        $this->assertSame($timeUuid, Value\Timeuuid::fromBinary((Value\Timeuuid::fromValue($timeUuid))->getBinary())?->getValue());
    }

    public function testTimeuuidAcceptsUndashedHexForm(): void {
        $timeuuid = 'bd23b48a-99de-11ed-a8fc-0242ac120002';
        $undashed = str_replace('-', '', $timeuuid);

        $this->assertSame($timeuuid, Value\Timeuuid::fromValue($undashed)->getValue());
        $this->assertSame(
            Value\Timeuuid::fromValue($timeuuid)->getBinary(),
            Value\Timeuuid::fromValue($undashed)->getBinary()
        );
    }

    public function testTimeuuidRejectsMalformedValue(): void {
        // Without validation these would be silently packed into wrong-length or
        // corrupt binary by getBinary() (non-hex coerced to 0).
        foreach ([
            'garbage',
            '12345',
            'bd23b48a-99de-11ed-a8fc-0242ac12000',           // one digit short
            'zd23b48a-99de-11ed-a8fc-0242ac120002',          // non-hex digit
            'bd23b48a99de11eda8fc0242ac12000',               // undashed, one digit short (31)
            'bd23b48a99de11eda8fc0242ac1200022',             // undashed, one digit too many (33)
        ] as $invalid) {
            try {
                Value\Timeuuid::fromValue($invalid);
                $this->fail('Expected ValueException for timeuuid: "' . $invalid . '"');
            } catch (\Cassandra\Exception\ValueException $e) {
                $this->assertSame(ExceptionCode::VALUE_UUID_INVALID_FORMAT->value, $e->getCode());
            }
        }
    }

    public function testTimeuuidRejectsUuidVersionsOtherThanOne(): void {
        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_TIMEUUID_INVALID_VERSION->value);

        Value\Timeuuid::fromValue('550e8400-e29b-41d4-a716-446655440000');
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

    public function testTupleInfoRejectsInvalidValueType(): void {
        $this->expectException(TypeInfoException::class);
        $this->expectExceptionCode(ExceptionCode::TYPEINFO_TUPLE_INVALID_VALUETYPE->value);

        (new \ReflectionClass(\Cassandra\TypeInfo\TupleInfo::class))->newInstanceArgs([[123]]);
    }

    public function testTupleRejectsUndeclaredValues(): void {
        $typeInfo = new \Cassandra\TypeInfo\TupleInfo([
            ValueFactory::getTypeInfoFromType(Type::INT),
        ]);

        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_TUPLE_UNDECLARED_VALUE->value);

        new Value\Tuple([1, 2], $typeInfo);
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

    public function testUdtInfoRejectsInvalidValueType(): void {
        $this->expectException(TypeInfoException::class);
        $this->expectExceptionCode(ExceptionCode::TYPEINFO_UDT_INVALID_VALUETYPE->value);

        (new \ReflectionClass(\Cassandra\TypeInfo\UDTInfo::class))->newInstanceArgs([['field' => 123], false]);
    }

    public function testUdtRejectsUndeclaredFields(): void {
        $typeInfo = new \Cassandra\TypeInfo\UDTInfo([
            'declared' => ValueFactory::getTypeInfoFromType(Type::INT),
        ], false);

        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_UDT_UNDECLARED_FIELD->value);

        new Value\UDT(['declared' => 1, 'extra' => 2], $typeInfo);
    }

    public function testUuid(): void {
        $uuid = '346c9059-7d07-47e6-91c8-092b50e8306f';
        $this->assertSame($uuid, Value\Uuid::fromBinary((Value\Uuid::fromValue($uuid))->getBinary())?->getValue());
    }

    public function testUuidAcceptsRawBinaryForm(): void {
        // The raw 16-byte wire form is accepted directly (e.g. re-binding a value
        // read with UuidEncodeOption::AS_BINARY) and normalizes to the canonical
        // lowercase string.
        $uuid = '346c9059-7d07-47e6-91c8-092b50e8306f';
        $raw = pack('H*', str_replace('-', '', $uuid));

        $fromRaw = Value\Uuid::fromValue($raw);
        $this->assertSame($raw, $fromRaw->getBinary());
        $this->assertSame($uuid, $fromRaw->getValue());

        // getBinary() is now a no-op round-trip of the raw bytes.
        $this->assertSame($raw, Value\Uuid::fromValue($uuid)->getBinary());
    }

    public function testUuidAcceptsUndashedHexForm(): void {
        // The compact 32-character undashed hex form is accepted and normalizes
        // to the canonical dashed lowercase string, matching the dashed form.
        $uuid = '346c9059-7d07-47e6-91c8-092b50e8306f';
        $undashed = str_replace('-', '', $uuid);

        $this->assertSame($uuid, Value\Uuid::fromValue($undashed)->getValue());
        $this->assertSame(
            Value\Uuid::fromValue($uuid)->getBinary(),
            Value\Uuid::fromValue($undashed)->getBinary()
        );
    }

    public function testUuidAcceptsUppercase(): void {
        $uuid = '346C9059-7D07-47E6-91C8-092B50E8306F';
        $this->assertSame(16, strlen(Value\Uuid::fromValue($uuid)->getBinary()));
    }

    #[RunInSeparateProcess]
    #[PreserveGlobalState(false)]
    public function testUuidRandomPreservesRandomBytesFailure(): void {
        eval('namespace Cassandra\\Value; function random_bytes(int $length): string { throw new \\Exception("random source failed"); }');

        try {
            Value\Uuid::random();
            $this->fail('Expected UUID generation to fail');
        } catch (ValueException $e) {
            $this->assertSame(ExceptionCode::VALUE_UUID_RANDOM_FAILED->value, $e->getCode());
            $previous = $e->getPrevious();
            $this->assertInstanceOf(\Exception::class, $previous);
            $this->assertSame('random source failed', $previous->getMessage());
        }
    }

    public function testUuidRejectsMalformedValue(): void {
        // Without validation these would be silently packed into wrong-length or
        // corrupt binary by getBinary() (non-hex coerced to 0). None is exactly
        // 16 bytes, so none is mistaken for the accepted raw binary form.
        foreach ([
            'garbage',
            '12345',
            '346c9059-7d07-47e6-91c8-092b50e8306',           // one digit short
            '346c9059-7d07-47e6-91c8-092b50e8306fx',         // trailing junk
            'zzzzzzzz-7d07-47e6-91c8-092b50e8306f',          // non-hex digits
            '346c90597d0747e691c8092b50e8306',               // undashed, one digit short (31)
            '346c90597d0747e691c8092b50e8306ff',             // undashed, one digit too many (33)
        ] as $invalid) {
            try {
                Value\Uuid::fromValue($invalid);
                $this->fail('Expected ValueException for uuid: "' . $invalid . '"');
            } catch (\Cassandra\Exception\ValueException $e) {
                $this->assertSame(ExceptionCode::VALUE_UUID_INVALID_FORMAT->value, $e->getCode());
            }
        }
    }

    public function testVarchar(): void {
        $varchar = 'abcABC123!#_';
        $this->assertSame($varchar, Value\Varchar::fromBinary((Value\Varchar::fromValue($varchar))->getBinary())->getValue());
    }

    public function testVarint(): void {

        foreach ([
            PHP_INT_MAX,
            PHP_INT_MIN,
        ] as $value) {
            $this->assertSame((string) $value, (Value\Varint::fromValue($value))->getValue());
            $this->assertSame($value, (Value\Varint::fromValue($value))->asInt());
            $this->assertSame((string) $value, Value\Varint::fromBinary((Value\Varint::fromValue($value))->getBinary())?->getValue());
            $this->assertSame($value, Value\Varint::fromBinary((Value\Varint::fromValue($value))->getBinary())->asInt());
        }

        foreach ([
            '999999999999999999999999999999999999999999999999999999999999999999',
            '-999999999999999999999999999999999999999999999999999999999999999999',
        ] as $value) {
            $this->assertSame($value, (Value\Varint::fromValue($value))->getValue());
            $this->assertSame($value, Value\Varint::fromBinary((Value\Varint::fromValue($value))->getBinary())?->getValue());
        }

        $this->assertSame('0', Value\Varint::fromBinary("\x00")?->getValue());
        $this->assertSame('1', Value\Varint::fromBinary("\x01")?->getValue());
        $this->assertSame('127', Value\Varint::fromBinary("\x7F")?->getValue());
        $this->assertSame('128', Value\Varint::fromBinary("\x00\x80")?->getValue());
        $this->assertSame('129', Value\Varint::fromBinary("\x00\x81")?->getValue());
        $this->assertSame('-1', Value\Varint::fromBinary("\xFF")?->getValue());
        $this->assertSame('-128', Value\Varint::fromBinary("\x80")?->getValue());
        $this->assertSame('-129', Value\Varint::fromBinary("\xFF\x7F")?->getValue());

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

    /**
     * No bytes spell no number. unpack('C*', '') hands back an empty array
     * rather than false, so the accumulator loop simply never ran and an empty
     * binary decoded to zero — which zero does not encode as ("\0" does), so it
     * was a value that could not be written back as what it was read as.
     */
    public function testVarintEmptyBinaryDecodesToNull(): void {
        // No bytes spell no number, and IntegerSerializer deserializes an empty
        // value to null. Not zero: that has an encoding of its own ("\0"), and
        // returning it is what this quietly did before — unpack('C*', '') hands
        // back an empty array rather than false, so the accumulator never ran.
        $this->assertNull(Value\Varint::fromBinary(''));

        // Zero itself is still a single zero byte, in both directions.
        $this->assertSame("\x00", Value\Varint::fromValue(0)->getBinary());
        $this->assertSame('0', Value\Varint::fromBinary("\x00")?->getValue());
    }

    public function testVarintNormalizesIntegerStrings(): void {
        // Leading zeros and a sign on zero are spellings, not values. The wire
        // encoding has no way to show them, so one kept on the PHP side would
        // not survive a round trip through the node.
        foreach ([
            ['007', '7'],
            ['-007', '-7'],
            ['0000000000000000000', '0'],
            ['-0000000000000000005', '-5'],
            ['-0', '0'],
            ['000000000000000000000000000000000000042', '42'],
        ] as [$input, $expected]) {
            $varint = Value\Varint::fromValue($input);

            $this->assertSame($expected, $varint->getValue(), "value for input '{$input}'");
            $this->assertSame(
                $expected,
                Value\Varint::fromBinary($varint->getBinary())?->getValue(),
                "roundtrip for input '{$input}'"
            );
        }
    }

    public function testVarintStringKeepsWholePhpIntRange(): void {
        // The whole of PHP's int range is available from a string, not just the
        // values below 10^18: a digit count that stays safely under the bound
        // would have asInt() refuse a number that fits perfectly well, while the
        // same value arriving from a node — eight bytes or fewer, so decoded as
        // an int — came back without complaint.
        foreach ([PHP_INT_MAX, PHP_INT_MIN, PHP_INT_MAX - 1, PHP_INT_MIN + 1, intdiv(PHP_INT_MAX, 10) * 9] as $value) {
            $fromString = Value\Varint::fromValue((string) $value);

            $this->assertSame($value, $fromString->asInt(), "asInt() for '{$value}'");
            $this->assertSame(
                (Value\Varint::fromValue($value))->getBinary(),
                $fromString->getBinary(),
                "binary for '{$value}'"
            );
        }

        // One past each end stays a string, there being no int for it. Derived
        // from the bounds rather than written out, so this says the same thing
        // on a 32-bit build as on a 64-bit one.
        $calculator = DecimalCalculator::get();

        $tooLargeValues = [
            $calculator->add1((string) PHP_INT_MAX),
            '-' . $calculator->add1(substr((string) PHP_INT_MIN, 1)),
        ];

        foreach ($tooLargeValues as $tooLarge) {
            $varint = Value\Varint::fromValue($tooLarge);

            $this->assertSame($tooLarge, $varint->getValue(), "value for '{$tooLarge}'");
            $this->assertSame(
                $tooLarge,
                Value\Varint::fromBinary($varint->getBinary())?->getValue(),
                "roundtrip for '{$tooLarge}'"
            );

            try {
                $varint->asInt();
                $this->fail("asInt() should refuse '{$tooLarge}'");
            } catch (ValueException $e) {
                $this->assertSame(ExceptionCode::VALUE_VARINT_OUT_OF_PHP_INT_RANGE->value, $e->getCode());
            }
        }
    }

    public function testVectorRejectsAssociativeValue(): void {
        $this->expectException(\Cassandra\Exception\ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_VECTOR_INVALID_VALUE_TYPE->value);

        // Non-0-indexed (associative) keys cannot be positionally serialized.
        Value\Vector::fromValue([10 => 1.0, 11 => 2.0, 12 => 3.0], Type::FLOAT, 3);
    }

    public function testVectorRejectsElementCountMismatch(): void {
        // Too many elements would be silently truncated; too few would index a
        // missing slot. Both must be rejected up front.
        foreach ([
            [1.0, 2.0],                 // fewer than dimensions
            [1.0, 2.0, 3.0, 4.0, 5.0],  // more than dimensions
        ] as $value) {
            try {
                Value\Vector::fromValue($value, Type::FLOAT, 3);
                $this->fail('Expected ValueException for vector with ' . count($value) . ' elements and 3 dimensions');
            } catch (\Cassandra\Exception\ValueException $e) {
                $this->assertSame(ExceptionCode::VALUE_VECTOR_DIMENSION_MISMATCH->value, $e->getCode());
            }
        }
    }

    public function testVectorRejectsInvalidDimensionDefinitions(): void {
        foreach ([0, -1, 8193] as $dimensions) {
            try {
                ValueFactory::getTypeInfoFromTypeDefinition([
                    'type' => Type::VECTOR,
                    'valueType' => Type::FLOAT,
                    'dimensions' => $dimensions,
                ]);
                $this->fail('Expected invalid vector dimensions to be rejected: ' . $dimensions);
            } catch (\Cassandra\Exception\TypeInfoException $e) {
                $this->assertSame(ExceptionCode::TYPEINFO_VECTOR_INVALID_DIMENSIONS->value, $e->getCode());
            }
        }
    }

    public function testVectorRoundTripEncodesAllElements(): void {
        $value = [1.5, -2.5, 3.5];
        $binary = Value\Vector::fromValue($value, Type::FLOAT, 3)->getBinary();

        // 3 float32 elements, fixed-length framing (no length prefixes).
        $this->assertSame(12, strlen($binary));
    }
}
