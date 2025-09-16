<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Exception\ExceptionCode;
use Cassandra\StringMath\DecimalCalculator;
use Cassandra\StringMath\DecimalCalculator\BCMath as BcCalculator;
use Cassandra\StringMath\DecimalCalculator\GMP as GmpCalculator;
use Cassandra\StringMath\DecimalCalculator\Native as NativeCalculator;
use Cassandra\Exception\StringMathException as StringMathException;

class DecimalCalculatorTest extends AbstractUnitTestCase {
    /**
     * @return array<string, array{string, DecimalCalculator, string, string}>
     */
    public static function providerAdd1(): array {
        $cases = [
            ['0', '1'],
            ['9', '10'],
            ['199', '200'],
            ['999999999999999999', '1000000000000000000'],
            ['999', '1000'],
            ['1000', '1001'],
            ['1099', '1100'],
            ['1299', '1300'],
            ['1999', '2000'],
            ['123456789', '123456790'],
            ['999999', '1000000'],
            ['000', '1'],
        ];

        /** @phpstan-ignore-next-line */
        return self::crossWithCalculators($cases);
    }

    /**
     * @return array<string, array{string, DecimalCalculator, string, int, string}>
     */
    public static function providerAddUnsignedInt8(): array {
        $cases = [
            ['0', 0, '0'],
            ['0', 1, '1'],
            ['1', 255, '256'],
            ['999', 1, '1000'],
            ['123456789', 255, '123457044'],
            ['245', 10, '255'],
            ['745', 255, '1000'],
            ['999745', 255, '1000000'],
            ['4294967295', 1, '4294967296'],
            ['18446744073709551615', 255, '18446744073709551870'],
            ['0005', 10, '15'],
        ];

        /** @phpstan-ignore-next-line */
        return self::crossWithCalculators($cases);
    }

    /**
     * @return array<string, array{string, DecimalCalculator, string, string, int}>
     */
    public static function providerDivideBy256(): array {
        $cases = [
            ['0', '0', 0],
            ['1', '0', 1],
            ['255', '0', 255],
            ['256', '1', 0],
            ['257', '1', 1],
            ['65535', '255', 255],
            ['2', '0', 2],
            ['254', '0', 254],
            ['510', '1', 254],
            ['511', '1', 255],
            ['512', '2', 0],
            ['1024', '4', 0],
            ['65536', '256', 0],
            ['4294967295', '16777215', 255],
            ['4294967296', '16777216', 0],
            ['18446744073709551615', '72057594037927935', 255],
            ['18446744073709551616', '72057594037927936', 0],
        ];

        /** @phpstan-ignore-next-line */
        return self::crossWithCalculators($cases);
    }

    /**
     * @return array<string, array{string, DecimalCalculator, string, string}>
     */
    public static function providerFromBinary(): array {
        $cases = [
            ['', '0'],
            ['\\x00', '0'],
            ['\\x01', '1'],
            ['\\x7F', '127'],
            ['\\x00\\x80', '128'],
            ['\\xFF', '-1'],
            ['\\x80', '-128'],
            ['\\xFF\\x7F', '-129'],
            ['\\x01\\x00', '256'],
            ['\\x00\\x00', '0'],
            ['\\x00\\x01', '1'],
            ['\\x7F\\xFF', '32767'],
            ['\\x80\\x00', '-32768'],
            ['\\xFF\\x00', '-256'],
            ['\\x00\\x80\\x00', '32768'],
            ['\\xFF\\x7F\\xFF', '-32769'],
            ['\\x00\\x01\\x00\\x00\\x00\\x00', '4294967296'],
            ['\\xFF\\xFF', '-1'],
        ];

        /** @phpstan-ignore-next-line */
        return self::crossWithCalculators($cases);
    }

    /**
     * @return array<string, array{string, DecimalCalculator, string, int, string}>
     */
    public static function providerMultiplyBy256(): array {
        $cases = [
            ['0', '0'],
            ['1', '256'],
            ['255', '65280'],
            ['123456', '31604736'],
            ['999999999999999999999999999999', '255999999999999999999999999999744'],
            ['2', '512'],
            ['10', '2560'],
            ['65535', '16776960'],
            ['65536', '16777216'],
            ['99999', '25599744'],
            ['4294967296', '1099511627776'],
        ];

        /** @phpstan-ignore-next-line */
        return self::crossWithCalculators($cases);
    }

    /**
     * @return array<string, array{string, DecimalCalculator, string, string}>
     */
    public static function providerSub1(): array {
        $cases = [
            ['0', '0'],
            ['000', '0'],
            ['1', '0'],
            ['1000', '999'],
            ['2001', '2000'],
            ['10', '9'],
            ['10000', '9999'],
            ['1000000', '999999'],
            ['999999', '999998'],
            ['999999999999999999999999999999', '999999999999999999999999999998'],
        ];

        /** @phpstan-ignore-next-line */
        return self::crossWithCalculators($cases);
    }

    /**
     * @return array<string, array{string, DecimalCalculator, string, string}>
     */
    public static function providerToBinary(): array {
        $cases = [
            ['0', '\\x00'],
            ['1', '\\x01'],
            ['127', '\\x7F'],
            ['128', '\\x00\\x80'],
            ['255', '\\x00\\xFF'],
            ['256', '\\x01\\x00'],
            ['-1', '\\xFF'],
            ['-2', '\\xFE'],
            ['-127', '\\x81'],
            ['-128', '\\x80'],
            ['-129', '\\xFF\\x7F'],
            ['2', '\\x02'],
            ['254', '\\x00\\xFE'],
            ['32767', '\\x7F\\xFF'],
            ['32768', '\\x00\\x80\\x00'],
            ['65535', '\\x00\\xFF\\xFF'],
            ['65536', '\\x01\\x00\\x00'],
            ['2147483647', '\\x7F\\xFF\\xFF\\xFF'],
            ['2147483648', '\\x00\\x80\\x00\\x00\\x00'],
            ['-256', '\\xFF\\x00'],
            ['-32768', '\\x80\\x00'],
            ['-32769', '\\xFF\\x7F\\xFF'],
            ['-2147483648', '\\x80\\x00\\x00\\x00'],
            ['-2147483649', '\\xFF\\x7F\\xFF\\xFF\\xFF'],
        ];

        /** @phpstan-ignore-next-line */
        return self::crossWithCalculators($cases);
    }

    /**
     * @dataProvider providerAdd1
     */
    public function testAdd1(string $impl, DecimalCalculator $calc, string $in, string $expected): void {
        $this->assertSame($expected, $calc->add1($in), "impl={$impl}");
    }

    /**
     * @dataProvider providerAddUnsignedInt8
     */
    public function testAddUnsignedInt8(string $impl, DecimalCalculator $calc, string $in, int $addend, string $expected): void {
        $this->assertSame($expected, $calc->addUnsignedInt8($in, $addend), "impl={$impl}");
    }

    public function testBcmathInvalidDecimalThrows(): void {

        $calc = new BcCalculator();
        $this->expectException(StringMathException::class);
        $this->expectExceptionCode(ExceptionCode::STRINGMATH_BCMATH_INVALID_DECIMAL->value);
        $calc->add1('abc');
    }

    /**
     * @dataProvider providerDivideBy256
     */
    public function testDivideBy256(string $impl, DecimalCalculator $calc, string $in, string $expectedQ, int $expectedR): void {
        $result = $calc->divideBy256($in);
        $this->assertSame($expectedQ, $result['quotient'], "impl={$impl}");
        $this->assertSame($expectedR, $result['remainder'], "impl={$impl}");
    }

    /**
     * @dataProvider providerFromBinary
     */
    public function testFromBinary(string $impl, DecimalCalculator $calc, string $binaryEscaped, string $expectedDecimal): void {
        $bin = $this->phpEscapedToBin($binaryEscaped);
        $this->assertSame($expectedDecimal, $calc->fromBinary($bin), "impl={$impl}");
    }

    /**
     * @dataProvider providerMultiplyBy256
     */
    public function testMultiplyBy256(string $impl, DecimalCalculator $calc, string $in, string $expected): void {
        $this->assertSame($expected, $calc->multiplyBy256($in), "impl={$impl}");
    }

    public function testNativeInvalidCharacterThrowsOnDivide(): void {
        $calc = new NativeCalculator();
        $this->expectException(StringMathException::class);
        $this->expectExceptionCode(ExceptionCode::STRINGMATH_NATIVE_INVALID_DECIMAL->value);
        $calc->divideBy256('12a3');
    }

    public function testNativeInvalidNumberThrowsOnDivide(): void {
        $calc = new NativeCalculator();
        $this->expectException(StringMathException::class);
        $this->expectExceptionCode(ExceptionCode::STRINGMATH_NATIVE_INVALID_DECIMAL->value);
        $calc->divideBy256('12.3');
    }

    public function testRoundTripDecimalBinary(): void {
        $values = [
            '-129', '-128', '-1', '0', '1', '2', '127', '128', '255', '256', '65535', '1000000',
        ];

        foreach ($this->availableCalculators() as [$impl, $calc]) {
            foreach ($values as $value) {
                $bin = $calc->toBinary($value);
                $dec = $calc->fromBinary($bin);
                $this->assertSame($value, $dec, "impl={$impl}, value={$value}");
            }
        }
    }

    public function testSetterGetReturnsSameInstance(): void {
        $native = new NativeCalculator();
        DecimalCalculator::set($native);
        $this->assertSame($native, DecimalCalculator::get());
    }

    /**
     * @dataProvider providerSub1
     */
    public function testSub1(string $impl, DecimalCalculator $calc, string $in, string $expected): void {
        $this->assertSame($expected, $calc->sub1($in), "impl={$impl}");
    }

    /**
     * @dataProvider providerToBinary
     */
    public function testToBinary(string $impl, DecimalCalculator $calc, string $decimal, string $expectedHexEscaped): void {
        $bin = $calc->toBinary($decimal);
        $asEscaped = $this->binToPhpEscaped($bin);
        $this->assertSame($expectedHexEscaped, $asEscaped, "impl={$impl}");
    }
    /**
     * @return array<int, array{string, DecimalCalculator}>
     */
    private static function availableCalculators(): array {
        $calculators = [];

        // GMP
        $gmp = new GmpCalculator();
        $calculators[] = ['gmp', $gmp];

        // BCMath
        $bcm = new BcCalculator();
        $calculators[] = ['bcmath', $bcm];

        // Native
        $calculators[] = ['native', new NativeCalculator()];

        return $calculators;
    }

    private function binToPhpEscaped(string $binary): string {
        $escaped = '';
        $len = strlen($binary);
        for ($i = 0; $i < $len; $i++) {
            $escaped .= sprintf('\\x%02X', ord($binary[$i]));
        }

        return $escaped;
    }

    /**
     * @param array<int, array<mixed>> $baseCases
     * @return array<string, array{string, DecimalCalculator, ...}>
     */
    private static function crossWithCalculators(array $baseCases): array {
        $data = [];
        $calculators = self::availableCalculators();
        foreach ($calculators as [$impl, $calc]) {
            foreach ($baseCases as $i => $case) {
                $key = $impl . '-' . $i;
                $data[$key] = array_merge([$impl, $calc], $case);
            }
        }

        /** @phpstan-ignore-next-line */
        return $data;
    }

    private function phpEscapedToBin(string $escaped): string {
        if ($escaped === '') {
            return '';
        }

        // Expect format like \xHH\xHH...
        $bytes = explode('\\x', $escaped);
        $bin = '';
        foreach ($bytes as $part) {
            if ($part === '') {
                continue;
            }
            $bin .= chr((int) hexdec(substr($part, 0, 2)));
        }

        return $bin;
    }
}
