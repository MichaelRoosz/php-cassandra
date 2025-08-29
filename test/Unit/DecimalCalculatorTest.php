<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\ExceptionCode;
use Cassandra\StringMath\DecimalCalculator;
use Cassandra\StringMath\DecimalCalculator\BCMath as BcCalculator;
use Cassandra\StringMath\DecimalCalculator\GMP as GmpCalculator;
use Cassandra\StringMath\DecimalCalculator\Native as NativeCalculator;
use Cassandra\StringMath\Exception as StringMathException;
use PHPUnit\Framework\TestCase;

class DecimalCalculatorTest extends TestCase {
    /**
     * @return array<string, array{string, DecimalCalculator, string, string}>
     */
    public static function providerAdd1(): array {
        $cases = [
            ['0', '1'],
            ['9', '10'],
            ['199', '200'],
            ['999999999999999999', '1000000000000000000'],
        ];

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
        ];

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
        ];

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
        ];

        return self::crossWithCalculators($cases);
    }

    /**
     * @return array<string, array{string, DecimalCalculator, string, int, string}>
     */
    public static function providerMultiplyByUnsignedInt8(): array {
        $cases = [
            ['0', 0, '0'],
            ['0', 1, '0'],
            ['1', 0, '0'],
            ['1', 1, '1'],
            ['123456', 2, '246912'],
            ['123456', 255, '31481280'],
        ];

        return self::crossWithCalculators($cases);
    }

    /**
     * @return array<string, array{string, DecimalCalculator, string, string}>
     */
    public static function providerSub1(): array {
        $cases = [
            ['0', '0'],
            ['1', '0'],
            ['1000', '999'],
            ['2001', '2000'],
        ];

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
        ];

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
        try {
            $calc = new BcCalculator();
        } catch (StringMathException) {
            $this->markTestSkipped('BCMath not available');

            return;
        }

        $this->expectException(StringMathException::class);
        $this->expectExceptionCode(ExceptionCode::STRINGMATH_CALCULATOR_BCMATH_INVALID_DECIMAL->value);
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
     * @dataProvider providerMultiplyByUnsignedInt8
     */
    public function testMultiplyByUnsignedInt8(string $impl, DecimalCalculator $calc, string $in, int $multiplier, string $expected): void {
        $this->assertSame($expected, $calc->multiplyByUnsignedInt8($in, $multiplier), "impl={$impl}");
    }

    public function testNativeInvalidCharacterThrowsOnDivide(): void {
        $calc = new NativeCalculator();
        $this->expectException(StringMathException::class);
        $this->expectExceptionCode(ExceptionCode::STRINGMATH_CALCULATOR_NATIVE_INVALID_CHARACTER->value);
        $calc->divideBy256('12a3');
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

        // GMP (optional)
        try {
            $gmp = new GmpCalculator();
            $calculators[] = ['gmp', $gmp];
        } catch (StringMathException) {
            // skip if extension not loaded
        }

        // BCMath (optional)
        try {
            $bcm = new BcCalculator();
            $calculators[] = ['bcmath', $bcm];
        } catch (StringMathException) {
            // skip if extension not loaded
        }

        // Native (always available)
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
            $bin .= chr(hexdec(substr($part, 0, 2)));
        }

        return $bin;
    }
}
