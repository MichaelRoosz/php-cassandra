<?php

declare(strict_types=1);

namespace Cassandra\Test\Unit;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Value\Decimal;
use Cassandra\Value\Varint;
use PHPUnit\Framework\Attributes\DataProvider;

final class DecimalWireTest extends AbstractUnitTestCase {
    /**
     * @return array<string, array{float, string}>
     */
    public static function floats(): array {
        return [
            'fraction' => [0.1, '0.1'],
            'negative fraction' => [-0.5, '-0.5'],
            'small exponent' => [1.5e-7, '0.00000015'],
            'whole' => [123.0, '123'],
            'large exponent' => [1.0e25, '10000000000000000000000000'],
            'negative zero' => [-0.0, '0'],
        ];
    }

    #[DataProvider('floats')]
    public function testAFloatKeepsItsValue(float $value, string $expected): void {
        // Regression: floats were formatted with no decimals, so 0.1 became 0.
        $decimal = Decimal::fromValue($value);
        $this->assertSame($expected, $decimal->getValue());
        $this->assertSame($expected, Decimal::fromBinary($decimal->getBinary())->getValue());
    }

    public function testANegativeScaleMultipliesByAPowerOfTen(): void {
        // Regression: the scale was read unsigned, so -2 became 4294967294 and hung the decoder.
        $this->assertSame('500', Decimal::fromBinary(pack('N', 0xFFFFFFFE) . "\x05")->getValue());
        $this->assertSame('0', Decimal::fromBinary(pack('N', 0xFFFFFFFF) . "\x00")->getValue());
        $this->assertSame('0.05', Decimal::fromBinary(pack('N', 2) . "\x05")->getValue());
    }

    public function testANonFiniteFloatIsRefused(): void {
        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_DECIMAL_NON_FINITE_FLOAT->value);
        Decimal::fromValue(INF);
    }

    public function testAnOversizedScaleIsRefused(): void {
        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_DECIMAL_SCALE_OUT_OF_RANGE->value);
        Decimal::fromBinary(pack('N', 0x7FFFFFFF) . "\x05");
    }

    public function testAnOversizedVarintIsRefused(): void {
        // Regression: converting it took time quadratic in its length.
        $this->expectException(ValueException::class);
        $this->expectExceptionCode(ExceptionCode::VALUE_VARINT_MAGNITUDE_TOO_LARGE->value);
        Varint::fromBinary(str_repeat("\x7F", Varint::MAX_MAGNITUDE_BYTES + 1));
    }
}
