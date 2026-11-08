<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;

final class Decimal extends ValueReadableWithLength {
    /**
     * The largest scale decoded from the wire, either way: far beyond any real
     * column, and small enough that expanding it stays cheap.
     */
    private const MAX_SCALE_MAGNITUDE = 100_000;

    private readonly string $value;

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    final public function __construct(string|int|float $value) {
        if (!is_numeric($value)) {
            throw new ValueException('Value must be a numeric value', ExceptionCode::VALUE_DECIMAL_NON_NUMERIC->value, [
                'value' => $value,
            ]);
        }

        if (is_float($value)) {
            if (!is_finite($value)) {
                throw new ValueException('Decimal value must be a finite number', ExceptionCode::VALUE_DECIMAL_NON_FINITE_FLOAT->value, [
                    'value' => $value,
                ]);
            }

            $this->value = self::floatToDecimalString($value);

            return;
        }

        $this->value = (string) $value;
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public static function fromBinary(
        string $binary,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): static {

        $length = strlen($binary);
        if ($length < 4) {
            throw new ValueException('Cannot unpack decimal binary data', ExceptionCode::VALUE_DECIMAL_UNPACK_FAILED->value, [
                'binary_length' => $length,
                'note' => 'expected >= 4 bytes (scale + varint)',
            ]);
        }
        /**
         * @var false|array<int> $scaleUnpacked
         */
        $scaleUnpacked = unpack('N', substr($binary, 0, 4));
        if ($scaleUnpacked === false) {
            throw new ValueException('Cannot unpack decimal scale', ExceptionCode::VALUE_DECIMAL_UNPACK_FAILED->value, [
                'binary_length' => $length,
            ]);
        }
        // The scale is a signed int32, which unpack('N') returns unsigned.
        $signShift = (PHP_INT_SIZE * 8) - 32;
        $scale = $scaleUnpacked[1] << $signShift >> $signShift;

        // Both expansions below allocate in proportion to the scale.
        if ($scale > self::MAX_SCALE_MAGNITUDE || $scale < -self::MAX_SCALE_MAGNITUDE) {
            throw new ValueException('Decimal scale is outside of the supported range', ExceptionCode::VALUE_DECIMAL_SCALE_OUT_OF_RANGE->value, [
                'scale' => $scale,
                'max_magnitude' => self::MAX_SCALE_MAGNITUDE,
            ]);
        }

        $varintBinary = substr($binary, 4);
        $unscaledVarint = Varint::fromBinary($varintBinary);
        $unscaled = $unscaledVarint->asString();

        if ($scale === 0) {
            $value = $unscaled;
        } elseif ($scale < 0) {
            // A negative scale multiplies by a power of ten.
            $value = $unscaled === '0' ? '0' : $unscaled . str_repeat('0', -$scale);
        } else {
            $isNegative = str_starts_with($unscaled, '-');
            $absUnscaled = $isNegative ? substr($unscaled, 1) : $unscaled;

            // Pad with zeros if necessary
            $absUnscaled = str_pad($absUnscaled, $scale + 1, '0', STR_PAD_LEFT);

            // Insert decimal point
            $integerPart = substr($absUnscaled, 0, -$scale);
            $decimalPart = substr($absUnscaled, -$scale);

            // Remove leading zeros from integer part, but keep at least one digit
            $integerPart = ltrim($integerPart, '0') ?: '0';

            $value = $integerPart . '.' . $decimalPart;
            if ($isNegative) {
                $value = '-' . $value;
            }
        }

        return new static($value);
    }

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_numeric($value)) {
            throw new ValueException('Invalid decimal value; expected numeric value', ExceptionCode::VALUE_DECIMAL_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        return new static($value);
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    final public static function fromValue(string|int|float $value): static {
        return new static($value);
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public function getBinary(): string {

        $scalePos = strpos($this->value, '.');
        $hasScale = $scalePos !== false;

        if ($hasScale) {
            $scale = strlen($this->value) - $scalePos - 1;
            $unscaled = substr($this->value, 0, $scalePos) . substr($this->value, $scalePos + 1);
        } else {
            $scale = 0;
            $unscaled = $this->value;
        }

        $binary = pack('N', $scale) . (new Varint($unscaled))->getBinary();

        return $binary;
    }

    #[\Override]
    public function getType(): Type {
        return Type::DECIMAL;
    }

    #[\Override]
    public function getValue(): string {
        return $this->value;
    }

    #[\Override]
    final public static function requiresDefinition(): bool {
        return false;
    }

    /**
     * A float's shortest round-trip spelling, with any exponent expanded: the
     * varint-based wire encoding cannot express one.
     */
    private static function floatToDecimalString(float $value): string {
        $spelled = var_export($value, true);
        $sign = str_starts_with($spelled, '-') ? '-' : '';
        $spelled = ltrim($spelled, '-');

        $exponentPos = stripos($spelled, 'e');
        $mantissa = $exponentPos === false ? $spelled : substr($spelled, 0, $exponentPos);
        $exponent = $exponentPos === false ? 0 : (int) substr($spelled, $exponentPos + 1);

        $dotPos = strpos($mantissa, '.');
        $digits = str_replace('.', '', $mantissa);
        $pointPos = ($dotPos === false ? strlen($mantissa) : $dotPos) + $exponent;

        if ($pointPos <= 0) {
            $decimal = '0.' . str_repeat('0', -$pointPos) . $digits;
        } elseif ($pointPos >= strlen($digits)) {
            $decimal = $digits . str_repeat('0', $pointPos - strlen($digits));
        } else {
            $decimal = substr($digits, 0, $pointPos) . '.' . substr($digits, $pointPos);
        }

        if (str_contains($decimal, '.')) {
            $decimal = rtrim(rtrim($decimal, '0'), '.');
        }

        $decimal = ltrim($decimal, '0');
        if ($decimal === '' || $decimal[0] === '.') {
            $decimal = '0' . $decimal;
        }

        return $decimal === '0' ? '0' : $sign . $decimal;
    }
}
