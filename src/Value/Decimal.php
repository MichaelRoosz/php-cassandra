<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;

final class Decimal extends ValueReadableWithLength {
    /**
     * Upper bound on the absolute scale accepted by {@see fromBinary()}. The
     * scale is a signed int32 (so a peer may declare a magnitude up to ~2.1
     * billion), and decoding expands the value into a plain decimal string whose
     * length grows with the scale (padding the fraction with leading zeros, or
     * appending trailing zeros for a negative scale). An unbounded scale
     * therefore lets a peer force a multi-gigabyte allocation from a handful of
     * bytes. This limit caps a single cell's expansion at ~100 KB while leaving
     * an enormous margin over any real decimal (real scales are at most a few
     * hundred), so a larger magnitude is treated as corrupt/hostile input.
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

        // String (or int) input. is_numeric() also accepts forms the varint
        // wire encoding cannot express verbatim: scientific notation ("1e5"),
        // a leading "+", and surrounding whitespace. Normalize those to a plain
        // decimal string so a value accepted here can always be encoded by
        // getBinary(); plain decimals are kept verbatim (see below).
        $this->value = self::normalizeNumericString((string) $value);
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
        // The scale is a signed int32; unpack('N') is unsigned, so sign-extend.
        $signShift = (PHP_INT_SIZE * 8) - 32;
        $scale = $scaleUnpacked[1] << $signShift >> $signShift;

        // Reject an absurd scale before expanding the value: both the positive
        // (str_pad) and negative (str_repeat) paths below allocate memory
        // proportional to the scale, so an unbounded value is a decode-side
        // denial-of-service vector.
        if ($scale > self::MAX_SCALE_MAGNITUDE || $scale < -self::MAX_SCALE_MAGNITUDE) {
            throw new ValueException('Decimal scale is outside of the supported range', ExceptionCode::VALUE_DECIMAL_SCALE_OUT_OF_RANGE->value, [
                'scale' => $scale,
                'max_magnitude' => self::MAX_SCALE_MAGNITUDE,
                'binary_length' => $length,
            ]);
        }

        $varintBinary = substr($binary, 4);
        $unscaledVarint = Varint::fromBinary($varintBinary);
        $unscaled = $unscaledVarint->asString();

        if ($scale === 0) {
            $value = $unscaled;
        } elseif ($scale < 0) {
            // Negative scale: value = unscaled * 10^(-scale), i.e. append zeros.
            $value = ($unscaled === '0') ? '0' : $unscaled . str_repeat('0', -$scale);
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
     * Converts a float to a plain decimal string without losing its value: the
     * shortest string that round-trips to the same float, with any scientific
     * notation expanded (the varint-based wire encoding cannot express an
     * exponent).
     */
    private static function floatToDecimalString(float $value): string {
        return self::scientificToDecimalString(var_export($value, true));
    }

    /**
     * Normalizes a numeric string to a plain decimal string.
     *
     * Plain decimal strings (optional leading "-", digits, optional fraction)
     * are returned verbatim so an explicitly specified scale — trailing zeros
     * such as in "1.50" — survives unchanged to the wire. Everything else that
     * is_numeric() accepts (a leading "+", surrounding whitespace, scientific
     * notation, or bare-point forms like ".5") is expanded so that a value
     * accepted at construction can always be serialized by getBinary().
     */
    private static function normalizeNumericString(string $value): string {
        $value = trim($value);

        if (preg_match('/^-?\d+(?:\.\d+)?$/', $value) === 1) {
            return $value;
        }

        return self::scientificToDecimalString($value);
    }

    /**
     * Expands a signed decimal string that may carry scientific notation and/or
     * a leading sign into a plain decimal string (the varint-based wire encoding
     * cannot express an exponent). Trailing fraction zeros are trimmed.
     */
    private static function scientificToDecimalString(string $value): string {
        $sign = '';
        if (str_starts_with($value, '-')) {
            $sign = '-';
            $value = substr($value, 1);
        } elseif (str_starts_with($value, '+')) {
            $value = substr($value, 1);
        }

        $exponentPos = stripos($value, 'e');
        if ($exponentPos === false) {
            $mantissa = $value;
            $exponent = 0;
        } else {
            $mantissa = substr($value, 0, $exponentPos);
            $exponent = (int) substr($value, $exponentPos + 1);
        }

        $dotPos = strpos($mantissa, '.');
        $integerLength = $dotPos === false ? strlen($mantissa) : $dotPos;
        $digits = str_replace('.', '', $mantissa);

        // Position of the decimal point within $digits after applying the exponent.
        $pointPos = $integerLength + $exponent;

        if ($pointPos <= 0) {
            $result = '0.' . str_repeat('0', -$pointPos) . $digits;
        } elseif ($pointPos >= strlen($digits)) {
            $result = $digits . str_repeat('0', $pointPos - strlen($digits));
        } else {
            $result = substr($digits, 0, $pointPos) . '.' . substr($digits, $pointPos);
        }

        return $sign . self::trimTrailingFractionZeros($result);
    }

    private static function trimTrailingFractionZeros(string $decimal): string {
        if (!str_contains($decimal, '.')) {
            return $decimal;
        }

        return rtrim(rtrim($decimal, '0'), '.');
    }
}
