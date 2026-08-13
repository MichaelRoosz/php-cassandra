<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\StringMathException;
use Cassandra\Exception\ValueException;
use Cassandra\StringMath\DecimalCalculator;
use Cassandra\StringUtil;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;
use Cassandra\Value\EncodeOption\VarintEncodeOption;

final class Varint extends ValueReadableWithLength implements ValueWithMultipleEncodings {
    /**
     * The wire-side counterpart of {@see self::MAX_MAGNITUDE_DIGITS}, applied
     * before the bytes are converted rather than after.
     *
     * Checked in bytes because the digit count is only known once the expensive
     * conversion has already been paid for, which is the very thing the digit
     * bound exists to bound. So this is a pre-filter rather than the decision:
     * it is set at the widest a value at the digit bound can occupy — 4096 nines
     * take 13607 bits, so 1701 bytes with the sign — which is what makes the two
     * sides agree, since anything {@see self::getBinary()} can produce gets past
     * it. A binary that wide can spell up to 4097 digits, and those last few are
     * refused by the digit bound once converted; the point of stopping here is
     * that the conversion stays inside the cost the digit bound allows.
     */
    final public const MAX_MAGNITUDE_BYTES = 1701;

    /**
     * Largest magnitude this class will convert, as a count of decimal digits.
     *
     * Converting between a varint's wire form and its decimal spelling is
     * quadratic in the length of the value on the pure-PHP calculator
     * ({@see \Cassandra\StringMath\DecimalCalculator\Native}), which is what a
     * build without gmp and without bcmath falls back to: it walks the whole
     * number once per byte. An unbounded magnitude is therefore not merely slow
     * but a decode-side denial of service — a few kilobytes of cell can cost
     * minutes of CPU, and the cell length is bounded only by the frame, so the
     * peer chooses it. The counterpart of {@see Decimal::MAX_SCALE_MAGNITUDE},
     * which bounds the memory a single cell can be expanded into; this bounds
     * the time.
     *
     * Applied on both sides, so the two agree: a value this class would refuse
     * to read back is refused on its way out as well, rather than being written
     * to a node and never read from one.
     *
     * 4096 digits is a number of some 13,600 bits — wider than any key, hash or
     * identifier a column holds, and orders of magnitude past the tens of digits
     * a real varint carries — so a value past it is corrupt or hostile input
     * rather than a limit a real one runs into. At the bound the pure-PHP
     * calculator converts in a fifth of a second; gmp and bcmath do it in
     * microseconds and are unaffected by the choice.
     */
    final public const MAX_MAGNITUDE_DIGITS = 4096;

    private readonly string|int $value;

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    final public function __construct(string|int $value) {

        if (is_int($value)) {
            $this->value = $value;

            return;
        }

        $isInteger = str_starts_with($value, '-') ? StringUtil::isDigits(substr($value, 1)) : StringUtil::isDigits($value);
        if (!$isInteger) {
            throw new ValueException('Invalid varint value; expected int or integer string', ExceptionCode::VALUE_VARINT_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        // Normalised before anything is decided about it. Leading zeros and a
        // sign on zero are spellings rather than values, and one kept here
        // would travel out to the application through {@see self::asString()}
        // and {@see self::getValue()} as part of the number —
        // "0000000000000000000" for what is simply 0. The wire encoding has no
        // way to show them, so the same value read back from the node would not
        // match what was written.
        $value = self::normalizeDecimalString($value);

        self::assertMagnitudeInRange($value);

        // Kept as a string only where PHP's int genuinely cannot hold it, and
        // measured against the real bound rather than a digit count that stays
        // safely below it. A count gives up the whole top of the range — every
        // value from 10^18 up to PHP_INT_MAX — and {@see self::asInt()} would
        // then refuse a number that fits perfectly well, while the same value
        // arriving from a node came back as an int: eight bytes or fewer is
        // exactly what PHP's int can hold, so {@see self::fromBinary()} never
        // takes the string path for one.
        $this->value = self::fitsInPhpInt($value) ? (int) $value : $value;
    }

    #[\Override]
    public static function allowsEmpty(): bool {
        return true;
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public function asConfigured(ValueEncodeConfig $valueEncodeConfig): mixed {
        return match ($valueEncodeConfig->varintEncodeOption) {
            VarintEncodeOption::AS_INT => $this->asInt(),
            VarintEncodeOption::AS_STRING => $this->asString(),
        };
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    public function asInt(): int {
        if (!is_int($this->value)) {
            throw new ValueException(
                'Value of Varint is outside of possible integer range for this system',
                ExceptionCode::VALUE_VARINT_OUT_OF_PHP_INT_RANGE->value,
                [
                    'php_int_size_bits' => PHP_INT_SIZE * 8,
                    'value' => $this->value,
                ]
            );
        }

        return $this->value;
    }

    public function asString(): string {
        return (string) $this->value;
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public static function fromBinary(
        string $binary,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): ?static {

        if (self::emptyBinaryIsNull($binary)) {
            return null;
        }

        $length = strlen($binary);

        if ($length > PHP_INT_SIZE) {
            if ($length > self::MAX_MAGNITUDE_BYTES) {
                throw new ValueException(
                    'Varint magnitude is outside of the supported range',
                    ExceptionCode::VALUE_VARINT_MAGNITUDE_TOO_LARGE->value,
                    [
                        'binary_length' => $length,
                        'max_binary_length' => self::MAX_MAGNITUDE_BYTES,
                        'max_digits' => self::MAX_MAGNITUDE_DIGITS,
                    ]
                );
            }

            try {
                $decimal = DecimalCalculator::get()->fromBinary($binary);
            } catch (StringMathException $e) {
                throw new ValueException('Failed to get decimal from binary', ExceptionCode::VALUE_VARINT_UNPACK_FAILED->value, [
                    'binary' => $binary,
                ], $e);
            }

            return new static($decimal);
        }

        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('C*', $binary);
        if ($unpacked === false) {
            throw new ValueException('Cannot unpack varint binary data', ExceptionCode::VALUE_VARINT_UNPACK_FAILED->value, [
                'binary_length' => strlen($binary),
            ]);
        }

        $value = 0;
        foreach ($unpacked as $i => $byte) {
            $value |= $byte << (($length - (int) $i) * 8);
        }

        $shift = (PHP_INT_SIZE - $length) * 8;

        return new static($value << $shift >> $shift);
    }

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_numeric($value) || is_float($value)) {
            throw new ValueException('Invalid varint value; expected int or integer string', ExceptionCode::VALUE_VARINT_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        return new static($value);
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    final public static function fromValue(string|int $value): static {
        return new static($value);
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public function getBinary(): string {

        if (is_int($this->value)) {
            return $this->getBinaryFromIntValue($this->value);
        }

        try {
            return DecimalCalculator::get()->toBinary($this->value);
        } catch (StringMathException $e) {
            throw new ValueException('Failed to get binary from decimal', ExceptionCode::VALUE_VARINT_UNPACK_FAILED->value, [
                'decimal' => $this->value,
            ], $e);
        }
    }

    #[\Override]
    public function getType(): Type {
        return Type::VARINT;
    }

    #[\Override]
    public function getValue(): string {
        return (string) $this->value;
    }

    #[\Override]
    public static function isEmptyValueMeaningless(): bool {
        return true;
    }

    #[\Override]
    final public static function requiresDefinition(): bool {
        return false;
    }

    /**
     * Refuse a magnitude this class will not convert; see
     * {@see self::MAX_MAGNITUDE_DIGITS}.
     *
     * @param string $decimal a canonical decimal string, as
     * {@see self::normalizeDecimalString()} produces
     *
     * @throws \Cassandra\Exception\ValueException
     */
    private static function assertMagnitudeInRange(string $decimal): void {

        $digits = str_starts_with($decimal, '-') ? strlen($decimal) - 1 : strlen($decimal);

        if ($digits <= self::MAX_MAGNITUDE_DIGITS) {
            return;
        }

        throw new ValueException(
            'Varint magnitude is outside of the supported range',
            ExceptionCode::VALUE_VARINT_MAGNITUDE_TOO_LARGE->value,
            [
                'digits' => $digits,
                'max_digits' => self::MAX_MAGNITUDE_DIGITS,
            ]
        );
    }

    /**
     * Whether a canonical decimal string names a value PHP's int can hold.
     *
     * Measured against PHP_INT_MAX — or, for a negative value, the magnitude of
     * PHP_INT_MIN, which is one larger — so the whole of the range is available,
     * and on a 32-bit build as well as a 64-bit one.
     *
     * Both sides are canonical digit strings by the time they meet, so a
     * difference in length settles it outright and equal lengths make a byte
     * comparison a numeric one.
     *
     * @param string $decimal a canonical decimal string, as
     * {@see self::normalizeDecimalString()} produces
     */
    private static function fitsInPhpInt(string $decimal): bool {

        $isNegative = str_starts_with($decimal, '-');

        $digits = $isNegative ? substr($decimal, 1) : $decimal;

        // The magnitude of the bound, which on the negative side is PHP_INT_MIN
        // without its sign and so one more than PHP_INT_MAX.
        $bound = $isNegative
            ? substr((string) PHP_INT_MIN, 1)
            : (string) PHP_INT_MAX;

        if (strlen($digits) !== strlen($bound)) {
            return strlen($digits) < strlen($bound);
        }

        return strcmp($digits, $bound) <= 0;
    }

    private function getBinaryFromIntValue(int $value): string {
        $isNegative = $value < 0;
        $breakValue = $isNegative ? -1 : 0;

        $result = [];
        do {
            $result[] = $value & 0xFF;
            $value >>= 8;
        } while ($value !== $breakValue);

        $length = count($result);

        // Check if the most significant bit is set, which could be interpreted as a negative number
        if (!$isNegative && ($result[$length - 1] & 0x80) !== 0) {
            // Add an extra byte with a 0x00 value to keep the number positive
            $result[] = 0;
        }
        // Check if the most significant bit is not set, which could be interpreted as a positive number
        elseif ($isNegative && ($result[$length - 1] & 0x80) === 0) {
            // Add an extra byte with a 0xFF value to keep the number negative
            $result[] = 0xFF;
        }

        return pack('C*', ...array_reverse($result));
    }

    /**
     * A validated integer string reduced to the one spelling that stands for
     * its value: no leading zeros, and no sign on zero.
     *
     * @param string $decimal digits with an optional leading '-', which is what
     * the constructor has established by the time this is reached
     */
    private static function normalizeDecimalString(string $decimal): string {

        $isNegative = str_starts_with($decimal, '-');

        $digits = ltrim($isNegative ? substr($decimal, 1) : $decimal, '0');

        if ($digits === '') {
            return '0';
        }

        return $isNegative ? '-' . $digits : $digits;
    }
}
