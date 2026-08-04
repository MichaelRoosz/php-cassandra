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
