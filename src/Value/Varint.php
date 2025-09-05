<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\ExceptionCode;
use Cassandra\StringMath\DecimalCalculator;
use Cassandra\StringMath\Exception as StringMathException;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;

final class Varint extends ValueReadableWithLength {
    protected readonly string|int $value;

    /**
     * @throws \Cassandra\Value\Exception
     */
    final public function __construct(string|int $value) {

        if (is_int($value)) {
            $this->value = $value;

            return;
        }

        $isInteger = str_starts_with($value, '-') ? ctype_digit(substr($value, 1)) : ctype_digit($value);
        if (!$isInteger) {
            throw new Exception('Invalid varint value; expected int or integer string', ExceptionCode::VALUE_VARINT_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        // convert to int value if it fits into PHP_INT_MAX, otherwise keep as string
        $length = strlen($value);
        if (str_starts_with($value, '-')) {
            $length--;
        }

        $this->value = match (PHP_INT_SIZE) {
            4 => $length <= 9 ? (int) $value : $value,
            8 => $length <= 18 ? (int) $value : $value, /** @phpstan-ignore match.alwaysTrue */
            default => $value,
        };
    }

    /**
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {
        $length = strlen($binary);

        if ($length > PHP_INT_SIZE) {
            try {
                $decimal = DecimalCalculator::get()->fromBinary($binary);
            } catch (StringMathException $e) {
                throw new Exception('Failed to get decimal from binary', ExceptionCode::VALUE_VARINT_UNPACK_FAILED->value, [
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
            throw new Exception('Cannot unpack varint binary data', ExceptionCode::VALUE_VARINT_UNPACK_FAILED->value, [
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
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_numeric($value) || is_float($value)) {
            throw new Exception('Invalid varint value; expected int or integer string', ExceptionCode::VALUE_VARINT_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        return new static($value);
    }

    /**
     * @throws \Cassandra\Value\Exception
     */
    final public static function fromValue(string|int $value): static {
        return new static($value);
    }

    /**
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    public function getBinary(): string {

        if (is_int($this->value)) {
            return $this->getBinaryFromIntValue($this->value);
        }

        try {
            return DecimalCalculator::get()->toBinary($this->value);
        } catch (StringMathException $e) {
            throw new Exception('Failed to get binary from decimal', ExceptionCode::VALUE_VARINT_UNPACK_FAILED->value, [
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

    /**
     * @throws \Cassandra\Value\Exception
     */
    public function getValueAsInt(): int {
        if (!is_int($this->value)) {
            throw new Exception(
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

    public function getValueAsString(): string {
        return (string) $this->value;
    }

    #[\Override]
    final public static function requiresDefinition(): bool {
        return false;
    }

    protected function getBinaryFromIntValue(int $value): string {
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
}
