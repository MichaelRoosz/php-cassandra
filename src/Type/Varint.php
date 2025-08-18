<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\ExceptionCode;
use Cassandra\StringMath\Calculator;
use Cassandra\TypeInfo\TypeInfo;

final class Varint extends TypeBase {
    protected readonly string|int $value;

    final public function __construct(string|int $value) {
        $this->value = $value;
    }

    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {
        $length = strlen($binary);

        if ($length > PHP_INT_SIZE) {
            $value = Calculator::get()->binaryToString($binary);

            return new static($value);
        }

        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('C*', $binary);
        if ($unpacked === false) {
            throw new Exception('Cannot unpack varint binary data', ExceptionCode::TYPE_VARINT_UNPACK_FAILED->value, [
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
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_string($value) && !is_int($value)) {
            throw new Exception('Invalid varint value; expected int or numeric string', ExceptionCode::TYPE_VARINT_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        return new static($value);
    }

    #[\Override]
    public function getBinary(): string {
        if (is_int($this->value)) {
            return $this->getBinaryFromIntValue($this->value);
        }

        return Calculator::get()->stringToBinary($this->value);
    }

    #[\Override]
    public function getValue(): string {
        return (string) $this->value;
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public function getValueAsInt(): int {
        if (!is_int($this->value)) {
            throw new Exception(
                'Value of Varint is outside of possible integer range for this system',
                ExceptionCode::TYPE_VARINT_OUT_OF_PHP_INT_RANGE->value,
                [
                    'php_int_size_bits' => PHP_INT_SIZE * 8,
                    'value' => $this->value,
                ]
            );
        }

        return $this->value;
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
