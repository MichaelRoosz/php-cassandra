<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\StringMath\Calculator;

class Varint extends TypeBase {
    protected string|int $value;

    final public function __construct(string|int $value) {
        $this->value = $value;
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     */
    public static function fromBinary(string $binary, null|int|array $definition = null): static {
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
            throw new Exception('Cannot unpack binary.');
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
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function fromValue(mixed $value, null|int|array $definition = null): static {
        if (!is_string($value) && !is_int($value)) {
            throw new Exception('Invalid value');
        }

        return new static($value);
    }

    public function getBinary(): string {
        if (is_int($this->value)) {
            return $this->getBinaryFromIntValue($this->value);
        }

        return Calculator::get()->stringToBinary($this->value);
    }

    public function getValue(): string {
        return (string) $this->value;
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public function getValueAsInt(): int {
        if (!is_int($this->value)) {
            throw new Exception('Value of Varint is outside of possible integer range (this system only supports signed ' . (PHP_INT_SIZE*8) . '-bit integers).');
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
