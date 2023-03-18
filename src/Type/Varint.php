<?php

declare(strict_types=1);

namespace Cassandra\Type;

class Varint extends Base
{
    protected ?int $_value = null;

    public function __construct(?int $value = null)
    {
        $this->_value = $value;
    }

    /**
     * @param mixed $value
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    protected static function create(mixed $value, null|int|array $definition): self
    {
        if ($value !== null && !is_int($value)) {
            throw new Exception('Invalid value type');
        }

        return new self($value);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public function binaryOfValue(): string
    {
        if ($this->_value === null) {
            throw new Exception('value is null');
        }

        return static::binary($this->_value);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public function parseValue(): ?int
    {
        if ($this->_value === null && $this->_binary !== null) {
            $this->_value = static::parse($this->_binary);
        }

        return $this->_value;
    }

    public function __toString(): string
    {
        return (string) $this->_value;
    }

    public static function binary(int $value): string
    {
        $isNegative = $value < 0;

        if ($isNegative) {
            $value = -$value;
            $value -= 1;
        }

        $result = [];
        do {
            $result[] = $isNegative ? ~$value & 0xFF : $value & 0xFF;
            $value >>= 8;
        } while ($value > 0);

        $length = count($result);

        // Check if the most significant bit is set, which could be interpreted as a negative number
        if (!$isNegative && ($result[$length - 1] & 0x80) !== 0) {
            // Add an extra byte with 0 value to keep the number positive
            $result[] = 0;
        }
        // Check if the most significant bit is not set, which could be interpreted as a positive number
        elseif ($isNegative && ($result[$length - 1] & 0x80) === 0) {
            // Add an extra byte with 0 value to keep the number positive
            $result[] = 0xFF;
        }

        // Reverse the result to get big endian
        $result = array_reverse($result);

        // Use pack() function to convert each byte to its corresponding character
        return pack('C*', ...$result);
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function parse(string $binary, null|int|array $definition = null): int
    {
        $value = 0;
        $length = strlen($binary);

        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('C*', $binary);
        if ($unpacked === false) {
            throw new Exception('Cannot unpack binary.');
        }

        foreach ($unpacked as $i => $byte) {
            $value |= $byte << (($length - (int)$i) * 8);
        }
        $shift = (PHP_INT_SIZE - $length) * 8;
        return $value << $shift >> $shift;
    }
}
