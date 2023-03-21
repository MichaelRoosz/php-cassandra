<?php

declare(strict_types=1);

namespace Cassandra\Type;

class Decimal extends Base
{
    protected ?string $_value = null;

    /**
     * @throws \Cassandra\Type\Exception
     */
    public function __construct(?string $value = null)
    {
        if (!is_numeric($value)) {
            throw new Exception('Incoming value must be numeric string.');
        }

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
        if ($value !== null && !is_string($value)) {
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
    public function parseValue(): ?string
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

    public static function binary(string $value): string
    {
        $pos = strpos($value, '.');
        $scaleLen = $pos === false ? 0 : strlen($value) - $pos - 1;
        if ($scaleLen) {
            $numericValue = (float)$value * pow(10, $scaleLen);
        } else {
            $numericValue = (int)$value;
        }

        $binary = pack('N', $scaleLen) . Varint::binary((int)$numericValue);
        return $binary;
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function parse(string $binary, null|int|array $definition = null): string
    {
        $length = strlen($binary);

        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('N1scale/C*', $binary);
        if ($unpacked === false) {
            throw new Exception('Cannot unpack binary.');
        }

        $valueByteLen = $length - 4;
        $value = 0;
        for ($i = 1; $i <= $valueByteLen; ++$i) {
            $value |= $unpacked[$i] << (($valueByteLen - $i) * 8);
        }

        $shift = (PHP_INT_SIZE - $valueByteLen) * 8;
        $value = (string) ($value << $shift >> $shift);

        if ($unpacked['scale'] === 0) {
            return $value;
        } elseif (strlen($value) > $unpacked['scale']) {
            return substr($value, 0, -$unpacked['scale']) . '.' . substr($value, -$unpacked['scale']);
        } else {
            return $value >= 0 ? sprintf("0.%0$unpacked[scale]d", $value) : sprintf("-0.%0$unpacked[scale]d", -$value);
        }
    }
}
