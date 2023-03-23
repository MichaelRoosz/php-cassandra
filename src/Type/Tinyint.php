<?php

declare(strict_types=1);

namespace Cassandra\Type;

class Tinyint extends Base
{
    use CommonResetValue;
    use CommonBinaryOfValue;
    use CommonToString;

    public const VALUE_MIN = -128;
    public const VALUE_MAX = 127;

    protected ?int $_value = null;

    /**
     * @throws \Cassandra\Type\Exception
     */
    public function __construct(?int $value = null)
    {
        if ($value !== null
            && ($value > self::VALUE_MAX || $value < self::VALUE_MIN)) {
            throw new Exception('Value "' . $value . '" is outside of possible range');
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
        if ($value !== null && !is_int($value)) {
            throw new Exception('Invalid value type');
        }

        return new self($value);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    protected function parseValue(): ?int
    {
        if ($this->_value === null && $this->_binary !== null) {
            $this->_value = static::parse($this->_binary);
        }

        return $this->_value;
    }

    public static function binary(int $value): string
    {
        return pack('c', $value);
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function parse(string $binary, null|int|array $definition = null): int
    {
        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('c', $binary);
        if ($unpacked === false) {
            throw new Exception('Cannot unpack binary');
        }

        return $unpacked[1];
    }
}
