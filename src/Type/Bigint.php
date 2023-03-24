<?php

declare(strict_types=1);

namespace Cassandra\Type;

use ReflectionClass;

class Bigint extends Base
{
    use CommonResetValue;
    use CommonBinaryOfValue;
    use CommonToString;

    protected ?int $_value = null;

    /**
     * @throws \Cassandra\Type\Exception
     */
    public function __construct(?int $value = null)
    {
        if (PHP_INT_SIZE < 8) {
            $className = (new ReflectionClass(static::class))->getShortName();
            throw new Exception('The ' . $className . ' data type requires a 64-bit system');
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

    /**
     * @throws \Cassandra\Type\Exception
     */
    public static function binary(int $value): string
    {
        if (PHP_INT_SIZE < 8) {
            $className = (new ReflectionClass(static::class))->getShortName();
            throw new Exception('The ' . $className . ' data type requires a 64-bit system');
        }

        return pack('J', $value);
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function parse(string $binary, null|int|array $definition = null): int
    {
        if (PHP_INT_SIZE < 8) {
            $className = (new ReflectionClass(static::class))->getShortName();
            throw new Exception('The ' . $className . ' data type requires a 64-bit system');
        }

        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('J', $binary);
        if ($unpacked === false) {
            throw new Exception('Cannot unpack binary.');
        }

        return $unpacked[1];
    }
}
