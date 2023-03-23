<?php

declare(strict_types=1);

namespace Cassandra\Type;

class Inet extends Base
{
    use Common;

    protected ?string $_value = null;

    public function __construct(?string $value = null)
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
        if ($value !== null && !is_string($value)) {
            throw new Exception('Invalid value type');
        }

        return new self($value);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    protected function parseValue(): ?string
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

    /**
     * @throws \Cassandra\Type\Exception
     */
    public static function binary(string $value): string
    {
        $binary = inet_pton($value);

        if ($binary === false) {
            throw new Exception('Cannot convert value to binary.');
        }

        return $binary;
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function parse(string $binary, null|int|array $definition = null): string
    {
        $inet = inet_ntop($binary);

        if ($inet === false) {
            throw new Exception('Cannot convert value to string.');
        }

        return $inet;
    }
}
