<?php

declare(strict_types=1);

namespace Cassandra\Type;

class Boolean extends Base
{
    protected ?bool $_value = null;

    public function __construct(?bool $value = null)
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
        if ($value !== null && !is_bool($value)) {
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

    public function parseValue(): ?bool
    {
        if ($this->_value === null && $this->_binary !== null) {
            $this->_value = static::parse($this->_binary);
        }

        return $this->_value;
    }

    public function __toString(): string
    {
        return $this->_value ? '(true)' : '(false)';
    }

    public static function binary(bool $value): string
    {
        return $value ? "\1" : "\0";
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @psalm-suppress ReservedWord
     * @throws void
     */
    public static function parse(string $binary, null|int|array $definition = null): bool
    {
        return $binary !== "\0";
    }
}
