<?php

declare(strict_types=1);

namespace Cassandra\Type;

class Varchar extends Base
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

    public static function binary(string $value): string
    {
        return $value;
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @psalm-suppress ReservedWord
     * @throws void
     */
    public static function parse(string $binary, null|int|array $definition = null): string
    {
        return $binary;
    }
}
