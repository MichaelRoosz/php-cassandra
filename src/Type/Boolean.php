<?php

declare(strict_types=1);

namespace Cassandra\Type;

class Boolean extends Base
{
    use CommonResetValue;
    use CommonBinaryOfValue;
    use CommonToString;

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

    protected function parseValue(): ?bool
    {
        if ($this->_value === null && $this->_binary !== null) {
            $this->_value = static::parse($this->_binary);
        }

        return $this->_value;
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
