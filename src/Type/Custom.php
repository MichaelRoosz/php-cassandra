<?php

declare(strict_types=1);

namespace Cassandra\Type;

class Custom extends Base
{
    /**
     * @var int|array<int|array<mixed>> $_definition
     */
    protected int|array $_definition;

    protected ?string $_value = null;

    /**
     * @param ?string $value
     * @param int|array<int|array<mixed>> $definition
     */
    public function __construct(?string $value, int|array $definition)
    {
        $this->_definition = $definition;
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

        if ($definition === null) {
            throw new Exception('Invalid definition');
        }

        return new self($value, $definition);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public function binaryOfValue(): string
    {
        if ($this->_value === null) {
            throw new Exception('value is null');
        }

        return static::binary($this->_value, $this->_definition);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public function parseValue(): ?string
    {
        if ($this->_value === null && $this->_binary !== null) {
            $this->_value = static::parse($this->_binary, $this->_definition);
        }

        return $this->_value;
    }

    protected function resetValue(): void {
        $this->_value = null;
    }

    public function __toString(): string
    {
        return (string) $this->_value;
    }

    /**
     * @param int|array<int|array<mixed>> $definition
     */
    public static function binary(string $value, int|array $definition): string
    {
        return pack('n', strlen($value)) . $value;
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function parse(string $binary, null|int|array $definition = null): string
    {
        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('n', substr($binary, 0, 2));
        if ($unpacked === false) {
            throw new Exception('Cannot unpack binary.');
        }

        $length = $unpacked[1];

        return substr($binary, 2, $length);
    }
}
