<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\Response\StreamReader;

class CollectionMap extends Base
{
    /**
     * @var array<int|array<mixed>> $_definition
     */
    protected array $_definition;

    /**
     * @var ?array<mixed> $_value
     */
    protected ?array $_value = null;

    /**
     * @param ?array<mixed> $value
     * @param array<int|array<mixed>> $definition
     */
    public function __construct(?array $value, array $definition)
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
        if ($value !== null && !is_array($value)) {
            throw new Exception('Invalid value type');
        }

        if (!is_array($definition)) {
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
     * @return ?array<mixed> $_value
     *
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\Response\Exception
     */
    public function parseValue(): ?array
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
        return (string) json_encode($this->_value);
    }

    /**
     * @param array<mixed> $value
     * @param array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function binary(array $value, array $definition): string
    {
        if (count($definition) < 2) {
            throw new Exception('invalid type definition');
        }

        [$keyType, $valueType] = array_values($definition);
        $binary = pack('N', count($value));

        /** @var Base|mixed $val */
        foreach ($value as $key => $val) {
            $keyPacked = Base::getBinaryByType($keyType, $key);

            $valuePacked = $val instanceof Base
                ? $val->getBinary()
                : Base::getBinaryByType($valueType, $val);

            $binary .= pack('N', strlen($keyPacked)) . $keyPacked;
            $binary .= pack('N', strlen($valuePacked)) . $valuePacked;
        }
        return $binary;
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     * @return array<mixed>
     *
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\Response\Exception
     */
    public static function parse(string $binary, null|int|array $definition = null): array
    {
        if (!is_array($definition)) {
            throw new Exception('invalid CollectionMap definition');
        }

        return (new StreamReader($binary))->readMap($definition);
    }
}
