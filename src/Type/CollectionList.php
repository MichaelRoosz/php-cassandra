<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\Response\StreamReader;

class CollectionList extends Base
{
    /**
     * @var int|array<int|array<mixed>> $_definition
     */
    protected int|array $_definition;

    /**
     * @var ?array<mixed> $_value
     */
    protected ?array $_value = null;

    /**
     * @param ?array<mixed> $value
     * @param int|array<int|array<mixed>> $definition
     */
    public function __construct(?array $value, int|array $definition)
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
     * @return ?array<mixed> $_value
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public function parseValue(): ?array
    {
        if ($this->_value === null && $this->_binary !== null) {
            $this->_value = static::parse($this->_binary, $this->_definition);
        }

        return $this->_value;
    }

    public function __toString(): string
    {
        return (string) json_encode($this->_value);
    }

    /**
     * @param array<mixed> $value
     * @param int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function binary(array $value, int|array $definition): string
    {
        $binary = pack('N', count($value));

        if (is_array($definition)) {
            if (count($definition) < 1) {
                throw new Exception('invalid type definition');
            } elseif (count($definition) === 1) {
                [$valueType] = array_values($definition);
            } else {
                $valueType = $definition;
            }
        } else {
            $valueType = $definition;
        }

        /** @var mixed $val */
        foreach ($value as $val) {
            $itemPacked = Base::getBinaryByType($valueType, $val);
            $binary .= pack('N', strlen($itemPacked)) . $itemPacked;
        }
        return $binary;
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     * @return array<mixed>
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public static function parse(string $binary, null|int|array $definition = null): array
    {
        if ($definition === null) {
            throw new Exception('invalid CollectionList definition');
        }

        return (new StreamReader($binary))->readList($definition);
    }
}
