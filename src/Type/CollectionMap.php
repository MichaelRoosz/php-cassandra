<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\Type;


use Cassandra\Response\StreamReader;

class CollectionMap extends TypeBase {
    /**
     * @var array<int|array<mixed>> $definition
     */
    protected array $definition;

    /**
     * @var array<mixed> $value
     */
    protected array $value;

    /**
     * @param array<mixed> $value
     * @param array<int|array<mixed>> $definition
     */
    public final function __construct(array $value, array $definition) {
        $this->definition = $definition;
        $this->value = $value;
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\Response\Exception
     */
    public static function fromBinary(string $binary, null|int|array $definition = null): static {
        if (!is_array($definition)) {
            throw new Exception('invalid CollectionMap definition');
        }

        return new static((new StreamReader($binary))->readMap($definition), $definition);
    }

    /**
     * @param mixed $value
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function fromValue(mixed $value, null|int|array $definition = null): static {
        if (!is_array($value)) {
            throw new Exception('Invalid value');
        }

        if (!is_array($definition)) {
            throw new Exception('Invalid type definition');
        }

        return new static($value, $definition);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public function getBinary(): string {
        if (count($this->definition) < 2) {
            throw new Exception('invalid type definition');
        }

        [$keyType, $valueType] = array_values($this->definition);
        $binary = pack('N', count($this->value));

        /** @var TypeBase|mixed $val */
        foreach ($this->value as $key => $val) {
            $keyPacked = Type::getBinaryByType($keyType, $key);

            $valuePacked = $val instanceof TypeBase
                ? $val->getBinary()
                : Type::getBinaryByType($valueType, $val);

            $binary .= pack('N', strlen($keyPacked)) . $keyPacked;
            $binary .= pack('N', strlen($valuePacked)) . $valuePacked;
        }

        return $binary;
    }

    /**
     * @return array<mixed> $value
     */
    public function getValue(): ?array {
        return $this->value;
    }
}
