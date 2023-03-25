<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\Response\StreamReader;
use Cassandra\Type;

class CollectionList extends TypeBase {
    /**
     * @var int|array<int|array<mixed>> $definition
     */
    protected int|array $definition;

    /**
     * @var array<mixed> $value
     */
    protected array $value;

    /**
     * @param array<mixed> $value
     * @param int|array<int|array<mixed>> $definition
     */
    public final function __construct(array $value, int|array $definition) {
        $this->definition = $definition;
        $this->value = $value;
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public static function fromBinary(string $binary, null|int|array $definition = null): static {
        if ($definition === null) {
            throw new Exception('Invalid definition');
        }

        return new static((new StreamReader($binary))->readList($definition), $definition);
    }

    /**
     * @param mixed $value
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function fromValue(mixed $value, null|int|array $definition = null): static {
        if (!is_array($value)) {
            throw new Exception('Invalid value type');
        }

        if ($definition === null) {
            throw new Exception('Invalid definition');
        }

        return new static($value, $definition);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public function getBinary(): string {
        $binary = pack('N', count($this->value));

        if (is_array($this->definition)) {
            $count = count($this->definition);

            if ($count < 1) {
                throw new Exception('invalid type definition');
            } elseif ($count === 1) {
                /** @psalm-suppress PossiblyUndefinedArrayOffset */
                [$valueType] = array_values($this->definition);
            } else {
                $valueType = $this->definition;
            }
        } else {
            $valueType = $this->definition;
        }

        /** @var mixed $val */
        foreach ($this->value as $val) {
            $itemPacked = Type::getBinaryByType($valueType, $val);
            $binary .= pack('N', strlen($itemPacked)) . $itemPacked;
        }

        return $binary;
    }

    /**
     * @return array<mixed>
     */
    public function getValue(): array {
        return $this->value;
    }
}
