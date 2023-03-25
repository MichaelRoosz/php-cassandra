<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\Response\StreamReader;
use Cassandra\Type;

class Tuple extends TypeBase {
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
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Type\Exception
     */
    public static function fromBinary(string $binary, null|int|array $definition = null): static {
        if (!is_array($definition)) {
            throw new Exception('invalid Tuple definition');
        }

        return new static((new StreamReader($binary))->readTuple($definition), $definition);
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
        $binary = '';
        $value = $this->value;

        foreach ($this->definition as $key => $type) {
            if ($value[$key] === null) {
                $binary .= "\xff\xff\xff\xff";
            } else {
                $valueBinary = $value[$key] instanceof TypeBase
                    ? $value[$key]->getBinary()
                    : Type::getBinaryByType($type, $value[$key]);

                $binary .= pack('N', strlen($valueBinary)) . $valueBinary;
            }
        }

        return $binary;
    }

    /**
     * @return array<mixed> $value
     */
    public function getValue(): array {
        return $this->value;
    }
}
