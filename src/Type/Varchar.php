<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\TypeInfo\TypeInfo;

class Varchar extends TypeBase {
    protected string $value;

    final public function __construct(string $value) {
        $this->value = $value;
    }

    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {
        return new static($binary);
    }

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_string($value)) {
            throw new Exception('Invalid value');
        }

        return new static($value);
    }

    #[\Override]
    public function getBinary(): string {
        return $this->value;
    }

    #[\Override]
    public function getValue(): string {
        return $this->value;
    }
}
