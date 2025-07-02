<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\TypeInfo\TypeInfo;

final class Boolean extends TypeBase {
    protected bool $value;

    final public function __construct(bool $value) {
        $this->value = $value;
    }

    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {
        return new static($binary !== "\0");
    }

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_bool($value)) {
            throw new Exception('Invalid value');
        }

        return new static($value);
    }

    #[\Override]
    public function getBinary(): string {
        return $this->value ? "\1" : "\0";
    }

    #[\Override]
    public function getValue(): bool {
        return $this->value;
    }
}
