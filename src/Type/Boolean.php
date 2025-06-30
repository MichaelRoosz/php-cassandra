<?php

declare(strict_types=1);

namespace Cassandra\Type;

class Boolean extends TypeBase {
    protected bool $value;

    final public function __construct(bool $value) {
        $this->value = $value;
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     */
    #[\Override]
    public static function fromBinary(string $binary, null|int|array $definition = null): static {
        return new static($binary !== "\0");
    }

    /**
     * @param mixed $value
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromValue(mixed $value, null|int|array $definition = null): static {
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
