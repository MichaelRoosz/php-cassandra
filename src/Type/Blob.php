<?php

declare(strict_types=1);

namespace Cassandra\Type;

final class Blob extends TypeBase {
    protected string $value;

    final public function __construct(string $value) {
        $this->value = $value;
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     */
    #[\Override]
    public static function fromBinary(string $binary, null|int|array $definition = null): static {
        return new static($binary);
    }

    /**
     * @param mixed $value
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromValue(mixed $value, null|int|array $definition = null): static {
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
