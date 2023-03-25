<?php

declare(strict_types=1);

namespace Cassandra\Type;

class Varchar extends TypeBase {
    protected string $value;

    public final function __construct(string $value) {
        $this->value = $value;
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     */
    public static function fromBinary(string $binary, null|int|array $definition = null): static {
        return new static($binary);
    }

    /**
     * @param mixed $value
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function fromValue(mixed $value, null|int|array $definition = null): static {
        if (!is_string($value)) {
            throw new Exception('Invalid value');
        }

        return new static($value);
    }

    public function getBinary(): string {
        return $this->value;
    }

    public function getValue(): string {
        return $this->value;
    }
}
