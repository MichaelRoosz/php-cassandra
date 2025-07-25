<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\TypeInfo\TypeInfo;

final class Inet extends TypeBase {
    protected string $value;

    final public function __construct(string $value) {
        $this->value = $value;
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {
        $inet = inet_ntop($binary);

        if ($inet === false) {
            throw new Exception('Cannot convert value to string.');
        }

        return new static($inet);
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

    /**
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public function getBinary(): string {
        $binary = inet_pton($this->value);

        if ($binary === false) {
            throw new Exception('Cannot convert value to binary.');
        }

        return $binary;
    }

    #[\Override]
    public function getValue(): string {
        return $this->value;
    }
}
