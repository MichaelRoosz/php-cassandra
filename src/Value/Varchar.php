<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\ExceptionCode;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;

class Varchar extends ValueReadableWithLength {
    protected readonly string $value;

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
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_string($value)) {
            throw new Exception('Invalid varchar value; expected string', ExceptionCode::TYPE_VARCHAR_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        return new static($value);
    }

    final public static function fromValue(string $value): static {
        return new static($value);
    }

    #[\Override]
    public function getBinary(): string {
        return $this->value;
    }

    #[\Override]
    public function getType(): Type {
        return Type::VARCHAR;
    }

    #[\Override]
    public function getValue(): string {
        return $this->value;
    }

    #[\Override]
    final public static function requiresDefinition(): bool {
        return false;
    }
}
