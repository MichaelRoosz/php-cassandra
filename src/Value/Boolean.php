<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\ExceptionCode;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;

final class Boolean extends ValueWithFixedLength {
    protected readonly bool $value;

    final public function __construct(bool $value) {
        $this->value = $value;
    }

    #[\Override]
    final public static function fixedLength(): int {
        return 1;
    }

    #[\Override]
    public static function fromBinary(
        string $binary,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): static {
        return new static($binary !== "\0");
    }

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_bool($value)) {
            throw new Exception('Invalid boolean value; expected bool', ExceptionCode::VALUE_BOOLEAN_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        return new static($value);
    }

    final public static function fromValue(bool $value): static {
        return new static($value);
    }

    #[\Override]
    public function getBinary(): string {
        return $this->value ? "\1" : "\0";
    }

    #[\Override]
    public function getType(): Type {
        return Type::BOOLEAN;
    }

    #[\Override]
    public function getValue(): bool {
        return $this->value;
    }

    #[\Override]
    final public static function isSerializedAsFixedLength(): bool {
        return true;
    }

    #[\Override]
    final public static function requiresDefinition(): bool {
        return false;
    }
}
