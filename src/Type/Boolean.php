<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\ExceptionCode;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;

final class Boolean extends TypeBase {
    protected readonly bool $value;

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
            throw new Exception('Invalid boolean value; expected bool', ExceptionCode::TYPE_BOOLEAN_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

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
}
