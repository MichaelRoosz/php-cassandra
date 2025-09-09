<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;

final class Blob extends ValueReadableWithLength {
    protected readonly string $value;

    final public function __construct(string $value) {
        $this->value = $value;
    }

    #[\Override]
    public static function fromBinary(
        string $binary,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): static {
        return new static($binary);
    }

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_string($value)) {
            throw new ValueException('Invalid blob value; expected string', ExceptionCode::VALUE_BLOB_INVALID_VALUE_TYPE->value, [
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
        return Type::BLOB;
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
