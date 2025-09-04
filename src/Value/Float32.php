<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\ExceptionCode;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;

/**
 * Single-precision floating-point number (32-bit precision - use the "Double" type for a PHP-like "float")
 */
final class Float32 extends ValueWithFixedLength {
    protected readonly float $value;

    final public function __construct(float $value) {
        $this->value = $value;
    }

    #[\Override]
    final public static function fixedLength(): int {
        return 4;
    }

    /**
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {
        /**
         * @var false|array<float> $unpacked
         */
        $unpacked = unpack('G', $binary);

        if ($unpacked === false) {
            throw new Exception('Cannot unpack float32 binary data', ExceptionCode::TYPE_FLOAT32_UNPACK_FAILED->value, [
                'binary_length' => strlen($binary),
                'expected_length' => 4,
            ]);
        }

        return new static($unpacked[1]);
    }

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_numeric($value)) {
            throw new Exception('Invalid float32 value; expected numeric', ExceptionCode::TYPE_FLOAT32_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        return new static((float) $value);
    }

    final public static function fromValue(float $value): static {
        return new static($value);
    }

    #[\Override]
    public function getBinary(): string {
        return pack('G', $this->value);
    }

    #[\Override]
    public function getType(): Type {
        return Type::FLOAT;
    }

    #[\Override]
    public function getValue(): float {
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
