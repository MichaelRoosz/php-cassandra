<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\TypeInfo\TypeInfo;

/**
 * Single-precision floating-point number (32-bit precision - use the "Double" type for a PHP-like "float")
 */
final class Float32 extends TypeBase {
    protected readonly float $value;

    final public function __construct(float $value) {
        $this->value = $value;
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {
        /**
         * @var false|array<float> $unpacked
         */
        $unpacked = unpack('G', $binary);

        if ($unpacked === false) {
            throw new Exception('Cannot unpack float32 binary data', Exception::CODE_FLOAT32_UNPACK_FAILED, [
                'binary_length' => strlen($binary),
                'expected_length' => 4,
            ]);
        }

        return new static($unpacked[1]);
    }

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_float($value)) {
            throw new Exception('Invalid float32 value; expected float', Exception::CODE_FLOAT32_INVALID_VALUE_TYPE, [
                'value_type' => gettype($value),
            ]);
        }

        return new static($value);
    }

    #[\Override]
    public function getBinary(): string {
        return pack('G', $this->value);
    }

    #[\Override]
    public function getValue(): float {
        return $this->value;
    }
}
