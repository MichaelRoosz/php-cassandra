<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\ExceptionCode;
use Cassandra\TypeInfo\TypeInfo;

/**
 * Double-precision floating-point number (same as a PHP "float", 64-bit precision)
 */
class Double extends TypeBase {
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
        $unpacked = unpack('E', $binary);

        if ($unpacked === false) {
            throw new Exception('Cannot unpack double binary data', ExceptionCode::TYPE_DOUBLE_UNPACK_FAILED->value, [
                'binary_length' => strlen($binary),
                'expected_length' => 8,
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
            throw new Exception('Invalid double value; expected float', ExceptionCode::TYPE_DOUBLE_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        return new static($value);
    }

    #[\Override]
    public function getBinary(): string {
        return pack('E', $this->value);
    }

    #[\Override]
    public function getValue(): float {
        return $this->value;
    }
}
