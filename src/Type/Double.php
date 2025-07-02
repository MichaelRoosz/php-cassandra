<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\TypeInfo\TypeInfo;

/**
 * Double-precision floating-point number (same as a PHP "float", 64-bit precision)
 */
final class Double extends TypeBase {
    protected float $value;

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
            throw new Exception('Cannot unpack binary');
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
            throw new Exception('Invalid value');
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
