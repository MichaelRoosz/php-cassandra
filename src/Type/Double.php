<?php

declare(strict_types=1);

namespace Cassandra\Type;

/**
 * Double-precision floating-point number (same as a PHP "float", 64-bit precision)
 */
class Double extends TypeBase {
    protected float $value;

    final public function __construct(float $value) {
        $this->value = $value;
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function fromBinary(string $binary, null|int|array $definition = null): static {
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
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function fromValue(mixed $value, null|int|array $definition = null): static {
        if (!is_float($value)) {
            throw new Exception('Invalid value');
        }

        return new static($value);
    }

    public function getBinary(): string {
        return pack('E', $this->value);
    }

    public function getValue(): float {
        return $this->value;
    }
}
