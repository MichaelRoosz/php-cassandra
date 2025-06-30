<?php

declare(strict_types=1);

namespace Cassandra\Type;

/**
 * Single-precision floating-point number (32-bit precision - use the "Double" type for a PHP-like "float")
 */
class Float32 extends TypeBase {
    protected float $value;

    final public function __construct(float $value) {
        $this->value = $value;
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromBinary(string $binary, null|int|array $definition = null): static {
        /**
         * @var false|array<float> $unpacked
         */
        $unpacked = unpack('G', $binary);

        if ($unpacked === false) {
            throw new Exception('Cannot unpack binary.');
        }

        return new static($unpacked[1]);
    }

    /**
     * @param mixed $value
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromValue(mixed $value, null|int|array $definition = null): static {
        if (!is_float($value)) {
            throw new Exception('Invalid value');
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
