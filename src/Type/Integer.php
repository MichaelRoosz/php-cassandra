<?php

declare(strict_types=1);

namespace Cassandra\Type;

class Integer extends TypeBase {
    final public const VALUE_MAX = 2147483647;
    final public const VALUE_MIN = -2147483648;

    protected int $value;

    /**
     * @throws \Cassandra\Type\Exception
     */
    final public function __construct(int $value) {
        if ($value > self::VALUE_MAX || $value < self::VALUE_MIN) {
            throw new Exception('Value "' . $value . '" is outside of possible range');
        }

        $this->value = $value;
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function fromBinary(string $binary, null|int|array $definition = null): static {
        $bits = PHP_INT_SIZE * 8 - 32;

        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('N', $binary);
        if ($unpacked === false) {
            throw new Exception('Cannot unpack binary.');
        }

        return new static($unpacked[1] << $bits >> $bits);
    }

    /**
     * @param mixed $value
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function fromValue(mixed $value, null|int|array $definition = null): static {
        if (!is_int($value)) {
            throw new Exception('Invalid value');
        }

        return new static($value);
    }

    public function getBinary(): string {
        return pack('N', $this->value);
    }

    public function getValue(): int {
        return $this->value;
    }
}
