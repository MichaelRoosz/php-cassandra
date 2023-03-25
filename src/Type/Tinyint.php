<?php

declare(strict_types=1);

namespace Cassandra\Type;

class Tinyint extends TypeBase {
    public final const VALUE_MAX = 127;
    public final const VALUE_MIN = -128;

    protected int $value;

    /**
     * @throws \Cassandra\Type\Exception
     */
    public final function __construct(int $value) {
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
        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('c', $binary);
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
        if (!is_int($value)) {
            throw new Exception('Invalid value');
        }

        return new static($value);
    }

    public function getBinary(): string {
        return pack('c', $this->value);
    }

    public function getValue(): int {
        return $this->value;
    }
}
