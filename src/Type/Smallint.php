<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\TypeInfo\TypeInfo;

final class Smallint extends TypeBase {
    final public const VALUE_MAX = 32767;
    final public const VALUE_MIN = -32768;

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
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {
        $bits = PHP_INT_SIZE * 8 - 16;

        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('n', $binary);
        if ($unpacked === false) {
            throw new Exception('Cannot unpack binary');
        }

        return new static($unpacked[1] << $bits >> $bits);
    }

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_int($value)) {
            throw new Exception('Invalid value');
        }

        return new static($value);
    }

    #[\Override]
    public function getBinary(): string {
        return pack('n', $this->value);
    }

    #[\Override]
    public function getValue(): int {
        return $this->value;
    }
}
