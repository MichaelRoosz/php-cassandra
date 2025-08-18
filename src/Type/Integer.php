<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\ExceptionCode;
use Cassandra\TypeInfo\TypeInfo;

class Integer extends TypeBase {
    final public const VALUE_MAX = 2147483647;
    final public const VALUE_MIN = -2147483648;

    protected readonly int $value;

    /**
     * @throws \Cassandra\Type\Exception
     */
    final public function __construct(int $value) {
        if ($value > self::VALUE_MAX || $value < self::VALUE_MIN) {
            throw new Exception('Integer value is outside of supported range', ExceptionCode::TYPE_INTEGER_OUT_OF_RANGE->value, [
                'value' => $value,
                'min' => self::VALUE_MIN,
                'max' => self::VALUE_MAX,
            ]);
        }

        $this->value = $value;
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {
        $bits = PHP_INT_SIZE * 8 - 32;

        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('N', $binary);
        if ($unpacked === false) {
            throw new Exception('Cannot unpack integer binary data', ExceptionCode::TYPE_INTEGER_UNPACK_FAILED->value, [
                'binary_length' => strlen($binary),
                'expected_length' => 4,
            ]);
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
            throw new Exception('Invalid integer value; expected int', ExceptionCode::TYPE_INTEGER_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
                'range' => [self::VALUE_MIN, self::VALUE_MAX],
            ]);
        }

        return new static($value);
    }

    #[\Override]
    public function getBinary(): string {
        return pack('N', $this->value);
    }

    #[\Override]
    public function getValue(): int {
        return $this->value;
    }
}
