<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\ExceptionCode;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;

class Integer extends TypeWithFixedLength {
    final public const VALUE_MAX = 2147483647;
    final public const VALUE_MIN = -2147483648;
    final protected const SIGNED_INT_SHIFT_BIT_SIZE = (PHP_INT_SIZE * 8) - 32;

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

    #[\Override]
    final public static function fixedLength(): int {
        return 4;
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {

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

        return new static(
            $unpacked[1]
            << self::SIGNED_INT_SHIFT_BIT_SIZE
            >> self::SIGNED_INT_SHIFT_BIT_SIZE
        );
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

    /**
     * @throws \Cassandra\Type\Exception
     */
    final public static function fromValue(int $value): static {
        return new static($value);
    }

    #[\Override]
    public function getBinary(): string {
        return pack('N', $this->value);
    }

    #[\Override]
    public function getType(): Type {
        return Type::INT;
    }

    #[\Override]
    public function getValue(): int {
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
