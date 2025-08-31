<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\ExceptionCode;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;

final class Tinyint extends TypeWithFixedLength {
    final public const VALUE_MAX = 127;
    final public const VALUE_MIN = -128;

    protected readonly int $value;

    /**
     * @throws \Cassandra\Type\Exception
     */
    final public function __construct(int $value) {
        if ($value > self::VALUE_MAX || $value < self::VALUE_MIN) {
            throw new Exception('Tinyint value is outside of supported range', ExceptionCode::TYPE_TINYINT_OUT_OF_RANGE->value, [
                'value' => $value,
                'min' => self::VALUE_MIN,
                'max' => self::VALUE_MAX,
            ]);
        }

        $this->value = $value;
    }

    #[\Override]
    final public static function fixedLength(): int {
        return 1;
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {
        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('c', $binary);
        if ($unpacked === false) {
            throw new Exception('Cannot unpack tinyint binary data', ExceptionCode::TYPE_TINYINT_UNPACK_FAILED->value, [
                'binary_length' => strlen($binary),
                'expected_length' => 1,
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
        if (!is_int($value)) {
            throw new Exception('Invalid tinyint value; expected int', ExceptionCode::TYPE_TINYINT_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        return new static($value);
    }

    #[\Override]
    public function getBinary(): string {
        return pack('c', $this->value);
    }

    #[\Override]
    public function getType(): Type {
        return Type::TINYINT;
    }

    #[\Override]
    public function getValue(): int {
        return $this->value;
    }

    #[\Override]
    final public static function isSerializedAsFixedLength(): bool {

        // note: logically tinyint is fixed length,
        // but in cassandra it is defined as variable length
        return false;
    }

    #[\Override]
    final public static function requiresDefinition(): bool {
        return false;
    }
}
