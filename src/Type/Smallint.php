<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\ExceptionCode;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;

final class Smallint extends TypeWithFixedLength {
    final public const VALUE_MAX = 32767;
    final public const VALUE_MIN = -32768;

    protected readonly int $value;

    /**
     * @throws \Cassandra\Type\Exception
     */
    final public function __construct(int $value) {
        if ($value > self::VALUE_MAX || $value < self::VALUE_MIN) {
            throw new Exception('Smallint value is outside of supported range', ExceptionCode::TYPE_SMALLINT_OUT_OF_RANGE->value, [
                'value' => $value,
                'min' => self::VALUE_MIN,
                'max' => self::VALUE_MAX,
            ]);
        }

        $this->value = $value;
    }

    #[\Override]
    final public static function fixedLength(): int {
        return 2;
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
            throw new Exception('Cannot unpack smallint binary data', ExceptionCode::TYPE_SMALLINT_UNPACK_FAILED->value, [
                'binary_length' => strlen($binary),
                'expected_length' => 2,
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
            throw new Exception('Invalid smallint value; expected int', ExceptionCode::TYPE_SMALLINT_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
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
        return pack('n', $this->value);
    }

    #[\Override]
    public function getType(): Type {
        return Type::SMALLINT;
    }

    #[\Override]
    public function getValue(): int {
        return $this->value;
    }

    #[\Override]
    final public static function isSerializedAsFixedLength(): bool {

        // note: logically smallint is fixed length,
        // but in cassandra it is defined as variable length
        return false;
    }

    #[\Override]
    final public static function requiresDefinition(): bool {
        return false;
    }
}
