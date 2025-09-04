<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\ExceptionCode;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;
use ReflectionClass;

class Bigint extends ValueWithFixedLength {
    protected readonly int $value;

    /**
     * @throws \Cassandra\Value\Exception
     */
    final public function __construct(int $value) {
        self::require64Bit();

        $this->value = $value;
    }

    #[\Override]
    final public static function fixedLength(): int {
        return 8;
    }

    /**
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    final public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {
        self::require64Bit();

        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('J', $binary);
        if ($unpacked === false) {
            throw new Exception('Cannot unpack bigint binary data', ExceptionCode::TYPE_BIGINT_UNPACK_FAILED->value, [
                'binary_length' => strlen($binary),
                'expected_length' => 8,
            ]);
        }

        return new static($unpacked[1]);
    }

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    final public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        self::require64Bit();

        if (!is_int($value)) {
            throw new Exception('Invalid bigint value; expected int', ExceptionCode::TYPE_BIGINT_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        return new static($value);
    }

    /**
     * @throws \Cassandra\Value\Exception
     */
    final public static function fromValue(int $value): static {
        return new static($value);
    }

    #[\Override]
    final public function getBinary(): string {
        return pack('J', $this->value);
    }

    #[\Override]
    public function getType(): Type {
        return Type::BIGINT;
    }

    #[\Override]
    final public function getValue(): int {
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

    /**
     * @throws \Cassandra\Value\Exception
     */
    final protected static function require64Bit(): void {
        if (PHP_INT_SIZE < 8) {
            $className = (new ReflectionClass(static::class))->getShortName();

            throw new Exception('The ' . $className . ' data type requires a 64-bit system', ExceptionCode::TYPE_BIGINT_64BIT_REQUIRED->value, [
                'class' => $className,
                'php_int_size_bytes' => PHP_INT_SIZE,
                'php_int_size_bits' => PHP_INT_SIZE * 8,
            ]);
        }
    }
}
