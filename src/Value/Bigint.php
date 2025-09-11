<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;

class Bigint extends ValueWithFixedLength {
    protected readonly int $value;

    final public function __construct(int $value) {
        $this->value = $value;
    }

    #[\Override]
    final public static function fixedLength(): int {
        return 8;
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    final public static function fromBinary(
        string $binary,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): static {

        if (PHP_INT_SIZE >= 8) {
            /**
             * @var false|array<int> $unpacked
             */
            $unpacked = unpack('J', $binary);
            if ($unpacked === false) {
                throw new ValueException('Cannot unpack bigint binary data', ExceptionCode::VALUE_BIGINT_UNPACK_FAILED->value, [
                    'binary_length' => strlen($binary),
                    'expected_length' => 8,
                ]);
            }

            return new static($unpacked[1]);

        }

        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('N2', $binary);
        if ($unpacked === false) {
            throw new ValueException('Cannot unpack bigint binary data', ExceptionCode::VALUE_BIGINT_UNPACK_FAILED->value, [
                'binary_length' => strlen($binary),
                'expected_length' => 8,
            ]);
        }

        $sign = $unpacked[1] & 0x80000000;
        $value = $unpacked[2] & 0x7FFFFFFF;

        $restOfSign = $unpacked[1] & 0x7FFFFFFF;
        $restOfValue = $unpacked[2] & 0x80000000;

        if ($restOfSign !== 0 || $restOfValue !== 0) {
            throw new ValueException('Bigint value out of 32-bit integer range, 64-bit php is required.', ExceptionCode::VALUE_BIGINT_UNPACK_FAILED->value);
        }

        return new static($sign | $value);
    }

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    final public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_int($value)) {
            throw new ValueException('Invalid bigint value; expected int', ExceptionCode::VALUE_BIGINT_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        return new static($value);
    }

    final public static function fromValue(int $value): static {
        return new static($value);
    }

    #[\Override]
    final public function getBinary(): string {

        if (PHP_INT_SIZE >= 8) {
            return pack('J', $this->value);
        } else {

            $sign = $this->value & 0x80000000;
            $value = $this->value & 0x7FFFFFFF;

            return pack('N2', $sign, $value);
        }
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
}
