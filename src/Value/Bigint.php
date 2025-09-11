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

        $highBytes = substr($binary, 0, 4);
        if ($highBytes === "\x00\x00\x00\x00") {
            $isPositiveRange = true;
        } elseif ($highBytes === "\xFF\xFF\xFF\xFF") {
            $isPositiveRange = false;
        } else {
            throw new ValueException('Bigint value out of 32-bit integer range, 64-bit php is required.', ExceptionCode::VALUE_BIGINT_UNPACK_FAILED->value);
        }

        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('N', $binary, 4);
        if ($unpacked === false) {
            throw new ValueException('Cannot unpack bigint binary data', ExceptionCode::VALUE_BIGINT_UNPACK_FAILED->value, [
                'binary_length' => strlen($binary),
                'expected_length' => 8,
            ]);
        }

        $value = $unpacked[1];

        if ($isPositiveRange) {
            if (($value & 0x80000000) !== 0) {
                throw new ValueException('Bigint value out of 32-bit integer range, 64-bit php is required.', ExceptionCode::VALUE_BIGINT_UNPACK_FAILED->value);
            }
        } else {
            if (($value & 0x80000000) === 0) {
                throw new ValueException('Bigint value out of 32-bit integer range, 64-bit php is required.', ExceptionCode::VALUE_BIGINT_UNPACK_FAILED->value);
            }
        }

        return new static($value);
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
        }

        $highBytes = $this->value < 0 ? "\xFF\xFF\xFF\xFF" : "\x00\x00\x00\x00";
        $lowBytes = pack('N', $this->value);

        return $highBytes . $lowBytes;
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
