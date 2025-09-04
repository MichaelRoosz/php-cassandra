<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\ExceptionCode;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;
use DateTimeImmutable;
use DateTimeInterface;
use Exception as PhpException;

final class Timestamp extends ValueWithFixedLength {
    protected readonly int $value;

    /**
     * @throws \Cassandra\Value\Exception
     */
    final public function __construct(int|string|DateTimeInterface $value) {
        self::require64Bit();

        if (is_int($value)) {
            $this->value = $value;

        } elseif (is_string($value)) {
            try {
                $date = new DateTimeImmutable($value);
                $timestamp = $date->getTimestamp();
                $milliseconds = ($timestamp * 1000) + (int) $date->format('v');
            } catch (PhpException $e) {
                throw new Exception('Invalid timestamp value; expected milliseconds as int, date in format YYYY-mm-dd HH:ii:ss.uuu as string, or DateTimeInterface', ExceptionCode::TYPE_TIMESTAMP_INVALID_VALUE_TYPE->value, [
                    'value_type' => gettype($value),
                    'expected_types' => ['int', 'string', DateTimeInterface::class],
                ], $e);
            }

            $this->value = $milliseconds;

        } else {
            $timestamp = $value->getTimestamp();
            $milliseconds = ($timestamp * 1000) + (int) $value->format('v');

            $this->value = $milliseconds;
        }
    }

    public function __toString(): string {
        return $this->asString();
    }

    /**
     * @throws \Cassandra\Value\Exception
     */
    public function asDateTime(): DateTimeImmutable {
        $seconds = intdiv($this->value, 1000);
        $microseconds = ($this->value % 1000) * 1000;

        try {
            $datetime = new DateTimeImmutable('@' . $seconds);
            $datetime = $datetime->modify('+' . $microseconds . ' microseconds');
        } catch (PhpException $e) {
            throw new Exception('Cannot convert timestamp to DateTimeImmutable', ExceptionCode::TYPE_TIMESTAMP_TO_DATETIME_FAILED->value, [
                'milliseconds' => $this->value,
            ], $e);
        }

        if ($datetime === false) {
            throw new Exception('Cannot convert timestamp to DateTimeImmutable', ExceptionCode::TYPE_TIMESTAMP_TO_DATETIME_FAILED->value, [
                'milliseconds' => $this->value,
            ]);
        }

        return $datetime;
    }

    public function asInteger(): int {
        return $this->value;
    }

    /**
     * @throws \Cassandra\Value\Exception
     */
    public function asString(): string {
        return $this->getValue();
    }

    #[\Override]
    final public static function fixedLength(): int {
        return 8;
    }

    /**
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {
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
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        self::require64Bit();

        if (!is_int($value) && !is_string($value) && !($value instanceof DateTimeInterface)) {
            throw new Exception('Invalid timestamp value; expected milliseconds as int, date in format YYYY-mm-dd HH:ii:ss.uuu as string, or DateTimeInterface', ExceptionCode::TYPE_TIMESTAMP_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
                'expected_types' => ['int', 'string', DateTimeInterface::class],
            ]);
        }

        return new static($value);
    }

    /**
     * @throws \Cassandra\Value\Exception
     */
    final public static function fromValue(int|string|DateTimeInterface $value): static {
        return new static($value);
    }

    #[\Override]
    public function getBinary(): string {
        return pack('J', $this->value);
    }

    #[\Override]
    public function getType(): Type {
        return Type::TIMESTAMP;
    }

    /**
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    public function getValue(): string {
        return $this->asDateTime()->format('Y-m-d H:i:s.vO');
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
    protected static function require64Bit(): void {
        if (PHP_INT_SIZE < 8) {
            $className = self::class;

            throw new Exception('The ' . $className . ' data type requires a 64-bit system', ExceptionCode::TYPE_TIMESTAMP_64BIT_REQUIRED->value, [
                'class' => $className,
                'php_int_size_bytes' => PHP_INT_SIZE,
                'php_int_size_bits' => PHP_INT_SIZE * 8,
            ]);
        }
    }
}
