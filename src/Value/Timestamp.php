<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;
use Cassandra\Value\EncodeOption\TimestampEncodeOption;
use DateTimeImmutable;
use DateTimeInterface;
use Exception as PhpException;

final class Timestamp extends ValueWithFixedLength implements ValueWithMultipleEncodings {
    protected readonly int $value;

    /**
     * @throws \Cassandra\Exception\ValueException
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
                throw new ValueException('Invalid timestamp value; expected milliseconds as int, date in format YYYY-mm-dd HH:ii:ss.uuu as string, or DateTimeInterface', ExceptionCode::VALUE_TIMESTAMP_INVALID_VALUE_TYPE->value, [
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
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public function asConfigured(ValueEncodeConfig $valueEncodeConfig): mixed {
        return match ($valueEncodeConfig->timestampEncodeOption) {
            TimestampEncodeOption::AS_DATETIME_IMMUTABLE => $this->asDateTime(),
            TimestampEncodeOption::AS_INT => $this->asInteger(),
            TimestampEncodeOption::AS_STRING => $this->asString(),
        };
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    public function asDateTime(): DateTimeImmutable {
        $seconds = intdiv($this->value, 1000);
        $microseconds = ($this->value % 1000) * 1000;

        try {
            $datetime = new DateTimeImmutable('@' . $seconds);
            $datetime = $datetime->modify('+' . $microseconds . ' microseconds');
        } catch (PhpException $e) {
            throw new ValueException('Cannot convert timestamp to DateTimeImmutable', ExceptionCode::VALUE_TIMESTAMP_TO_DATETIME_FAILED->value, [
                'milliseconds' => $this->value,
            ], $e);
        }

        if ($datetime === false) {
            throw new ValueException('Cannot convert timestamp to DateTimeImmutable', ExceptionCode::VALUE_TIMESTAMP_TO_DATETIME_FAILED->value, [
                'milliseconds' => $this->value,
            ]);
        }

        return $datetime;
    }

    public function asInteger(): int {
        return $this->value;
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    public function asString(): string {
        return $this->getValue();
    }

    #[\Override]
    final public static function fixedLength(): int {
        return 8;
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public static function fromBinary(
        string $binary,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): static {
        self::require64Bit();

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
     * @param mixed $value
     * 
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        self::require64Bit();

        if (!is_int($value) && !is_string($value) && !($value instanceof DateTimeInterface)) {
            throw new ValueException('Invalid timestamp value; expected milliseconds as int, date in format YYYY-mm-dd HH:ii:ss.uuu as string, or DateTimeInterface', ExceptionCode::VALUE_TIMESTAMP_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
                'expected_types' => ['int', 'string', DateTimeInterface::class],
            ]);
        }

        return new static($value);
    }

    /**
     * @throws \Cassandra\Exception\ValueException
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
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public function getValue(): string {
        return $this->asDateTime()->format('Y-m-d H:i:s.vO');
    }

    #[\Override]
    final public static function isSerializedAsFixedLength(): bool {
        return true;
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    public static function now(): static {
        return new static(new DateTimeImmutable());
    }

    #[\Override]
    final public static function requiresDefinition(): bool {
        return false;
    }
}
