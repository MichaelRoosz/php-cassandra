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
    private readonly int $value;

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    final public function __construct(int|string|DateTimeInterface $value) {
        self::require64Bit();

        if (is_int($value)) {
            $this->value = $value;

        } elseif (is_string($value)) {
            if (!preg_match('/^[+-]?\d{4,}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d{1,6})?)?(?:Z|[+-]\d{2}:?\d{2})?)?$/', $value)) {
                self::throwInvalidTimestamp($value);
            }

            try {
                $date = new DateTimeImmutable($value);
                $parseErrors = DateTimeImmutable::getLastErrors();
                if ($parseErrors !== false && ($parseErrors['warning_count'] > 0 || $parseErrors['error_count'] > 0)) {
                    self::throwInvalidTimestamp($value);
                }
                $milliseconds = self::millisecondsFromDateTime($date);
            } catch (ValueException $e) {
                throw $e;
            } catch (PhpException $e) {
                self::throwInvalidTimestamp($value, $e);
            }

            $this->value = $milliseconds;

        } else {
            $this->value = self::millisecondsFromDateTime($value);
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
        self::assertExactBinaryLength($binary);

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

    /**
     * Convert without allowing integer multiplication to overflow to float.
     *
     * @throws \Cassandra\Exception\ValueException
     */
    private static function millisecondsFromDateTime(DateTimeInterface $value): int {
        $seconds = $value->getTimestamp();
        $milliseconds = (int) $value->format('v');
        $maximumSeconds = intdiv(PHP_INT_MAX, 1000);
        $minimumSeconds = intdiv(PHP_INT_MIN, 1000) - 1;

        if ($seconds > $maximumSeconds || $seconds < $minimumSeconds) {
            self::throwTimestampOutOfRange($value);
        }

        if ($seconds === $minimumSeconds) {
            // Multiplying this second directly would overflow before its
            // positive millisecond fraction brought it back into range.
            $safeBase = ($seconds + 1) * 1000;
            $minimumMilliseconds = PHP_INT_MIN - $safeBase + 1000;
            if ($milliseconds < $minimumMilliseconds) {
                self::throwTimestampOutOfRange($value);
            }

            return $safeBase + ($milliseconds - 1000);
        }

        $result = $seconds * 1000;
        if ($seconds === $maximumSeconds && $milliseconds > PHP_INT_MAX - $result) {
            self::throwTimestampOutOfRange($value);
        }

        return $result + $milliseconds;
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    private static function throwInvalidTimestamp(string|DateTimeInterface $value, ?PhpException $previous = null): never {
        throw new ValueException(
            'Invalid timestamp value; expected milliseconds as int, date in ISO 8601 format as string, or DateTimeInterface',
            ExceptionCode::VALUE_TIMESTAMP_INVALID_VALUE_TYPE->value,
            [
                'value_type' => get_debug_type($value),
                'expected_types' => ['int', 'string', DateTimeInterface::class],
            ],
            $previous,
        );
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    private static function throwTimestampOutOfRange(DateTimeInterface $value): never {
        throw new ValueException(
            'Timestamp value is outside the range representable as integer milliseconds',
            ExceptionCode::VALUE_TIMESTAMP_OUT_OF_RANGE->value,
            [
                'seconds' => $value->getTimestamp(),
                'milliseconds_fraction' => (int) $value->format('v'),
                'minimum_milliseconds' => PHP_INT_MIN,
                'maximum_milliseconds' => PHP_INT_MAX,
            ]
        );
    }
}
