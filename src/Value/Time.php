<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;
use Cassandra\Value\EncodeOption\TimeEncodeOption;
use DateTimeImmutable;
use DateTimeInterface;
use Exception as PhpException;

final class Time extends ValueWithFixedLength implements ValueWithMultipleEncodings {
    final public const VALUE_MAX = 86399999999999;

    protected readonly int $value;

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    final public function __construct(int|string|DateTimeInterface $value) {

        self::require64Bit();

        if (is_int($value)) {
            if ($value > self::VALUE_MAX || $value < 0) {
                throw new ValueException('Time value is outside of supported range', ExceptionCode::VALUE_TIME_OUT_OF_RANGE->value, [
                    'value' => $value,
                    'min' => 0,
                    'max' => self::VALUE_MAX,
                ]);
            }

            $this->value = $value;

        } elseif (is_string($value)) {

            if (!preg_match('/^(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?$/', $value, $matches)) {
                throw new ValueException('Invalid time string; expected HH:MM:SS(.fffffffff)', ExceptionCode::VALUE_TIME_INVALID_STRING->value, [
                    'value' => $value,
                ]);
            }

            $hours = (int) $matches[1];
            $minutes = (int) $matches[2];
            $seconds = (int) $matches[3];
            $nanoseconds = isset($matches[4]) ? (int) str_pad($matches[4], 9, '0') : 0;

            if (
                $hours < 0 || $hours >= 24
                || $minutes < 0 || $minutes >= 60
                || $seconds < 0 || $seconds >= 60
            ) {
                throw new ValueException('Invalid time format; expected HH:MM:SS(.fffffffff) within 24h range', ExceptionCode::VALUE_TIME_INVALID_FORMAT->value, [
                    'value' => $value,
                    'hours' => $hours,
                    'minutes' => $minutes,
                    'seconds' => $seconds,
                    'nanoseconds' => $nanoseconds,
                ]);
            }

            $totalNanoseconds = (
                ($hours * 3600000000000) +
                ($minutes * 60000000000) +
                ($seconds * 1000000000) +
                $nanoseconds
            );

            $this->value = $totalNanoseconds;

        } else {
            $hoursInNanoseconds = (int) $value->format('H') * 3600000000000;
            $minutesInNanoseconds = (int) $value->format('i') * 60000000000;
            $secondsInNanoseconds = (int) $value->format('s') * 1000000000;
            $microsecondsInNanoseconds = (int) $value->format('u') * 1000;

            $totalNanoseconds = $hoursInNanoseconds + $minutesInNanoseconds + $secondsInNanoseconds + $microsecondsInNanoseconds;

            $this->value = $totalNanoseconds;
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
        return match ($valueEncodeConfig->timeEncodeOption) {
            TimeEncodeOption::AS_DATETIME_IMMUTABLE => $this->asDateTime(),
            TimeEncodeOption::AS_INT => $this->asInteger(),
            TimeEncodeOption::AS_STRING => $this->asString(),
        };
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    public function asDateTime(): DateTimeImmutable {

        try {
            return new DateTimeImmutable('1970-01-01 ' . $this->asString());
        } catch (PhpException $e) {
            throw new ValueException('Invalid time value; cannot create DateTimeImmutable', ExceptionCode::VALUE_TIME_INVALID_DATETIME_STRING->value, [
                'value' => $this->value,
                'note' => 'This may happen if the time is out of range for DateTimeImmutable',
                'error' => $e->getMessage(),
            ], $e);
        }
    }

    public function asInteger(): int {
        return $this->value;
    }

    public function asString(): string {
        $seconds = intdiv($this->value, 1000000000);
        $remaining_nanoseconds = $this->value % 1000000000;

        $hours = intdiv($seconds, 3600);
        $remaining_seconds = $seconds % 3600;

        $minutes = intdiv($remaining_seconds, 60);
        $remaining_seconds %= 60;

        $formatted_time = sprintf('%02d:%02d:%02d.%09d', $hours, $minutes, $remaining_seconds, $remaining_nanoseconds);

        return $formatted_time;
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
            throw new ValueException('Invalid time value; expected nanoseconds as int, time in format HH:MM:SS(.fffffffff) as string, or DateTimeInterface', ExceptionCode::VALUE_TIME_INVALID_VALUE_TYPE->value, [
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
        return Type::TIME;
    }

    #[\Override]
    public function getValue(): string {
        return $this->asString();
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
