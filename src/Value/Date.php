<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;
use Cassandra\Value\EncodeOption\DateEncodeOption;
use DateInterval;
use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;
use Exception as PhpException;

final class Date extends ValueWithFixedLength implements ValueWithMultipleEncodings {
    final public const VALUE_INT_MAX = 4_294_967_295;
    final public const VALUE_INT_MIN = 0;

    private const VALUE_2_31 = 2_147_483_648;
    private const VALUE_INT_0000_01_01 = 2_146_764_120;
    private const VALUE_INT_9999_12_31 = 2_150_416_544;

    private readonly int $value;

    /**
     * @param int|string|DateTimeInterface $value An integer is the raw `date`
     *   wire value: a 32-bit unsigned integer with the epoch (1970-01-01) at the
     *   center of the range (2^31), so 2^31 is 1970-01-01, 2^31 + 1 is
     *   1970-01-02, and so on. A string must be in `YYYY-mm-dd` format.
     * @throws \Cassandra\Exception\ValueException
     */
    final public function __construct(int|string|DateTimeInterface $value) {

        self::require64Bit();

        if (is_int($value)) {
            if ($value > self::VALUE_INT_MAX || $value < self::VALUE_INT_MIN) {
                throw new ValueException('Unsigned integer value is outside of supported range', ExceptionCode::VALUE_INTEGER_OUT_OF_RANGE->value, [
                    'value' => $value,
                    'min' => self::VALUE_INT_MIN,
                    'max' => self::VALUE_INT_MAX,
                ]);
            }

            $this->value = $value;

        } elseif (is_string($value)) {

            if (!preg_match('/^[+-]?\d{4,}-\d{2}-\d{2}$/', $value)) {
                throw new ValueException('Invalid date string format; expected "YYYY-MM-DD"', ExceptionCode::VALUE_DATE_INVALID_STRING_FORMAT->value, [
                    'value' => $value,
                ]);
            }

            $firstChar = substr($value, 0, 1);
            if ($firstChar !== '+' && $firstChar !== '-') {
                $value = '+' . $value; // Ensure the date string has a sign
            }

            try {
                $valueAsDate = new DateTimeImmutable($value, new DateTimeZone('UTC'));
                $parseErrors = DateTimeImmutable::getLastErrors();
                if ($parseErrors !== false && ($parseErrors['warning_count'] > 0 || $parseErrors['error_count'] > 0)) {
                    throw new ValueException('Invalid date string format; expected "YYYY-MM-DD"', ExceptionCode::VALUE_DATE_INVALID_STRING_FORMAT->value, [
                        'value' => $value,
                        'note' => 'The date contains invalid calendar fields',
                    ]);
                }
            } catch (ValueException $e) {
                throw $e;
            } catch (PhpException $e) {
                throw new ValueException('Invalid date string format; expected "YYYY-MM-DD"', ExceptionCode::VALUE_DATE_INVALID_STRING_FORMAT->value, [
                    'value' => $value,
                    'note' => 'This may happen if the date is out of range for DateTimeImmutable',
                ], $e);
            }

            $valueAsInt = self::VALUE_2_31 + self::daysSinceEpoch($valueAsDate);

            if ($valueAsInt > self::VALUE_INT_MAX || $valueAsInt < self::VALUE_INT_MIN) {
                throw new ValueException('Unsigned integer value is outside of supported range', ExceptionCode::VALUE_INTEGER_OUT_OF_RANGE->value, [
                    'value' => $valueAsInt,
                    'min' => self::VALUE_INT_MIN,
                    'max' => self::VALUE_INT_MAX,
                ]);
            }

            $this->value = $valueAsInt;

        } else { // DateTimeInterface

            // Anchor both ends at UTC midnight so the day count is exact and
            // independent of the ambient timezone. The value's own calendar date
            // (as it presents in its timezone) is what the user means by "date".
            // Note: the calendar fields are transferred numerically rather than
            // through a formatted string - PHP would re-parse a year with more
            // than four digits (e.g. "10000-07-10") as a two-digit year.
            try {
                $baseDate = new DateTimeImmutable('1970-01-01', new DateTimeZone('UTC'));
                $valueAsDate = $baseDate
                    ->setDate((int) $value->format('Y'), (int) $value->format('n'), (int) $value->format('j'))
                    ->setTime(0, 0, 0);
                // Unreachable: setDate() accepts any int, and the range is checked before.
                // @codeCoverageIgnoreStart
            } catch (PhpException $e) {
                throw new ValueException('Invalid date value; cannot create DateTimeImmutable', ExceptionCode::VALUE_DATE_OUT_OF_RANGE->value, [
                    'value' => $value->format('Y-m-d'),
                ], $e);
                // @codeCoverageIgnoreEnd
            }
            $valueAsInt = self::VALUE_2_31 + self::daysSinceEpoch($valueAsDate);

            if ($valueAsInt > self::VALUE_INT_MAX || $valueAsInt < self::VALUE_INT_MIN) {
                throw new ValueException('Unsigned integer value is outside of supported range', ExceptionCode::VALUE_INTEGER_OUT_OF_RANGE->value, [
                    'value' => $valueAsInt,
                    'min' => self::VALUE_INT_MIN,
                    'max' => self::VALUE_INT_MAX,
                ]);
            }

            $this->value = $valueAsInt;
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
        return match ($valueEncodeConfig->dateEncodeOption) {
            DateEncodeOption::AS_DATETIME_IMMUTABLE => $this->asDateTime(),
            DateEncodeOption::AS_INT => $this->asInteger(),
            DateEncodeOption::AS_STRING => $this->asString(),
        };
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    public function asDateTime(): DateTimeImmutable {
        $baseDate = new DateTimeImmutable('1970-01-01', new DateTimeZone('UTC'));
        $daysSinceBaseDate = $this->value - self::VALUE_2_31;

        try {
            $interval = new DateInterval('P' . abs($daysSinceBaseDate) . 'D');
            // Unreachable: a day count of at most 2^31 is always a valid interval.
            // @codeCoverageIgnoreStart
        } catch (PhpException $e) {
            throw new ValueException('Invalid date value; cannot create DateInterval', ExceptionCode::VALUE_DATE_OUT_OF_RANGE->value, [
                'value' => $this->value,
                'note' => 'This may happen if the date is out of range for DateTimeImmutable',
            ], $e);
            // @codeCoverageIgnoreEnd
        }

        try {
            if ($daysSinceBaseDate < 0) {
                return $baseDate->sub($interval);
            } else {
                return $baseDate->add($interval);
            }
            // Unreachable: every date of the CQL range fits a DateTimeImmutable.
            // @codeCoverageIgnoreStart
        } catch (PhpException $e) {
            throw new ValueException('Invalid date value; cannot create DateTimeImmutable', ExceptionCode::VALUE_DATE_OUT_OF_RANGE->value, [
                'value' => $this->value,
                'note' => 'This may happen if the date is out of range for DateTimeImmutable',
            ], $e);
            // @codeCoverageIgnoreEnd
        }
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
        return 4;
    }

    /**
     * What {@see self::getValue()} returns for a wire value, without building
     * one: this is the per-cell cost of reading a date column.
     *
     * @throws \Cassandra\Exception\ValueException
     */
    public static function formatWireValue(int $value): string {
        if ($value >= self::VALUE_INT_0000_01_01 && $value <= self::VALUE_INT_9999_12_31) {
            return gmdate('Y-m-d', ($value - self::VALUE_2_31) * 86400);
        }

        $date = new self($value);

        if ($value > self::VALUE_INT_9999_12_31) {
            return '+' . $date->asDateTime()->format('Y-m-d');
        }

        return $date->asDateTime()->format('Y-m-d');
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
        $unpacked = unpack('N', $binary);
        if ($unpacked === false) {
            throw new ValueException('Cannot unpack unsigned integer binary data', ExceptionCode::VALUE_INTEGER_UNPACK_FAILED->value, [
                'binary_length' => strlen($binary),
                'expected_length' => 4,
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
            throw new ValueException(
                'Invalid date value; expected a 32-bit unsigned integer with the epoch (1970-01-01) at the center of the range (2^31), a date in format YYYY-mm-dd as string, or DateTimeInterface'
                , ExceptionCode::VALUE_DATE_INVALID_VALUE_TYPE->value,
                [
                    'value_type' => gettype($value),
                    'expected_types' => ['int', 'string', DateTimeInterface::class],
                ]
            );
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
        return pack('N', $this->value);
    }

    #[\Override]
    public function getType(): Type {
        return Type::DATE;
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public function getValue(): string {
        return self::formatWireValue($this->value);
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
     * @throws \Cassandra\Exception\ValueException
     */
    public static function today(): static {
        return new static(new DateTimeImmutable());
    }

    /**
     * Counted from the timestamp, because DateInterval holds days in 32 bits and
     * wraps for the far ends of the range. The value is a UTC midnight, so the
     * division is exact.
     */
    private static function daysSinceEpoch(DateTimeImmutable $midnight): int {
        return intdiv($midnight->getTimestamp(), 86400);
    }

}
