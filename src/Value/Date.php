<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\ExceptionCode;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;
use DateInterval;
use DateTimeImmutable;
use DateTimeInterface;
use Exception as PhpException;

final class Date extends ValueWithFixedLength {
    final public const VALUE_INT_MAX = 4_294_967_295;
    final public const VALUE_INT_MIN = 0;

    final protected const VALUE_2_31 = 2_147_483_648;
    final protected const VALUE_INT_9999_12_31 = 2_150_416_544;

    protected readonly int $value;

    /**
     * @param int|string|DateTimeInterface $value 
     * @throws \Cassandra\Value\Exception
     */
    final public function __construct(int|string|DateTimeInterface $value) {

        if (is_int($value)) {
            if ($value > self::VALUE_INT_MAX || $value < self::VALUE_INT_MIN) {
                throw new Exception('Unsigned integer value is outside of supported range', ExceptionCode::VALUE_INTEGER_OUT_OF_RANGE->value, [
                    'value' => $value,
                    'min' => self::VALUE_INT_MIN,
                    'max' => self::VALUE_INT_MAX,
                ]);
            }

            $this->value = $value;

        } elseif (is_string($value)) {

            if (!preg_match('/^[+-]?\d{4,}-\d{1,2}-\d{1,2}$/', $value)) {
                throw new Exception('Invalid date string format; expected "YYYY-MM-DD"', ExceptionCode::VALUE_DATE_INVALID_STRING_FORMAT->value, [
                    'value' => $value,
                ]);
            }

            $firstChar = substr($value, 0, 1);
            if ($firstChar !== '+' && $firstChar !== '-') {
                $value = '+' . $value; // Ensure the date string has a sign
            }

            try {
                $valueAsDate = new DateTimeImmutable($value);
            } catch (PhpException $e) {
                throw new Exception('Invalid date string format; expected "YYYY-MM-DD"', ExceptionCode::VALUE_DATE_INVALID_STRING_FORMAT->value, [
                    'value' => $value,
                    'note' => 'This may happen if the date is out of range for DateTimeImmutable',
                ], $e);
            }

            $baseDate = new DateTimeImmutable('1970-01-01');
            $interval = $baseDate->diff($valueAsDate);

            $valueAsInt = self::VALUE_2_31 + $this->getDayCountFromInterval($interval);

            if ($valueAsInt > self::VALUE_INT_MAX || $valueAsInt < self::VALUE_INT_MIN) {
                throw new Exception('Unsigned integer value is outside of supported range', ExceptionCode::VALUE_INTEGER_OUT_OF_RANGE->value, [
                    'value' => $valueAsInt,
                    'min' => self::VALUE_INT_MIN,
                    'max' => self::VALUE_INT_MAX,
                ]);
            }

            $this->value = $valueAsInt;

        } else { // DateTimeInterface

            $baseDate = new DateTimeImmutable('1970-01-01');
            $interval = $baseDate->diff($value);

            $valueAsInt = self::VALUE_2_31 + $this->getDayCountFromInterval($interval);

            if ($valueAsInt > self::VALUE_INT_MAX || $valueAsInt < self::VALUE_INT_MIN) {
                throw new Exception('Unsigned integer value is outside of supported range', ExceptionCode::VALUE_INTEGER_OUT_OF_RANGE->value, [
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
     * @throws \Cassandra\Value\Exception
     */
    public function asDateTime(): DateTimeImmutable {
        $baseDate = new DateTimeImmutable('1970-01-01');
        $daysSinceBaseDate = $this->value - self::VALUE_2_31;

        try {
            $interval = new DateInterval('P' . abs($daysSinceBaseDate) . 'D');
        } catch (PhpException $e) {
            throw new Exception('Invalid date value; cannot create DateInterval', ExceptionCode::VALUE_DATE_OUT_OF_RANGE->value, [
                'value' => $this->value,
                'note' => 'This may happen if the date is out of range for DateTimeImmutable',
            ], $e);
        }

        try {
            if ($daysSinceBaseDate < 0) {
                return $baseDate->sub($interval);
            } else {
                return $baseDate->add($interval);
            }
        } catch (PhpException $e) {
            throw new Exception('Invalid date value; cannot create DateTimeImmutable', ExceptionCode::VALUE_DATE_OUT_OF_RANGE->value, [
                'value' => $this->value,
                'note' => 'This may happen if the date is out of range for DateTimeImmutable',
            ], $e);
        }
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
        return 4;
    }

    /**
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {
        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('N', $binary);
        if ($unpacked === false) {
            throw new Exception('Cannot unpack unsigned integer binary data', ExceptionCode::VALUE_INTEGER_UNPACK_FAILED->value, [
                'binary_length' => strlen($binary),
                'expected_length' => 4,
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

        if (!is_int($value) && !is_string($value) && !($value instanceof DateTimeInterface)) {
            throw new Exception(
                'Invalid date value; expected number of days since 1970-01-01 as integer, date in format YYYY-mm-dd as string, or DateTimeInterface'
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
     * @throws \Cassandra\Value\Exception
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
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    public function getValue(): string {
        if ($this->value > self::VALUE_INT_9999_12_31) {
            return '+' . $this->asDateTime()->format('Y-m-d');
        } else {
            return $this->asDateTime()->format('Y-m-d');
        }
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
    protected function getDayCountFromInterval(DateInterval $interval): int {
        $dayCount = $interval->format('%r%a');

        if (str_starts_with($dayCount, '--')) {
            if ($dayCount === '--2147483648') {
                $dayCount = '-2147483648'; // Special case for minimum date
            } else {
                throw new Exception('Unsigned integer value is outside of supported range', ExceptionCode::VALUE_INTEGER_OUT_OF_RANGE->value, [
                    'value' => $dayCount,
                    'min' => self::VALUE_INT_MIN,
                    'max' => self::VALUE_INT_MAX,
                ]);
            }
        }

        return (int) $dayCount;
    }

}
