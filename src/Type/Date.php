<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\TypeInfo\TypeInfo;
use DateInterval;
use DateTimeImmutable;
use DateTimeInterface;

final class Date extends TypeBase {
    final public const VALUE_2_31 = 2_147_483_648;
    final public const VALUE_MAX = 4_294_967_295;
    final public const VALUE_MIN = 0;

    protected int $value;

    /**
     * @throws \Cassandra\Type\Exception
     */
    final public function __construct(int $value) {
        if ($value > self::VALUE_MAX || $value < self::VALUE_MIN) {
            throw new Exception('Unsigned integer value is outside of supported range', Exception::CODE_INTEGER_OUT_OF_RANGE, [
                'value' => $value,
                'min' => self::VALUE_MIN,
                'max' => self::VALUE_MAX,
            ]);
        }

        $this->value = $value;
    }

    /**
     * @throws \Exception
     */
    public function __toString(): string {
        return $this->toString();
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {
        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('N', $binary);
        if ($unpacked === false) {
            throw new Exception('Cannot unpack unsigned integer binary data', Exception::CODE_INTEGER_UNPACK_FAILED, [
                'binary_length' => strlen($binary),
                'expected_length' => 4,
            ]);
        }

        return new static($unpacked[1]);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public static function fromDateTime(DateTimeInterface $value): static {
        $baseDate = new DateTimeImmutable('1970-01-01');
        $interval = $baseDate->diff($value);

        $days = self::VALUE_2_31 + (int) $interval->format('%r%a');

        return new static($days);
    }

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Type\Exception
     * @throws \Exception
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {

        if (is_string($value)) {
            return self::fromString($value);
        }

        if (!is_int($value)) {
            throw new Exception('Invalid date value; expected days as int', Exception::CODE_DATE_INVALID_VALUE_TYPE, [
                'value_type' => gettype($value),
            ]);
        }

        return new static($value);
    }

    /**
     * @throws \Exception
     * @throws \Cassandra\Type\Exception
     */
    public static function fromString(string $value): static {
        $inputDate = new DateTimeImmutable($value);

        return self::fromDateTime($inputDate);
    }

    #[\Override]
    public function getBinary(): string {
        return pack('N', $this->value);
    }

    #[\Override]
    public function getValue(): int {
        return $this->value;
    }

    /**
     * @throws \Exception
     */
    public function toDateTime(): DateTimeImmutable {
        $baseDate = new DateTimeImmutable('1970-01-01');
        $interval = new DateInterval('P' . abs($this->value - self::VALUE_2_31) . 'D');

        if ($this->value >= 0) {
            return $baseDate->add($interval);
        } else {
            return $baseDate->sub($interval);
        }
    }

    /**
     * @throws \Exception
     */
    public function toString(): string {
        return $this->toDateTime()->format('Y-m-d');
    }
}
