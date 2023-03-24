<?php

declare(strict_types=1);

namespace Cassandra\Type;

use DateInterval;
use DateTimeImmutable;
use DateTimeInterface;

class Date extends PhpInt {
    /**
     * @throws \Exception
     */
    public function __toString(): string {
        $value = $this->parseValue();

        if ($value === null) {
            return 'null';
        }

        return self::toString($value);
    }
    /**
     * @throws \Cassandra\Type\Exception
     */
    public static function fromDateTime(DateTimeInterface $value): self {
        $baseDate = new DateTimeImmutable('1970-01-01');
        $interval = $baseDate->diff($value);

        $days = (int) $interval->format('%r%a');

        return new self($days);
    }

    /**
     * @throws \Exception
     * @throws \Cassandra\Type\Exception
     */
    public static function fromString(string $value): self {
        $inputDate = new DateTimeImmutable($value);

        return self::fromDateTime($inputDate);
    }

    /**
     * @throws \Exception
     */
    public static function toDateTime(int $value): DateTimeImmutable {
        $baseDate = new DateTimeImmutable('1970-01-01');
        $interval = new DateInterval('P' . abs($value) . 'D');

        if ($value >= 0) {
            return $baseDate->add($interval);
        } else {
            return $baseDate->sub($interval);
        }
    }

    /**
     * @throws \Exception
     */
    public static function toString(int $value): string {
        return self::toDateTime($value)->format('Y-m-d');
    }
}
