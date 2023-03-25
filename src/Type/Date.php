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
        return $this->toString();
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public static function fromDateTime(DateTimeInterface $value): static {
        $baseDate = new DateTimeImmutable('1970-01-01');
        $interval = $baseDate->diff($value);

        $days = (int) $interval->format('%r%a');

        return new static($days);
    }

    /**
     * @throws \Exception
     * @throws \Cassandra\Type\Exception
     */
    public static function fromString(string $value): static {
        $inputDate = new DateTimeImmutable($value);

        return self::fromDateTime($inputDate);
    }

    /**
     * @throws \Exception
     */
    public function toDateTime(): DateTimeImmutable {
        $baseDate = new DateTimeImmutable('1970-01-01');
        $interval = new DateInterval('P' . abs($this->value) . 'D');

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
