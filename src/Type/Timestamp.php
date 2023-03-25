<?php

declare(strict_types=1);

namespace Cassandra\Type;

use DateTimeImmutable;
use DateTimeInterface;

class Timestamp extends Bigint {
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
        $timestamp = $value->getTimestamp();
        $microseconds = (int) $value->format('u');
        $milliseconds = $timestamp * 1000 + intdiv($microseconds, 1000);

        return new static($milliseconds);
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
     * @throws \Cassandra\Type\Exception
     */
    public function toDateTime(): DateTimeImmutable {
        $seconds = intdiv($this->value, 1000);
        $microseconds = ($this->value % 1000) * 1000;
        $datetime = new DateTimeImmutable('@' . $seconds);
        $datetime = $datetime->modify('+' . $microseconds . ' microseconds');

        if ($datetime === false) {
            throw new Exception('invalid value');
        }

        return $datetime;
    }

    /**
     * @throws \Exception
     */
    public function toString(): string {
        return $this->toDateTime()->format('Y-m-d H:i:s.uO');
    }
}
