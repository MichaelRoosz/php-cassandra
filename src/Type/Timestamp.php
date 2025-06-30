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
     * @param mixed $value
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     * @throws \Exception
     */
    #[\Override]
    public static function fromValue(mixed $value, null|int|array $definition = null): static {
        self::require64Bit();

        if (is_string($value)) {
            return self::fromString($value);
        }

        if (!is_int($value)) {
            throw new Exception('Invalid value');
        }

        return new static($value);
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
