<?php

declare(strict_types=1);

namespace Cassandra\Type;

use DateTimeImmutable;
use DateTimeInterface;

class Timestamp extends Bigint
{
    public static function fromDateTime(DateTimeInterface $value): self
    {
        $timestamp = $value->getTimestamp();
        $microseconds = (int) $value->format('u');
        $milliseconds = $timestamp * 1000 + intdiv($microseconds, 1000);

        return new self($milliseconds);
    }

    /**
     * @throws \Exception
     */
    public static function fromString(string $value): self
    {
        $inputDate = new DateTimeImmutable($value);

        return self::fromDateTime($inputDate);
    }

    /**
     * @throws \Exception
     * @throws \Cassandra\Type\Exception
     */
    public static function toDateTime(int $value): DateTimeImmutable
    {
        $seconds = intdiv($value, 1000);
        $microseconds = ($value % 1000) * 1000;
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
    public static function toString(int $value): string
    {
        return self::toDateTime($value)->format('Y-m-d H:i:s.uO');
    }

    /**
     * @throws \Exception
     */
    public function __toString(): string
    {
        if ($this->_value === null) {
            return '';
        }

        return self::toString($this->_value);
    }
}
