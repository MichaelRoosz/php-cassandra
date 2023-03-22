<?php

declare(strict_types=1);

namespace Cassandra\Type;

use DateInterval;
use DateTimeImmutable;
use DateTimeInterface;

class Time extends Bigint
{
    /**
     * @throws \Cassandra\Type\Exception
     */
    public static function fromString(string $value): self
    {
        if (preg_match('/^(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?$/', $value, $matches)) {
            $hours = (int) $matches[1];
            $minutes = (int) $matches[2];
            $seconds = (int) $matches[3];
            $nanoseconds = isset($matches[4]) ? (int) str_pad($matches[4], 9, '0') : 0;

            if ($hours >= 0 && $hours < 24 && $minutes >= 0 && $minutes < 60 && $seconds >= 0 && $seconds < 60) {
                $totalNanoseconds = (
                    ($hours * 60 * 60 * 1000000000) +
                    ($minutes * 60 * 1000000000) +
                    ($seconds * 1000000000) +
                    $nanoseconds
                );

                return new self($totalNanoseconds);
            } else {
                throw new Exception('Invalid time format');
            }
        } else {
            throw new Exception('Invalid time string');
        }
    }

    public static function fromDateTime(DateTimeInterface $value): self
    {
        $copy = DateTimeImmutable::createFromInterface($value);
        $midnight = $copy->setTime(0, 0, 0, 0);

        $interval = $value->diff($midnight);

        return self::fromDateInterval($interval);
    }

    public static function fromDateInterval(DateInterval $value): self
    {
        $hoursInNanoseconds = $value->h * 3600 * 1000000000;
        $minutesInNanoseconds = $value->i * 60 * 1000000000;
        $secondsInNanoseconds = $value->s * 1000000000;

        $microseconds = (int)($value->f * 1000000);
        $microsecondsInNanoseconds = $microseconds * 1000;

        $totalNanoseconds = $hoursInNanoseconds + $minutesInNanoseconds + $secondsInNanoseconds + $microsecondsInNanoseconds;

        return new self($totalNanoseconds);
    }

    public static function toString(int $value): string
    {
        $seconds = intdiv($value, 1000000000);
        $remaining_nanoseconds = $value % 1000000000;

        $hours = intdiv($seconds, 3600);
        $remaining_seconds = $seconds % 3600;

        $minutes = intdiv($remaining_seconds, 60);
        $remaining_seconds %= 60;

        $formatted_time = sprintf('%02d:%02d:%02d', $hours, $minutes, $remaining_seconds);

        if ($remaining_nanoseconds > 0) {
            $formatted_nanoseconds = sprintf('.%09d', $remaining_nanoseconds);
            $formatted_time .= $formatted_nanoseconds;
        }

        return $formatted_time;
    }

    /**
     * @throws \Exception
     */
    public static function toDateInterval(int $value): DateInterval
    {
        $duration = 'PT';

        $hours = intdiv($value, 3600000000000);
        $value %= 3600000000000;

        $minutes = intdiv($value, 60000000000);
        $value %= 60000000000;

        $seconds = intdiv($value, 1000000000);
        $value %= 1000000000;

        if ($hours > 0) {
            $duration .= $hours . 'H';
        }

        if ($minutes > 0) {
            $duration .= $minutes . 'M';
        }

        if ($seconds > 0) {
            $duration .= $seconds . 'S';
        }

        $interval = new DateInterval($duration);

        if ($value) {
            $microsecondsInSeconds = $value / 1000000000;
            $interval->f = $microsecondsInSeconds;
        }

        return $interval;
    }

    public function __toString(): string
    {
        if ($this->_value === null) {
            return '(null)';
        }

        return self::toString($this->_value);
    }
}
