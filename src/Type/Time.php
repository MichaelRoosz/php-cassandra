<?php

declare(strict_types=1);

namespace Cassandra\Type;

use DateInterval;
use DateTimeImmutable;
use DateTimeInterface;

class Time extends Bigint {
    public const VALUE_MAX = 86399999999999;

    public function __toString(): string {
        return $this->toString();
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public static function fromDateInterval(DateInterval $value): static {
        $hoursInNanoseconds = (int) $value->format('%h') * 3600000000000;
        $minutesInNanoseconds = (int) $value->format('%i') * 60000000000;
        $secondsInNanoseconds = (int) $value->format('%s') * 1000000000;

        $microsecondsInNanoseconds = (int) $value->format('%f') * 1000;

        $totalNanoseconds = $hoursInNanoseconds + $minutesInNanoseconds + $secondsInNanoseconds + $microsecondsInNanoseconds;

        return new static($totalNanoseconds);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public static function fromDateTime(DateTimeInterface $value): static {
        $copy = DateTimeImmutable::createFromInterface($value);
        $midnight = $copy->setTime(0, 0, 0, 0);

        $interval = $value->diff($midnight);

        return self::fromDateInterval($interval);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public static function fromString(string $value): static {
        if (preg_match('/^(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?$/', $value, $matches)) {
            $hours = (int) $matches[1];
            $minutes = (int) $matches[2];
            $seconds = (int) $matches[3];
            $nanoseconds = isset($matches[4]) ? (int) str_pad($matches[4], 9, '0') : 0;

            if ($hours >= 0 && $hours < 24 && $minutes >= 0 && $minutes < 60 && $seconds >= 0 && $seconds < 60) {
                $totalNanoseconds = (
                    ($hours * 3600000000000) +
                    ($minutes * 60000000000) +
                    ($seconds * 1000000000) +
                    $nanoseconds
                );

                return new static($totalNanoseconds);
            } else {
                throw new Exception('Invalid time format');
            }
        } else {
            throw new Exception('Invalid time string');
        }
    }

    /**
     * @throws \Exception
     */
    public function toDateInterval(): DateInterval {
        $value = $this->value;
        $isNegative = $value < 0;
        $sign = $isNegative ? '' : '+';

        $duration = '';

        $hours = intdiv($value, 3600000000000);
        $value %= 3600000000000;

        $minutes = intdiv($value, 60000000000);
        $value %= 60000000000;

        $seconds = intdiv($value, 1000000000);
        $value %= 1000000000;

        $microseconds = intdiv($value, 1000);

        if ($hours > 0) {
            $duration .= $sign . $hours . ' hours ';
        }

        if ($minutes > 0) {
            $duration .= $sign . $minutes . ' minutes ';
        }

        if ($seconds > 0) {
            $duration .= $sign . $seconds . ' seconds ';
        }

        if ($microseconds) {
            $duration .= $sign . $microseconds . ' microseconds ';
        }

        $interval = DateInterval::createFromDateString($duration);
        if ($interval === false) {
            throw new Exception('Cannot convert Time to DateInterval');
        }

        return $interval;
    }

    public function toString(): string {
        $seconds = intdiv($this->value, 1000000000);
        $remaining_nanoseconds = $this->value % 1000000000;

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
     * @throws \Cassandra\Type\Exception
     */
    protected function validateValue() : void {
        if ($this->value > self::VALUE_MAX || $this->value < 0) {
            throw new Exception('Value "' . $this->value . '" is outside of possible range');
        }
    }
}
