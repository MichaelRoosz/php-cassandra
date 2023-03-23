<?php

declare(strict_types=1);

namespace Cassandra\Type;

use DateInterval;

class Duration extends Base
{
    use CommonResetValue;
    use CommonBinaryOfValue;

    /**
     * @var ?array{ months: int, days: int, nanoseconds: int } $_value
     */
    protected ?array $_value = null;

    /**
     * @param ?array{ months: int, days: int, nanoseconds: int } $value
     *
     * @throws \Cassandra\Type\Exception
     */
    public function __construct(?array $value = null)
    {
        if ($value !== null &&
            (
                !($value['months'] <= 0 && $value['days'] <= 0 && $value['nanoseconds'] <= 0)
                &&
                !($value['months'] >= 0 && $value['days'] >= 0 && $value['nanoseconds'] >= 0)
            )
        ) {
            throw new Exception('Invalid value type - all values must be either positive or negative');
        }

        $this->_value = $value;
    }

    /**
     * @param mixed $value
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    protected static function create(mixed $value, null|int|array $definition): self
    {
        if ($value !== null && !is_array($value)) {
            throw new Exception('Invalid value type');
        }

        if (!isset($value['months']) || !is_int($value['months'])) {
            throw new Exception('Invalid value type - key "months" not set or invalid data type');
        }

        if (!isset($value['days']) || !is_int($value['days'])) {
            throw new Exception('Invalid value type - key "days" not set or invalid data type');
        }

        if (!isset($value['nanoseconds']) || !is_int($value['nanoseconds'])) {
            throw new Exception('Invalid value type - key "nanoseconds" not set or invalid data type');
        }

        return new self([
            'months' => $value['months'],
            'days' => $value['days'],
            'nanoseconds' => $value['nanoseconds'],
        ]);
    }

    /**
     * @return ?array{ months: int, days: int, nanoseconds: int }
     *
     * @throws \Cassandra\Type\Exception
     */
    protected function parseValue(): ?array
    {
        if ($this->_value === null && $this->_binary !== null) {
            $this->_value = static::parse($this->_binary);
        }

        return $this->_value;
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public static function fromDateInterval(DateInterval $value): self
    {
        $months = ($value->y * 12) + $value->m;
        $days = $value->d;
        $secondsInNanoseconds = $value->s * 1000000000;

        $hoursInNanoseconds = $value->h * 3600 * 1000000000;
        $minutesInNanoseconds = $value->i * 60 * 1000000000;
        $secondsInNanoseconds = $value->s * 1000000000;

        $microseconds = (int)($value->f * 1000000);
        $microsecondsInNanoseconds = $microseconds * 1000;

        $totalNanoseconds = $hoursInNanoseconds + $minutesInNanoseconds + $secondsInNanoseconds + $microsecondsInNanoseconds;

        return new self([
            'months' => $months,
            'days' => $days,
            'nanoseconds' => $totalNanoseconds
        ]);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public static function fromString(string $value): self
    {
        $pattern = '/(?<sign>-)?';
        foreach ([
            'years' => 'y',
            'months' => 'mo',
            'weeks' => 'w',
            'days' => 'd',
            'hours' => 'h',
            'minutes' => 'm',
            'seconds' => 's',
            'milliseconds' => 'ms',
            'microseconds' => '(?:us|µs)',
            'nanoseconds' => 'ns',
        ] as $name => $unit) {
            $pattern .= '(?:(?<'. $name . '>\d+)' . $unit . ')?';
        }
        $pattern .= '/';

        $matches = [];
        preg_match($pattern, $value, $matches);

        $isNegative = !empty($matches['sign']);

        $months = 0;
        foreach ([
            'years' => 12,
            'months' => 1,
        ] as $key => $factor) {
            if (isset($matches[$key])) {
                if ($isNegative) {
                    $months -= (int)$matches[$key] * $factor;
                } else {
                    $months += (int)$matches[$key] * $factor;
                }
            }
        }

        $days = 0;
        foreach ([
            'weeks' => 7,
            'days' => 1,
        ] as $key => $factor) {
            if (isset($matches[$key])) {
                if ($isNegative) {
                    $days -= (int)$matches[$key] * $factor;
                } else {
                    $days += (int)$matches[$key] * $factor;
                }
            }
        }

        $nanoseconds = 0;
        foreach ([
            'hours' => 3600000000000,
            'minutes' => 60000000000,
            'seconds' => 1000000000,
            'milliseconds' => 1000000,
            'microseconds' => 1000,
            'nanoseconds' => 1,

        ] as $key => $factor) {
            if (isset($matches[$key])) {
                if ($isNegative) {
                    $nanoseconds -= (int)$matches[$key] * $factor;
                } else {
                    $nanoseconds += (int)$matches[$key] * $factor;
                }
            }
        }

        return new self([
            'months' => $months,
            'days' => $days,
            'nanoseconds' => $nanoseconds
        ]);
    }

    /**
     * @param array{ months: int, days: int, nanoseconds: int } $value
     *
     * @throws \Exception
     */
    public static function toDateInterval(array $value): DateInterval
    {
        $years = intdiv($value['months'], 12);
        $value['months'] %= 12;

        $duration = 'P';

        if ($years) {
            $duration .= $years . 'Y';
        }

        if ($value['months']) {
            $duration .= $value['months'] . 'M';
        }

        if ($value['days']) {
            $duration .= $value['days'] . 'D';
        }

        if ($value['nanoseconds']) {
            $duration .= 'T';

            $hours = intdiv($value['nanoseconds'], 3600000000000);
            $value['nanoseconds'] %= 3600000000000;

            $minutes = intdiv($value['nanoseconds'], 60000000000);
            $value['nanoseconds'] %= 60000000000;

            $seconds = intdiv($value['nanoseconds'], 1000000000);
            $value['nanoseconds'] %= 1000000000;

            if ($hours) {
                $duration .= $hours . 'H';
            }

            if ($minutes) {
                $duration .= $minutes . 'M';
            }

            if ($seconds) {
                $duration .= $seconds . 'S';
            }
        }

        $interval = new DateInterval($duration);

        if ($value['nanoseconds']) {
            $microsecondsInSeconds = $value['nanoseconds'] / 1000000000;
            $interval->f = $microsecondsInSeconds;
        }

        return $interval;
    }

    /**
     * @param array{ months: int, days: int, nanoseconds: int } $value
     */
    public static function toString(array $value): string
    {
        $isNegative = $value['months'] < 0 || $value['days'] < 0 || $value['nanoseconds'] < 0;
        if ($isNegative) {
            $duration = '-';
        } else {
            $duration = '';
        }

        $years = intdiv($value['months'], 12);
        $value['months'] %= 12;

        $weeks = intdiv($value['days'], 7);
        $value['days'] %= 7;

        if ($isNegative) {
            $years = abs($years);
            $value['months'] = abs($value['months']);
            $weeks = abs($weeks);
            $value['days'] = abs($value['days']);
        }

        if ($years) {
            $duration .= $years . 'y';
        }

        if ($value['months']) {
            $duration .= $value['months'] . 'mo';
        }

        if ($weeks) {
            $duration .= $weeks . 'w';
        }

        if ($value['days']) {
            $duration .= $value['days'] . 'd';
        }

        $hours = intdiv($value['nanoseconds'], 3600000000000);
        $value['nanoseconds'] %= 3600000000000;

        $minutes = intdiv($value['nanoseconds'], 60000000000);
        $value['nanoseconds'] %= 60000000000;

        $seconds = intdiv($value['nanoseconds'], 1000000000);
        $value['nanoseconds'] %= 1000000000;

        $milliseconds = intdiv($value['nanoseconds'], 1000000);
        $value['nanoseconds'] %= 1000000 ;

        $microseconds = intdiv($value['nanoseconds'], 1000);
        $value['nanoseconds'] %= 1000;


        if ($isNegative) {
            $hours = abs($hours);
            $minutes = abs($minutes);
            $seconds = abs($seconds);
            $milliseconds = abs($milliseconds);
            $microseconds = abs($microseconds);
            $value['nanoseconds'] = abs($value['nanoseconds']);
        }

        if ($hours) {
            $duration .= $hours . 'h';
        }

        if ($minutes) {
            $duration .= $minutes . 'm';
        }

        if ($seconds) {
            $duration .= $seconds . 's';
        }

        if ($milliseconds) {
            $duration .= $milliseconds . 'ms';
        }

        if ($microseconds) {
            $duration .= $microseconds . 'us';
        }

        if ($value['nanoseconds']) {
            $duration .= $value['nanoseconds'] . 'ns';
        }

        return $duration;
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public function __toString(): string
    {
        $value = $this->parseValue();

        if ($value === null) {
            return 'null';
        }

        return self::toString($value);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public static function encodeVint(int $number): string
    {
        if ($number < 0) {
            throw new Exception('Negative values are not supported');
        }

        $extraBytes = [];
        $extraBytesCount = 0;
        $mask = 0x80;

        while (true) {
            $cur = $number & 0xFF;
            $next = $number >> 8;

            if ($next === 0 && ($cur & $mask) === 0) {
                $number = $cur;
                break;
            }

            if ($extraBytesCount === 8) {
                throw new Exception('Invalid value');
            }

            $extraBytes[] = $cur;
            $extraBytesCount++;

            $mask |= $mask >> 1;
            $number = $next;
        }

        if ($extraBytesCount < 8) {
            $mask <<= 1;
        }

        $firstByte = $mask | $number;

        return pack('C*', $firstByte, ...array_reverse($extraBytes));
    }

    /**
     * @param array<int> $vint
     * @throws \Cassandra\Type\Exception
     */
    public static function decodeVint(array $vint, int &$pos = 0): int
    {
        $byte = $vint[$pos];
        $extraBytes = 0;

        while (($byte & 0x80) !== 0) {
            $extraBytes++;
            $byte <<= 1;
        }

        $totalBytes = $extraBytes + 1;

        if ($pos + $totalBytes > count($vint)) {
            throw new Exception('Invalid data value');
        }

        $decodedValue = ($byte & 0x7F) >> $extraBytes;

        for ($i = $pos + 1; $i < $pos + $totalBytes; $i++) {
            $decodedValue <<= 8;
            $decodedValue |= $vint[$i];
        }

        $pos += $totalBytes;

        if ($decodedValue < 0) {
            throw new Exception('Values greater than PHP_INT_MAX are not supported');
        }

        return $decodedValue;
    }

    /**
     * @param array{ months: int, days: int, nanoseconds: int } $value
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function binary(array $value): string
    {
        $monthsEncoded = ($value['months'] >> 31) ^ ($value['months'] << 1);
        $daysEncoded = ($value['days'] >> 31) ^ ($value['days'] << 1);
        $nanosecondsEncoded = ($value['nanoseconds'] >> 63) ^ ($value['nanoseconds'] << 1);

        return self::encodeVint($monthsEncoded)
                . self::encodeVint($daysEncoded)
                . self::encodeVint($nanosecondsEncoded);
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     * @return array{ months: int, days: int, nanoseconds: int }
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function parse(string $binary, null|int|array $definition = null): array
    {
        /**
         * @var false|array<int> $values
         */
        $values = unpack('C*', $binary);
        if ($values === false) {
            throw new Exception('Cannot unpack duration.');
        }

        $values = array_values($values);

        $pos = 0;

        $monthsEncoded = self::decodeVint($values, $pos);
        $daysEncoded = self::decodeVint($values, $pos);
        $nanosecondsEncoded = self::decodeVint($values, $pos);

        return [
            'months' => ($monthsEncoded >> 1) ^ -($monthsEncoded & 1),
            'days' => ($daysEncoded >> 1) ^ -($daysEncoded & 1),
            'nanoseconds' => ($nanosecondsEncoded >> 1) ^ -($nanosecondsEncoded & 1)
        ];
    }
}
