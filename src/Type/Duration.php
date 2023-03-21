<?php

declare(strict_types=1);

namespace Cassandra\Type;

use DateInterval;

class Duration extends Base
{
    /**
     * @var ?array{ months: int, days: int, nanoseconds: int } $_value
     */
    protected ?array $_value = null;

    /**
     * @param ?array{ months: int, days: int, nanoseconds: int } $value
     */
    public function __construct(?array $value = null)
    {
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

        if (
            !($value['months'] <= 0 && $value['days'] <= 0 && $value['nanoseconds'] <= 0)
            &&
            !($value['months'] >= 0 && $value['days'] >= 0 && $value['nanoseconds'] >= 0)
        ) {
            throw new Exception('Invalid value type - all values must be either positive or negative');
        }

        return new self([
            'months' => $value['months'],
            'days' => $value['days'],
            'nanoseconds' => $value['nanoseconds'],
        ]);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public function binaryOfValue(): string
    {
        if ($this->_value === null) {
            throw new Exception('value is null');
        }

        return static::binary($this->_value);
    }

    /**
     * @return ?array{ months: int, days: int, nanoseconds: int }
     *
     * @throws \Cassandra\Type\Exception
     */
    public function parseValue(): ?array
    {
        if ($this->_value === null && $this->_binary !== null) {
            $this->_value = static::parse($this->_binary);
        }

        return $this->_value;
    }

    public static function fromDateInterval(DateInterval $value): self
    {
        $months = ($value->y * 12) + $value->m;
        $days = $value->d;
        $secondsInNanoseconds = $value->s * 1000000000;

        $hoursInNanoseconds = $value->h * 3600 * 1000000000;
        $minutesInNanoseconds = $value->i * 60 * 1000000000;
        $secondsInNanoseconds = $value->s * 1000000000;

        $microseconds = $value->f * 1000000;
        $microsecondsInNanoseconds = $microseconds * 1000;

        $totalNanoseconds = $hoursInNanoseconds + $minutesInNanoseconds + $secondsInNanoseconds + $microsecondsInNanoseconds;

        return new self([
            'months' => $months,
            'days' => $days,
            'nanoseconds' => (int)$totalNanoseconds
        ]);
    }

    public static function fromString(string $value): self
    {
        $pattern = '/P((\d+)Y)?((\d+)M)?((\d+)D)?(T((\d+)H)?((\d+)M)?((\d+(?:\.\d+)?)S)?)?/';
        preg_match($pattern, $value, $matches);

        $years = isset($matches[2]) ? intval($matches[2]) : 0;
        $months = isset($matches[4]) ? intval($matches[4]) : 0;
        $days = isset($matches[6]) ? intval($matches[6]) : 0;
        $hours = isset($matches[9]) ? intval($matches[9]) : 0;
        $minutes = isset($matches[11]) ? intval($matches[11]) : 0;
        $seconds = isset($matches[13]) ? floatval($matches[13]) : 0;

        $months += $years * 12;
        $nanoseconds = ($days * 86400 + $hours * 3600 + $minutes * 60 + $seconds) * 1000000000;

        return new self([
            'months' => $months,
            'days' => $days,
            'nanoseconds' => (int)$nanoseconds
        ]);
    }

    /**
     * @param array{ months: int, days: int, nanoseconds: int } $value
     */
    public static function toString(array $value): string
    {
        $totalSeconds = $value['nanoseconds'] / 1000000000;
        $hours = floor($totalSeconds / 3600);
        $totalSeconds %= 3600;
        $minutes = floor($totalSeconds / 60);
        $seconds = $totalSeconds % 60;

        $years = floor($value['months'] / 12);
        $value['months'] %= 12;

        $duration = 'P';

        if ($years > 0) {
            $duration .= $years . 'Y';
        }

        if ($value['months'] > 0) {
            $duration .= $value['months'] . 'M';
        }

        if ($value['days'] > 0 || $hours > 0 || $minutes > 0 || $seconds > 0) {
            $duration .= 'T';
        }

        if ($value['days'] > 0) {
            $duration .= $value['days'] . 'D';
        }

        if ($hours > 0) {
            $duration .= $hours . 'H';
        }

        if ($minutes > 0) {
            $duration .= $minutes . 'M';
        }

        if ($seconds > 0) {
            $duration .= $seconds . 'S';
        }

        return $duration;
    }

    public function __toString(): string
    {
        if ($this->_value === null) {
            return '(null)';
        }

        return self::toString($this->_value);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    protected static function encodeVint(int $n): string
    {
        $binary = decbin($n);
        $numBytes = (int)ceil(strlen($binary) / 8);

        if ($numBytes < 8) {
            $firstByte = str_repeat('1', $numBytes) . str_repeat('0', 8 - $numBytes);
        } elseif ($numBytes === 8) {
            $firstByte = str_repeat('1', 8);
        } else {
            throw new Exception('invalid value: ' . $n);
        }

        $binaryPadded = str_pad($binary, ($numBytes * 8), '0', STR_PAD_LEFT);
        $bytes = str_split($firstByte . $binaryPadded, 8);

        $encodedVint = '';
        foreach ($bytes as $byte) {
            $encodedVint .= chr((int)bindec($byte));
        }

        return $encodedVint;
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    protected static function decodeVint(string $vint, int &$pos = 0): int
    {
        $data = str_split($vint);
        $binary = str_pad(decbin(ord($data[$pos])), 8, '0', STR_PAD_LEFT);

        $firstZeroPos = strpos($binary, '0');
        if ($firstZeroPos === false) {
            $extraBytes = 8;
        } else {
            $extraBytes = $firstZeroPos;
        }

        $totalBytes = $extraBytes + 1;
        $numBits = ($totalBytes * 8) - $extraBytes;

        if ($pos + $totalBytes > count($data)) {
            throw new Exception('invalid data value');
        }

        for ($i = $pos + 1; $i < $pos + $totalBytes; $i++) {
            $binary .= str_pad(decbin(ord($data[$i])), 8, '0', STR_PAD_LEFT);
        }

        $integerBits = substr($binary, $extraBytes + 1, $numBits);
        $decodedValue = (int)bindec($integerBits);

        $pos += $totalBytes;

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
        $nsEncoded = ($value['nanoseconds'] >> 63) ^ ($value['nanoseconds'] << 1);

        return self::encodeVint($monthsEncoded)
                . self::encodeVint($daysEncoded)
                . self::encodeVint($nsEncoded);
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     * @return array{ months: int, days: int, nanoseconds: int }
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function parse(string $binary, null|int|array $definition = null): array
    {
        $pos = 0;

        $monthsEncoded = self::decodeVint($binary, $pos);
        $daysEncoded = self::decodeVint($binary, $pos);
        $nsEncoded = self::decodeVint($binary, $pos);

        return [
            'months' => ($monthsEncoded >> 1) ^ -($monthsEncoded & 1),
            'days' => ($daysEncoded >> 1) ^ -($daysEncoded & 1),
            'nanoseconds' => ($nsEncoded >> 1) ^ -($nsEncoded & 1)
        ];
    }
}
