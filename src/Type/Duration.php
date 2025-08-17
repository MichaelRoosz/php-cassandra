<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\TypeInfo\TypeInfo;
use DateInterval;

final class Duration extends TypeBase {
    final protected const INT32_MAX = 2147483647;
    final protected const INT32_MIN = -2147483648;

    final protected const PATTERNS = [
        '/P'
            . '(?<years>\d+)?'
            . '-'
            . '(?<months>\d+)?'
            . '-'
            . '(?<days>\d+)?'
            . '(?:'
                . 'T'
                . '(?<hours>\d+)?'
                . ':'
                . '(?<minutes>\d+)?'
                . ':'
                . '(?<seconds>\d+)?'
            . ')?'
            . '/',
        '/P'
            . '(?:(?<years>\d+)Y)?'
            . '(?:(?<months>\d+)M)?'
            . '(?:(?<days>\d+)D)?'
            . '(?:(?<weeks>\d+)W)?'
            . '(?:'
            . 'T'
                . '(?:(?<hours>\d+)H)?'
                . '(?:(?<minutes>\d+)M)?'
                . '(?:(?<seconds>\d+)S)?'
            . ')?'
            . '/',
        '/P'
            . '(?:(?<weeks>\d+)W)?'
            . '/',
        '/(?<sign>[+-])?'
            . '(?:(?<years>\d+)y)?'
            . '(?:(?<months>\d+)mo)?'
            . '(?:(?<weeks>\d+)w)?'
            . '(?:(?<days>\d+)d)?'
            . '(?:(?<hours>\d+)h)?'
            . '(?:(?<minutes>\d+)m)?'
            . '(?:(?<seconds>\d+)s)?'
            . '(?:(?<milliseconds>\d+)ms)?'
            . '(?:(?<microseconds>\d+)(?:us|µs))?'
            . '(?:(?<nanoseconds>\d+)ns)?'
            . '/',
    ];

    /**
     * @var array{ months: int, days: int, nanoseconds: int } $value
     */
    protected readonly array $value;

    /**
     * @param array{ months: int, days: int, nanoseconds: int }|string|DateInterval $value
     *
     * @throws \Cassandra\Type\Exception
     */
    final public function __construct(array|string|DateInterval $value) {
        self::require64Bit();

        if (is_array($value)) {
            $this->value = $this->validateValue($value);
        } elseif (is_string($value)) {
            $this->value = $this->nativeValueFromString($value);
        } else {
            $this->value = $this->nativeValueFromDateInterval($value);
        }
    }

    public function __toString(): string {
        return $this->asString();
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public function asDateInterval(): DateInterval {
        $value = $this->value;

        $isNegative = $value['months'] < 0 || $value['days'] < 0 || $value['nanoseconds'] < 0;
        $sign = $isNegative ? '-' : '+';

        $years = intdiv($value['months'], 12);
        $months = $value['months'] % 12;

        $weeks = intdiv($value['days'], 7);
        $days = $value['days'] % 7;

        $nanoseconds = $value['nanoseconds'];

        if ($isNegative) {
            $years = abs($years);
            $months = abs($months);
            $days = abs($days);
            $weeks = abs($weeks);
        }

        $duration = '';

        if ($years) {
            $duration .= $sign . $years . ' years ';
        }

        if ($months) {
            $duration .= $sign . $months . ' months ';
        }

        if ($weeks) {
            $duration .= $sign . $weeks . ' weeks ';
        }

        if ($days) {
            $duration .= $sign . $days . ' days ';
        }

        if ($nanoseconds) {
            $hours = intdiv($nanoseconds, 3600000000000);
            $nanoseconds %= 3600000000000;

            $minutes = intdiv($nanoseconds, 60000000000);
            $nanoseconds %= 60000000000;

            $seconds = intdiv($nanoseconds, 1000000000);
            $nanoseconds %= 1000000000;

            $microseconds = intdiv($nanoseconds, 1000);

            if ($isNegative) {
                $hours = abs($hours);
                $minutes = abs($minutes);
                $seconds = abs($seconds);
                $microseconds = abs($microseconds);
            }

            if ($hours) {
                $duration .= $sign . $hours . ' hours ';
            }

            if ($minutes) {
                $duration .= $sign . $minutes . ' minutes ';
            }

            if ($seconds) {
                $duration .= $sign . $seconds . ' seconds ';
            }

            if ($microseconds) {
                $duration .= $sign . $microseconds . ' microseconds ';
            }
        }

        $interval = DateInterval::createFromDateString($duration);
        if ($interval === false) {
            throw new Exception(
                'Cannot convert Duration to DateInterval',
                Exception::CODE_DURATION_TO_DATEINTERVAL_FAILED, [
                    'duration_string' => $duration,
                    'value' => $this->value,
                ]
            );
        }

        return $interval;
    }

    public function asDateIntervalString(): string {
        $value = $this->value;

        $isNegative = $value['months'] < 0 || $value['days'] < 0 || $value['nanoseconds'] < 0;

        $years = intdiv($value['months'], 12);
        $months = $value['months'] % 12;

        $days = $value['days'];

        $nanoseconds = $value['nanoseconds'];

        if ($isNegative) {
            $years = abs($years);
            $months = abs($months);
            $days = abs($days);
        }

        $duration = 'P';

        if ($years) {
            $duration .= $years . 'Y';
        }

        if ($months) {
            $duration .= $months . 'M';
        }

        if ($days) {
            $duration .= $days . 'D';
        }

        if ($nanoseconds) {
            $hours = intdiv($nanoseconds, 3600000000000);
            $nanoseconds %= 3600000000000;

            $minutes = intdiv($nanoseconds, 60000000000);
            $nanoseconds %= 60000000000;

            $seconds = intdiv($nanoseconds, 1000000000);

            if ($isNegative) {
                $hours = abs($hours);
                $minutes = abs($minutes);
                $seconds = abs($seconds);
            }

            $duration .= 'T';

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

        if ($duration === 'P') {
            $duration = 'PT0S';
        }

        return $duration;
    }

    public function asNativeValue(): array {
        return $this->value;
    }

    public function asString(): string {
        $value = $this->value;

        $isNegative = $value['months'] < 0 || $value['days'] < 0 || $value['nanoseconds'] < 0;
        if ($isNegative) {
            $duration = '-';
        } else {
            $duration = '';
        }

        $years = intdiv($value['months'], 12);
        $months = $value['months'] % 12;
        $days = $value['days'];

        $nanoseconds = $value['nanoseconds'];

        if ($isNegative) {
            $years = abs($years);
            $months = abs($months);
            $days = abs($days);
        }

        if ($years) {
            $duration .= $years . 'y';
        }

        if ($months) {
            $duration .= $months . 'mo';
        }

        if ($days) {
            $duration .= $days . 'd';
        }

        $hours = intdiv($nanoseconds, 3600000000000);
        $nanoseconds %= 3600000000000;

        $minutes = intdiv($nanoseconds, 60000000000);
        $nanoseconds %= 60000000000;

        $seconds = intdiv($nanoseconds, 1000000000);
        $nanoseconds %= 1000000000;

        $milliseconds = intdiv($nanoseconds, 1000000);
        $nanoseconds %= 1000000 ;

        $microseconds = intdiv($nanoseconds, 1000);
        $nanoseconds %= 1000;

        if ($isNegative) {
            $hours = abs($hours);
            $minutes = abs($minutes);
            $seconds = abs($seconds);
            $milliseconds = abs($milliseconds);
            $microseconds = abs($microseconds);
            $nanoseconds = abs($nanoseconds);
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

        if ($nanoseconds) {
            $duration .= $nanoseconds . 'ns';
        }

        if ($duration === '') {
            $duration = '0s';
        }

        return $duration;
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {
        self::require64Bit();

        /**
         * @var false|array<int> $values
         */
        $values = unpack('C*', $binary);
        if ($values === false) {
            throw new Exception('Cannot unpack duration binary data', Exception::CODE_DURATION_UNPACK_FAILED, [
                'binary_length' => strlen($binary),
            ]);
        }

        $values = array_values($values);

        $pos = 0;

        $monthsEncoded = self::decodeVint($values, $pos);
        $daysEncoded = self::decodeVint($values, $pos);
        $nanosecondsEncoded = self::decodeVint($values, $pos);

        $value = [
            'months' => ($monthsEncoded >> 1) ^ -($monthsEncoded & 1),
            'days' => ($daysEncoded >> 1) ^ -($daysEncoded & 1),
            'nanoseconds' => (($nanosecondsEncoded >> 1) & 0x7FFFFFFFFFFFFFFF) ^ -($nanosecondsEncoded & 1),
        ];

        return new static($value);
    }

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        self::require64Bit();

        if (!is_array($value) && !is_string($value) && !$value instanceof DateInterval) {
            throw new Exception(
                'Invalid duration value; expected array, string or DateInterval',
                Exception::CODE_DURATION_INVALID_VALUE_TYPE,
                [
                    'value_type' => gettype($value),
                    'value' => $value,
                ]
            );
        }

        /** @psalm-suppress MixedArgumentTypeCoercion */
        /** @phpstan-ignore argument.type */
        return new static($value);
    }

    #[\Override]
    public function getBinary(): string {
        $monthsEncoded = ($this->value['months'] >> 31) ^ ($this->value['months'] << 1);
        $daysEncoded = ($this->value['days'] >> 31) ^ ($this->value['days'] << 1);
        $nanosecondsEncoded = ($this->value['nanoseconds'] >> 63) ^ ($this->value['nanoseconds'] << 1);

        return self::encodeVint($monthsEncoded)
                . self::encodeVint($daysEncoded)
                . self::encodeVint($nanosecondsEncoded);
    }

    #[\Override]
    public function getValue(): string {
        return $this->asString();
    }

    /**
     * @param array<int> $vint
     * @throws \Cassandra\Type\Exception
     */
    protected static function decodeVint(array $vint, int &$pos = 0): int {
        $byte = $vint[$pos];
        $extraBytes = 0;

        while (($byte & 0x80) !== 0) {
            $extraBytes++;
            $byte <<= 1;
        }

        $totalBytes = $extraBytes + 1;

        if ($pos + $totalBytes > count($vint)) {
            throw new Exception('Invalid duration VInt data', Exception::CODE_DURATION_VINT_INVALID_DATA, [
                'position' => $pos,
                'required_bytes' => $totalBytes,
                'available_bytes' => count($vint),
            ]);
        }

        $decodedValue = ($byte & 0x7F) >> $extraBytes;

        for ($i = $pos + 1; $i < $pos + $totalBytes; $i++) {
            $decodedValue <<= 8;
            $decodedValue |= $vint[$i];
        }

        $pos += $totalBytes;

        return $decodedValue;
    }

    protected static function encodeVint(int $number): string {
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
                break;
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
     * @return array{ months: int, days: int, nanoseconds: int }
     * @throws \Cassandra\Type\Exception
     */
    protected function nativeValueFromDateInterval(DateInterval $value): array {
        self::require64Bit();

        $months = ((int) $value->format('%r%y') * 12) + (int) $value->format('%r%m');
        $days = (int) $value->format('%r%d');

        $hoursInNanoseconds = (int) $value->format('%r%h') * 3600000000000;
        $minutesInNanoseconds = (int) $value->format('%r%i') * 60000000000;
        $secondsInNanoseconds = (int) $value->format('%r%s') * 1000000000;
        $microsecondsInNanoseconds = (int) $value->format('%r%f') * 1000;

        $totalNanoseconds = $hoursInNanoseconds + $minutesInNanoseconds
            + $secondsInNanoseconds + $microsecondsInNanoseconds;

        return [
            'months' => $months,
            'days' => $days,
            'nanoseconds' => $totalNanoseconds,
        ];
    }

    /**
     * @return array{ months: int, days: int, nanoseconds: int }
     * @throws \Cassandra\Type\Exception
     */
    protected function nativeValueFromString(string $value): array {
        self::require64Bit();

        $foundPattern = false;
        foreach (self::PATTERNS as $pattern) {
            $matches = [];
            if (preg_match($pattern, $value, $matches) === 1) {
                $foundPattern = true;

                break;
            }
        }

        if (!$foundPattern) {
            throw new Exception(
                'Invalid duration value; expected string in ISO 8601 format',
                Exception::CODE_DURATION_INVALID_VALUE_TYPE, [
                    'givenValue' => $value,
                ]
            );
        }

        $isNegative = isset($matches['sign']) && $matches['sign'] === '-';

        $months = 0;
        foreach ([
            'years' => 12,
            'months' => 1,
        ] as $key => $factor) {
            if (isset($matches[$key])) {
                if ($isNegative) {
                    $months += (int) ('-' . $matches[$key]) * $factor;
                } else {
                    $months += (int) $matches[$key] * $factor;
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
                    $days += (int) ('-' . $matches[$key]) * $factor;
                } else {
                    $days += (int) $matches[$key] * $factor;
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
                    $nanoseconds += (int) ('-' . $matches[$key]) * $factor;
                } else {
                    $nanoseconds += (int) $matches[$key] * $factor;
                }
            }
        }

        return [
            'months' => $months,
            'days' => $days,
            'nanoseconds' => $nanoseconds,
        ];
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    protected static function require64Bit(): void {
        if (PHP_INT_SIZE < 8) {
            throw new Exception(
                'The Duration data type requires a 64-bit system',
                Exception::CODE_DURATION_64BIT_REQUIRED, [
                    'php_int_size_bytes' => PHP_INT_SIZE,
                    'php_int_size_bits' => PHP_INT_SIZE * 8,
                ]
            );
        }
    }

    /**
     * @param array<mixed> $value
     * @return array{ months: int, days: int, nanoseconds: int }
     * 
     * @throws \Cassandra\Type\Exception
     */
    protected function validateValue(array $value): array {

        // validate months
        if (!isset($value['months']) || !is_int($value['months'])) {
            throw new Exception(
                'Invalid duration value - "months" must be provided as int',
                Exception::CODE_DURATION_MONTHS_INVALID, [
                    'provided' => $value['months'] ?? null,
                    'provided_type' => isset($value['months']) ? gettype($value['months']) : 'missing',
                ]
            );
        }
        $months = $value['months'];

        if ($months < self::INT32_MIN || $months > self::INT32_MAX) {
            throw new Exception(
                'Invalid duration value - "months" is out of int32 range',
                Exception::CODE_DURATION_MONTHS_OUT_OF_RANGE, [
                    'value' => $months,
                    'min' => self::INT32_MIN,
                    'max' => self::INT32_MAX,
                ]
            );
        }

        // validate days
        if (!isset($value['days']) || !is_int($value['days'])) {
            throw new Exception(
                'Invalid duration value - "days" must be provided as int',
                Exception::CODE_DURATION_DAYS_INVALID, [
                    'provided' => $value['days'] ?? null,
                    'provided_type' => isset($value['days']) ? gettype($value['days']) : 'missing',
                ]
            );
        }
        $days = $value['days'];

        if ($days < self::INT32_MIN || $days > self::INT32_MAX) {
            throw new Exception(
                'Invalid duration value - "days" is out of int32 range',
                Exception::CODE_DURATION_DAYS_OUT_OF_RANGE, [
                    'value' => $days,
                    'min' => self::INT32_MIN,
                    'max' => self::INT32_MAX,
                ]
            );
        }

        // validate nanoseconds
        if (!isset($value['nanoseconds']) || !is_int($value['nanoseconds'])) {
            throw new Exception(
                'Invalid duration value - "nanoseconds" must be provided as int',
                Exception::CODE_DURATION_NANOSECONDS_INVALID, [
                    'provided' => $value['nanoseconds'] ?? null,
                    'provided_type' => isset($value['nanoseconds']) ? gettype($value['nanoseconds']) : 'missing',
                ]
            );
        }
        $nanoseconds = $value['nanoseconds'];

        // validate that months, days and nanoseconds are either all positive or all negative
        if (!($months <= 0 && $days <= 0 && $nanoseconds <= 0)
            && !($months >= 0 && $days >= 0 && $nanoseconds >= 0)
        ) {
            throw new Exception(
                'Invalid duration value - sign mismatch across months, days and nanoseconds',
                Exception::CODE_DURATION_SIGN_MISMATCH, [
                    'months' => $months,
                    'days' => $days,
                    'nanoseconds' => $nanoseconds,
                ]
            );
        }

        return [
            'months' => $months,
            'days' => $days,
            'nanoseconds' => $nanoseconds,
        ];
    }
}
