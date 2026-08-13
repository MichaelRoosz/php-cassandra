<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;
use Cassandra\Value\EncodeOption\DurationEncodeOption;
use Cassandra\VIntCodec;
use DateInterval;
use Exception as PhpException;

final class Duration extends ValueReadableWithoutLength implements ValueWithMultipleEncodings {
    private const INT32_MAX = 2147483647;
    private const INT32_MIN = -2147483647 - 1;

    private const PATTERN_COMPONENTS = [
        'years', 'months', 'weeks', 'days', 'hours',
        'minutes', 'seconds', 'milliseconds', 'microseconds', 'nanoseconds',
    ];

    /**
     * The ISO 8601 forms accept an optional leading sign, which is how
     * {@see self::asDateIntervalString()} spells a negative duration and how
     * {@see self::asString()} already spelled one. ISO 8601 itself has no
     * negative duration, but the type does — Cassandra's `duration` is signed —
     * so a driver that emits these strings has to be able to read its own output
     * back.
     */
    private const PATTERNS = [
        '/^(?<sign>[+-])?P'
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
            . '$/',
        '/^(?<sign>[+-])?P'
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
            . '$/',
        '/^(?<sign>[+-])?P'
            . '(?:(?<weeks>\d+)W)?'
            . '$/',
        '/^(?<sign>[+-])?'
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
            . '$/',
    ];

    /**
     * @var array{ months: int, days: int, nanoseconds: int } $value
     */
    private readonly array $value;

    /**
     * @param array{ months: int, days: int, nanoseconds: int }|string|DateInterval $value
     *
     * @throws \Cassandra\Exception\ValueException
     */
    final public function __construct(array|string|DateInterval $value) {
        self::require64Bit();

        if (is_array($value)) {
            $this->value = $this->validateValue($value);
        } elseif (is_string($value)) {
            $this->value = $this->validateValue($this->nativeValueFromString($value));
        } else {
            $this->value = $this->validateValue($this->nativeValueFromDateInterval($value));
        }
    }

    public function __toString(): string {
        return $this->asString();
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public function asConfigured(ValueEncodeConfig $valueEncodeConfig): mixed {
        return match ($valueEncodeConfig->durationEncodeOption) {
            DurationEncodeOption::AS_DATEINTERVAL => $this->asDateInterval(),
            DurationEncodeOption::AS_DATEINTERVAL_STRING => $this->asDateIntervalString(),
            DurationEncodeOption::AS_NATIVE_VALUE => $this->asNativeValue(),
            DurationEncodeOption::AS_STRING => $this->asString(),
        };
    }

    /**
     * Note that DateInterval carries microseconds and this type carries
     * nanoseconds, so the last three digits are truncated towards zero here — a
     * duration whose whole value is sub-microsecond therefore comes back as a
     * zero interval. Use {@see self::asString()}, which spells all nine, or
     * {@see self::asNativeValue()} where no precision may be lost. The
     * counterpart of the note on {@see self::asDateIntervalString()}, which
     * carries whole seconds only, and of the one on {@see Time::asDateTime()}.
     *
     * The months and days are kept apart from the time part rather than being
     * folded into it, as they are on the wire: a month is not a fixed number of
     * days and a day is not a fixed number of hours, which is the whole reason
     * Cassandra's `duration` carries the three separately.
     *
     * @throws \Cassandra\Exception\ValueException
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

        if ($duration === '') {
            $duration = '0 seconds';
        }

        try {
            $interval = DateInterval::createFromDateString($duration);
        } catch (PhpException $e) {
            throw new ValueException(
                'Cannot convert Duration to DateInterval',
                ExceptionCode::VALUE_DURATION_TO_DATEINTERVAL_FAILED->value, [
                    'duration_string' => $duration,
                    'value' => $this->value,
                ],
                $e
            );
        }

        if ($interval === false) {
            throw new ValueException(
                'Cannot convert Duration to DateInterval',
                ExceptionCode::VALUE_DURATION_TO_DATEINTERVAL_FAILED->value, [
                    'duration_string' => $duration,
                    'value' => $this->value,
                ]
            );
        }

        return $interval;
    }

    /**
     * The duration as an ISO 8601 duration string.
     *
     * A negative duration is spelled with a leading '-', as
     * {@see self::asString()} spells one: every component of this type carries
     * the same sign, so one marker in front says it for all of them. ISO 8601
     * has no negative duration of its own, but dropping the sign would make a
     * negative duration and its positive counterpart the same string — and
     * reading that back through {@see self::fromValue()} would silently flip the
     * sign. {@see self::PATTERNS} accepts the marker for exactly that reason.
     *
     * Note that this is an ISO 8601 duration and so carries whole seconds only:
     * a duration with a sub-second part is truncated towards zero here. Use
     * {@see self::asString()}, which spells milliseconds, microseconds and
     * nanoseconds, or {@see self::asNativeValue()} where no precision may be
     * lost.
     */
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

        $duration = $isNegative ? '-P' : 'P';

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

            // The designator is written only once there is something for it to
            // introduce. A duration whose whole time part is sub-second — the
            // precision this format does not carry — leaves all three of these
            // at zero, and a bare 'T' is not a duration any reader accepts, this
            // class's own {@see self::PATTERNS} included.
            if ($hours || $minutes || $seconds) {
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
        }

        // Nothing survived: an all-zero duration, or one whose only component is
        // the sub-second part this format cannot carry. Either way it is zero to
        // this format's precision, and the sign goes with it — a signed zero
        // would claim a direction the value no longer has.
        if ($duration === 'P' || $duration === '-P') {
            $duration = 'PT0S';
        }

        return $duration;
    }

    /**
     * @return array{ months: int, days: int, nanoseconds: int }
     */
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
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\VIntCodecException
     */
    #[\Override]
    public static function fromBinary(
        string $binary,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): static {

        return self::fromStream(new StreamReader($binary), typeInfo: $typeInfo, valueEncodeConfig: $valueEncodeConfig);
    }

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        self::require64Bit();

        if (!is_array($value) && !is_string($value) && !$value instanceof DateInterval) {
            throw new ValueException(
                'Invalid duration value; expected array, string or DateInterval',
                ExceptionCode::VALUE_DURATION_INVALID_VALUE_TYPE->value,
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

    /**
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\VIntCodecException
     */
    #[\Override]
    final public static function fromStream(
        StreamReader $stream,
        ?int $length = null,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): static {

        self::require64Bit();

        $months = $stream->readSignedVint32();
        $days = $stream->readSignedVint32();
        $nanoseconds = $stream->readSignedVint64();

        $value = [
            'months' => $months,
            'days' => $days,
            'nanoseconds' => $nanoseconds,
        ];

        return new static($value);
    }

    /**
     * @param array{ months: int, days: int, nanoseconds: int }|string|DateInterval $value
     *
     * @throws \Cassandra\Exception\ValueException
     */
    final public static function fromValue(array|string|DateInterval $value): static {
        return new static($value);
    }

    /**
     * @throws \Cassandra\Exception\VIntCodecException
     */
    #[\Override]
    public function getBinary(): string {

        $vIntCodec = new VIntCodec();

        return $vIntCodec->encodeSignedVint32($this->value['months'])
                . $vIntCodec->encodeSignedVint32($this->value['days'])
                . $vIntCodec->encodeSignedVint64($this->value['nanoseconds']);
    }

    #[\Override]
    public function getType(): Type {
        return Type::DURATION;
    }

    #[\Override]
    public function getValue(): string {
        return $this->asString();
    }

    #[\Override]
    final public static function requiresDefinition(): bool {
        return false;
    }

    /**
     * The patterns consist of optional components only, so they can match
     * without capturing anything (e.g. an arbitrary garbage string against the
     * sign-prefixed pattern). A match counts only if at least one component
     * actually captured a value.
     *
     * @param array<array-key, string> $matches
     */
    private static function hasDurationComponent(array $matches): bool {
        foreach (self::PATTERN_COMPONENTS as $component) {
            if (isset($matches[$component]) && $matches[$component] !== '') {
                return true;
            }
        }

        return false;
    }

    /**
     * @return array{ months: int, days: int, nanoseconds: int }
     */
    private function nativeValueFromDateInterval(DateInterval $value): array {

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
     * @throws \Cassandra\Exception\ValueException
     */
    private function nativeValueFromString(string $value): array {

        $matches = null;
        foreach (self::PATTERNS as $pattern) {
            $patternMatches = [];
            if (
                preg_match($pattern, $value, $patternMatches) === 1
                && self::hasDurationComponent($patternMatches)
            ) {
                $matches = $patternMatches;

                break;
            }
        }

        if ($matches === null) {
            throw new ValueException(
                'Invalid duration value; expected string in ISO 8601 format',
                ExceptionCode::VALUE_DURATION_INVALID_VALUE_TYPE->value, [
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
     * @param array<mixed> $value
     * @return array{ months: int, days: int, nanoseconds: int }
     *
     * @throws \Cassandra\Exception\ValueException
     */
    private function validateValue(array $value): array {

        // validate months
        if (!isset($value['months']) || !is_int($value['months'])) {
            throw new ValueException(
                'Invalid duration value - "months" must be provided as int',
                ExceptionCode::VALUE_DURATION_MONTHS_INVALID->value, [
                    'provided' => $value['months'] ?? null,
                    'provided_type' => isset($value['months']) ? gettype($value['months']) : 'missing',
                ]
            );
        }
        $months = $value['months'];

        if ($months < self::INT32_MIN || $months > self::INT32_MAX) {
            throw new ValueException(
                'Invalid duration value - "months" is out of int32 range',
                ExceptionCode::VALUE_DURATION_MONTHS_OUT_OF_RANGE->value, [
                    'value' => $months,
                    'min' => self::INT32_MIN,
                    'max' => self::INT32_MAX,
                ]
            );
        }

        // validate days
        if (!isset($value['days']) || !is_int($value['days'])) {
            throw new ValueException(
                'Invalid duration value - "days" must be provided as int',
                ExceptionCode::VALUE_DURATION_DAYS_INVALID->value, [
                    'provided' => $value['days'] ?? null,
                    'provided_type' => isset($value['days']) ? gettype($value['days']) : 'missing',
                ]
            );
        }
        $days = $value['days'];

        if ($days < self::INT32_MIN || $days > self::INT32_MAX) {
            throw new ValueException(
                'Invalid duration value - "days" is out of int32 range',
                ExceptionCode::VALUE_DURATION_DAYS_OUT_OF_RANGE->value, [
                    'value' => $days,
                    'min' => self::INT32_MIN,
                    'max' => self::INT32_MAX,
                ]
            );
        }

        // validate nanoseconds
        if (!isset($value['nanoseconds']) || !is_int($value['nanoseconds'])) {
            throw new ValueException(
                'Invalid duration value - "nanoseconds" must be provided as int',
                ExceptionCode::VALUE_DURATION_NANOSECONDS_INVALID->value, [
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
            throw new ValueException(
                'Invalid duration value - sign mismatch across months, days and nanoseconds',
                ExceptionCode::VALUE_DURATION_SIGN_MISMATCH->value, [
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
