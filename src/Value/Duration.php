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
    private const DIGITS = '0123456789';
    private const INT32_MAX = 2147483647;
    private const INT32_MIN = -2147483647 - 1;

    /** Digit count of both bounds below. */
    private const INT_DIGIT_COUNT = 19;

    /**
     * PHP's int bounds as digit strings, for the width check in
     * {@see self::accumulateComponent()}. Spelled out rather than derived:
     * constant expressions cannot cast, and deriving them per call was the
     * costliest part of that check. 64-bit only, which
     * {@see self::require64Bit()} guarantees on every path that reaches them.
     */
    private const INT_MAX_DIGITS = '9223372036854775807';

    /** Magnitude of PHP_INT_MIN, one larger than {@see self::INT_MAX_DIGITS}. */
    private const INT_MIN_MAGNITUDE_DIGITS = '9223372036854775808';

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
     * Widest a component may be and still skip the checks in
     * {@see self::accumulateComponent()}: below 10^6, and the largest factor
     * being 3.6e12 (nanoseconds per hour), the product stays under 3.6e18
     * against an int reaching 9.2e18, and the cast cannot saturate. Only the
     * running sum can still overflow.
     */
    private const UNCHECKED_DIGIT_COUNT = 6;

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
     * Add one component of a parsed duration to its total, keeping every step
     * inside PHP's int.
     *
     * Each step fails differently. (int) does not overflow a digit string wider
     * than an int, it saturates at PHP_INT_MAX, so the width is checked as a
     * string beforehand — otherwise "99999999999999999999ns" silently becomes a
     * duration of PHP_INT_MAX nanoseconds. The multiplication and the addition
     * do overflow, producing a float that {@see self::validateValue()} would
     * then reject as a type error rather than as the out-of-range it is; both
     * are bounded against what is left of the range. The bound follows the
     * component's sign, so PHP_INT_MIN stays reachable.
     *
     * Narrow components skip all of it; see {@see self::UNCHECKED_DIGIT_COUNT}.
     *
     * @param string $digits plain decimal digits, unsigned. Guaranteed by both
     * callers — the string parser captures with `\d+`, and
     * {@see self::dateIntervalField()} validates — which keeps that check off
     * this path.
     * @param positive-int $factor value of one of this component in the total's
     * unit. Narrowed because the checks below divide the bound by it.
     *
     * @throws \Cassandra\Exception\ValueException
     */
    private static function accumulateComponent(
        int $total,
        string $digits,
        bool $isNegative,
        int $factor,
        ExceptionCode $outOfRange,
        string $component,
    ): int {

        $length = strlen($digits);
        if ($length === 0) {
            return $total;
        }

        if ($length <= self::UNCHECKED_DIGIT_COUNT) {
            // The common case: no bound below can be reached, and the cast reads
            // through any leading zeros exactly.
            $value = $isNegative ? -(int) $digits : (int) $digits;
        } else {
            // Normalised so the width tested is the number's, not its spelling's.
            if ($digits[0] === '0') {
                $digits = ltrim($digits, '0');
                $length = strlen($digits);

                if ($length === 0) {
                    return $total;
                }
            }

            // Before the cast, which saturates rather than overflowing. Equal
            // widths make a byte comparison a numeric one, both sides being
            // plain digit strings.
            $bound = $isNegative ? self::INT_MIN_MAGNITUDE_DIGITS : self::INT_MAX_DIGITS;

            if (
                $length > self::INT_DIGIT_COUNT
                || ($length === self::INT_DIGIT_COUNT && strcmp($digits, $bound) > 0)
            ) {
                throw self::componentOutOfRange($outOfRange, $component, $digits);
            }

            // Only a magnitude at the bound's full width must be cast negative;
            // PHP_INT_MIN has no positive counterpart to negate.
            if ($isNegative) {
                $value = $length === self::INT_DIGIT_COUNT ? (int) ('-' . $digits) : -(int) $digits;
            } else {
                $value = (int) $digits;
            }

            if (
                $isNegative
                    ? $value < intdiv(PHP_INT_MIN, $factor)
                    : $value > intdiv(PHP_INT_MAX, $factor)
            ) {
                throw self::componentOutOfRange($outOfRange, $component, $digits);
            }
        }

        $scaled = $value * $factor;

        // The one step no narrow component makes safe: the total carries
        // everything accumulated before it.
        if (
            $isNegative
                ? $total < PHP_INT_MIN - $scaled
                : $total > PHP_INT_MAX - $scaled
        ) {
            throw self::componentOutOfRange($outOfRange, $component, $digits);
        }

        return $total + $scaled;
    }

    /**
     * A component PHP's int cannot hold; see {@see self::accumulateComponent()}.
     * The digits are reported by count rather than in full, an arbitrarily long
     * string having no place in an exception context.
     */
    private static function componentOutOfRange(ExceptionCode $code, string $component, string $magnitude): ValueException {

        return new ValueException(
            'Invalid duration value - "' . $component . '" is outside the range this type can carry',
            $code->value,
            [
                'component' => $component,
                'digit_count' => strlen($magnitude),
                'min' => PHP_INT_MIN,
                'max' => PHP_INT_MAX,
            ]
        );
    }

    /**
     * One DateInterval field as the digits and sign
     * {@see self::accumulateComponent()} takes.
     *
     * format('%r%y') is the only spelling of a field together with the
     * interval's invert flag, and the two can contradict each other: a negative
     * field inside an inverted interval yields "--5", which is not a number and
     * contributes nothing — as the (int) cast this replaces also made of it.
     * Validated here rather than in the accumulator because this is the only
     * caller whose input can be anything but digits.
     *
     * @return array{string, bool} the digits, and whether they are negative
     */
    private static function dateIntervalField(DateInterval $value, string $format): array {

        $field = $value->format($format);

        $isNegative = $field !== '' && $field[0] === '-';
        $digits = $isNegative ? substr($field, 1) : $field;

        if (strspn($digits, self::DIGITS) !== strlen($digits)) {
            return ['', false];
        }

        return [$digits, $isNegative];
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
     *
     * @throws \Cassandra\Exception\ValueException
     */
    private function nativeValueFromDateInterval(DateInterval $value): array {

        $months = 0;
        foreach ([
            '%r%y' => 12,
            '%r%m' => 1,
        ] as $format => $factor) {
            [$digits, $isNegative] = self::dateIntervalField($value, $format);

            $months = self::accumulateComponent(
                $months,
                $digits,
                $isNegative,
                $factor,
                ExceptionCode::VALUE_DURATION_MONTHS_OUT_OF_RANGE,
                'months',
            );
        }

        [$dayDigits, $daysAreNegative] = self::dateIntervalField($value, '%r%d');
        $days = self::accumulateComponent(
            0,
            $dayDigits,
            $daysAreNegative,
            1,
            ExceptionCode::VALUE_DURATION_DAYS_OUT_OF_RANGE,
            'days',
        );

        $nanoseconds = 0;
        foreach ([
            '%r%h' => 3600000000000,
            '%r%i' => 60000000000,
            '%r%s' => 1000000000,
            '%r%f' => 1000,
        ] as $format => $factor) {
            [$digits, $isNegative] = self::dateIntervalField($value, $format);

            $nanoseconds = self::accumulateComponent(
                $nanoseconds,
                $digits,
                $isNegative,
                $factor,
                ExceptionCode::VALUE_DURATION_NANOSECONDS_OUT_OF_RANGE,
                'nanoseconds',
            );
        }

        return [
            'months' => $months,
            'days' => $days,
            'nanoseconds' => $nanoseconds,
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
            $digits = $matches[$key] ?? '';
            if ($digits === '') {
                continue;
            }

            $months = self::accumulateComponent(
                $months,
                $digits,
                $isNegative,
                $factor,
                ExceptionCode::VALUE_DURATION_MONTHS_OUT_OF_RANGE,
                'months',
            );
        }

        $days = 0;
        foreach ([
            'weeks' => 7,
            'days' => 1,
        ] as $key => $factor) {
            $digits = $matches[$key] ?? '';
            if ($digits === '') {
                continue;
            }

            $days = self::accumulateComponent(
                $days,
                $digits,
                $isNegative,
                $factor,
                ExceptionCode::VALUE_DURATION_DAYS_OUT_OF_RANGE,
                'days',
            );
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
            $digits = $matches[$key] ?? '';
            if ($digits === '') {
                continue;
            }

            $nanoseconds = self::accumulateComponent(
                $nanoseconds,
                $digits,
                $isNegative,
                $factor,
                ExceptionCode::VALUE_DURATION_NANOSECONDS_OUT_OF_RANGE,
                'nanoseconds',
            );
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
