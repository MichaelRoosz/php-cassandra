<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;

final class Decimal extends ValueReadableWithLength {
    /**
     * Upper bound on the absolute exponent accepted from a value in scientific
     * notation, the encode-side counterpart of {@see MAX_SCALE_MAGNITUDE}.
     *
     * The wire encoding is an unscaled varint and a scale, so it cannot carry an
     * exponent: {@see scientificToDecimalString()} expands one into the plain
     * decimal string it stands for, which means "1e50000000" — three bytes of
     * is_numeric()-approved input — becomes a fifty-megabyte string, and a
     * larger exponent exhausts memory outright. Bounded at the same magnitude as
     * a decoded scale, which leaves an enormous margin over any real decimal
     * while keeping a single value's expansion to ~100 KB.
     */
    private const MAX_EXPONENT_MAGNITUDE = 100_000;
    /**
     * Upper bound on the absolute scale this class will carry, in either
     * direction.
     *
     * On the way in ({@see fromBinary()}) the scale is a signed int32, so a peer
     * may declare a magnitude up to ~2.1 billion, and decoding expands the value
     * into a plain decimal string whose length grows with it (padding the
     * fraction with leading zeros, or appending trailing zeros for a negative
     * scale). An unbounded scale therefore lets a peer force a multi-gigabyte
     * allocation from a handful of bytes. This limit caps a single cell's
     * expansion at ~100 KB while leaving an enormous margin over any real
     * decimal (real scales are at most a few hundred), so a larger magnitude is
     * treated as corrupt/hostile input.
     *
     * On the way out it is applied by {@see self::assertScaleInRange()}, so that
     * the two sides agree: {@see self::getBinary()} takes the scale straight
     * from the fraction it was given, and without the same bound this class
     * would encode values it then refuses to decode — a decimal that can be
     * written to a node and never read back from one.
     */
    private const MAX_SCALE_MAGNITUDE = 100_000;

    private readonly string $value;

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    final public function __construct(string|int|float $value) {
        if (!is_numeric($value)) {
            throw new ValueException('Value must be a numeric value', ExceptionCode::VALUE_DECIMAL_NON_NUMERIC->value, [
                'value' => $value,
            ]);
        }

        if (is_float($value)) {
            if (!is_finite($value)) {
                throw new ValueException('Decimal value must be a finite number', ExceptionCode::VALUE_DECIMAL_NON_FINITE_FLOAT->value, [
                    'value' => $value,
                ]);
            }

            $decimal = self::floatToDecimalString($value);
        } else {
            // String (or int) input. is_numeric() also accepts forms the varint
            // wire encoding cannot express verbatim: scientific notation ("1e5"),
            // a leading "+", and surrounding whitespace. Normalize those to a plain
            // decimal string so a value accepted here can always be encoded by
            // getBinary(); plain decimals are kept verbatim (see below).
            $decimal = self::normalizeNumericString((string) $value);
        }

        // Refused here rather than at getBinary(), so that a scale this class
        // could not read back is reported against the value that named it
        // instead of at the moment it is sent.
        self::assertScaleInRange($decimal);
        self::assertUnscaledInRange($decimal);

        $this->value = $decimal;
    }

    #[\Override]
    public static function allowsEmpty(): bool {
        return true;
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public static function fromBinary(
        string $binary,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): ?static {

        if (self::emptyBinaryIsNull($binary)) {
            return null;
        }

        $length = strlen($binary);
        if ($length < 5) {
            throw new ValueException('Cannot unpack decimal binary data', ExceptionCode::VALUE_DECIMAL_UNPACK_FAILED->value, [
                'binary_length' => $length,
                'note' => 'expected >= 5 bytes (4-byte scale + at least one varint byte)',
            ]);
        }
        /**
         * @var false|array<int> $scaleUnpacked
         */
        $scaleUnpacked = unpack('N', substr($binary, 0, 4));
        if ($scaleUnpacked === false) {
            throw new ValueException('Cannot unpack decimal scale', ExceptionCode::VALUE_DECIMAL_UNPACK_FAILED->value, [
                'binary_length' => $length,
            ]);
        }
        // The scale is a signed int32; unpack('N') is unsigned, so sign-extend.
        $signShift = (PHP_INT_SIZE * 8) - 32;
        $scale = $scaleUnpacked[1] << $signShift >> $signShift;

        // Reject an absurd scale before expanding the value: both the positive
        // (str_pad) and negative (str_repeat) paths below allocate memory
        // proportional to the scale, so an unbounded value is a decode-side
        // denial-of-service vector.
        if ($scale > self::MAX_SCALE_MAGNITUDE || $scale < -self::MAX_SCALE_MAGNITUDE) {
            throw new ValueException('Decimal scale is outside of the supported range', ExceptionCode::VALUE_DECIMAL_SCALE_OUT_OF_RANGE->value, [
                'scale' => $scale,
                'max_magnitude' => self::MAX_SCALE_MAGNITUDE,
                'binary_length' => $length,
            ]);
        }

        $varintBinary = substr($binary, 4);

        // Non-null by the length guard above, which leaves at least one byte for
        // the unscaled part; asked anyway because only that guard says so, and
        // Varint::fromBinary() reports an empty value as null.
        $unscaledVarint = Varint::fromBinary($varintBinary);
        if ($unscaledVarint === null) {
            throw new ValueException('Cannot unpack decimal binary data', ExceptionCode::VALUE_DECIMAL_UNPACK_FAILED->value, [
                'binary_length' => $length,
                'note' => 'the unscaled varint part is empty',
            ]);
        }

        $unscaled = $unscaledVarint->asString();

        if ($scale === 0) {
            $value = $unscaled;
        } elseif ($scale < 0) {
            // Negative scale: value = unscaled * 10^(-scale), i.e. append zeros.
            $value = ($unscaled === '0') ? '0' : $unscaled . str_repeat('0', -$scale);
        } else {
            $isNegative = str_starts_with($unscaled, '-');
            $absUnscaled = $isNegative ? substr($unscaled, 1) : $unscaled;

            // Pad with zeros if necessary
            $absUnscaled = str_pad($absUnscaled, $scale + 1, '0', STR_PAD_LEFT);

            // Insert decimal point
            $integerPart = substr($absUnscaled, 0, -$scale);
            $decimalPart = substr($absUnscaled, -$scale);

            // Remove leading zeros from integer part, but keep at least one digit
            $integerPart = ltrim($integerPart, '0') ?: '0';

            $value = $integerPart . '.' . $decimalPart;
            if ($isNegative) {
                $value = '-' . $value;
            }
        }

        return new static($value);
    }

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_numeric($value)) {
            throw new ValueException('Invalid decimal value; expected numeric value', ExceptionCode::VALUE_DECIMAL_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        return new static($value);
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    final public static function fromValue(string|int|float $value): static {
        return new static($value);
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public function getBinary(): string {

        $scalePos = strpos($this->value, '.');
        $hasScale = $scalePos !== false;

        if ($hasScale) {
            $scale = strlen($this->value) - $scalePos - 1;
            $unscaled = substr($this->value, 0, $scalePos) . substr($this->value, $scalePos + 1);
        } else {
            $scale = 0;
            $unscaled = $this->value;
        }

        $binary = pack('N', $scale) . (new Varint($unscaled))->getBinary();

        return $binary;
    }

    #[\Override]
    public function getType(): Type {
        return Type::DECIMAL;
    }

    #[\Override]
    public function getValue(): string {
        return $this->value;
    }

    #[\Override]
    public static function isEmptyValueMeaningless(): bool {
        return true;
    }

    #[\Override]
    final public static function requiresDefinition(): bool {
        return false;
    }

    /**
     * Refuse a scale {@see self::fromBinary()} would not read back; see
     * {@see self::MAX_SCALE_MAGNITUDE}.
     *
     * Only a positive scale can be reached from here — a plain decimal string
     * has as many fraction digits as it has, and nothing on the encode side
     * produces a negative scale — so this is the encode-side half of the bound
     * fromBinary() applies to both signs. The two meet exactly: a value decoded
     * at the limit is constructed with a fraction of the same length and passes
     * this on its way back out.
     *
     * @param string $decimal a plain decimal string, as
     * {@see self::normalizeNumericString()} and {@see self::floatToDecimalString()}
     * produce
     *
     * @throws \Cassandra\Exception\ValueException
     */
    private static function assertScaleInRange(string $decimal): void {

        $pointPosition = strpos($decimal, '.');
        if ($pointPosition === false) {
            return;
        }

        $scale = strlen($decimal) - $pointPosition - 1;
        if ($scale <= self::MAX_SCALE_MAGNITUDE) {
            return;
        }

        throw new ValueException('Decimal scale is outside of the supported range', ExceptionCode::VALUE_DECIMAL_SCALE_OUT_OF_RANGE->value, [
            'scale' => $scale,
            'max_magnitude' => self::MAX_SCALE_MAGNITUDE,
        ]);
    }

    /**
     * Refuse an unscaled part {@see Varint} will not convert.
     *
     * {@see self::getBinary()} encodes the value as a scale and an unscaled
     * varint, so a decimal is exactly as wide as that varint and inherits its
     * bound; see {@see Varint::MAX_MAGNITUDE_DIGITS} for why there is one. This
     * is the same division of labour as {@see self::MAX_SCALE_MAGNITUDE}: that
     * bounds how far a value may be expanded, this bounds how long converting
     * it may take.
     *
     * Applied at construction rather than at getBinary(), for the reason the
     * scale check gives — a value this class cannot encode should be reported
     * against whoever named it, not at the moment it is sent.
     *
     * The two bounds stay orthogonal, and deliberately so: a value like
     * "1e-100000" has a scale of 100000 and an unscaled part of 1, so it is
     * enormous to spell and trivial to convert, and only the scale bound has
     * anything to say about it. "1e100000" is the other way about — no scale at
     * all, and an unscaled part of 100001 digits — and only this one catches it.
     * Which is why the count below is of significant digits: the leading zeros
     * of the first are dropped by {@see Varint::__construct()} before anything
     * is converted, and cost nothing.
     *
     * @param string $decimal a plain decimal string, as
     * {@see self::normalizeNumericString()} and {@see self::floatToDecimalString()}
     * produce
     *
     * @throws \Cassandra\Exception\ValueException
     */
    private static function assertUnscaledInRange(string $decimal): void {

        $digits = strlen(ltrim(str_replace(['-', '.'], '', $decimal), '0'));

        if ($digits <= Varint::MAX_MAGNITUDE_DIGITS) {
            return;
        }

        throw new ValueException('Decimal has more digits than the unscaled value can carry', ExceptionCode::VALUE_DECIMAL_UNSCALED_TOO_LARGE->value, [
            'digits' => $digits,
            'max_digits' => Varint::MAX_MAGNITUDE_DIGITS,
        ]);
    }

    /**
     * @param string $decimal a plain decimal string: an optional leading '-',
     * digits, and an optional '.' followed by digits
     */
    private static function canonicalizeDecimalString(string $decimal): string {

        $isNegative = str_starts_with($decimal, '-');
        $magnitude = $isNegative ? substr($decimal, 1) : $decimal;

        $pointPosition = strpos($magnitude, '.');
        if ($pointPosition === false) {
            $integerPart = $magnitude;
            $fractionPart = '';
        } else {
            $integerPart = substr($magnitude, 0, $pointPosition);
            $fractionPart = substr($magnitude, $pointPosition);
        }

        $integerPart = ltrim($integerPart, '0');
        if ($integerPart === '') {
            $integerPart = '0';
        }

        $magnitude = $integerPart . $fractionPart;

        // Nothing but zeros and the point, i.e. a value of zero however many
        // digits it is spelled with.
        if ($isNegative && strspn($magnitude, '0.') === strlen($magnitude)) {
            return $magnitude;
        }

        return $isNegative ? '-' . $magnitude : $magnitude;
    }

    /**
     * Converts a float to a plain decimal string without losing its value: the
     * shortest string that round-trips to the same float, with any scientific
     * notation expanded (the varint-based wire encoding cannot express an
     * exponent).
     *
     * @throws \Cassandra\Exception\ValueException a float's exponent is at most
     * ~±324, so {@see MAX_EXPONENT_MAGNITUDE} is unreachable from here; it is
     * declared because the expansion is shared with the string path.
     */
    private static function floatToDecimalString(float $value): string {
        return self::scientificToDecimalString(var_export($value, true));
    }

    /**
     * Normalizes a numeric string to a plain decimal string.
     *
     * Plain decimal strings (optional leading "-", digits, optional fraction)
     * keep their fraction verbatim, so an explicitly specified scale — trailing
     * zeros such as in "1.50" — survives unchanged to the wire. Leading zeros of
     * the integer part are dropped.
     *
     * Everything else that is_numeric() accepts (a leading "+", surrounding
     * whitespace, scientific notation, or bare-point forms like ".5") is
     * expanded so that a value accepted at construction can always be serialized
     * by getBinary().
     *
     * @throws \Cassandra\Exception\ValueException
     */
    private static function normalizeNumericString(string $value): string {
        $value = trim($value);

        $matches = [];
        if (preg_match('/^(?<sign>-?)(?<integer>\d+)(?<fraction>\.\d+)?$/', $value, $matches) === 1) {
            return self::canonicalizeDecimalString(
                $matches['sign'] . $matches['integer'] . ($matches['fraction'] ?? '')
            );
        }

        return self::scientificToDecimalString($value);
    }

    /**
     * Expands a signed decimal string that may carry scientific notation and/or
     * a leading sign into a plain decimal string (the varint-based wire encoding
     * cannot express an exponent). Trailing fraction zeros are trimmed.
     *
     * @throws \Cassandra\Exception\ValueException
     */
    private static function scientificToDecimalString(string $value): string {
        $sign = '';
        if (str_starts_with($value, '-')) {
            $sign = '-';
            $value = substr($value, 1);
        } elseif (str_starts_with($value, '+')) {
            $value = substr($value, 1);
        }

        $exponentPos = stripos($value, 'e');
        if ($exponentPos === false) {
            $mantissa = $value;
            $exponent = 0;
        } else {
            $mantissa = substr($value, 0, $exponentPos);
            $exponent = (int) substr($value, $exponentPos + 1);

            // Refused before the expansion below allocates towards it; see
            // {@see MAX_EXPONENT_MAGNITUDE}.
            if ($exponent > self::MAX_EXPONENT_MAGNITUDE || $exponent < -self::MAX_EXPONENT_MAGNITUDE) {
                throw new ValueException('Decimal exponent is outside of the supported range', ExceptionCode::VALUE_DECIMAL_EXPONENT_OUT_OF_RANGE->value, [
                    'exponent' => $exponent,
                    'max_magnitude' => self::MAX_EXPONENT_MAGNITUDE,
                ]);
            }
        }

        $dotPos = strpos($mantissa, '.');
        $integerLength = $dotPos === false ? strlen($mantissa) : $dotPos;
        $digits = str_replace('.', '', $mantissa);

        // Position of the decimal point within $digits after applying the exponent.
        $pointPos = $integerLength + $exponent;

        if ($pointPos <= 0) {
            $result = '0.' . str_repeat('0', -$pointPos) . $digits;
        } elseif ($pointPos >= strlen($digits)) {
            $result = $digits . str_repeat('0', $pointPos - strlen($digits));
        } else {
            $result = substr($digits, 0, $pointPos) . '.' . substr($digits, $pointPos);
        }

        return self::canonicalizeDecimalString($sign . self::trimTrailingFractionZeros($result));
    }

    private static function trimTrailingFractionZeros(string $decimal): string {
        if (!str_contains($decimal, '.')) {
            return $decimal;
        }

        return rtrim(rtrim($decimal, '0'), '.');
    }
}
