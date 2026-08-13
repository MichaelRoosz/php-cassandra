<?php

declare(strict_types=1);

namespace Cassandra\StringMath\DecimalCalculator;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\StringMathException;
use Cassandra\StringMath\DecimalCalculator;
use Cassandra\StringUtil;

final class Native extends DecimalCalculator {
    /**
     * How many decimal digits are carried in a single PHP int while
     * multiplying, dividing or adding.
     *
     * Sixteen on a 64-bit build and six on a 32-bit one: the widest chunk for
     * which every intermediate below stays inside a signed platform integer.
     * Both the largest product formed here ((CHUNK_RADIX - 1) * 256 + 255) and
     * the largest dividend (255 * CHUNK_RADIX + CHUNK_RADIX - 1) come to
     * 256 * CHUNK_RADIX - 1, so that alone is the bound: 2.56e18 against
     * 2^63 - 1 (~9.22e18) where an int is eight bytes, 2.56e8 against
     * 2^31 - 1 (~2.15e9) where it is four. Sixteen digits per chunk rather
     * than six means a third of the substr, str_pad and intdiv calls per
     * conversion wherever the platform can carry them.
     */
    private const CHUNK_DIGITS = 6 + (10 * (PHP_INT_SIZE >> 3));

    /**
     * The base of one working chunk, i.e. 10 ** {@see self::CHUNK_DIGITS}.
     */
    private const CHUNK_RADIX = 10 ** self::CHUNK_DIGITS;

    /**
     * @throws \Cassandra\Exception\StringMathException
     */
    #[\Override]
    public function add1(string $decimal): string {

        if (!StringUtil::isDigits($decimal)) {
            throw new StringMathException(
                'Invalid decimal string',
                ExceptionCode::STRINGMATH_NATIVE_INVALID_DECIMAL->value,
                ['decimal' => $decimal]
            );
        }

        $length = strlen($decimal);
        $carry = true;

        for ($i = $length - 1; $i >= 0; $i--) {
            if ($decimal[$i] !== '9') {
                $decimal[$i] = chr((ord($decimal[$i]) + 1) & 0xFF);
                $carry = false;

                break;
            }

            $decimal[$i] = '0';
        }

        if ($carry) {
            $decimal = '1' . $decimal;
        }

        $decimal = ltrim($decimal, '0') ?: '0';

        return $decimal;
    }

    /**
     * @throws \Cassandra\Exception\StringMathException
     */
    #[\Override]
    public function addUnsignedInt8(string $decimal, int $addend): string {

        if (!StringUtil::isDigits($decimal)) {
            throw new StringMathException(
                'Invalid decimal string',
                ExceptionCode::STRINGMATH_NATIVE_INVALID_DECIMAL->value,
                ['decimal' => $decimal]
            );
        }

        if ($addend === 0) {
            return ltrim($decimal, '0') ?: '0';
        }

        if ($addend < 0 || $addend > 255) {
            throw new StringMathException(
                'Invalid addend',
                ExceptionCode::STRINGMATH_NATIVE_INVALID_ADDEND->value,
                ['addend' => $addend]
            );
        }

        // Only the digits the carry actually reaches are rewritten; an addend of
        // at most 255 stops within the first few of them, so the rest of the
        // number is left where it is instead of being rebuilt digit by digit.
        // That is what makes this O(1) work on top of the one string copy,
        // rather than O(digits) — and this is called once per byte by
        // {@see DecimalCalculator::fromBinary()}.
        $result = $decimal;
        $carry = $addend;

        for ($i = strlen($result) - 1; $i >= 0 && $carry > 0; $i--) {
            $sum = (ord($result[$i]) - 48) + $carry;
            $result[$i] = chr(48 + ($sum % 10));
            $carry = intdiv($sum, 10);
        }

        while ($carry > 0) {
            $result = chr(48 + ($carry % 10)) . $result;
            $carry = intdiv($carry, 10);
        }

        $result = ltrim($result, '0');

        return $result === '' ? '0' : $result;
    }

    /** 
     * @throws \Cassandra\Exception\StringMathException
     */
    #[\Override]
    public function divideBy256(string $decimal): array {

        if ($decimal === '0') {
            return [
                'quotient' => '0',
                'remainder' => 0,
            ];
        }

        if (!StringUtil::isDigits($decimal)) {
            throw new StringMathException(
                'Invalid decimal string',
                ExceptionCode::STRINGMATH_NATIVE_INVALID_DECIMAL->value,
                ['decimal' => $decimal]
            );
        }

        // Padded up to a whole number of chunks so every step below divides the
        // same width, which is what lets the chunk base be a constant.
        $length = strlen($decimal);
        $remainderDigits = $length % self::CHUNK_DIGITS;
        if ($remainderDigits !== 0) {
            $decimal = str_repeat('0', self::CHUNK_DIGITS - $remainderDigits) . $decimal;
            $length += self::CHUNK_DIGITS - $remainderDigits;
        }

        $carry = 0;
        $quotient = '';

        for ($start = 0; $start < $length; $start += self::CHUNK_DIGITS) {
            $acc = ($carry * self::CHUNK_RADIX) + (int) substr($decimal, $start, self::CHUNK_DIGITS);
            $quotient .= str_pad((string) intdiv($acc, 256), self::CHUNK_DIGITS, '0', STR_PAD_LEFT);
            $carry = $acc % 256;
        }

        $quotient = ltrim($quotient, '0');

        return [
            'quotient' => $quotient === '' ? '0' : $quotient,
            'remainder' => $carry,
        ];
    }

    /**
     * @throws \Cassandra\Exception\StringMathException
     */
    #[\Override]
    public function multiplyBy256(string $decimal): string {
        if ($decimal === '0') {
            return '0';
        }

        if (!StringUtil::isDigits($decimal)) {
            throw new StringMathException(
                'Invalid decimal string',
                ExceptionCode::STRINGMATH_NATIVE_INVALID_DECIMAL->value,
                ['decimal' => $decimal]
            );
        }

        // The chunks are produced least-significant first and collected in an
        // array rather than being prepended to a string: a prepend copies
        // everything accumulated so far, which would make one multiplication
        // quadratic in its own length and the conversion above it cubic.
        $carry = 0;
        $chunks = [];

        for ($end = strlen($decimal); $end > 0; $end -= self::CHUNK_DIGITS) {
            $start = max(0, $end - self::CHUNK_DIGITS);
            $product = ((int) substr($decimal, $start, $end - $start) * 256) + $carry;
            $chunks[] = str_pad((string) ($product % self::CHUNK_RADIX), self::CHUNK_DIGITS, '0', STR_PAD_LEFT);
            $carry = intdiv($product, self::CHUNK_RADIX);
        }

        if ($carry > 0) {
            $chunks[] = (string) $carry;
        }

        $result = ltrim(implode('', array_reverse($chunks)), '0');

        return $result === '' ? '0' : $result;
    }

    /**
     * @throws \Cassandra\Exception\StringMathException
     */
    #[\Override]
    public function sub1(string $decimal): string {

        $decimal = ltrim($decimal, '0') ?: '0';
        if ($decimal === '0') {
            return '0';
        }

        if (!StringUtil::isDigits($decimal)) {
            throw new StringMathException(
                'Invalid decimal string',
                ExceptionCode::STRINGMATH_NATIVE_INVALID_DECIMAL->value,
                ['decimal' => $decimal]
            );
        }

        $length = strlen($decimal);
        for ($i = $length - 1; $i >= 0; $i--) {
            if ($decimal[$i] !== '0') {
                $decimal[$i] = chr((ord($decimal[$i]) - 1) & 0xFF);

                break;
            }

            $decimal[$i] = '9';
        }

        $decimal = ltrim($decimal, '0') ?: '0';

        return $decimal;
    }
}
