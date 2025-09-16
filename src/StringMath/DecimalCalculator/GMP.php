<?php

declare(strict_types=1);

namespace Cassandra\StringMath\DecimalCalculator;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\StringMathException;
use Cassandra\StringMath\DecimalCalculator;
use GMP as GMPValue;

use function gmp_add, gmp_init, gmp_mul, gmp_strval, gmp_sub, gmp_intval, gmp_div_qr;

final class GMP extends DecimalCalculator {
    /**
     * @throws \Cassandra\Exception\StringMathException
     */
    public function __construct() {
        if (!extension_loaded('gmp')) {
            throw new StringMathException(
                'GMP extension is required',
                ExceptionCode::STRINGMATH_GMP_EXTENSION_NOT_LOADED->value
            );
        }
    }

    /**
     * @throws \Cassandra\Exception\StringMathException
     */
    #[\Override]
    public function add1(string $decimal): string {

        if (!ctype_digit($decimal)) {
            throw new StringMathException(
                'Invalid decimal string',
                ExceptionCode::STRINGMATH_GMP_INVALID_DECIMAL->value,
                ['decimal' => $decimal]
            );
        }

        $result = gmp_add(gmp_init($decimal, 10), 1);

        return gmp_strval($result, 10);
    }

    /**
     * @throws \Cassandra\Exception\StringMathException
     */
    #[\Override]
    public function addUnsignedInt8(string $decimal, int $addend): string {

        if (!ctype_digit($decimal)) {
            throw new StringMathException(
                'Invalid decimal string',
                ExceptionCode::STRINGMATH_GMP_INVALID_DECIMAL->value,
                ['decimal' => $decimal]
            );
        }

        if ($addend === 0) {
            return ltrim($decimal, '0') ?: '0';
        }

        if ($addend < 0 || $addend > 255) {
            throw new StringMathException(
                'Invalid addend',
                ExceptionCode::STRINGMATH_GMP_INVALID_ADDEND->value,
                ['addend' => $addend]
            );
        }

        $result = gmp_add(gmp_init($decimal, 10), $addend);

        return gmp_strval($result, 10);
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

        if (!ctype_digit($decimal)) {
            throw new StringMathException(
                'Invalid decimal string',
                ExceptionCode::STRINGMATH_GMP_INVALID_DECIMAL->value,
                ['decimal' => $decimal]
            );
        }

        /** @var GMPValue $q */
        /** @var GMPValue $r */
        [$q, $r] = gmp_div_qr(gmp_init($decimal, 10), 256, GMP_ROUND_ZERO);

        return [
            'quotient' => gmp_strval($q, 10),
            'remainder' => gmp_intval($r),
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

        if (!ctype_digit($decimal)) {
            throw new StringMathException(
                'Invalid decimal string',
                ExceptionCode::STRINGMATH_GMP_INVALID_DECIMAL->value,
                ['decimal' => $decimal]
            );
        }

        $result = gmp_mul(gmp_init($decimal, 10), 256);

        return gmp_strval($result, 10);
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

        if (!ctype_digit($decimal)) {
            throw new StringMathException(
                'Invalid decimal string',
                ExceptionCode::STRINGMATH_GMP_INVALID_DECIMAL->value,
                ['decimal' => $decimal]
            );
        }

        $result = gmp_sub(gmp_init($decimal, 10), 1);

        return gmp_strval($result, 10);
    }
}
