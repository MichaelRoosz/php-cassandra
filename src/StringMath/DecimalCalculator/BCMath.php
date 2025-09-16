<?php

declare(strict_types=1);

namespace Cassandra\StringMath\DecimalCalculator;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\StringMathException;
use Cassandra\StringMath\DecimalCalculator;
use DivisionByZeroError;

use function bcadd, bcdiv, bcmod, bcmul, bcsub;

final class BCMath extends DecimalCalculator {
    /**
     * @throws \Cassandra\Exception\StringMathException
     */
    public function __construct() {
        if (!extension_loaded('bcmath')) {
            throw new StringMathException(
                'BCMath extension is required',
                ExceptionCode::STRINGMATH_BCMATH_EXTENSION_NOT_LOADED->value
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
                ExceptionCode::STRINGMATH_BCMATH_INVALID_DECIMAL->value,
                ['decimal' => $decimal]
            );
        }

        return bcadd($decimal, '1', 0);
    }

    /**
     * @throws \Cassandra\Exception\StringMathException
     */
    #[\Override]
    public function addUnsignedInt8(string $decimal, int $addend): string {

        if (!ctype_digit($decimal)) {
            throw new StringMathException(
                'Invalid decimal string',
                ExceptionCode::STRINGMATH_BCMATH_INVALID_DECIMAL->value,
                ['decimal' => $decimal]
            );
        }

        if ($addend === 0) {
            return ltrim($decimal, '0') ?: '0';
        }

        if ($addend < 0 || $addend > 255) {
            throw new StringMathException(
                'Invalid addend',
                ExceptionCode::STRINGMATH_BCMATH_INVALID_ADDEND->value,
                ['addend' => $addend]
            );
        }

        return bcadd($decimal, (string) $addend, 0);
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
                ExceptionCode::STRINGMATH_BCMATH_INVALID_DECIMAL->value,
                ['decimal' => $decimal]
            );
        }

        try {
            $remainder = (int) bcmod($decimal, '256');
            $quotient = bcdiv($decimal, '256', 0);
        } catch (DivisionByZeroError $e) {
            throw new StringMathException(
                'Division by zero',
                ExceptionCode::STRINGMATH_BCMATH_DIVISION_BY_ZERO->value,
                ['decimal' => $decimal],
                $e
            );
        }

        return [
            'quotient' => $quotient,
            'remainder' => $remainder,
        ];
    }

    #[\Override]
    public function multiplyBy256(string $decimal): string {
        if ($decimal === '0') {
            return '0';
        }

        if (!ctype_digit($decimal)) {
            throw new StringMathException(
                'Invalid decimal string',
                ExceptionCode::STRINGMATH_BCMATH_INVALID_DECIMAL->value,
                ['decimal' => $decimal]
            );
        }

        return bcmul($decimal, '256', 0);
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
                ExceptionCode::STRINGMATH_BCMATH_INVALID_DECIMAL->value,
                ['decimal' => $decimal]
            );
        }

        return bcsub($decimal, '1', 0);
    }
}
