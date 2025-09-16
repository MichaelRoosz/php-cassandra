<?php

declare(strict_types=1);

namespace Cassandra\StringMath\DecimalCalculator;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\StringMathException;
use Cassandra\StringMath\DecimalCalculator;

final class Native extends DecimalCalculator {
    /**
     * @throws \Cassandra\Exception\StringMathException
     */
    #[\Override]
    public function add1(string $decimal): string {

        if (!ctype_digit($decimal)) {
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
                $decimal[$i] = chr(ord($decimal[$i]) + 1);
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

        if (!ctype_digit($decimal)) {
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

        $carry = $addend;
        $out = [];
        $length = strlen($decimal);
        for ($i = $length - 1; $i >= 0; $i--) {
            $digit = ord($decimal[$i]) - 48;
            $sum = $digit + $carry;
            $out[] = chr(48 + ($sum % 10));
            $carry = intdiv($sum, 10);
        }

        while ($carry > 0) {
            $out[] = chr(48 + ($carry % 10));
            $carry = intdiv($carry, 10);
        }

        $result = strrev(implode('', $out));
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

        if (!ctype_digit($decimal)) {
            throw new StringMathException(
                'Invalid decimal string',
                ExceptionCode::STRINGMATH_NATIVE_INVALID_DECIMAL->value,
                ['decimal' => $decimal]
            );
        }

        $carry = 0;
        $out = [];
        $started = false;
        $length = strlen($decimal);

        for ($i = 0; $i < $length; $i++) {
            $digit = ord($decimal[$i]) - 48;
            $acc = ($carry * 10) + $digit;
            $q = intdiv($acc, 256);
            $carry = $acc % 256;
            if ($q !== 0 || $started) {
                $out[] = chr(48 + $q);
                $started = true;
            }
        }

        if (!$started) {
            return [
                'quotient' => '0',
                'remainder' => $carry,
            ];
        }

        return [
            'quotient' => implode('', $out),
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

        if (!ctype_digit($decimal)) {
            throw new StringMathException(
                'Invalid decimal string',
                ExceptionCode::STRINGMATH_NATIVE_INVALID_DECIMAL->value,
                ['decimal' => $decimal]
            );
        }

        $multiplier = 256;

        $carry = 0;
        $out = [];
        $length = strlen($decimal);
        for ($i = $length - 1; $i >= 0; $i--) {
            $digit = ord($decimal[$i]) - 48;
            $product = ($digit * $multiplier) + $carry;
            $out[] = chr(48 + ($product % 10));
            $carry = intdiv($product, 10);
        }

        while ($carry > 0) {
            $out[] = chr(48 + ($carry % 10));
            $carry = intdiv($carry, 10);
        }

        $result = strrev(implode('', $out));
        $result = ltrim($result, '0');

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

        if (!ctype_digit($decimal)) {
            throw new StringMathException(
                'Invalid decimal string',
                ExceptionCode::STRINGMATH_NATIVE_INVALID_DECIMAL->value,
                ['decimal' => $decimal]
            );
        }

        $length = strlen($decimal);
        for ($i = $length - 1; $i >= 0; $i--) {
            if ($decimal[$i] !== '0') {
                $decimal[$i] = chr(ord($decimal[$i]) - 1);

                break;
            }

            $decimal[$i] = '9';
        }

        $decimal = ltrim($decimal, '0') ?: '0';

        return $decimal;
    }
}
