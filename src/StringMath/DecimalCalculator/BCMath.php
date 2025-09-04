<?php

declare(strict_types=1);

namespace Cassandra\StringMath\DecimalCalculator;

use Cassandra\ExceptionCode;
use Cassandra\StringMath\DecimalCalculator;
use Cassandra\StringMath\Exception;
use DivisionByZeroError;

final class BCMath extends DecimalCalculator {
    /**
     * @throws \Cassandra\StringMath\Exception
     */
    public function __construct() {
        if (!extension_loaded('bcmath')) {
            throw new Exception(
                'BCMath extension is required',
                ExceptionCode::STRINGMATH_BCMATH_EXTENSION_NOT_LOADED->value
            );
        }
    }

    /**
     * @throws \Cassandra\StringMath\Exception
     */
    #[\Override]
    public function add1(string $decimal): string {

        if (!is_numeric($decimal)) {
            throw new Exception(
                'Invalid decimal string',
                ExceptionCode::STRINGMATH_CALCULATOR_BCMATH_INVALID_DECIMAL->value,
                ['decimal' => $decimal]
            );
        }

        $result = bcadd($decimal, '1', 0);
        $result = ltrim($result, '0');

        return $result === '' ? '0' : $result;
    }

    /**
     * @throws \Cassandra\StringMath\Exception
     */
    #[\Override]
    public function addUnsignedInt8(string $decimal, int $addend): string {
        if ($addend === 0) {
            return $decimal;
        }

        if (!is_numeric($decimal)) {
            throw new Exception(
                'Invalid decimal string',
                ExceptionCode::STRINGMATH_CALCULATOR_BCMATH_INVALID_DECIMAL->value,
                ['decimal' => $decimal]
            );
        }

        $result = bcadd($decimal, (string) $addend, 0);
        $result = ltrim($result, '0');

        return $result === '' ? '0' : $result;
    }

    /**
     * @throws \Cassandra\StringMath\Exception
     */
    #[\Override]
    public function divideBy256(string $decimal): array {
        if ($decimal === '0') {
            return [
                'quotient' => '0',
                'remainder' => 0,
            ];
        }

        if (!is_numeric($decimal)) {
            throw new Exception(
                'Invalid decimal string',
                ExceptionCode::STRINGMATH_CALCULATOR_BCMATH_INVALID_DECIMAL->value,
                ['decimal' => $decimal]
            );
        }

        try {
            $remainder = (int) bcmod($decimal, '256');
            $quotient = bcdiv($decimal, '256', 0);
        } catch (DivisionByZeroError $e) {
            throw new Exception(
                'Division by zero',
                ExceptionCode::STRINGMATH_CALCULATOR_BCMATH_DIVISION_BY_ZERO->value,
                ['decimal' => $decimal],
                $e
            );
        }

        $quotient = ltrim($quotient, '0');

        return [
            'quotient' => $quotient === '' ? '0' : $quotient,
            'remainder' => $remainder,
        ];
    }

    #[\Override]
    public function fromBinary(string $binary): string {
        if ($binary === '') {
            return '0';
        }

        $isNegative = (ord($binary[0]) & 0x80) !== 0;

        if ($isNegative) {
            $length = strlen($binary);
            for ($i = 0; $i < $length; $i++) {
                $binary[$i] = ~$binary[$i];
            }
        }

        $decimal = '0';
        $length = strlen($binary);
        for ($i = 0; $i < $length; $i++) {
            $decimal = bcmul($decimal, '256', 0);
            $decimal = bcadd($decimal, (string) ord($binary[$i]), 0);
        }

        if ($isNegative) {
            $decimal = bcadd($decimal, '1', 0);

            return '-' . $decimal;
        }

        return $decimal;
    }

    /**
     * @throws \Cassandra\StringMath\Exception
     */
    #[\Override]
    public function multiplyByUnsignedInt8(string $decimal, int $multiplier): string {
        if ($decimal === '0' || $multiplier === 0) {
            return '0';
        }
        if ($multiplier === 1) {
            return $decimal;
        }

        if (!is_numeric($decimal)) {
            throw new Exception(
                'Invalid decimal string',
                ExceptionCode::STRINGMATH_CALCULATOR_BCMATH_INVALID_DECIMAL->value,
                ['decimal' => $decimal]
            );
        }

        $product = bcmul($decimal, (string) $multiplier, 0);
        $product = ltrim($product, '0');

        return $product === '' ? '0' : $product;
    }

    /**
     * @throws \Cassandra\StringMath\Exception
     */
    #[\Override]
    public function sub1(string $decimal): string {
        if ($decimal === '0') {
            return '0';
        }

        if (!is_numeric($decimal)) {
            throw new Exception(
                'Invalid decimal string',
                ExceptionCode::STRINGMATH_CALCULATOR_BCMATH_INVALID_DECIMAL->value,
                ['decimal' => $decimal]
            );
        }

        $result = bcsub($decimal, '1', 0);
        $result = ltrim($result, '0');

        return $result === '' ? '0' : $result;
    }

    /**
     * @throws \Cassandra\StringMath\Exception
     */
    #[\Override]
    public function toBinary(string $decimal): string {
        $isNegative = str_starts_with($decimal, '-');
        if ($isNegative) {
            $decimal = substr($decimal, 1);
        }

        $decimal = ltrim($decimal, '0') ?: '0';
        if ($isNegative) {
            if ($decimal === '0') {
                $isNegative = false;
            } else {
                $decimal = $this->sub1($decimal);
            }
        }

        $bytes = [];
        while ($decimal !== '0') {
            ['quotient' => $decimal, 'remainder' => $remainder] = $this->divideBy256($decimal);
            $bytes[] = chr($remainder);
        }

        $binary = count($bytes) > 0 ? implode('', array_reverse($bytes)) : '';
        $length = strlen($binary);

        if ($isNegative) {
            for ($i = 0; $i < $length; $i++) {
                $binary[$i] = ~$binary[$i];
            }
        }

        if (!$isNegative && ($length === 0 || (ord($binary[0]) & 0x80) !== 0)) {
            $binary = chr(0) . $binary;
        } elseif ($isNegative && ($length === 0 || (ord($binary[0]) & 0x80) === 0)) {
            $binary = chr(0xFF) . $binary;
        }

        return $binary;
    }
}
