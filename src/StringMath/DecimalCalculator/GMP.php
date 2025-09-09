<?php

declare(strict_types=1);

namespace Cassandra\StringMath\DecimalCalculator;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\StringMathException;
use Cassandra\StringMath\DecimalCalculator;

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

    #[\Override]
    public function add1(string $decimal): string {
        $res = gmp_add($decimal, '1');
        $str = gmp_strval($res, 10);
        $str = ltrim($str, '0');

        return $str === '' ? '0' : $str;
    }

    #[\Override]
    public function addUnsignedInt8(string $decimal, int $addend): string {
        if ($addend === 0) {
            return $decimal;
        }

        $res = gmp_add($decimal, (string) $addend);
        $str = gmp_strval($res, 10);
        $str = ltrim($str, '0');

        return $str === '' ? '0' : $str;
    }

    #[\Override]
    public function divideBy256(string $decimal): array {
        if ($decimal === '0') {
            return [
                'quotient' => '0',
                'remainder' => 0,
            ];
        }

        $n = gmp_init($decimal, 10);
        $q = gmp_div_q($n, '256');
        $r = gmp_div_r($n, '256');
        $remainder = (int) gmp_strval($r, 10);
        $qs = gmp_strval($q, 10);
        $qs = ltrim($qs, '0');

        return [
            'quotient' => $qs === '' ? '0' : $qs,
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

        $value = gmp_init(0, 10);
        $length = strlen($binary);
        for ($i = 0; $i < $length; $i++) {
            $value = gmp_mul($value, 256);
            $value = gmp_add($value, ord($binary[$i]));
        }

        if ($isNegative) {
            $value = gmp_add($value, 1);

            return '-' . gmp_strval($value, 10);
        }

        return gmp_strval($value, 10);
    }

    #[\Override]
    public function multiplyByUnsignedInt8(string $decimal, int $multiplier): string {
        if ($decimal === '0' || $multiplier === 0) {
            return '0';
        }
        if ($multiplier === 1) {
            return $decimal;
        }

        $res = gmp_mul($decimal, (string) $multiplier);
        $str = gmp_strval($res, 10);
        $str = ltrim($str, '0');

        return $str === '' ? '0' : $str;
    }

    #[\Override]
    public function sub1(string $decimal): string {
        if ($decimal === '0') {
            return '0';
        }

        $res = gmp_sub($decimal, '1');
        $str = gmp_strval($res, 10);
        $str = ltrim($str, '0');

        return $str === '' ? '0' : $str;
    }

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
