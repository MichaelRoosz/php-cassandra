<?php

declare(strict_types=1);

namespace Cassandra\StringMath\Calculator;

use Cassandra\ExceptionCode;
use Cassandra\StringMath\Calculator;
use Cassandra\StringMath\Exception;

final class Native extends Calculator {
    /**
     * @throws \Cassandra\StringMath\Exception
     */
    #[\Override]
    public function decimalBinaryToString(string $binary): string {
        if ($binary === '') {
            return '0';
        }

        $isNegative = (ord($binary[0]) & 0x80) !== 0;

        // If negative, invert the bytes; we'll add 1 at the end (two's complement)
        if ($isNegative) {
            $length = strlen($binary);
            for ($i = 0; $i < $length; $i++) {
                $binary[$i] = ~$binary[$i];
            }
        }

        // Convert big-endian bytes to decimal string via repeated *256 and +byte
        $decimal = '0';
        $length = strlen($binary);
        for ($i = 0; $i < $length; $i++) {
            $decimal = $this->multiplyDecimalByUnsignedInt8($decimal, 256);
            $decimal = $this->addUnsignedInt8($decimal, ord($binary[$i]));
        }

        if ($isNegative) {
            $decimal = $this->stringUnsignedAdd1($decimal);

            return '-' . $decimal;
        }

        return $decimal;
    }

    #[\Override]
    public function decimalStringToBinary(string $string): string {
        $isNegative = str_starts_with($string, '-');
        if ($isNegative) {
            $string = substr($string, 1);
        }

        // Normalize and handle -0
        $string = ltrim($string, '0') ?: '0';
        if ($isNegative) {
            if ($string === '0') {
                $isNegative = false;
            } else {
                $string = $this->stringUnsignedSub1($string);
            }
        }

        // Repeated division by 256 to collect bytes
        $bytes = [];
        while ($string !== '0') {
            $string = $this->stringUnsignedDiv256($string, $remainder);
            $bytes[] = chr($remainder);
        }

        // Assemble big-endian
        $binary = count($bytes) > 0 ? implode('', array_reverse($bytes)) : '';
        $length = strlen($binary);

        if ($isNegative) {
            for ($i = 0; $i < $length; $i++) {
                $binary[$i] = ~$binary[$i];
            }
        }

        // Sign-extend to preserve sign
        if (!$isNegative && ($length === 0 || (ord($binary[0]) & 0x80) !== 0)) {
            $binary = chr(0) . $binary;
        } elseif ($isNegative && ($length === 0 || (ord($binary[0]) & 0x80) === 0)) {
            $binary = chr(0xFF) . $binary;
        }

        return $binary;
    }

    #[\Override]
    public function decimalUnsignedSub1(string $string): string {

        // avoid underflow
        if ($string === '0') {
            return '0';
        }

        $length = strlen($string);
        for ($i = $length - 1; $i >= 0; $i--) {
            if ($string[$i] !== '0') {
                $string[$i] = (string) ((int) $string[$i] - 1);

                break;
            }

            $string[$i] = '9';
        }

        $string = ltrim($string, '0') ?: '0';

        return $string;
    }

    #[\Override]
    public function stringUnsignedAdd1(string $string): string {
        $length = strlen($string);
        $carry = true;

        for ($i = $length - 1; $i >= 0; $i--) {
            if ($string[$i] !== '9') {
                $string[$i] = (string) ((int) $string[$i] + 1);
                $carry = false;

                break;
            }

            $string[$i] = '0';
        }

        if ($carry) {
            $string = '1' . $string;
        }

        return $string;
    }

    /**
     * Add an unsigned 8-bit integer (0..255) to an unsigned base-10 decimal string.
     */
    protected function addUnsignedInt8(string $decimal, int $addend): string {
        if ($addend === 0) {
            return $decimal;
        }

        $carry = $addend;
        $out = [];
        for ($i = strlen($decimal) - 1; $i >= 0; $i--) {
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
     * Multiply an unsigned base-10 decimal string by an unsigned 8-bit integer (0..255).
     */
    protected function multiplyByUnsignedInt8(string $decimal, int $multiplier): string {
        if ($decimal === '0') {
            return '0';
        }
        if ($multiplier === 1) {
            return $decimal;
        }
        if ($multiplier === 0) {
            return '0';
        }

        $carry = 0;
        $out = [];
        for ($i = strlen($decimal) - 1; $i >= 0; $i--) {
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
     * Divide an unsigned base-10 decimal string by 256.
     * Returns quotient and sets $remainder (0..255).
     *
     * @param-out int $remainder
     */
    protected function stringUnsignedDiv256(string $string, ?int &$remainder): string {
        $length = strlen($string);
        $carry = 0;
        $out = [];
        $started = false;

        for ($i = 0; $i < $length; $i++) {
            $digit = ord($string[$i]) - 48;
            if ($digit < 0 || $digit > 9) {
                throw new Exception(
                    'Invalid character in string',
                    ExceptionCode::STRINGMATH_CALCULATOR_NATIVE_INVALID_CHARACTER->value,
                    [
                        'character' => $string[$i],
                    ]
                );
            }

            $acc = ($carry * 10) + $digit;
            $q = intdiv($acc, 256);
            $carry = $acc % 256;
            if ($q !== 0 || $started) {
                $out[] = chr(48 + $q);
                $started = true;
            }
        }

        $remainder = $carry;
        if (!$started) {
            return '0';
        }

        return implode('', $out);
    }
}
