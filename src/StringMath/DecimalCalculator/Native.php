<?php

declare(strict_types=1);

namespace Cassandra\StringMath\DecimalCalculator;

use Cassandra\ExceptionCode;
use Cassandra\StringMath\DecimalCalculator;
use Cassandra\StringMath\Exception;

final class Native extends DecimalCalculator {
    #[\Override]
    public function add1(string $decimal): string {
        $length = strlen($decimal);
        $carry = true;

        for ($i = $length - 1; $i >= 0; $i--) {
            if ($decimal[$i] !== '9') {
                $decimal[$i] = (string) ((int) $decimal[$i] + 1);
                $carry = false;

                break;
            }

            $decimal[$i] = '0';
        }

        if ($carry) {
            $decimal = '1' . $decimal;
        }

        return $decimal;
    }

    #[\Override]
    public function addUnsignedInt8(string $decimal, int $addend): string {
        if ($addend === 0) {
            return $decimal;
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
     * @throws \Cassandra\StringMath\Exception
     */
    #[\Override]
    public function divideBy256(string $decimal): array {
        $carry = 0;
        $out = [];
        $started = false;
        $length = strlen($decimal);

        for ($i = 0; $i < $length; $i++) {
            $digit = ord($decimal[$i]) - 48;
            if ($digit < 0 || $digit > 9) {
                throw new Exception(
                    'Invalid character in string',
                    ExceptionCode::STRINGMATH_CALCULATOR_NATIVE_INVALID_CHARACTER->value,
                    [
                        'character' => $decimal[$i],
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

    #[\Override]
    public function fromBinary(string $binary): string {
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
            $decimal = $this->multiplyByUnsignedInt8($decimal, 256);
            $decimal = $this->addUnsignedInt8($decimal, ord($binary[$i]));
        }

        if ($isNegative) {
            $decimal = $this->add1($decimal);

            return '-' . $decimal;
        }

        return $decimal;
    }

    #[\Override]
    public function multiplyByUnsignedInt8(string $decimal, int $multiplier): string {
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

    #[\Override]
    public function sub1(string $decimal): string {

        // avoid underflow
        if ($decimal === '0') {
            return '0';
        }

        $length = strlen($decimal);
        for ($i = $length - 1; $i >= 0; $i--) {
            if ($decimal[$i] !== '0') {
                $decimal[$i] = (string) ((int) $decimal[$i] - 1);

                break;
            }

            $decimal[$i] = '9';
        }

        $decimal = ltrim($decimal, '0') ?: '0';

        return $decimal;
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

        // Normalize and handle -0
        $decimal = ltrim($decimal, '0') ?: '0';
        if ($isNegative) {
            if ($decimal === '0') {
                $isNegative = false;
            } else {
                $decimal = $this->sub1($decimal);
            }
        }

        // Repeated division by 256 to collect bytes
        $bytes = [];
        while ($decimal !== '0') {
            ['quotient' => $decimal, 'remainder' => $remainder] = $this->divideBy256($decimal);
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
}
