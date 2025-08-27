<?php

declare(strict_types=1);

namespace Cassandra\StringMath\Calculator;

use Cassandra\StringMath\Calculator;

final class BCMath extends Calculator {
    public function __construct() {
        if (!extension_loaded('bcmath')) {
            throw new \RuntimeException('BCMath extension is required');
        }
    }

    #[\Override]
    public function binaryToString(string $binary): string {
        $isNegative = (ord($binary[0]) & 0x80) !== 0;

        if ($isNegative) {
            for ($i = 0; $i < strlen($binary); $i++) {
                $binary[$i] = ~$binary[$i];
            }
        }

        // Convert big-endian bytes to decimal string via repeated *256 and +byte
        $decimal = '0';
        $length = strlen($binary);
        for ($i = 0; $i < $length; $i++) {
            $decimal = $this->multiplyBySmall($decimal, 256);
            $decimal = $this->addSmall($decimal, ord($binary[$i]));
        }

        if ($isNegative) {
            $string = bcadd($decimal, '1');

            return '-' . $string;
        }

        return $decimal;
    }

    #[\Override]
    public function stringToBinary(string $string): string {
        $isNegative = str_starts_with($string, '-');
        if ($isNegative) {
            $string = substr($string, 1);
            $string = bcsub($string, '1');
        }

        $binary = '';
        $byte = 0;
        $bits = 0;

        while (bccomp($string, '0') !== 0) {
            $modulo = bcmod($string, '2') === '1';
            $string = bcdiv($string, '2', 0);

            if ($modulo) {
                $byte |= 1 << $bits;
            }

            $bits++;

            if ($bits === 8) {
                $binary = chr($byte) . $binary;
                $byte = 0;
                $bits = 0;
            }
        }

        if ($bits > 0) {
            $binary = chr($byte) . $binary;
        }

        if ($isNegative) {
            for ($i = 0; $i < strlen($binary); $i++) {
                $binary[$i] = ~$binary[$i];
            }
        }

        $length = strlen($binary);

        // Check if the most significant bit is set, which could be interpreted as a negative number
        if (!$isNegative && ($length === 0 || (ord($binary[0]) & 0x80) !== 0)) {
            // Add an extra byte with a 0x00 value to keep the number positive
            $binary = chr(0) . $binary;
        }
        // Check if the most significant bit is not set, which could be interpreted as a positive number
        elseif ($isNegative && ($length === 0 || (ord($binary[0]) & 0x80) === 0)) {
            // Add an extra byte with a 0xFF value to keep the number negative
            $binary = chr(0xFF) . $binary;
        }

        return $binary;
    }

    #[\Override]
    public function stringUnsignedAdd1(string $string): string {
        return bcadd($string, '1');
    }

    #[\Override]
    public function stringUnsignedSub1(string $string): string {
        return bcsub($string, '1');
    }

    /**
     * Add a small integer (0..255) to an unsigned base-10 decimal string.
     */
    protected function addSmall(string $decimal, int $addend): string {
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

    protected function hexToDecimalString(string $hexString): string {
        $hexString = strtoupper($hexString);
        $decimalStr = '0';

        for ($i = 0; $i < strlen($hexString); $i++) {
            $currentHexDigit = $hexString[$i];
            $decimalValue = (string) hexdec($currentHexDigit);

            // Multiply existing decimal number by 16 and add current decimal value
            $decimalStr = bcadd(bcmul($decimalStr, '16'), $decimalValue);
        }

        return $decimalStr;
    }

    /**
     * Multiply an unsigned base-10 decimal string by a small integer (e.g., 256).
     */
    protected function multiplyBySmall(string $decimal, int $multiplier): string {
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
}
