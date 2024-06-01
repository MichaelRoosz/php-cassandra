<?php

declare(strict_types=1);

namespace Cassandra\StringMath\Calculator;

use Cassandra\StringMath\Calculator;
use Cassandra\StringMath\Exception;

class Native extends Calculator {
    /**
     * @throws \Cassandra\StringMath\Exception
     */
    public function binaryToString(string $binary): string {
        $isNegative = (ord($binary[0]) & 0x80) !== 0;

        if ($isNegative) {
            for ($i = 0; $i < strlen($binary); $i++) {
                $binary[$i] = ~$binary[$i];
            }
        }

        $hex = bin2hex($binary);

        $string = $this->hexToDecimalString($hex);

        if ($isNegative) {
            $string = $this->stringUnsignedAdd1($string);

            return '-' . $string;
        }

        return $string;
    }

    public function stringToBinary(string $string): string {
        $isNegative = str_starts_with($string, '-');
        if ($isNegative) {
            $string = substr($string, 1);
            $string = $this->stringUnsignedSub1($string);
        }

        $binary = '';
        $byte = 0;
        $bits = 0;
        while ($string !== '0') {
            $string = $this->stringUnsignedDiv2($string, $modulo);

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

    public function stringUnsignedDiv2(string $string, ?bool &$modulo = null): string {
        $length = strlen($string);
        $carry = false;
        $firstCarry = $length > 0 && $string[0] === '1';
        for ($i = 0; $i < $length; $i++) {
            switch ($string[$i]) {
                case '0':
                    $string[$i] = $carry ? '5' : '0';
                    $carry = false;

                    break;
                case '1':
                    $string[$i] = $carry ? '5' : '0';
                    $carry = true;

                    break;
                case '2':
                    $string[$i] = $carry ? '6' : '1';
                    $carry = false;

                    break;
                case '3':
                    $string[$i] = $carry ? '6' : '1';
                    $carry = true;

                    break;
                case '4':
                    $string[$i] = $carry ? '7' : '2';
                    $carry = false;

                    break;
                case '5':
                    $string[$i] = $carry ? '7' : '2';
                    $carry = true;

                    break;
                case '6':
                    $string[$i] = $carry ? '8' : '3';
                    $carry = false;

                    break;
                case '7':
                    $string[$i] = $carry ? '8' : '3';
                    $carry = true;

                    break;
                case '8':
                    $string[$i] = $carry ? '9' : '4';
                    $carry = false;

                    break;
                case '9':
                    $string[$i] = $carry ? '9' : '4';
                    $carry = true;

                    break;
            }
        }

        $modulo = $carry;

        if ($firstCarry && $length > 1) {
            $string = substr($string, 1);
        }

        return $string;
    }

    public function stringUnsignedSub1(string $string): string {
        $length = strlen($string);
        for ($i = $length - 1; $i >= 0; $i--) {
            if ($string[$i] !== '0') {
                $string[$i] = (string) ((int) $string[$i] - 1);

                break;
            }

            $string[$i] = '9';
        }

        return $string;
    }

    /**
     * @throws \Cassandra\StringMath\Exception
     */
    protected function hexToDecimalString(string $hexString): string {
        $hexString = strtoupper($hexString);
        $hexChars = '0123456789ABCDEF';
        $decimalStr = '0';

        for ($i = 0; $i < strlen($hexString); $i++) {
            $currentHexDigit = $hexString[$i];
            $decimalValue = strpos($hexChars, $currentHexDigit);
            if ($decimalValue === false) {
                throw new Exception('Invalid value - not a hexadecimal string');
            }

            // Multiply existing decimal number by 16 and add current decimal value
            $carry = 0;
            $tempDecimalStr = '';
            for ($j = strlen($decimalStr) - 1; $j >= 0; $j--) {
                $product = ((int) $decimalStr[$j] * 16) + $decimalValue + $carry;
                $carry = (int) ($product / 10);
                $tempDecimalStr = (string) ($product % 10) . $tempDecimalStr;

                // Update decimal value for next iteration
                $decimalValue = 0;
            }

            // Add carry if there's any
            if ($carry > 0) {
                $tempDecimalStr = (string) $carry . $tempDecimalStr;
            }

            $decimalStr = $tempDecimalStr;
        }

        return $decimalStr;
    }
}
