<?php

declare(strict_types=1);

namespace Cassandra\StringMath\Calculator;

use Cassandra\StringMath\Calculator;

final class GMP extends Calculator {
    public function __construct() {
        if (!extension_loaded('gmp')) {
            throw new \RuntimeException('GMP extension is required');
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

        $hex = bin2hex($binary);
        $gmpNumber = gmp_init('0x' . $hex, 16);
        $string = gmp_strval($gmpNumber);

        if ($isNegative) {
            $string = gmp_strval(gmp_add($gmpNumber, 1));

            return '-' . $string;
        }

        return $string;
    }

    #[\Override]
    public function stringToBinary(string $string): string {
        $isNegative = str_starts_with($string, '-');
        if ($isNegative) {
            $string = substr($string, 1);
            $gmpNumber = gmp_sub($string, 1);
        } else {
            $gmpNumber = gmp_init($string);
        }

        $binary = '';
        $byte = 0;
        $bits = 0;

        while (gmp_cmp($gmpNumber, 0) !== 0) {
            $modulo = gmp_cmp(gmp_mod($gmpNumber, 2), 1) === 0;
            $gmpNumber = gmp_div($gmpNumber, 2, GMP_ROUND_ZERO);

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
        return gmp_strval(gmp_add($string, 1));
    }

    #[\Override]
    public function stringUnsignedSub1(string $string): string {
        return gmp_strval(gmp_sub($string, 1));
    }
}
