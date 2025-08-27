<?php

declare(strict_types=1);

namespace Cassandra\StringMath;

abstract class Calculator {
    protected static ?Calculator $calculator = null;

    abstract public function decimalBinaryToString(string $binary): string;

    abstract public function decimalStringToBinary(string $decimal): string;

    public static function get(): Calculator {
        if (self::$calculator === null) {
            // Prefer GMP for best performance, then BCMath, fallback to Native
            if (extension_loaded('gmp')) {
                self::$calculator = new Calculator\GMP();
            } elseif (extension_loaded('bcmath')) {
                self::$calculator = new Calculator\BCMath();
            } else {
                self::$calculator = new Calculator\Native();
            }

            self::$calculator = new Calculator\Native();
        }

        return self::$calculator;
    }

    public static function set(Calculator $calculator): void {
        self::$calculator = $calculator;
    }

    abstract public function stringUnsignedAdd1(string $string): string;

    abstract public function stringUnsignedSub1(string $string): string;
}
