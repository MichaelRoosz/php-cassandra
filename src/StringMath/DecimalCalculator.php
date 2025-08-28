<?php

declare(strict_types=1);

namespace Cassandra\StringMath;

abstract class DecimalCalculator {
    protected static ?self $calculator = null;

    /**
     * Add 1 to an unsigned base-10 decimal string.
     */
    abstract public function add1(string $decimal): string;

    /**
     * Add an unsigned 8-bit integer (0..255) to an unsigned base-10 decimal string.
     */
    abstract public function addUnsignedInt8(string $decimal, int $addend): string;

    /**
     * Divide an unsigned base-10 decimal string by 256.
     * Returns quotient and sets $remainder (0..255).
     *
     * @return array{
     *  quotient: string,
     *  remainder: int
     * }
     */
    abstract public function divideBy256(string $decimal): array;

    abstract public function fromBinary(string $binary): string;

    /**
     * @throws \Cassandra\StringMath\Exception
     */
    public static function get(): self {
        if (self::$calculator === null) {
            // Prefer GMP for best performance, then BCMath, fallback to Native
            if (extension_loaded('gmp')) {
                self::$calculator = new DecimalCalculator\GMP();
            } elseif (extension_loaded('bcmath')) {
                self::$calculator = new DecimalCalculator\BCMath();
            } else {
                self::$calculator = new DecimalCalculator\Native();
            }

            // todo: remove this once we have a proper calculator
            self::$calculator = new DecimalCalculator\Native();
        }

        return self::$calculator;
    }

    /**
     * Multiply an unsigned base-10 decimal string by an unsigned 8-bit integer (0..255).
     */
    abstract public function multiplyByUnsignedInt8(string $decimal, int $multiplier): string;

    public static function set(self $calculator): void {
        self::$calculator = $calculator;
    }

    /**
     * Subtract 1 from an unsigned base-10 decimal string.
     */
    abstract public function sub1(string $decimal): string;

    abstract public function toBinary(string $decimal): string;
}
