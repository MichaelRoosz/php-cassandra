<?php

declare(strict_types=1);

namespace Cassandra\StringMath;

abstract class DecimalCalculator {
    protected static ?self $calculator = null;

    /**
     * Add 1 to an unsigned base-10 decimal string.
     * 
     * @throws \Cassandra\Exception\StringMathException
     */
    abstract public function add1(string $decimal): string;

    /**
     * Add an unsigned 8-bit integer (0..255) to an unsigned base-10 decimal string.
     * 
     * @throws \Cassandra\Exception\StringMathException
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
     * 
     * @throws \Cassandra\Exception\StringMathException
     */
    abstract public function divideBy256(string $decimal): array;

    /**
     * @throws \Cassandra\Exception\StringMathException
     */
    public function fromBinary(string $binary): string {
        if ($binary === '') {
            return '0';
        }

        $isNegative = (ord($binary[0]) & 0x80) !== 0;

        if ($isNegative) {
            $length = strlen($binary);
            for ($i = 0; $i < $length; $i++) {
                $binary[$i] = chr(~ord($binary[$i]) & 0xFF);
            }
        }

        $decimal = '0';
        $length = strlen($binary);
        for ($i = 0; $i < $length; $i++) {
            $decimal = $this->multiplyBy256($decimal);
            $decimal = $this->addUnsignedInt8($decimal, ord($binary[$i]));
        }

        if ($isNegative) {
            $decimal = $this->add1($decimal);

            return '-' . $decimal;
        }

        return $decimal;
    }

    /**
     * @throws \Cassandra\Exception\StringMathException
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
        }

        return self::$calculator;
    }

    /**
     * Multiply an unsigned base-10 decimal string by 256.
     * 
     * @throws \Cassandra\Exception\StringMathException
     */
    abstract public function multiplyBy256(string $decimal): string;

    public static function set(self $calculator): void {
        self::$calculator = $calculator;
    }

    /**
     * Subtract 1 from an unsigned base-10 decimal string.
     * 
     * @throws \Cassandra\Exception\StringMathException
     */
    abstract public function sub1(string $decimal): string;

    /**
     * @throws \Cassandra\Exception\StringMathException
     */
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
                $binary[$i] = chr(~ord($binary[$i]) & 0xFF);
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
