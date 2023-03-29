<?php

declare(strict_types=1);

namespace Cassandra\StringMath;

abstract class Calculator {
    protected static ?Calculator $calculator = null;

    abstract public function binaryToString(string $binary) : string;

    abstract public function stringToBinary(string $string) : string;

    abstract public function stringUnsignedAdd1(string $string) : string;

    abstract public function stringUnsignedDiv2(string $string, ?bool &$modulo = null) : string;

    abstract public function stringUnsignedSub1(string $string) : string;

    public static function get() : Calculator {
        if (self::$calculator === null) {
            self::$calculator = new Calculator\Native();
        }

        return self::$calculator;
    }

    public static function set(Calculator $calculator) : void {
        self::$calculator = $calculator;
    }
}
