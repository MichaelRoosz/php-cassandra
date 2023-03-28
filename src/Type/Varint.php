<?php

declare(strict_types=1);

namespace Cassandra\Type;

use GMP;

class Varint extends TypeBase {
    protected static ?bool $gmpAvailable = null;

    protected ?GMP $gmpValue;
    protected int $nativeValue;

    /**
     * @throws \Cassandra\Type\Exception
     */
    public final function __construct(string|int|GMP $value) {
        if (self::gmpAvailable()) {
            $this->nativeValue = 0;

            if ($value instanceof GMP) {
                $this->gmpValue = $value;
            } else {
                $this->gmpValue = gmp_init($value);
            }

            return;
        }

        if ($value instanceof GMP) {
            throw new Exception('The php gmp extension is required for GMP values.');
        }

        $this->nativeValue = (int) $value;
        $this->gmpValue = null;

        if (is_string($value) && (string) $this->nativeValue !== $value) {
            throw new Exception('Value of Varint is outside of possible range (this system only supports signed ' . (PHP_INT_SIZE*8) . '-bit integers). Install the gmp php extension for support of bigger numbers.');
        }
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     */
    public static function fromBinary(string $binary, null|int|array $definition = null): static {
        if (self::gmpAvailable()) {
            $isNegative = (ord($binary[0]) & 0x80) !== 0;

            if ($isNegative) {
                for ($i = 0; $i < strlen($binary); $i++) {
                    $binary[$i] = ~$binary[$i];
                }
            }

            $value = gmp_import($binary, 1, GMP_MSW_FIRST | GMP_LITTLE_ENDIAN);

            if ($isNegative) {
                //$value = gmp_add($value, 1);
                //$value = gmp_neg($value);
                $value = gmp_com($value);
            }

            return new static($value);
        }

        $value = 0;
        $length = strlen($binary);

        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('C*', $binary);
        if ($unpacked === false) {
            throw new Exception('Cannot unpack binary.');
        }

        if (count($unpacked) > PHP_INT_SIZE) {
            throw new Exception('Value of Varint is outside of possible range (this system only supports signed ' . (PHP_INT_SIZE*8) . '-bit integers). Install the gmp php extension for support of bigger numbers.');
        }

        foreach ($unpacked as $i => $byte) {
            $value |= $byte << (($length - (int) $i) * 8);
        }

        $shift = (PHP_INT_SIZE - $length) * 8;

        return new static($value << $shift >> $shift);
    }

    /**
     * @param mixed $value
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function fromValue(mixed $value, null|int|array $definition = null): static {
        if (self::gmpAvailable()) {
            if (!($value instanceof GMP) && !is_string($value) && !is_int($value)) {
                throw new Exception('Invalid value');
            }

            return new static($value);
        }

        if (!is_string($value) && !is_int($value)) {
            throw new Exception('Invalid value');
        }

        return new static($value);
    }

    public function getBinary(): string {
        if ($this->gmpValue !== null) {
            $isNegative = gmp_sign($this->gmpValue) === -1;

            if ($isNegative) {
                $value = gmp_add($this->gmpValue, 1);
            } else {
                $value = $this->gmpValue;
            }

            $binary = gmp_export($value, 1, GMP_MSW_FIRST | GMP_LITTLE_ENDIAN);

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
            #var_dump(bin2hex($binary));

            return $binary;
        }

        $value = $this->nativeValue;
        $isNegative = $value < 0;
        $breakValue = $isNegative ? -1 : 0;

        $result = [];
        do {
            $result[] = $value & 0xFF;
            $value >>= 8;
        } while ($value !== $breakValue);

        $length = count($result);

        // Check if the most significant bit is set, which could be interpreted as a negative number
        if (!$isNegative && ($result[$length - 1] & 0x80) !== 0) {
            // Add an extra byte with a 0x00 value to keep the number positive
            $result[] = 0;
        }
        // Check if the most significant bit is not set, which could be interpreted as a positive number
        elseif ($isNegative && ($result[$length - 1] & 0x80) === 0) {
            // Add an extra byte with a 0xFF value to keep the number negative
            $result[] = 0xFF;
        }

        return pack('C*', ...array_reverse($result));
    }

    public function getValue(): string {
        if ($this->gmpValue !== null) {
            return gmp_strval($this->gmpValue);
        }

        return (string) $this->nativeValue;
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public function getValueAsGmp(): GMP {
        if ($this->gmpValue === null) {
            throw new Exception('The php gmp extension is required for GMP values.');
        }

        return $this->gmpValue;
    }

    public function getValueAsInt(): int {
        if ($this->gmpValue !== null) {
            return gmp_intval($this->gmpValue);
        }

        return $this->nativeValue;
    }

    protected static function gmpAvailable() : bool {
        if (self::$gmpAvailable === null) {
            self::$gmpAvailable = extension_loaded('gmp');
        }

        return self::$gmpAvailable;
    }
}
