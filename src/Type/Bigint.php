<?php

declare(strict_types=1);

namespace Cassandra\Type;

use ReflectionClass;

class Bigint extends TypeBase {
    protected int $value;

    /**
     * @throws \Cassandra\Type\Exception
     */
    final public function __construct(int $value) {
        self::require64Bit();

        $this->value = $value;

        $this->validateValue();
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function fromBinary(string $binary, null|int|array $definition = null): static {
        self::require64Bit();

        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('J', $binary);
        if ($unpacked === false) {
            throw new Exception('Cannot unpack binary.');
        }

        return new static($unpacked[1]);
    }

    /**
     * @param mixed $value
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function fromValue(mixed $value, null|int|array $definition = null): static {
        self::require64Bit();

        if (!is_int($value)) {
            throw new Exception('Invalid value');
        }

        return new static($value);
    }

    public function getBinary(): string {
        return pack('J', $this->value);
    }

    public function getValue(): int {
        return $this->value;
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    protected static function require64Bit(): void {
        if (PHP_INT_SIZE < 8) {
            $className = (new ReflectionClass(static::class))->getShortName();

            throw new Exception('The ' . $className . ' data type requires a 64-bit system');
        }
    }

    protected function validateValue(): void {
    }
}
