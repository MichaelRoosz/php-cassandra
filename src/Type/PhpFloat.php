<?php

declare(strict_types=1);

namespace Cassandra\Type;

class PhpFloat extends Base {
    use CommonResetValue;
    use CommonBinaryOfValue;
    use CommonToString;

    protected ?float $_value = null;

    public function __construct(?float $value = null) {
        $this->_value = $value;
    }

    public static function binary(float $value): string {
        return strrev(pack('g', $value));
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function parse(string $binary, null|int|array $definition = null): float {
        /**
         * @var false|array<float> $unpacked
         */
        $unpacked = unpack('g', strrev($binary));

        if ($unpacked === false) {
            throw new Exception('Cannot unpack binary.');
        }

        return $unpacked[1];
    }

    /**
     * @param mixed $value
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    protected static function create(mixed $value, null|int|array $definition): self {
        if ($value !== null && !is_float($value)) {
            throw new Exception('Invalid value type');
        }

        return new self($value);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    protected function parseValue(): ?float {
        if ($this->_value === null && $this->_binary !== null) {
            $this->_value = static::parse($this->_binary);
        }

        return $this->_value;
    }
}
