<?php

declare(strict_types=1);

namespace Cassandra\Type;

class Custom extends Base {
    use CommonResetValue;
    use CommonToString;

    protected string $_javaClassName;

    protected ?string $_value = null;

    public function __construct(?string $value, string $javaClassName = '') {
        $this->_javaClassName = $javaClassName;
        $this->_value = $value;
    }

    public function getJavaClassName(): string {
        return $this->_javaClassName;
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public function binaryOfValue(): string {
        if ($this->_value === null) {
            throw new Exception('value is null');
        }

        return static::binary($this->_value);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public function parseValue(): ?string {
        if ($this->_value === null && $this->_binary !== null) {
            $this->_value = static::parse($this->_binary);
        }

        return $this->_value;
    }

    public static function binary(string $value): string {
        return pack('n', strlen($value)) . $value;
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function parse(string $binary, null|int|array $definition = null): string {
        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('n', substr($binary, 0, 2));
        if ($unpacked === false) {
            throw new Exception('Cannot unpack binary.');
        }

        $length = $unpacked[1];

        return substr($binary, 2, $length);
    }

    /**
     * @param mixed $value
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    protected static function create(mixed $value, null|int|array $definition): self {
        throw new Exception('not implemented');
    }
}
