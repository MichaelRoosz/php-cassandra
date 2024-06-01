<?php

declare(strict_types=1);

namespace Cassandra\Type;

class Custom extends TypeBase {
    protected string $javaClassName;

    protected string $value;

    final public function __construct(string $value, string $javaClassName = '') {
        $this->javaClassName = $javaClassName;
        $this->value = $value;
    }

    /**
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    public static function fromBinary(string $binary, null|int|array $definition = null, string $javaClassName = ''): static {
        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('n', substr($binary, 0, 2));
        if ($unpacked === false) {
            throw new Exception('Cannot unpack binary.');
        }

        $length = $unpacked[1];

        return new static(substr($binary, 2, $length), $javaClassName);
    }

    /**
     * @param mixed $value
     * @param null|int|array<int|array<mixed>> $definition
     */
    public static function fromValue(mixed $value, null|int|array $definition = null): static {
        if (!is_string($value)) {
            throw new Exception('Invalid value');
        }

        return new static($value);
    }

    public function getBinary(): string {
        return pack('n', strlen($this->value)) . $this->value;
    }

    public function getJavaClassName(): string {
        return $this->javaClassName;
    }

    public function getValue(): string {
        return $this->value;
    }
}
