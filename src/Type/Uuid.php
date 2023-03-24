<?php

declare(strict_types=1);

namespace Cassandra\Type;

class Uuid extends Base {
    use CommonResetValue;
    use CommonBinaryOfValue;
    use CommonToString;

    protected ?string $_value = null;

    public function __construct(?string $value = null) {
        $this->_value = $value;
    }

    public static function binary(string $value): string {
        return pack('H*', str_replace('-', '', $value));
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    public static function parse(string $binary, null|int|array $definition = null): string {
        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('n8', $binary);

        if ($unpacked === false) {
            throw new Exception('Cannot unpack binary.');
        }

        return sprintf(
            '%04x%04x-%04x-%04x-%04x-%04x%04x%04x',
            $unpacked[1],
            $unpacked[2],
            $unpacked[3],
            $unpacked[4],
            $unpacked[5],
            $unpacked[6],
            $unpacked[7],
            $unpacked[8]
        );
    }

    /**
     * @param mixed $value
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    protected static function create(mixed $value, null|int|array $definition): self {
        if ($value !== null && !is_string($value)) {
            throw new Exception('Invalid value type');
        }

        return new self($value);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    protected function parseValue(): ?string {
        if ($this->_value === null && $this->_binary !== null) {
            $this->_value = static::parse($this->_binary);
        }

        return $this->_value;
    }
}
