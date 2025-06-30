<?php

declare(strict_types=1);

namespace Cassandra\Type;

class Uuid extends TypeBase {
    protected string $value;

    final public function __construct(string $value) {
        $this->value = $value;
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromBinary(string $binary, null|int|array $definition = null): static {
        /**
         * @var false|array<int> $unpacked
         */
        $unpacked = unpack('n8', $binary);

        if ($unpacked === false) {
            throw new Exception('Cannot unpack binary.');
        }

        return new static(sprintf(
            '%04x%04x-%04x-%04x-%04x-%04x%04x%04x',
            $unpacked[1],
            $unpacked[2],
            $unpacked[3],
            $unpacked[4],
            $unpacked[5],
            $unpacked[6],
            $unpacked[7],
            $unpacked[8]
        ));
    }

    /**
     * @param mixed $value
     * @param null|int|array<int|array<mixed>> $definition
     *
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromValue(mixed $value, null|int|array $definition = null): static {
        if (!is_string($value)) {
            throw new Exception('Invalid value');
        }

        return new static($value);
    }

    #[\Override]
    public function getBinary(): string {
        return pack('H*', str_replace('-', '', $this->value));
    }

    #[\Override]
    public function getValue(): string {
        return $this->value;
    }
}
