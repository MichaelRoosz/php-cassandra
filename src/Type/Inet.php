<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\TypeInfo\TypeInfo;

final class Inet extends TypeBase {
    protected string $value;

    final public function __construct(string $value) {
        $this->value = $value;
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static {
        $inet = inet_ntop($binary);

        if ($inet === false) {
            throw new Exception('Cannot convert inet binary to string', Exception::CODE_INET_TO_STRING_FAILED, [
                'binary_length' => strlen($binary),
            ]);
        }

        return new static($inet);
    }

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_string($value)) {
            throw new Exception('Invalid inet value; expected string', Exception::CODE_INET_INVALID_VALUE_TYPE, [
                'value_type' => gettype($value),
            ]);
        }

        return new static($value);
    }

    /**
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    public function getBinary(): string {
        $binary = inet_pton($this->value);

        if ($binary === false) {
            throw new Exception('Cannot convert inet string to binary', Exception::CODE_INET_TO_BINARY_FAILED, [
                'value' => $this->value,
            ]);
        }

        return $binary;
    }

    #[\Override]
    public function getValue(): string {
        return $this->value;
    }
}
