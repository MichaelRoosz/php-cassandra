<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\ExceptionCode;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;

final class Inet extends ValueReadableWithLength {
    protected readonly string $value;

    final public function __construct(string $value) {
        $this->value = $value;
    }

    /**
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    public static function fromBinary(
        string $binary,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): static {
        $inet = inet_ntop($binary);

        if ($inet === false) {
            throw new Exception('Cannot convert inet binary to string', ExceptionCode::VALUE_INET_TO_STRING_FAILED->value, [
                'binary_length' => strlen($binary),
            ]);
        }

        return new static($inet);
    }

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_string($value)) {
            throw new Exception('Invalid inet value; expected string', ExceptionCode::VALUE_INET_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        return new static($value);
    }

    /**
     * @throws \Cassandra\Value\Exception
     * @throws \Cassandra\Response\Exception
     */
    #[\Override]
    final public static function fromStream(
        StreamReader $stream,
        ?int $length = null,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): static {

        if ($length !== 4 && $length !== 16) {
            throw new Exception(
                message: 'Invalid inet length byte',
                code: ExceptionCode::VALUE_INET_INVALID_LENGTH->value,
                context: [
                    'method' => __METHOD__,
                    'address_length' => $length,
                    'offset' => $stream->pos(),
                ]
            );
        }

        $inet = inet_ntop($stream->read($length));
        if ($inet === false) {
            throw new Exception(
                message: 'Cannot parse inet address',
                code: ExceptionCode::VALUE_INET_PARSE_FAIL->value,
                context: [
                    'method' => __METHOD__,
                    'address_length' => $length,
                    'offset' => $stream->pos(),
                ]
            );
        }

        return new static($inet);
    }

    final public static function fromValue(string $value): static {
        return new static($value);
    }

    /**
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    public function getBinary(): string {
        $binary = inet_pton($this->value);

        if ($binary === false) {
            throw new Exception('Cannot convert inet string to binary', ExceptionCode::VALUE_INET_TO_BINARY_FAILED->value, [
                'value' => $this->value,
            ]);
        }

        return $binary;
    }

    #[\Override]
    public function getType(): Type {
        return Type::INET;
    }

    #[\Override]
    public function getValue(): string {
        return $this->value;
    }

    #[\Override]
    final public static function requiresDefinition(): bool {
        return false;
    }
}
