<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Response\StreamReader;
use Cassandra\Type;
use Cassandra\TypeInfo\TypeInfo;

final class Inet extends ValueReadableWithLength {
    /** The value in its packed 4- or 16-byte wire form. */
    private readonly string $binary;

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    final public function __construct(string $value, bool $isInBinaryForm = false) {

        if ($isInBinaryForm) {
            $binaryLength = strlen($value);

            if ($binaryLength !== 4 && $binaryLength !== 16) {
                throw new ValueException(
                    'Invalid inet value; expected a IPv4 or IPv6 address in binary form (4 or 16 bytes)',
                    ExceptionCode::VALUE_INET_INVALID_ADDRESS->value,
                    [
                        'value' => $value,
                        'length' => $binaryLength,
                    ]
                );
            }

            $this->binary = $value;

            return;
        }

        // inet_pton() raises a native ValueError for a null byte rather than
        // reporting an invalid address with false, so that one is settled here.
        if (!str_contains($value, "\0")) {
            $binary = inet_pton($value);
            if ($binary !== false) {
                $this->binary = $binary;

                return;
            }
        }

        throw new ValueException(
            'Invalid inet value; expected an IPv4 or IPv6 address',
            ExceptionCode::VALUE_INET_INVALID_ADDRESS->value,
            [
                'value' => $value,
            ]
        );
    }

    #[\Override]
    public static function allowsEmpty(): bool {
        return true;
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public static function fromBinary(
        string $binary,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): ?static {

        if (self::emptyBinaryIsNull($binary)) {
            return null;
        }

        return new static($binary, isInBinaryForm: true);
    }

    /**
     * @param mixed $value
     *
     * @throws \Cassandra\Exception\ValueException
     */
    #[\Override]
    public static function fromMixedValue(mixed $value, ?TypeInfo $typeInfo = null): static {
        if (!is_string($value)) {
            throw new ValueException('Invalid inet value; expected string', ExceptionCode::VALUE_INET_INVALID_VALUE_TYPE->value, [
                'value_type' => gettype($value),
            ]);
        }

        return new static($value, isInBinaryForm: false);
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ResponseException
     */
    #[\Override]
    final public static function fromStream(
        StreamReader $stream,
        ?int $length = null,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): static {

        if ($length !== 4 && $length !== 16) {
            throw new ValueException(
                message: 'Invalid inet length byte',
                code: ExceptionCode::VALUE_INET_INVALID_LENGTH->value,
                context: [
                    'method' => __METHOD__,
                    'address_length' => $length,
                    'offset' => $stream->pos(),
                ]
            );
        }

        $binary = $stream->read($length);

        return new static($binary, isInBinaryForm: true);
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     */
    final public static function fromValue(string $value): static {
        return new static($value, isInBinaryForm: false);
    }

    #[\Override]
    public function getBinary(): string {
        return $this->binary;
    }

    #[\Override]
    public function getType(): Type {
        return Type::INET;
    }

    #[\Override]
    public function getValue(): string {
        $inet = inet_ntop($this->binary);

        if ($inet === false) {
            throw new ValueException('Cannot convert inet binary to string', ExceptionCode::VALUE_INET_TO_STRING_FAILED->value, [
                'binary_length' => strlen($this->binary),
            ]);
        }

        return $inet;
    }

    #[\Override]
    public static function isEmptyValueMeaningless(): bool {
        return true;
    }

    #[\Override]
    final public static function requiresDefinition(): bool {
        return false;
    }
}
