<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Response\StreamReader;
use Cassandra\TypeInfo\TypeInfo;

abstract class ValueWithFixedLength extends ValueBase {
    /**
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    #[\Override]
    abstract public static function fromBinary(
        string $binary,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): static;

    /**
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ResponseException
     */
    #[\Override]
    public static function fromStream(
        StreamReader $stream,
        ?int $length = null,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): static {

        // A declared length that disagrees with the type's fixed size means the
        // stream and our idea of the schema have diverged; reading fixedLength()
        // bytes anyway would silently corrupt every value after this one.
        if ($length !== null && $length !== static::fixedLength()) {
            throw new ValueException('Invalid data length for fixed-length type', ExceptionCode::VALUE_INVALID_DATA_LENGTH->value, [
                'class' => static::class,
                'length' => $length,
                'expected_length' => static::fixedLength(),
            ]);
        }

        $binary = $stream->read(static::fixedLength());

        return static::fromBinary($binary, typeInfo: $typeInfo, valueEncodeConfig: $valueEncodeConfig);
    }

    #[\Override]
    final public static function hasFixedLength(): bool {
        return true;
    }

    #[\Override]
    final public static function isReadableWithoutLength(): bool {
        return true;
    }
    /**
     * Reject malformed cells before passing them to unpack(). Besides accepting
     * trailing bytes, unpack() emits a warning for short input; an application
     * error handler may turn that warning into a native ErrorException.
     *
     * @throws \Cassandra\Exception\ValueException
     */
    final protected static function assertExactBinaryLength(string $binary): void {
        $length = strlen($binary);
        $expectedLength = static::fixedLength();
        if ($length === $expectedLength) {
            return;
        }

        throw new ValueException('Invalid data length for fixed-length type', ExceptionCode::VALUE_INVALID_DATA_LENGTH->value, [
            'class' => static::class,
            'length' => $length,
            'expected_length' => $expectedLength,
        ]);
    }
}
