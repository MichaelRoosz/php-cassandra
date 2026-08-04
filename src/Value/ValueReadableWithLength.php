<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\ValueException;
use Cassandra\Response\StreamReader;
use Cassandra\TypeInfo\TypeInfo;

abstract class ValueReadableWithLength extends ValueBase {
    #[\Override]
    final public static function fixedLength(): int {
        return -1;
    }

    /**
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     */
    #[\Override]
    abstract public static function fromBinary(
        string $binary,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): ?static;

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
        if ($length === null || $length < 0) {
            throw new ValueException('Invalid data length', ExceptionCode::VALUE_INVALID_DATA_LENGTH->value, [
                'length' => $length,
            ]);
        }

        $binary = $stream->read($length);

        $value = static::fromBinary($binary, typeInfo: $typeInfo, valueEncodeConfig: $valueEncodeConfig);

        // fromBinary() reports null for an empty value of a type that declares
        // isEmptyValueMeaningless(), which is a reading this method has no way
        // to hand back: it produces a value object.
        // The cell length is known one level up, where
        // {@see \Cassandra\Response\StreamReader::emptyValue()} settles an empty
        // cell before any decoder is reached — so this is the length being
        // wrong rather than a value that happens to be null.
        if ($value === null) {
            throw new ValueException('Invalid data length', ExceptionCode::VALUE_INVALID_DATA_LENGTH->value, [
                'class' => static::class,
                'length' => $length,
                'note' => 'an empty value of this type denotes null and has no value object',
            ]);
        }

        return $value;
    }

    #[\Override]
    final public static function hasFixedLength(): bool {
        return false;
    }

    #[\Override]
    final public static function isReadableWithoutLength(): bool {
        return false;
    }

    #[\Override]
    final public static function isSerializedAsFixedLength(): bool {
        return false;
    }
}
