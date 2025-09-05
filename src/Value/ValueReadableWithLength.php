<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\ExceptionCode;
use Cassandra\Response\StreamReader;
use Cassandra\TypeInfo\TypeInfo;

abstract class ValueReadableWithLength extends ValueBase {
    #[\Override]
    final public static function fixedLength(): int {
        return -1;
    }

    /**
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    abstract public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static;

    /**
     * @throws \Cassandra\Value\Exception
     * @throws \Cassandra\Response\Exception
     */
    #[\Override]
    public static function fromStream(StreamReader $stream, ?int $length = null, ?TypeInfo $typeInfo = null): static {
        if ($length === null || $length < 0) {
            throw new Exception('Invalid data length', ExceptionCode::VALUE_INVALID_DATA_LENGTH->value, [
                'length' => $length,
            ]);
        }

        $binary = $stream->read($length);

        return static::fromBinary($binary);
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
