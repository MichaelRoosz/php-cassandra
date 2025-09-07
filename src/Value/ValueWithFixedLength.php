<?php

declare(strict_types=1);

namespace Cassandra\Value;

use Cassandra\Response\StreamReader;
use Cassandra\TypeInfo\TypeInfo;

abstract class ValueWithFixedLength extends ValueBase {
    /**
     * @throws \Cassandra\Value\Exception
     */
    #[\Override]
    abstract public static function fromBinary(
        string $binary,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): static;

    /**
     * @throws \Cassandra\Value\Exception
     * @throws \Cassandra\Response\Exception
     */
    #[\Override]
    public static function fromStream(
        StreamReader $stream,
        ?int $length = null,
        ?TypeInfo $typeInfo = null,
        ?ValueEncodeConfig $valueEncodeConfig = null
    ): static {

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
}
