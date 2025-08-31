<?php

declare(strict_types=1);

namespace Cassandra\Type;

use Cassandra\Response\StreamReader;
use Cassandra\TypeInfo\TypeInfo;

abstract class TypeWithFixedLength extends TypeBase {
    /**
     * @throws \Cassandra\Type\Exception
     */
    #[\Override]
    abstract public static function fromBinary(string $binary, ?TypeInfo $typeInfo = null): static;

    /**
     * @throws \Cassandra\Type\Exception
     * @throws \Cassandra\Response\Exception
     */
    #[\Override]
    public static function fromStream(StreamReader $stream, ?int $length = null, ?TypeInfo $typeInfo = null): static {

        $binary = $stream->read(static::fixedLength());

        return static::fromBinary($binary);
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
