<?php

declare(strict_types=1);

namespace Cassandra\Type;

abstract class TypeReadableWithoutLength extends TypeBase {
    #[\Override]
    final public static function fixedLength(): int {
        return -1;
    }

    #[\Override]
    final public static function hasFixedLength(): bool {
        return false;
    }

    #[\Override]
    final public static function isReadableWithoutLength(): bool {
        return true;
    }

    #[\Override]
    final public static function isSerializedAsFixedLength(): bool {
        return false;
    }
}
