<?php

declare(strict_types=1);

namespace Cassandra\Value;

abstract class ValueReadableWithoutLength extends ValueBase {
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
