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

    final protected static function maximumCollectionEntryCount(
        int $remainingLength,
        ?int $declaredLength,
        int $minimumBytesPerEntry,
    ): int {

        $available = max(0, $remainingLength);

        if ($declaredLength !== null) {
            $available = min($available, max(0, $declaredLength - 4));
        }

        return intdiv($available, $minimumBytesPerEntry);
    }
}
