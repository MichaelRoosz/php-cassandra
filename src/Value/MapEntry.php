<?php

declare(strict_types=1);

namespace Cassandra\Value;

final class MapEntry {
    public function __construct(
        public readonly mixed $key,
        public readonly mixed $value,
    ) {
    }
}
