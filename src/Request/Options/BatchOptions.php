<?php

declare(strict_types=1);

namespace Cassandra\Request\Options;

final class BatchOptions extends QueryOptions {
    public function __construct(
        public ?int $serialConsistency = null,
        public ?int $defaultTimestamp = null,
        public ?string $keyspace = null,
        public ?int $nowInSeconds = null,
    ) {
    }
}
