<?php

declare(strict_types=1);

namespace Cassandra\Request\Options;

use Cassandra\SerialConsistency;

final class BatchOptions extends RequestOptions {
    public function __construct(
        public readonly ?SerialConsistency $serialConsistency = null,
        public readonly ?int $defaultTimestamp = null,
        public readonly ?string $keyspace = null,
        public readonly ?int $nowInSeconds = null,
    ) {
    }
}
