<?php

declare(strict_types=1);

namespace Cassandra\Request\Options;

class QueryOptions extends RequestOptions {
    public function __construct(
        public readonly ?int $pageSize = null,
        public readonly ?string $pagingState = null,
        public readonly ?int $serialConsistency = null,
        public readonly ?int $defaultTimestamp = null,
        public readonly ?bool $namesForValues = null,
        public readonly ?string $keyspace = null,
        public readonly ?int $nowInSeconds = null,
    ) {
    }
}
