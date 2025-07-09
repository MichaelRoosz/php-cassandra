<?php

declare(strict_types=1);

namespace Cassandra\Request\Options;

class QueryOptions extends RequestOptions {
    public function __construct(
        public ?bool $skipMetadata = null,
        public ?int $pageSize = null,
        public ?string $pagingState = null,
        public ?int $serialConsistency = null,
        public ?int $defaultTimestamp = null,
        public ?bool $namesForValues = null,
        public ?string $keyspace = null,
        public ?int $nowInSeconds = null,
    ) {
    }
}
