<?php

declare(strict_types=1);

namespace Cassandra\Request\Options;

use Cassandra\SerialConsistency;

class QueryOptions extends RequestOptions {
    public function __construct(
        public readonly ?int $pageSize = null,
        public readonly ?string $pagingState = null,
        public readonly ?SerialConsistency $serialConsistency = null,
        public readonly ?int $defaultTimestamp = null,
        public readonly ?bool $namesForValues = null,
        public readonly ?string $keyspace = null,
        public readonly ?int $nowInSeconds = null,
    ) {
    }

    public function withNamesForValues(bool $namesForValues): self {
        return new self(
            pageSize: $this->pageSize,
            pagingState: $this->pagingState,
            serialConsistency: $this->serialConsistency,
            defaultTimestamp: $this->defaultTimestamp,
            namesForValues: $namesForValues,
            keyspace: $this->keyspace,
            nowInSeconds: $this->nowInSeconds,
        );
    }
}
