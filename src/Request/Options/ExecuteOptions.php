<?php

declare(strict_types=1);

namespace Cassandra\Request\Options;

final class ExecuteOptions extends QueryOptions {
    public function __construct(
        public readonly ?bool $skipMetadata = null,
        ?int $pageSize = null,
        ?string $pagingState = null,
        ?int $serialConsistency = null,
        ?int $defaultTimestamp = null,
        ?bool $namesForValues = null,
        ?string $keyspace = null,
        ?int $nowInSeconds = null,
    ) {
        parent::__construct(
            pageSize: $pageSize,
            pagingState: $pagingState,
            serialConsistency: $serialConsistency,
            defaultTimestamp: $defaultTimestamp,
            namesForValues: $namesForValues,
            keyspace: $keyspace,
            nowInSeconds: $nowInSeconds
        );
    }

    public function withSkipMetadata(bool $skipMetadata): self {
        return new self(
            skipMetadata: $skipMetadata,
            pageSize: $this->pageSize,
            pagingState: $this->pagingState,
            serialConsistency: $this->serialConsistency,
            defaultTimestamp: $this->defaultTimestamp,
            namesForValues: $this->namesForValues,
            keyspace: $this->keyspace,
            nowInSeconds: $this->nowInSeconds
        );
    }
}
