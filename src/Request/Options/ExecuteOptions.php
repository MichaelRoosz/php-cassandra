<?php

declare(strict_types=1);

namespace Cassandra\Request\Options;

use Cassandra\SerialConsistency;

final class ExecuteOptions extends QueryOptions {
    public function __construct(
        public readonly ?bool $skipMetadata = null,
        ?int $pageSize = null,
        ?string $pagingState = null,
        ?SerialConsistency $serialConsistency = null,
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

    #[\Override]
    public function withNamesForValues(bool $namesForValues): self {
        return new self(
            skipMetadata: $this->skipMetadata,
            pageSize: $this->pageSize,
            pagingState: $this->pagingState,
            serialConsistency: $this->serialConsistency,
            defaultTimestamp: $this->defaultTimestamp,
            namesForValues: $namesForValues,
            keyspace: $this->keyspace,
            nowInSeconds: $this->nowInSeconds,
        );
    }

    public function withPagingState(string $pagingState): self {
        return new self(
            skipMetadata: $this->skipMetadata,
            pageSize: $this->pageSize,
            pagingState: $pagingState,
            serialConsistency: $this->serialConsistency,
            defaultTimestamp: $this->defaultTimestamp,
            namesForValues: $this->namesForValues,
            keyspace: $this->keyspace,
            nowInSeconds: $this->nowInSeconds
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
