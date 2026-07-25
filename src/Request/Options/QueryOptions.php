<?php

declare(strict_types=1);

namespace Cassandra\Request\Options;

use Cassandra\SerialConsistency;

class QueryOptions extends RequestOptions {
    public function __construct(
        public readonly bool $autoPrepare = true,
        public readonly ?int $pageSize = null,
        public readonly ?string $pagingState = null,
        public readonly ?SerialConsistency $serialConsistency = null,
        public readonly ?int $defaultTimestamp = null,
        public readonly ?bool $namesForValues = null,
        public readonly ?string $keyspace = null,
        public readonly ?int $nowInSeconds = null,

        /**
         * How long to wait for the server to answer this request, in seconds,
         * overriding the connection default. Null uses the connection default.
         */
        public readonly ?float $requestTimeoutInSeconds = null,
    ) {
    }

    public function withKeyspace(string $keyspace): self {
        return new self(
            autoPrepare: $this->autoPrepare,
            pageSize: $this->pageSize,
            pagingState: $this->pagingState,
            serialConsistency: $this->serialConsistency,
            defaultTimestamp: $this->defaultTimestamp,
            namesForValues: $this->namesForValues,
            keyspace: $keyspace,
            nowInSeconds: $this->nowInSeconds,
            requestTimeoutInSeconds: $this->requestTimeoutInSeconds,
        );
    }

    public function withNamesForValues(bool $namesForValues): self {
        return new self(
            autoPrepare: $this->autoPrepare,
            pageSize: $this->pageSize,
            pagingState: $this->pagingState,
            serialConsistency: $this->serialConsistency,
            defaultTimestamp: $this->defaultTimestamp,
            namesForValues: $namesForValues,
            keyspace: $this->keyspace,
            nowInSeconds: $this->nowInSeconds,
            requestTimeoutInSeconds: $this->requestTimeoutInSeconds,
        );
    }

    public function withPagingState(string $pagingState): self {
        return new self(
            autoPrepare: $this->autoPrepare,
            pageSize: $this->pageSize,
            pagingState: $pagingState,
            serialConsistency: $this->serialConsistency,
            defaultTimestamp: $this->defaultTimestamp,
            namesForValues: $this->namesForValues,
            keyspace: $this->keyspace,
            nowInSeconds: $this->nowInSeconds,
            requestTimeoutInSeconds: $this->requestTimeoutInSeconds,
        );
    }
}
