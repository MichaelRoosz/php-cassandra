<?php

declare(strict_types=1);

namespace Cassandra\Request\Options;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\RequestException;
use Cassandra\SerialConsistency;

class QueryOptions extends RequestOptions {
    /**
     * @throws \Cassandra\Exception\RequestException
     */
    public function __construct(
        public readonly bool $autoPrepare = true,

        /**
         * How many rows the node puts in one page of the result, or null to
         * leave it to the node's own default.
         *
         * Every positive value is sent unchanged. Zero and negative values are
         * invalid because they cannot describe a useful result page.
         */
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
        self::assertValidRequestTimeout($requestTimeoutInSeconds);

        if ($pageSize !== null && $pageSize <= 0) {
            throw new RequestException(
                'Invalid page size: it must be greater than zero, or null to use the server default',
                ExceptionCode::REQUEST_INVALID_PAGE_SIZE->value,
                ['page_size' => $pageSize]
            );
        }
    }

    /**
     * @throws \Cassandra\Exception\RequestException
     */
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

    /**
     * @throws \Cassandra\Exception\RequestException
     */
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

    /**
     * The same options with no keyspace at all, for a request that has to go
     * out on a protocol version where the keyspace option does not exist; see
     * {@see \Cassandra\Request\Request::clearDefaultKeyspace()}.
     *
     * @throws \Cassandra\Exception\RequestException
     */
    public function withoutKeyspace(): self {
        return new self(
            autoPrepare: $this->autoPrepare,
            pageSize: $this->pageSize,
            pagingState: $this->pagingState,
            serialConsistency: $this->serialConsistency,
            defaultTimestamp: $this->defaultTimestamp,
            namesForValues: $this->namesForValues,
            keyspace: null,
            nowInSeconds: $this->nowInSeconds,
            requestTimeoutInSeconds: $this->requestTimeoutInSeconds,
        );
    }

    /**
     * @throws \Cassandra\Exception\RequestException
     */
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
