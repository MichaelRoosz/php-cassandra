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
         * Every positive value the protocol can carry is sent unchanged. Zero
         * and negative values are invalid because they cannot describe a useful
         * result page, and a value past {@see RequestOptions::INT32_MAX} is
         * invalid because the option goes out as a signed 32-bit `[int]`: it
         * would otherwise be truncated to its low four bytes and reach the
         * coordinator as some other page size entirely.
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
        self::assertValidNowInSeconds($nowInSeconds);

        if ($pageSize !== null && ($pageSize <= 0 || $pageSize > self::INT32_MAX)) {
            throw new RequestException(
                'Invalid page size: it must be greater than zero and fit in a signed 32-bit integer, or be null to use the server default',
                ExceptionCode::REQUEST_INVALID_PAGE_SIZE->value,
                [
                    'page_size' => $pageSize,
                    'minimum' => 1,
                    'maximum' => self::INT32_MAX,
                ]
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
