<?php

declare(strict_types=1);

namespace Cassandra\Request\Options;

use Cassandra\SerialConsistency;

final class BatchOptions extends RequestOptions {
    /**
     * @throws \Cassandra\Exception\RequestException
     */
    public function __construct(
        public readonly ?SerialConsistency $serialConsistency = null,
        public readonly ?int $defaultTimestamp = null,
        public readonly ?string $keyspace = null,
        public readonly ?int $nowInSeconds = null,

        /**
         * How long to wait for the server to answer this request, in seconds,
         * overriding the connection default. Null uses the connection default.
         */
        public readonly ?float $requestTimeoutInSeconds = null,
    ) {
        self::assertValidRequestTimeout($requestTimeoutInSeconds);
    }

    /**
     * @throws \Cassandra\Exception\RequestException
     */
    public function withKeyspace(string $keyspace): self {
        return new self(
            serialConsistency: $this->serialConsistency,
            defaultTimestamp: $this->defaultTimestamp,
            keyspace: $keyspace,
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
            serialConsistency: $this->serialConsistency,
            defaultTimestamp: $this->defaultTimestamp,
            keyspace: null,
            nowInSeconds: $this->nowInSeconds,
            requestTimeoutInSeconds: $this->requestTimeoutInSeconds,
        );
    }
}
