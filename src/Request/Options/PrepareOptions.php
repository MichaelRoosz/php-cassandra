<?php

declare(strict_types=1);

namespace Cassandra\Request\Options;

final class PrepareOptions extends RequestOptions {
    /**
     * @throws \Cassandra\Exception\RequestException
     */
    public function __construct(
        public readonly ?string $keyspace = null,

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
            keyspace: $keyspace,
            requestTimeoutInSeconds: $this->requestTimeoutInSeconds,
        );
    }
}
