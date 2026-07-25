<?php

declare(strict_types=1);

namespace Cassandra\Connection;

final class StreamNodeConfig extends NodeConfig {
    public function __construct(
        string $host = 'localhost',
        int $port = 9042,
        string $username = '',
        string $password = '',
        public readonly float $connectTimeoutInSeconds = 5,

        /**
         * Receive/send timeout of the transport, in seconds. Fractional values
         * are honoured; a value of 0 (or less) disables the timeout.
         *
         * This is a stall timeout: it bounds how long the stream makes no
         * progress at all, not how long a whole request body takes, so a large
         * frame on a slow link does not trip it.
         *
         * The default is deliberately above Cassandra's own coordinator
         * timeouts (range_request_timeout and request_timeout default to 10s),
         * so that the server gets a chance to answer with a proper error
         * instead of the client tearing the connection down first. Operations
         * with a higher server-side timeout — TRUNCATE defaults to 60s — need
         * a larger value.
         *
         * Disabling it is not recommended: a deadline is only noticed once the
         * read the client is blocked in returns, so this is also how often
         * request timeouts and heartbeats get to be checked. At 0 a silent
         * server leaves the client blocked in a read forever, and neither
         * {@see ConnectionOptions::$requestTimeoutInSeconds} nor the heartbeat
         * can fire. Lower it instead if you want tighter deadlines — with the
         * default of 15s, a request timeout of 30s fires somewhere between 30s
         * and 45s.
         */
        public readonly float $timeoutInSeconds = 15,

        public readonly bool $persistent = false,

        /**
         * @var array<string,mixed> $sslOptions
         * see https://www.php.net/manual/en/context.ssl.php
         *
         * Passing a non-empty array enables TLS: if the host has no explicit
         * scheme, "tls://" is used automatically. PHP's secure defaults
         * (verify_peer, verify_peer_name enabled; allow_self_signed disabled)
         * apply to any option not set here. An empty array connects in
         * plaintext unless the host itself carries a "tls://"/"ssl://" scheme.
         */
        public readonly array $sslOptions = [],
    ) {
        parent::__construct(
            host: $host,
            port: $port,
            username: $username,
            password: $password,
        );
    }

    #[\Override]
    public function getNodeClass(): string {
        return Stream::class;
    }
}
