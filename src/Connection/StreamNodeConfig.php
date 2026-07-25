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
        public readonly float $timeoutInSeconds = 30,
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
