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
         */
        public readonly array $sslOptions = [
            'verify_peer' => true,
            'verify_peer_name' => true,
            'allow_self_signed' => false,
        ],
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
