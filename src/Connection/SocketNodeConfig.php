<?php

declare(strict_types=1);

namespace Cassandra\Connection;

final class SocketNodeConfig extends NodeConfig {
    public function __construct(
        string $host = 'localhost',
        int $port = 9042,
        string $username = '',
        string $password = '',

        /** 
         * @var array<int,int|array<mixed>> $socketOptions
         * see https://www.php.net/manual/en/function.socket-get-option.php
         */
        public readonly array $socketOptions = [
            SO_RCVTIMEO => ['sec' => 30, 'usec' => 0],
            SO_SNDTIMEO => ['sec' => 5, 'usec' => 0],
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
        return Socket::class;
    }
}
