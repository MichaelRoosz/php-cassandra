<?php

declare(strict_types=1);

namespace Cassandra\Connection;

final class SocketNodeConfig extends NodeConfig {
    public const DEFAULT_SO_RCVTIMEO = ['sec' => 30, 'usec' => 0];
    public const DEFAULT_SO_SNDTIMEO = ['sec' => 5, 'usec' => 0];

    /** 
     * @var array<int|array<mixed>> $socketOptions
     * see https://www.php.net/manual/en/function.socket-get-option.php
     */
    public readonly array $socketOptions;

    /** 
     * @param array<int|array<mixed>> $socketOptions
     * see https://www.php.net/manual/en/function.socket-get-option.php
     */
    public function __construct(
        string $host = 'localhost',
        int $port = 9042,
        string $username = '',
        string $password = '',
        array $socketOptions = [
            SO_RCVTIMEO => self::DEFAULT_SO_RCVTIMEO,
            SO_SNDTIMEO => self::DEFAULT_SO_SNDTIMEO,
        ],
    ) {
        parent::__construct(
            host: $host,
            port: $port,
            username: $username,
            password: $password,
        );

        if (!isset($socketOptions[SO_RCVTIMEO])) {
            $socketOptions[SO_RCVTIMEO] = self::DEFAULT_SO_RCVTIMEO;
        }

        if (!isset($socketOptions[SO_SNDTIMEO])) {
            $socketOptions[SO_SNDTIMEO] = self::DEFAULT_SO_SNDTIMEO;
        }

        $this->socketOptions = $socketOptions;
    }

    #[\Override]
    public function getNodeClass(): string {
        return Socket::class;
    }
}
