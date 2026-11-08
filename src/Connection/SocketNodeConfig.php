<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\SocketException;

final class SocketNodeConfig extends NodeConfig {
    public const DEFAULT_SO_RCVTIMEO = ['sec' => 10, 'usec' => 0];
    public const DEFAULT_SO_SNDTIMEO = ['sec' => 5, 'usec' => 0];

    /** 
     * @var array<int|array<mixed>> $socketOptions
     * see https://www.php.net/manual/en/function.socket-get-option.php
     */
    public readonly array $socketOptions;

    /** 
     * @param array<int|array<mixed>> $socketOptions
     * see https://www.php.net/manual/en/function.socket-get-option.php;
     * SO_RCVTIMEO and SO_SNDTIMEO default to DEFAULT_SO_RCVTIMEO and DEFAULT_SO_SNDTIMEO
     *
     * @throws \Cassandra\Exception\SocketException
     */
    public function __construct(
        string $host = 'localhost',
        int $port = 9042,
        string $username = '',
        string $password = '',
        array $socketOptions = [],
    ) {
        // Checked before SO_RCVTIMEO is named: without the extension the
        // constant does not exist, and PHP raises an Error of its own.
        if (!extension_loaded('sockets')) {
            throw new SocketException(
                'The sockets extension is required for SocketNodeConfig; use StreamNodeConfig when ext-sockets is unavailable',
                ExceptionCode::SOCKET_EXTENSION_NOT_LOADED->value,
                [
                    'extension' => 'sockets',
                    'alternative_config' => StreamNodeConfig::class,
                ]
            );
        }

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
