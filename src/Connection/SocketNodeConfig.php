<?php

declare(strict_types=1);

namespace Cassandra\Connection;

final class SocketNodeConfig extends NodeConfig {
    /**
     * Receive/send timeouts of the transport. Both are stall timeouts: they
     * bound how long the socket makes no progress at all, not how long a whole
     * request body takes, so a large frame on a slow link does not trip them.
     *
     * The receive default is deliberately above Cassandra's own coordinator
     * timeouts (range_request_timeout and request_timeout default to 10s), so
     * that the server gets a chance to answer with a proper error instead of
     * the client tearing the connection down first. Operations with a higher
     * server-side timeout — TRUNCATE defaults to 60s — need a larger value.
     */
    public const DEFAULT_SO_RCVTIMEO = ['sec' => 15, 'usec' => 0];
    public const DEFAULT_SO_SNDTIMEO = ['sec' => 10, 'usec' => 0];

    /**
     * @var array<int|array<mixed>> $socketOptions
     * see https://www.php.net/manual/en/function.socket-get-option.php
     */
    public readonly array $socketOptions;

    /**
     * @param array<int|array<mixed>> $socketOptions
     * see https://www.php.net/manual/en/function.socket-get-option.php
     *
     * SO_RCVTIMEO / SO_SNDTIMEO drive the receive/send timeouts of the
     * transport. Both the 'sec' and the 'usec' component are honoured, so
     * sub-second timeouts work; `['sec' => 0, 'usec' => 0]` disables the
     * timeout, matching the meaning of the socket option itself.
     *
     * Disabling SO_RCVTIMEO is not recommended: a deadline is only noticed once
     * the read the client is blocked in returns, so the receive timeout is also
     * how often request timeouts and heartbeats get to be checked. Without it a
     * silent server leaves the client blocked in a read forever, and neither
     * {@see \Cassandra\Connection\ConnectionOptions::$requestTimeoutInSeconds}
     * nor the heartbeat can fire. Lower it instead if you want tighter
     * deadlines — with the default of 15s, a request timeout of 30s fires
     * somewhere between 30s and 45s.
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
        /**
         * Timeout for establishing the connection, in seconds. Fractional
         * values are allowed; it must be greater than zero, as an unbounded
         * connect would let an unreachable host wedge the client for as long
         * as the kernel keeps retrying.
         */
        public readonly float $connectTimeoutInSeconds = 5,
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
