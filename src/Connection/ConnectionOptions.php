<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Protocol\ProtocolVersion;
use Cassandra\ReleaseConstants;

final class ConnectionOptions {
    public function __construct(
        public readonly bool $enableCompression = false,
        public readonly bool $throwOnOverload = false,
        public readonly NodeSelectionStrategy $nodeSelectionStrategy = NodeSelectionStrategy::Random,
        public readonly int $preparedResultCacheSize = 100,

        /** @var ProtocolVersion[] $allowedProtocolVersions */
        public readonly array $allowedProtocolVersions = ProtocolVersion::PREFRED_ORDER,
        public readonly ProtocolVersion $initialProtocolVersion = ProtocolVersion::V4,

        /**
         * How long to wait for the server's answer to a request, in seconds,
         * before giving up with a {@see \Cassandra\Exception\RequestTimeoutException}.
         * Null waits indefinitely.
         *
         * This is the client-side counterpart of Cassandra's own coordinator
         * timeouts, and it is what governs a slow query — not the transport
         * timeouts, which only bound how long a connection may be completely
         * silent. Operations that are legitimately slower than this default,
         * TRUNCATE above all (60s server-side), need a larger value; see
         * {@see \Cassandra\Connection::setRequestTimeout()} and the per-call
         * argument of {@see \Cassandra\Connection::syncRequest()}.
         */
        public readonly ?float $requestTimeoutInSeconds = 30,

        /**
         * How many timed-out async statements a connection may accumulate
         * before it is closed instead.
         *
         * Giving up on an async statement leaves the connection and its other
         * statements alone, at the cost of holding that statement's stream id
         * back until its late answer arrives — the id cannot be reused before
         * then without risking one request being resolved by another's
         * response. A server that keeps timing out would therefore tie up more
         * and more ids, so past this many the connection is dropped and starts
         * over with a clean set.
         */
        public readonly int $maxOrphanedStreams = 24,

        /**
         * How long the connection may stay silent before an OPTIONS heartbeat
         * is sent to prove it is still alive, in seconds. Null disables
         * heartbeats.
         *
         * This is the only thing that can tell a dead connection from a quiet
         * one, whether the client is waiting for an event or for the answer to
         * a slow request — both look identical at the socket. Because stream
         * ids are multiplexed, the heartbeat is answered on its own stream
         * while a slow request is still being computed, so a broken connection
         * surfaces after roughly this interval plus the heartbeat timeout,
         * however generous the request timeout is.
         *
         * Roughly, because a silent connection is only looked at between reads:
         * the transport's stall window is how often the interval and the
         * timeout below get to be judged, so both are rounded up to a multiple
         * of it. Lower that window if the heartbeat has to bite sooner.
         */
        public readonly ?float $heartbeatIntervalInSeconds = 30,

        /**
         * How long to wait for the answer to a heartbeat before treating the
         * connection as dead, in seconds. Like the interval above, it is judged
         * at the granularity of the transport's stall window, so setting it
         * below that window does not make detection any quicker.
         */
        public readonly float $heartbeatTimeoutInSeconds = 5,
    ) {
    }

    /**
     * @return array<string,string>
     */
    public function asStartupOptions(): array {

        $options = [
            'CQL_VERSION' => '3.0.0',
            'DRIVER_NAME' => ReleaseConstants::PHP_CASSANDRA_DRIVER_NAME,
            'DRIVER_VERSION' => ReleaseConstants::PHP_CASSANDRA_DRIVER_VERSION,
        ];

        if ($this->enableCompression) {
            $options['COMPRESSION'] = 'lz4';
        }

        if ($this->throwOnOverload) {
            $options['THROW_ON_OVERLOAD'] = '1';
        }

        return $options;
    }

    /**
     * @deprecated Use asStartupOptions() instead.
     * @return array<string,string>
     */
    public function toArray(): array {
        return $this->asStartupOptions();
    }
}
