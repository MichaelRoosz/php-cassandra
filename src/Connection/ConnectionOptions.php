<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Exception\ConnectionException;
use Cassandra\Exception\ExceptionCode;
use Cassandra\Protocol\ProtocolVersion;
use Cassandra\ReleaseConstants;

final class ConnectionOptions {
    public readonly ProtocolVersion $initialProtocolVersion;

    /**
     * @throws \Cassandra\Exception\ConnectionException
     */
    public function __construct(
        /**
         * Whether to ask the node for a compressed connection.
         *
         * A request rather than a requirement: a node whose SUPPORTED names no
         * compression at all is talked to uncompressed rather than refused, so a
         * connection that came up is not proof that it is compressed. Only a
         * node that lists algorithms without the one this driver speaks (lz4) is
         * a configuration to correct, and that fails the handshake; see
         * {@see Handshake::negotiate()}.
         */
        public readonly bool $enableCompression = false,
        public readonly bool $throwOnOverload = false,
        public readonly NodeSelectionStrategy $nodeSelectionStrategy = NodeSelectionStrategy::Random,
        public readonly int $preparedResultCacheSize = 100,

        /** @var ProtocolVersion[] $allowedProtocolVersions */
        public readonly array $allowedProtocolVersions = ProtocolVersion::PREFERRED_ORDER,
        ?ProtocolVersion $initialProtocolVersion = null,

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
         * surfaces after this interval plus the heartbeat timeout, however
         * generous the request timeout is.
         *
         * Reads are bounded by when the probe is next due, so this holds even
         * during a wait that has no deadline of its own and whatever the
         * transport timeouts are set to — including not at all.
         *
         * One corner is beyond it: a connection that has handed out every stream
         * id the protocol allows cannot send the probe either, since claiming an
         * id would mean waiting for one. If on top of that every one of those
         * requests waits indefinitely ({@see self::$requestTimeoutInSeconds}
         * null, and no timeout on any of them) and the caller waits without a
         * timeout as well, nothing is left that could notice the node is gone.
         * Any one of the three breaks it: a bounded request timeout, whether the
         * connection default or one on a single request, lets the requests run
         * out and orphan their ids until
         * {@see self::$maxOrphanedStreams} replaces the connection, and a finite
         * timeout on the wait ends it on its own.
         */
        public readonly ?float $heartbeatIntervalInSeconds = 30,

        /**
         * How long to wait for the answer to a heartbeat before treating the
         * connection as dead, in seconds.
         */
        public readonly float $heartbeatTimeoutInSeconds = 5,
    ) {
        if ($allowedProtocolVersions === []) {
            throw new ConnectionException(
                'Invalid protocol versions: at least one allowed version is required',
                ExceptionCode::CONNECTION_INVALID_OPTIONS->value
            );
        }

        foreach ($allowedProtocolVersions as $version) {
            if (!$version instanceof ProtocolVersion) {
                throw new ConnectionException(
                    'Invalid protocol versions: every allowed version must be a ProtocolVersion',
                    ExceptionCode::CONNECTION_INVALID_OPTIONS->value,
                    ['invalid_type' => get_debug_type($version)]
                );
            }
        }

        if ($initialProtocolVersion === null) {
            if (in_array(ProtocolVersion::V4, $allowedProtocolVersions, true)) {
                $initialProtocolVersion = ProtocolVersion::V4;
            } else {
                /** @var ProtocolVersion $initialProtocolVersion */
                $initialProtocolVersion = reset($allowedProtocolVersions);
                foreach ($allowedProtocolVersions as $version) {
                    if ($version->value < $initialProtocolVersion->value) {
                        $initialProtocolVersion = $version;
                    }
                }
            }
        }

        if (!in_array($initialProtocolVersion, $allowedProtocolVersions, true)) {
            throw new ConnectionException(
                'Invalid initial protocol version: it must be included in the allowed versions',
                ExceptionCode::CONNECTION_INVALID_OPTIONS->value,
                ['initial_protocol_version' => $initialProtocolVersion->inOptionFormat()]
            );
        }

        $this->initialProtocolVersion = $initialProtocolVersion;
        // Rejected rather than clamped, because every non-positive value here
        // means something the caller cannot have wanted: a request that is out
        // of time before it is sent, a probe on every single read, or a
        // heartbeat that is overdue the moment it goes out and so declares a
        // healthy node dead. "No timeout" is spelled null for the two that
        // allow it, and the heartbeat timeout only matters when the interval
        // has already turned heartbeats on.
        //
        // NAN is tested for separately because it passes every comparison below:
        // it is neither greater than zero nor less than or equal to it. Left
        // through, it would reach {@see Deadline::at()} and produce a deadline no
        // clock ever compares greater than, i.e. a request that silently waits
        // for good — and it would be the one way to get that past
        // {@see Deadline::assertValidRequestTimeout()}, which refuses it for
        // {@see \Cassandra\Connection::setRequestTimeout()} and for the per-call
        // arguments.
        if ($requestTimeoutInSeconds !== null && is_nan($requestTimeoutInSeconds)) {
            throw new ConnectionException(
                'Invalid request timeout: it must be a number greater than zero, or null to wait indefinitely',
                ExceptionCode::CONNECTION_INVALID_OPTIONS->value,
                [
                    'request_timeout_seconds' => 'NAN',
                ]
            );
        }

        if ($requestTimeoutInSeconds !== null && $requestTimeoutInSeconds <= 0.0) {
            throw new ConnectionException(
                'Invalid request timeout: it must be greater than zero, or null to wait indefinitely',
                ExceptionCode::CONNECTION_INVALID_OPTIONS->value,
                [
                    'request_timeout_seconds' => $requestTimeoutInSeconds,
                ]
            );
        }

        // The two heartbeat values have to be finite as well, which the request
        // timeout does not: INF there asks for an unbounded wait and
        // {@see Deadline::at()} normalises it to exactly that. Here it would
        // instead put INF where a deadline is expected —
        // {@see HeartbeatMonitor::getNextActionAt()} hands it to every read — and
        // a read bounded by INF is not an unbounded read but a bounded one that
        // never comes due, which is what tells {@see Session::readResponseUntil()}
        // to swallow the transport's stall window rather than treat it as the
        // last judgement available. Heartbeats are switched off with null.
        if ($heartbeatIntervalInSeconds !== null && !is_finite($heartbeatIntervalInSeconds)) {
            throw new ConnectionException(
                'Invalid heartbeat interval: it must be a finite number greater than zero, or null to disable heartbeats',
                ExceptionCode::CONNECTION_INVALID_OPTIONS->value,
                [
                    'heartbeat_interval_seconds' => is_nan($heartbeatIntervalInSeconds) ? 'NAN' : ($heartbeatIntervalInSeconds > 0.0 ? 'INF' : '-INF'),
                ]
            );
        }

        if ($heartbeatIntervalInSeconds !== null && $heartbeatIntervalInSeconds <= 0.0) {
            throw new ConnectionException(
                'Invalid heartbeat interval: it must be greater than zero, or null to disable heartbeats',
                ExceptionCode::CONNECTION_INVALID_OPTIONS->value,
                [
                    'heartbeat_interval_seconds' => $heartbeatIntervalInSeconds,
                ]
            );
        }

        if ($heartbeatIntervalInSeconds !== null && !is_finite($heartbeatTimeoutInSeconds)) {
            throw new ConnectionException(
                'Invalid heartbeat timeout: it must be a finite number greater than zero',
                ExceptionCode::CONNECTION_INVALID_OPTIONS->value,
                [
                    'heartbeat_timeout_seconds' => is_nan($heartbeatTimeoutInSeconds) ? 'NAN' : ($heartbeatTimeoutInSeconds > 0.0 ? 'INF' : '-INF'),
                ]
            );
        }

        if ($heartbeatIntervalInSeconds !== null && $heartbeatTimeoutInSeconds <= 0.0) {
            throw new ConnectionException(
                'Invalid heartbeat timeout: it must be greater than zero',
                ExceptionCode::CONNECTION_INVALID_OPTIONS->value,
                [
                    'heartbeat_timeout_seconds' => $heartbeatTimeoutInSeconds,
                ]
            );
        }

        if ($preparedResultCacheSize < 0) {
            throw new ConnectionException(
                'Invalid prepared result cache size: it must be zero or greater, where zero disables the cache',
                ExceptionCode::CONNECTION_INVALID_OPTIONS->value,
                [
                    'prepared_result_cache_size' => $preparedResultCacheSize,
                    'minimum' => 0,
                ]
            );
        }

        if ($maxOrphanedStreams < 0) {
            throw new ConnectionException(
                'Invalid maximum orphaned streams: it must be zero or greater, where zero replaces the connection as soon as one stream id is held back',
                ExceptionCode::CONNECTION_INVALID_OPTIONS->value,
                [
                    'max_orphaned_streams' => $maxOrphanedStreams,
                    'minimum' => 0,
                ]
            );
        }
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
