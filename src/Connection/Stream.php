<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\StreamException;
use Cassandra\Request\Request;
use ErrorException;
use Throwable;

final class Stream extends NodeImplementation implements IoNode {
    /**
     * How many times in a row stream_select() may fail before the connection is
     * given up on.
     *
     * PHP reports an interrupted select() exactly as it reports a failed one,
     * with no errno to tell the two apart, so a failure is retried rather than
     * taken as fatal — for as long as the stall window allows. A transport
     * configured without a stall window ({@see StreamNodeConfig::$timeoutInSeconds}
     * zero or less) has no such bound at all: the elapsed-time test can never
     * fire against an infinite window, so a select() failing for a real reason
     * would be retried forever, spinning on a connection that cannot recover.
     * This is what ends it — high enough that a burst of signals is still ridden
     * out, finite so that a genuine failure is eventually reported.
     *
     * Counted consecutively: a select() that comes back cleanly, even with
     * nothing to report, clears the tally.
     *
     * The socket transport needs no equivalent. socket_select() reports EINTR
     * through an errno, so it retries only what really is an interruption and
     * fails everything else on the spot.
     */
    private const MAX_CONSECUTIVE_SELECT_FAILURES = 1000;

    /**
     * PHP has no "never time out" value for stream_set_timeout(), so an
     * effectively unreachable one is used when timeouts are disabled.
     */
    private const UNLIMITED_STREAM_TIMEOUT_SECONDS = 365 * 24 * 60 * 60;

    /**
     * The stream timeout currently applied, tracked so that the blocking-mode
     * fallback only re-applies it when it actually changes.
     */
    private ?float $appliedReceiveTimeout = null;

    private StreamNodeConfig $config;
    private bool $isBlockingIo = false;
    private float $receiveTimeout = 10.0;
    private float $sendTimeout = 10.0;

    /**
     * @var ?resource $stream
     */
    private $stream = null;

    /**
     * @throws \Cassandra\Exception\StreamException
     */
    public function __construct(
        NodeConfig $config
    ) {
        if (!($config instanceof StreamNodeConfig)) {
            throw new StreamException(
                message: 'Invalid node configuration type for Stream transport',
                code: ExceptionCode::STREAM_INVALID_CONFIG->value,
                context: [
                    'expected_class' => StreamNodeConfig::class,
                    'actual_class' => get_debug_type($config),
                ]
            );
        }
        $this->config = $config;

        if (!is_finite($config->connectTimeoutInSeconds) || $config->connectTimeoutInSeconds <= 0.0) {
            throw new StreamException(
                message: 'Invalid connect timeout: it must be a finite number greater than zero',
                code: ExceptionCode::STREAM_INVALID_CONFIG->value,
                context: ['connect_timeout_seconds' => $config->connectTimeoutInSeconds]
            );
        }

        if (!is_finite($config->timeoutInSeconds)) {
            throw new StreamException(
                message: 'Invalid stream timeout: it must be a finite number',
                code: ExceptionCode::STREAM_INVALID_CONFIG->value,
                context: ['timeout_seconds' => is_nan($config->timeoutInSeconds) ? 'NAN' : ($config->timeoutInSeconds > 0.0 ? 'INF' : '-INF')]
            );
        }

        // Fractional timeouts are kept as configured; a non-positive value
        // disables the timeout entirely.
        $timeout = $config->timeoutInSeconds > 0.0 ? $config->timeoutInSeconds : self::NO_TIMEOUT;

        $this->sendTimeout = $timeout;
        $this->receiveTimeout = $timeout;
    }

    public function __destruct() {
        $this->close();
    }

    #[\Override]
    public function close(): void {
        if ($this->stream) {
            $stream = $this->stream;
            $this->stream = null;
            fclose($stream);
        }
    }

    /**
     * @throws \Cassandra\Exception\StreamException
     */
    #[\Override]
    public function connect(): void {
        try {
            $this->connectInternal();
        } catch (StreamException $e) {
            throw $e;
        } catch (Throwable $e) {
            throw new StreamException(
                message: 'Stream connect failed',
                code: ExceptionCode::STREAM_CONNECT_FAILED->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'connect',
                ],
                previous: $e,
            );
        }
    }

    #[\Override]
    public function getConfig(): StreamNodeConfig {
        return $this->config;
    }

    /**
     * @throws \Cassandra\Exception\StreamException
     */
    #[\Override]
    public function readAvailableDataFromSource(int $expectedLength, int $upperBoundaryLength, ?float $readDeadline): string {
        try {
            return $this->readAvailableDataFromSourceInternal($expectedLength, $upperBoundaryLength, $readDeadline);
        } catch (StreamException $e) {
            throw $e;
        } catch (Throwable $e) {
            throw new StreamException(
                message: 'Stream read failed',
                code: ExceptionCode::STREAM_READ_FAILED->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'readAvailableDataFromSource',
                    'expectedLength' => $expectedLength,
                    'upperBoundaryLength' => $upperBoundaryLength,
                    'read_deadline' => $readDeadline,
                ],
                previous: $e,
            );
        }
    }

    /**
     * @throws \Cassandra\Exception\StreamException
     */
    #[\Override]
    public function write(string $data): void {
        try {
            $this->writeInternal($data);
        } catch (StreamException $e) {
            throw $e;
        } catch (Throwable $e) {
            throw new StreamException(
                message: 'Stream write failed',
                code: ExceptionCode::STREAM_WRITE_FAILED->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'write',
                ],
                previous: $e,
            );
        }
    }

    /**
     * @throws \Cassandra\Exception\StreamException
     * @throws \Cassandra\Exception\RequestException
     */
    #[\Override]
    public function writeRequest(Request $request): void {
        $this->write($request->__toString());
    }

    /**
     * Bound a blocking-mode read by the caller's deadline as well as by the
     * stall window, by narrowing the stream's own timeout for its duration.
     *
     * Only reached when the stream could not be switched to non-blocking mode,
     * where stream_select() would do this instead. Returns the timeout the read
     * will run under, or null when the deadline has already passed, so the
     * caller can skip the read altogether and can tell which of the two bounds
     * it got. The timeout is only re-applied when the value actually changes,
     * which spares the call for the unbounded reads that keep asking for the
     * same stall window.
     *
     * @param resource $stream
     *
     * @throws \Cassandra\Exception\StreamException
     */
    private function applyReceiveTimeout($stream, ?float $readDeadline): ?float {

        $remaining = $this->narrowToReadDeadline($this->receiveTimeout, $readDeadline);
        if ($remaining === null) {
            return null;
        }

        if ($this->appliedReceiveTimeout === $remaining) {
            return $remaining;
        }

        [$seconds, $microseconds] = $this->splitTimeout($remaining);

        // What is left of the deadline rounded away to nothing, so there is no
        // point arming a timeout for it: the read is skipped as if the deadline
        // had passed. This also keeps the stream away from a {0, 0} timeout,
        // which the socket transport reads as "no timeout" — see
        // {@see Socket::applyReceiveTimeout()}.
        if ($seconds === 0 && $microseconds === 0) {
            return null;
        }

        $this->setStreamTimeout(
            $stream,
            $seconds ?? self::UNLIMITED_STREAM_TIMEOUT_SECONDS,
            $microseconds
        );

        $this->appliedReceiveTimeout = $remaining;

        return $remaining;
    }

    /**
     * @param resource $stream
     *
     * @throws \Cassandra\Exception\StreamException
     */
    private function checkForReadTimeout($stream, float $start, int $expectedLength, int $upperBoundaryLength, bool $waitForData): void {

        if (microtime(true) - $start >= $this->receiveTimeout) {
            throw new StreamException(
                message: 'Stream read timed out',
                code: ExceptionCode::STREAM_TIMEOUT_DURING_READ->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'readAvailableDataFromSource',
                    'expectedLength' => $expectedLength,
                    'upperBoundaryLength' => $upperBoundaryLength,
                    'waitForData' => $waitForData,
                    'receive_timeout_seconds' => $this->describeTimeout($this->receiveTimeout),
                    'meta' => stream_get_meta_data($stream),
                ]
            );
        }
    }

    /**
     * @param resource $stream
     *
     * @throws \Cassandra\Exception\StreamException
     */
    private function checkForWriteTimeout($stream, float $lastProgressAt): void {

        if (microtime(true) - $lastProgressAt >= $this->sendTimeout) {
            throw new StreamException(
                message: 'Stream write timed out',
                code: ExceptionCode::STREAM_TIMEOUT_DURING_WRITE->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'write',
                    'send_timeout_seconds' => $this->describeTimeout($this->sendTimeout),
                    'meta' => stream_get_meta_data($stream),
                ]
            );
        }
    }

    /**
     * @throws \Cassandra\Exception\StreamException
     * @throws \Throwable from a warning-promoting application error handler
     */
    private function connectInternal(): void {
        if ($this->stream !== null) {
            return;
        }

        $context = stream_context_create(
            options: [
                'ssl' => $this->config->sslOptions,
            ]
        );

        // If ssl options were configured but the host carries no explicit
        // scheme, connect via TLS — otherwise the ssl options would be
        // silently ignored and the connection would be plaintext.
        $host = $this->config->host;
        if (!str_contains($host, '://')) {
            $host = ($this->config->sslOptions !== [] ? 'tls://' : 'tcp://') . $host;
        }

        // A port may only be appended to an IPv6 literal after enclosing the
        // address in brackets. Accept bare and already bracketed literals,
        // with or without an explicit transport scheme.
        $hostParts = explode('://', $host, 2);
        $scheme = $hostParts[0];
        $address = $hostParts[1] ?? '';
        if (str_contains($address, ':') && !str_starts_with($address, '[')) {
            $address = '[' . $address . ']';
        }
        $host = $scheme . '://' . $address;

        // Suppressed because the failure is reported through $errorCode and
        // $errorMessage below, which is what those by-reference parameters are
        // for: PHP's warning says the same thing a second time, and would reach
        // an application's error handler ahead of the StreamException.
        $stream = @stream_socket_client(
            address: $host . ':' . $this->config->port,
            error_code: $errorCode,
            error_message: $errorMessage,
            timeout: $this->config->connectTimeoutInSeconds,
            flags: STREAM_CLIENT_CONNECT,
            context: $context
        );

        if ($stream === false) {
            /** @psalm-suppress TypeDoesNotContainType */
            if (!is_string($errorMessage)) {
                $errorMessage = 'Unknown error';
            }

            /** @psalm-suppress TypeDoesNotContainType */
            if (!is_int($errorCode)) {
                $errorCode = 0;
            }

            throw new StreamException(
                message: $errorMessage,
                code: ExceptionCode::STREAM_CONNECT_FAILED->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'connect',
                    'connect_timeout_seconds' => $this->config->connectTimeoutInSeconds,
                    'ssl_options' => $this->config->sslOptions,
                    'system_error_code' => $errorCode,
                ]
            );
        }

        $this->isBlockingIo = stream_set_blocking($stream, enable: false) === false;

        // Only relevant when the stream could not be switched to non-blocking
        // mode; the non-blocking paths enforce the timeouts via stream_select().
        [$timeoutSeconds, $timeoutMicroseconds] = $this->splitTimeout($this->receiveTimeout);
        $this->setStreamTimeout(
            $stream,
            $timeoutSeconds ?? self::UNLIMITED_STREAM_TIMEOUT_SECONDS,
            $timeoutMicroseconds
        );
        $this->appliedReceiveTimeout = $this->receiveTimeout;

        $this->stream = $stream;
    }

    /**
     * @throws \Cassandra\Exception\StreamException
     * @throws \Throwable from a warning-promoting application error handler
     */
    private function readAvailableDataFromSourceInternal(int $expectedLength, int $upperBoundaryLength, ?float $readDeadline): string {

        if ($this->stream === null) {
            throw new StreamException(
                message: 'Stream transport not connected',
                code: ExceptionCode::STREAM_NOT_CONNECTED_DURING_READ->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'readAvailableDataFromSource',
                    'expectedLength' => $expectedLength,
                    'upperBoundaryLength' => $upperBoundaryLength,
                    'read_deadline' => $readDeadline,
                ]
            );
        }

        $stream = $this->stream;

        if ($expectedLength < 1) {
            return '';
        }

        $start = microtime(true);
        $waitForData = $this->mayBlock($readDeadline);

        // Whether the read below is the one that can pass judgement on the
        // connection, i.e. whether it was given the whole stall window rather
        // than a shorter deadline of the caller's. Recorded when the timeout is
        // armed instead of re-derived from the clock afterwards: where nothing
        // narrows it, the two are the same duration, and comparing the elapsed
        // time against the stall window can only tell them apart by luck.
        $stallWindowArmed = false;

        if (!$this->isBlockingIo) {
            $hasData = $this->selectStreamForRead($stream, $start, $expectedLength, $upperBoundaryLength, $waitForData, $readDeadline);
            if (!$hasData) {
                return '';
            }
        } elseif (!$waitForData) {
            // Blocking fallback, asked for "whatever is there right now":
            // fread() alone cannot answer that, since it would sit in the stream
            // timeout, which is exactly what a caller passing a deadline that
            // has already passed asked not to happen. Readiness is settled with
            // a zero-timeout stream_select() instead — that works on a blocking
            // stream just as well — and only then is the stream read, which now
            // returns what has arrived without waiting for more. Without this
            // the polling calls could never take anything off a blocking
            // stream, and would report an idle connection forever.
            $hasData = $this->selectStreamForRead($stream, $start, $expectedLength, $upperBoundaryLength, false, $readDeadline);
            if (!$hasData) {
                return '';
            }
        } else {
            // Blocking fallback: the deadline is enforced by the stream's own
            // timeout rather than by select().
            $appliedTimeout = $this->applyReceiveTimeout($stream, $readDeadline);
            if ($appliedTimeout === null) {
                // The deadline passed between mayBlock() and here.
                return '';
            }

            $stallWindowArmed = $appliedTimeout >= $this->receiveTimeout;
        }

        $readLength = $this->isBlockingIo ? $expectedLength : max($expectedLength, $upperBoundaryLength);

        $readData = @fread($stream, $readLength);
        if ($readData === false) {

            if (feof($stream)) {
                throw new StreamException(
                    message: 'Stream connection reset by peer',
                    code: ExceptionCode::STREAM_RESET_BY_PEER_DURING_READ->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'readAvailableDataFromSource',
                        'expectedLength' => $expectedLength,
                        'upperBoundaryLength' => $upperBoundaryLength,
                        'read_deadline' => $readDeadline,
                        'meta' => stream_get_meta_data($stream),
                    ]
                );
            }

            if (stream_get_meta_data($stream)['timed_out']) {
                // Only the stall window running out means the connection went
                // quiet for too long; the caller's deadline, applied as the
                // stream timeout above, is not the transport's failure.
                if (!$stallWindowArmed) {
                    return '';
                }

                throw new StreamException(
                    message: 'Stream read timed out',
                    code: ExceptionCode::STREAM_TIMEOUT_DURING_READ->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'readAvailableDataFromSource',
                        'expectedLength' => $expectedLength,
                        'upperBoundaryLength' => $upperBoundaryLength,
                        'read_deadline' => $readDeadline,
                        'meta' => stream_get_meta_data($stream),
                    ]
                );
            }

            throw new StreamException(
                message: 'Stream read failed',
                code: ExceptionCode::STREAM_READ_FAILED->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'readAvailableDataFromSource',
                    'expectedLength' => $expectedLength,
                    'upperBoundaryLength' => $upperBoundaryLength,
                    'read_deadline' => $readDeadline,
                    'bytes_read' => 0,
                    'meta' => stream_get_meta_data($stream),
                ]
            );
        }

        if ($readData === '') {
            if (feof($stream)) {
                throw new StreamException(
                    message: 'Stream connection reset by peer',
                    code: ExceptionCode::STREAM_RESET_BY_PEER_DURING_READ->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'readAvailableDataFromSource',
                        'expectedLength' => $expectedLength,
                        'upperBoundaryLength' => $upperBoundaryLength,
                        'read_deadline' => $readDeadline,
                        'meta' => stream_get_meta_data($stream),
                    ]
                );
            }

            if ($stallWindowArmed && stream_get_meta_data($stream)['timed_out']) {
                throw new StreamException(
                    message: 'Stream read timed out',
                    code: ExceptionCode::STREAM_TIMEOUT_DURING_READ->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'readAvailableDataFromSource',
                        'expectedLength' => $expectedLength,
                        'upperBoundaryLength' => $upperBoundaryLength,
                        'read_deadline' => $readDeadline,
                        'meta' => stream_get_meta_data($stream),
                    ]
                );
            }
        }

        return $readData;
    }

    /**
     * @param resource $stream
     * 
     * @throws \Cassandra\Exception\StreamException
     */
    private function selectStreamForRead($stream, float $start, int $expectedLength, int $upperBoundaryLength, bool $waitForData, ?float $readDeadline): bool {

        $selectFailures = 0;

        do {
            $read = [ $stream ];
            $write = null;
            $except = null;

            if ($waitForData) {
                // Wait at most for the remaining receive timeout; the per-stream
                // timeout set via stream_set_timeout() has no effect on
                // non-blocking streams, so it must be enforced here.
                $remaining = $this->receiveTimeout - (microtime(true) - $start);

                if ($remaining <= 0.0) {
                    // Selecting with a zero timeout would return immediately and
                    // busy-spin, so enforce the receive timeout right away.
                    $this->checkForReadTimeout($stream, $start, $expectedLength, $upperBoundaryLength, $waitForData);

                    continue;
                }

                // And at most for what the caller still has, so a request
                // budget is honoured to the second instead of to the stall
                // window — and so a transport with no stall window at all does
                // not swallow the deadline entirely.
                $remaining = $this->narrowToReadDeadline($remaining, $readDeadline);
                if ($remaining === null) {
                    return false;
                }

                [$remainingSeconds, $remainingMicroseconds] = $this->splitTimeout($remaining);

                $selectResult = @stream_select(
                    read: $read,
                    write: $write,
                    except: $except,
                    seconds: $remainingSeconds,
                    microseconds: $remainingMicroseconds
                );
            } else {
                $selectResult = @stream_select(
                    read: $read,
                    write: $write,
                    except: $except,
                    seconds: 0
                );
            }

            if ($selectResult === false) {
                // A signal interrupts stream_select() and it reports that the
                // same way as a real failure, with no errno for PHP to tell
                // them apart — so a process that takes signals must not lose
                // its connection over one. A closed peer is the one case that
                // is unambiguous; anything else is retried for as long as the
                // receive timeout allows, which is what the socket transport
                // does on EINTR. Only once the window is used up without a
                // single successful select is it reported as a failure.
                if (feof($stream)) {
                    throw new StreamException(
                        message: 'Stream connection reset by peer',
                        code: ExceptionCode::STREAM_RESET_BY_PEER_DURING_READ->value,
                        context: [
                            'host' => $this->config->host,
                            'port' => $this->config->port,
                            'operation' => 'readAvailableDataFromSource',
                            'expectedLength' => $expectedLength,
                            'upperBoundaryLength' => $upperBoundaryLength,
                            'waitForData' => $waitForData,
                            'meta' => stream_get_meta_data($stream),
                        ]
                    );
                }

                $selectFailures++;

                if (!$waitForData) {
                    return false;
                }

                // The stall window is what normally ends these retries. Where
                // the transport was configured without one it can never fire,
                // so the tally is what ends them instead; see
                // {@see self::MAX_CONSECUTIVE_SELECT_FAILURES}.
                if (
                    microtime(true) - $start >= $this->receiveTimeout
                    || $selectFailures >= self::MAX_CONSECUTIVE_SELECT_FAILURES
                ) {
                    throw new StreamException(
                        message: 'Stream select failed',
                        code: ExceptionCode::STREAM_SELECT_FAILED->value,
                        context: [
                            'host' => $this->config->host,
                            'port' => $this->config->port,
                            'operation' => 'readAvailableDataFromSource',
                            'expectedLength' => $expectedLength,
                            'upperBoundaryLength' => $upperBoundaryLength,
                            'waitForData' => $waitForData,
                            'bytes_read' => 0,
                            'select_failures' => $selectFailures,
                            'meta' => stream_get_meta_data($stream),
                        ]
                    );
                }

                continue;
            }

            if ($selectResult === 0) {
                if ($waitForData) {
                    $selectFailures = 0;
                    $this->checkForReadTimeout($stream, $start, $expectedLength, $upperBoundaryLength, $waitForData);

                    continue;
                }

                return false;
            }

            break;

        } while (true);

        return true;
    }

    /**
     * @param resource $stream
     * @param int $selectFailures how many times in a row select() has failed
     * across the calls of one write, kept by the caller because each call makes
     * a single attempt; see {@see self::MAX_CONSECUTIVE_SELECT_FAILURES}
     *
     * @throws \Cassandra\Exception\StreamException
     */
    private function selectStreamForWrite($stream, float $lastProgressAt, int &$selectFailures): bool {
        $read = null;
        $write = [ $stream ];
        $except = null;

        // Wait at most for what is left of the stall window, so that several
        // select() calls without progress cannot add up to a multiple of the
        // configured send timeout.
        $remaining = $this->sendTimeout - (microtime(true) - $lastProgressAt);

        if ($remaining <= 0.0) {
            $this->checkForWriteTimeout($stream, $lastProgressAt);

            return false;
        }

        [$remainingSeconds, $remainingMicroseconds] = $this->splitTimeout($remaining);

        $selectResult = @stream_select(
            read: $read,
            write: $write,
            except: $except,
            seconds: $remainingSeconds,
            microseconds: $remainingMicroseconds,
        );

        if ($selectResult === false) {

            if (feof($stream)) {
                throw new StreamException(
                    message: 'Stream connection reset by peer',
                    code: ExceptionCode::STREAM_RESET_BY_PEER_DURING_WRITE->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'write',
                        'meta' => stream_get_meta_data($stream),
                    ]
                );
            }

            $selectFailures++;

            // As on the read side: an interrupted select is indistinguishable
            // from a failed one, so it is retried by the caller's loop for as
            // long as the stall window allows rather than costing the
            // connection — and where the transport was configured without a
            // stall window, for as long as the tally allows, that test being the
            // only one of the two that can fire there; see
            // {@see self::MAX_CONSECUTIVE_SELECT_FAILURES}.
            if (
                microtime(true) - $lastProgressAt <= $this->sendTimeout
                && $selectFailures < self::MAX_CONSECUTIVE_SELECT_FAILURES
            ) {
                return false;
            }

            throw new StreamException(
                message: 'Stream select failed',
                code: ExceptionCode::STREAM_SELECT_FAILED->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'write',
                    'send_timeout_seconds' => $this->describeTimeout($this->sendTimeout),
                    'select_failures' => $selectFailures,
                    'meta' => stream_get_meta_data($stream),
                ]
            );
        }

        $selectFailures = 0;

        if ($selectResult === 0) {
            $this->checkForWriteTimeout($stream, $lastProgressAt);

            return false;
        }

        return true;
    }

    /**
     * Apply a stream timeout without allowing a warning, native exception or
     * Error raised by PHP's stream implementation to cross the transport
     * boundary.
     *
     * @param resource $stream
     *
     * @throws \Cassandra\Exception\StreamException
     */
    private function setStreamTimeout($stream, int $seconds, int $microseconds): void {
        set_error_handler(function (int $severity, string $message, string $file, int $line): never {
            $previous = new ErrorException($message, 0, $severity, $file, $line);

            throw new StreamException(
                message: 'Failed to set stream timeout',
                code: ExceptionCode::STREAM_SET_TIMEOUT_FAILED->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'stream_set_timeout',
                ],
                previous: $previous,
            );
        });

        try {
            $success = stream_set_timeout($stream, $seconds, $microseconds);
        } finally {
            restore_error_handler();
        }

        if ($success) {
            return;
        }

        throw new StreamException(
            message: 'Failed to set stream timeout',
            code: ExceptionCode::STREAM_SET_TIMEOUT_FAILED->value,
            context: [
                'host' => $this->config->host,
                'port' => $this->config->port,
                'operation' => 'stream_set_timeout',
            ],
        );
    }

    /**
     * @throws \Cassandra\Exception\StreamException
     * @throws \Throwable from a warning-promoting application error handler
     */
    private function writeInternal(string $data): void {
        if ($this->stream === null) {
            throw new StreamException(
                message: 'Stream transport not connected',
                code: ExceptionCode::STREAM_NOT_CONNECTED_DURING_WRITE->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'write',
                    'bytes_remaining' => strlen($data),
                ]
            );
        }

        $stream = $this->stream;

        if (strlen($data) < 1) {
            return;
        }

        // The send timeout is a stall timeout, not a deadline for the whole
        // payload: it bounds how long the stream makes no progress at all, so
        // writing a large frame over a slow but healthy connection cannot trip it.
        $lastProgressAt = microtime(true);

        // A blocking stream normally needs no select(): fwrite() waits for room
        // of its own accord, bounded by the stream timeout. But a pass that comes
        // back without moving a byte would go straight into another fwrite(), and
        // on a blocking stream that is a tight loop burning a core until the
        // stall window runs out — the read side selects even in blocking mode for
        // the same reason. So once a pass makes no progress, writability is
        // waited for with select() from then on: bounded by what is left of the
        // window, and free while the stream is writable.
        $selectBeforeWrite = !$this->isBlockingIo;

        $selectFailures = 0;

        do {
            if ($selectBeforeWrite) {
                $canWrite = $this->selectStreamForWrite($stream, $lastProgressAt, $selectFailures);
                if (!$canWrite) {
                    continue;
                }
            }

            // Suppressed because the failure is inspected and reported below,
            // as a StreamException carrying the stream's metadata. A peer that
            // went away is an ordinary outcome, not a diagnostic an application
            // should have to filter out of its logs.
            $sentBytes = @fwrite($stream, $data);
            if ($sentBytes === false) {

                if (feof($stream)) {
                    throw new StreamException(
                        message: 'Stream connection reset by peer',
                        code: ExceptionCode::STREAM_RESET_BY_PEER_DURING_WRITE->value,
                        context: [
                            'host' => $this->config->host,
                            'port' => $this->config->port,
                            'operation' => 'write',
                            'meta' => stream_get_meta_data($stream),
                        ]
                    );
                }

                if (stream_get_meta_data($stream)['timed_out']) {
                    throw new StreamException(
                        message: 'Stream write timed out',
                        code: ExceptionCode::STREAM_TIMEOUT_DURING_WRITE->value,
                        context: [
                            'host' => $this->config->host,
                            'port' => $this->config->port,
                            'operation' => 'write',
                            'send_timeout_seconds' => $this->describeTimeout($this->sendTimeout),
                            'meta' => stream_get_meta_data($stream),
                        ]
                    );
                }

                throw new StreamException(
                    message: 'Stream write failed',
                    code: ExceptionCode::STREAM_WRITE_FAILED->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'write',
                        'meta' => stream_get_meta_data($stream),
                    ]
                );
            }

            if ($sentBytes === 0) {

                $this->checkForWriteTimeout($stream, $lastProgressAt);

                // Back to the top so the stream is selected for writability
                // again instead of spinning on fwrite().
                $selectBeforeWrite = true;

                continue;
            }

            $data = substr($data, $sentBytes);

            // Bytes moved, so the stall window starts over.
            $lastProgressAt = microtime(true);

        } while ($data !== '');
    }

}
