<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\SocketException;
use Socket as PhpSocket;
use Cassandra\Request\Request;
use ErrorException;

final class Socket extends NodeImplementation implements IoNode {
    /**
     * The SO_RCVTIMEO currently applied to the socket, tracked so that the
     * blocking-mode fallback only re-applies it when it actually changes.
     */
    private ?float $appliedReceiveTimeout = null;

    private SocketNodeConfig $config;
    private float $connectTimeout = 5.0;
    private bool $isBlockingIo = false;
    private float $receiveTimeout = 10.0;
    private float $sendTimeout = 10.0;
    private ?PhpSocket $socket = null;

    /**
     * @throws \Cassandra\Exception\SocketException
     */
    public function __construct(
        NodeConfig $config
    ) {
        if (!($config instanceof SocketNodeConfig)) {
            throw new SocketException(
                message: 'Invalid node configuration type for Socket transport',
                code: ExceptionCode::SOCKET_INVALID_CONFIG->value,
                context: [
                    'expected_class' => SocketNodeConfig::class,
                    'actual_class' => get_debug_type($config),
                ]
            );
        }
        $this->config = $config;

        [
            'sendTimeout' => $this->sendTimeout,
            'receiveTimeout' => $this->receiveTimeout,
        ] = $this->getTimeoutsFromConfig();

        if (!is_finite($config->connectTimeoutInSeconds) || $config->connectTimeoutInSeconds <= 0.0) {
            throw new SocketException(
                message: 'Invalid connect timeout: it must be greater than zero',
                code: ExceptionCode::SOCKET_INVALID_CONFIG->value,
                context: [
                    'connect_timeout_seconds' => $config->connectTimeoutInSeconds,
                ]
            );
        }

        $this->connectTimeout = $config->connectTimeoutInSeconds;
    }

    public function __destruct() {
        $this->close();
    }

    #[\Override]
    public function close(): void {
        if ($this->socket === null) {
            return;
        }

        $socket = $this->socket;
        $this->socket = null;

        $this->closeSocket($socket, true);
    }

    /**
     * @throws \Cassandra\Exception\SocketException
     */
    #[\Override]
    public function connect(): void {
        if ($this->socket !== null) {
            return;
        }

        if (str_contains($this->config->host, '://')) {
            throw new SocketException(
                message: 'The socket transport does not support URL schemes in the host (e.g. "tls://"); use a plain hostname or IP, or use StreamNodeConfig for TLS connections',
                code: ExceptionCode::SOCKET_INVALID_CONFIG->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                ]
            );
        }

        $addresses = $this->resolveHost();

        $lastException = null;

        foreach ($addresses as ['family' => $addressFamily, 'address' => $address]) {
            try {
                $this->socket = $this->connectToAddress($addressFamily, $address);

                return;

            } catch (SocketException $e) {
                $lastException = $e;
            }
        }

        throw $lastException ?? new SocketException(
            message: 'Socket connect failed: no usable address for host',
            code: ExceptionCode::SOCKET_CONNECT_FAILED->value,
            context: [
                'host' => $this->config->host,
                'port' => $this->config->port,
                'operation' => 'connect',
            ]
        );
    }

    #[\Override]
    public function getConfig(): SocketNodeConfig {
        return $this->config;
    }

    /**
     * @throws \Cassandra\Exception\SocketException
     */
    #[\Override]
    public function readAvailableDataFromSource(int $expectedLength, int $upperBoundaryLength, ?float $readDeadline): string {

        if ($this->socket === null) {
            throw new SocketException(
                message: 'Socket transport not connected',
                code: ExceptionCode::SOCKET_NOT_CONNECTED_DURING_READ->value,
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

        $socket = $this->socket;

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
            $hasData = $this->selectSocketForRead($socket, $start, $expectedLength, $upperBoundaryLength, $waitForData, $readDeadline);
            if (!$hasData) {
                return '';
            }
        } elseif (!$waitForData) {
            // Blocking fallback, asked for "whatever is there right now":
            // socket_read() alone cannot answer that, since it would sit in the
            // receive timeout, which is exactly what a caller passing a deadline
            // that has already passed asked not to happen. Readiness is settled
            // with a zero-timeout select() instead — that works on a blocking
            // socket just as well — and only then is the socket read, which now
            // returns what has arrived without waiting for more. Without this
            // the polling calls could never take anything off a blocking
            // socket, and would report an idle connection forever.
            $hasData = $this->selectSocketForRead($socket, $start, $expectedLength, $upperBoundaryLength, false, $readDeadline);
            if (!$hasData) {
                return '';
            }
        } else {
            // Blocking fallback: the deadline is enforced by SO_RCVTIMEO
            // rather than by select().
            $appliedTimeout = $this->applyReceiveTimeout($readDeadline);
            if ($appliedTimeout === null) {
                // The deadline passed between mayBlock() and here.
                return '';
            }

            $stallWindowArmed = $appliedTimeout >= $this->receiveTimeout;
        }

        $readLength = $this->isBlockingIo ? $expectedLength : max($expectedLength, $upperBoundaryLength);
        do {
            // Suppressed because the errno is read and reported below: every
            // outcome that matters becomes a SocketException carrying more
            // context than PHP's warning does. Left unsuppressed, an ordinary
            // connection reset would raise a warning *and* the exception, and an
            // application whose error handler turns warnings into exceptions
            // would get the warning in place of the driver's own report.
            $readData = @socket_read($socket, $readLength, PHP_BINARY_READ);
            if ($readData === false) {
                $errorCode = socket_last_error($socket);

                if ($errorCode === SOCKET_EINTR) {
                    if ($waitForData) {
                        $this->checkForReceiveTimeout($start, $expectedLength, $upperBoundaryLength);

                        if ($this->isBlockingIo) {
                            // The read below runs under SO_RCVTIMEO, which the
                            // signal has just used up part of — and the option
                            // arms a duration, not a deadline, so going straight
                            // back in would hand the read the caller's whole
                            // budget a second time. Re-narrowed against the
                            // deadline instead, so that a stream of signals
                            // cannot add up to a multiple of it.
                            $appliedTimeout = $this->applyReceiveTimeout($readDeadline);
                            if ($appliedTimeout === null) {
                                return '';
                            }

                            $stallWindowArmed = $appliedTimeout >= $this->receiveTimeout;
                        }

                        continue;
                    }

                    return '';
                }

                if (
                    $errorCode === SOCKET_EWOULDBLOCK
                    || $errorCode === SOCKET_EAGAIN /* @phpstan-ignore identical.alwaysFalse */
                ) {
                    // A blocking socket reports an expired SO_RCVTIMEO this way.
                    // Only a stall window that has run out means the connection
                    // itself went quiet for too long; the caller's deadline
                    // expiring is not the socket's failure to report.
                    if ($stallWindowArmed) {
                        throw new SocketException(
                            message: 'Socket read timed out',
                            code: ExceptionCode::SOCKET_TIMEOUT_DURING_READ->value,
                            context: [
                                'host' => $this->config->host,
                                'port' => $this->config->port,
                                'operation' => 'readAvailableDataFromSource',
                                'expectedLength' => $expectedLength,
                                'upperBoundaryLength' => $upperBoundaryLength,
                                'bytes_read' => 0,
                                'socket_options' => $this->config->socketOptions,
                            ]
                        );
                    }

                    return '';
                }

                if (
                    $errorCode === SOCKET_ECONNRESET
                    || $errorCode === SOCKET_ENOTCONN
                    || $errorCode === SOCKET_ECONNABORTED
                ) {
                    throw new SocketException(
                        message: 'Socket connection reset by peer during read.',
                        code: ExceptionCode::SOCKET_RESET_BY_PEER_DURING_READ->value,
                        context: [
                            'host' => $this->config->host,
                            'port' => $this->config->port,
                            'operation' => 'readAvailableDataFromSource',
                            'expectedLength' => $expectedLength,
                            'upperBoundaryLength' => $upperBoundaryLength,
                            'bytes_read' => 0,
                            'socket_options' => $this->config->socketOptions,
                        ]
                    );
                }

                if ($errorCode === SOCKET_ETIMEDOUT) {
                    if (!$stallWindowArmed) {
                        // The caller's deadline, applied as SO_RCVTIMEO above,
                        // rather than the transport's stall window.
                        return '';
                    }

                    throw new SocketException(
                        message: 'Socket read timed out',
                        code: ExceptionCode::SOCKET_TIMEOUT_DURING_READ->value,
                        context: [
                            'host' => $this->config->host,
                            'port' => $this->config->port,
                            'operation' => 'readAvailableDataFromSource',
                            'expectedLength' => $expectedLength,
                            'upperBoundaryLength' => $upperBoundaryLength,
                            'bytes_read' => 0,
                            'socket_options' => $this->config->socketOptions,
                        ]
                    );
                }

                throw new SocketException(
                    message: 'Socket read failed: ' . socket_strerror($errorCode),
                    code: ExceptionCode::SOCKET_READ_FAILED->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'readAvailableDataFromSource',
                        'expectedLength' => $expectedLength,
                        'upperBoundaryLength' => $upperBoundaryLength,
                        'read_deadline' => $readDeadline,
                        'bytes_read' => 0,
                        'socket_options' => $this->config->socketOptions,
                        'system_error_code' => $errorCode,
                    ]
                );
            }

            if ($readData === '') {
                throw new SocketException(
                    message: 'Socket connection reset by peer during read.',
                    code: ExceptionCode::SOCKET_RESET_BY_PEER_DURING_READ->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'readAvailableDataFromSource',
                        'expectedLength' => $expectedLength,
                        'upperBoundaryLength' => $upperBoundaryLength,
                        'bytes_read' => 0,
                        'socket_options' => $this->config->socketOptions,
                    ]
                );
            }

            break;

        } while (true);

        return $readData;
    }

    /**
     * @throws \Cassandra\Exception\SocketException
     */
    #[\Override]
    public function write(string $data): void {
        if ($this->socket === null) {
            throw new SocketException(
                message: 'Socket transport not connected',
                code: ExceptionCode::SOCKET_NOT_CONNECTED_DURING_WRITE->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'write',
                ]
            );
        }

        $socket = $this->socket;

        if (strlen($data) < 1) {
            return;
        }

        // The send timeout is a stall timeout, not a deadline for the whole
        // payload: it bounds how long the socket makes no progress at all, so
        // writing a large frame over a slow but healthy connection cannot trip
        // it. This mirrors SO_SNDTIMEO, which POSIX defines per send() call.
        $lastProgressAt = microtime(true);

        // A blocking socket normally needs no select(): socket_write() waits for
        // room of its own accord, bounded by SO_SNDTIMEO. But a pass that comes
        // back without moving a byte would go straight into another
        // socket_write(), and on a blocking socket that is a tight loop burning
        // a core until the stall window runs out — the read side selects even in
        // blocking mode for the same reason. So once a pass makes no progress,
        // writability is waited for with select() from then on: bounded by what
        // is left of the window, and free while the socket is writable.
        $selectBeforeWrite = !$this->isBlockingIo;

        do {
            if ($selectBeforeWrite) {
                $canWrite = $this->selectSocketForWrite($socket, $lastProgressAt);
                if (!$canWrite) {
                    continue;
                }
            }

            $bufferErrors = 0;
            do {
                // Suppressed for the reason given in readAvailableDataFromSource().
                $sentBytes = @socket_write($socket, $data);

                if ($sentBytes === 0) {
                    $this->checkForWriteTimeout($lastProgressAt);

                    // Back to the outer loop so the socket is selected for
                    // writability again instead of spinning on socket_write().
                    $selectBeforeWrite = true;

                    continue 2;
                }

                if ($sentBytes === false) {
                    $errorCode = socket_last_error($socket);

                    if (
                        $errorCode === SOCKET_EWOULDBLOCK
                        || $errorCode === SOCKET_EAGAIN /* @phpstan-ignore identical.alwaysFalse */
                        || $errorCode === SOCKET_EINTR
                    ) {

                        $this->checkForWriteTimeout($lastProgressAt);

                        // Back to the outer loop so the socket is selected for
                        // writability again instead of spinning on socket_write().
                        $selectBeforeWrite = true;

                        continue 2;
                    }

                    if (
                        $errorCode === SOCKET_ECONNRESET
                        || $errorCode === SOCKET_EPIPE
                        || $errorCode === SOCKET_ENOTCONN
                        || $errorCode === SOCKET_ECONNABORTED
                    ) {
                        throw new SocketException(
                            message: 'Socket connection reset by peer during write.',
                            code: ExceptionCode::SOCKET_RESET_BY_PEER_DURING_WRITE->value,
                            context: [
                                'host' => $this->config->host,
                                'port' => $this->config->port,
                                'operation' => 'write',
                                'socket_options' => $this->config->socketOptions,
                                'system_error_code' => $errorCode,
                            ]
                        );
                    }

                    if ($errorCode === SOCKET_ETIMEDOUT) {
                        throw new SocketException(
                            message: 'Socket write timed out',
                            code: ExceptionCode::SOCKET_TIMEOUT_DURING_WRITE->value,
                            context: [
                                'host' => $this->config->host,
                                'port' => $this->config->port,
                                'operation' => 'write',
                                'socket_options' => $this->config->socketOptions,
                            ]
                        );
                    }

                    if ($errorCode === SOCKET_ENOBUFS) {
                        $bufferErrors++;

                        if ($bufferErrors >= 3) {
                            throw new SocketException(
                                message: 'Socket write failed: ' . socket_strerror($errorCode),
                                code: ExceptionCode::SOCKET_WRITE_FAILED->value,
                                context: [
                                    'host' => $this->config->host,
                                    'port' => $this->config->port,
                                    'operation' => 'write',

                                    'socket_options' => $this->config->socketOptions,
                                    'system_error_code' => $errorCode,
                                ]
                            );
                        }

                        usleep(1000);

                        continue;
                    }

                    throw new SocketException(
                        message: 'Socket write failed: ' . socket_strerror($errorCode),
                        code: ExceptionCode::SOCKET_WRITE_FAILED->value,
                        context: [
                            'host' => $this->config->host,
                            'port' => $this->config->port,
                            'operation' => 'write',

                            'socket_options' => $this->config->socketOptions,
                            'system_error_code' => $errorCode,
                        ]
                    );
                }

                $bufferErrors = 0;
                $data = substr($data, $sentBytes);

                // Bytes moved, so the stall window starts over.
                $lastProgressAt = microtime(true);

            } while ($data !== '');

            break;

        } while (true);
    }

    /**
     * @throws \Cassandra\Exception\SocketException
     * @throws \Cassandra\Exception\RequestException
     */
    #[\Override]
    public function writeRequest(Request $request): void {
        $this->write($request->__toString());
    }

    /**
     * Bound a blocking-mode read by the caller's deadline as well as by the
     * stall window, by narrowing SO_RCVTIMEO for the duration of the read.
     *
     * Only reached when the socket could not be switched to non-blocking mode,
     * where select() would do this instead. Returns the timeout the read will
     * run under, or null when the deadline has already passed, so the caller
     * can skip the read altogether and can tell which of the two bounds it got.
     * The option is only touched when the value actually changes, which spares
     * the syscall for the unbounded reads that keep asking for the same stall
     * window.
     *
     * @throws \Cassandra\Exception\SocketException
     */
    private function applyReceiveTimeout(?float $readDeadline): ?float {

        if ($this->socket === null) {
            return null;
        }

        $remaining = $this->narrowToReadDeadline($this->receiveTimeout, $readDeadline);
        if ($remaining === null) {
            return null;
        }

        if ($this->appliedReceiveTimeout === $remaining) {
            return $remaining;
        }

        [$seconds, $microseconds] = $this->splitTimeout($remaining);

        // What is left of the deadline rounded away to nothing. SO_RCVTIMEO
        // spells {0, 0} as "no timeout", so applying it would turn the caller's
        // deadline into a read that blocks until the peer says something — the
        // exact opposite of what was asked for. There is no time left to wait
        // anyway, so the read is skipped as if the deadline had passed.
        if ($seconds === 0 && $microseconds === 0) {
            return null;
        }

        // A null seconds value means "no timeout", which the socket option
        // spells as zero.
        $this->setSocketOption($this->socket, SOL_SOCKET, SO_RCVTIMEO, [
            'sec' => $seconds ?? 0,
            'usec' => $seconds === null ? 0 : $microseconds,
        ]);

        $this->appliedReceiveTimeout = $remaining;

        return $remaining;
    }

    /**
     * @throws \Cassandra\Exception\SocketException
     */
    private function checkForReceiveTimeout(float $start, int $expectedLength, int $upperBoundaryLength): void {

        if (microtime(true) - $start >= $this->receiveTimeout) {
            throw new SocketException(
                message: 'Socket read timed out',
                code: ExceptionCode::SOCKET_TIMEOUT_DURING_READ->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'readAvailableDataFromSource',
                    'expectedLength' => $expectedLength,
                    'upperBoundaryLength' => $upperBoundaryLength,
                    'bytes_read' => 0,
                    'socket_options' => $this->config->socketOptions,
                ]
            );
        }
    }

    /**
     * @throws \Cassandra\Exception\SocketException
     */
    private function checkForWriteTimeout(float $lastProgressAt): void {

        if (microtime(true) - $lastProgressAt >= $this->sendTimeout) {
            throw new SocketException(
                message: 'Socket write timed out',
                code: ExceptionCode::SOCKET_TIMEOUT_DURING_WRITE->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'write',
                    'socket_options' => $this->config->socketOptions,
                ]
            );
        }
    }

    private function closeSocket(PhpSocket $socket, bool $shutdown): void {
        @socket_set_option($socket, SOL_SOCKET, SO_LINGER, [
            'l_onoff' => 1,
            'l_linger' => 1,
        ]);

        if ($shutdown) {
            // Deliberately suppressed rather than checked. The shutdown is a
            // courtesy — it sends the peer a FIN so a healthy connection ends
            // cleanly — and the socket is closed either way, so its failing
            // carries nothing worth acting on. It fails routinely: a peer that
            // reset the connection leaves the socket unconnected, so ENOTCONN
            // here is the normal outcome of the very path that closes a
            // connection most often.
            //
            // Left unsuppressed it raises a PHP warning on every reset. That is
            // more than noise: this runs from disconnect() while the transport
            // failure that caused it is still propagating, and from __destruct()
            // at shutdown, so an application whose error handler turns warnings
            // into exceptions — as Symfony's and Laravel's do — would see that
            // handler fire in both places, replacing the real SocketException
            // with a misleading one and throwing from a destructor.
            @socket_shutdown($socket);
        }

        socket_close($socket);
    }

    /**
     * Open a connection to one already-resolved address.
     *
     * The address is always a numeric IP literal, so the retry loop below cannot
     * trigger repeated name resolution.
     *
     * @throws \Cassandra\Exception\SocketException
     */
    private function connectToAddress(int $addressFamily, string $address): PhpSocket {

        socket_clear_error();

        // Suppressed for the reason given in readAvailableDataFromSource().
        $socket = @socket_create($addressFamily, SOCK_STREAM, SOL_TCP);
        if ($socket === false) {
            $errorCode = socket_last_error();

            throw new SocketException(
                message: 'Socket create failed: ' . socket_strerror($errorCode),
                code: ExceptionCode::SOCKET_CREATE_FAILED->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'resolved_address' => $address,
                    'address_family' => $addressFamily === AF_INET6 ? 'AF_INET6' : 'AF_INET',
                    'operation' => 'socket_create',
                    'system_error_code' => $errorCode,
                ]
            );
        }

        $this->setSocketOption($socket, SOL_TCP, TCP_NODELAY, 1);

        foreach ($this->config->socketOptions as $optname => $optval) {
            $this->setSocketOption(
                $socket,
                SOL_SOCKET,
                (int) $optname,
                $this->validateSocketOptionValue($optval, (int) $optname),
            );
        }

        // Whatever SO_RCVTIMEO the previous socket was left with says nothing
        // about this one, which starts from the configured options above. Kept,
        // it would let applyReceiveTimeout() skip the one call that narrows the
        // timeout, and a blocking read would then run under the wider window
        // instead of the caller's deadline.
        $this->appliedReceiveTimeout = null;

        $this->isBlockingIo = socket_set_nonblock($socket) === false;

        $start = microtime(true);
        do {
            // Suppressed for the reason given in readAvailableDataFromSource(),
            // and with one of its own: connect() works through every address the
            // host resolves to, so a node reachable over IPv4 but not IPv6 would
            // raise a warning on the way to a connection that succeeds.
            $result = @socket_connect($socket, $address, $this->config->port);
            if ($result === false) {

                $errorCode = socket_last_error($socket);

                if ($errorCode === SOCKET_EISCONN) {
                    break;
                }

                if ($errorCode === SOCKET_EINTR) {

                    if (microtime(true) - $start > $this->connectTimeout) {
                        $this->closeSocket($socket, false);

                        throw new SocketException(
                            message: 'Socket connect timed out',
                            code: ExceptionCode::SOCKET_TIMEOUT_DURING_CONNECT->value,
                            context: [
                                'host' => $this->config->host,
                                'port' => $this->config->port,
                                'resolved_address' => $address,
                                'operation' => 'connect',
                                'socket_options' => $this->config->socketOptions,
                            ]
                        );
                    }

                    continue;
                }

                if (
                    $errorCode === SOCKET_EINPROGRESS
                    || $errorCode === SOCKET_EALREADY
                    || $errorCode === SOCKET_EAGAIN
                ) {

                    try {
                        $this->waitForConnect($socket, $start);

                    } catch (SocketException $e) {
                        $this->closeSocket($socket, false);

                        throw $e;
                    }

                    break;
                }

                if ($errorCode === SOCKET_ETIMEDOUT) {
                    $this->closeSocket($socket, false);

                    throw new SocketException(
                        message: 'Socket connect timed out',
                        code: ExceptionCode::SOCKET_TIMEOUT_DURING_CONNECT->value,
                        context: [
                            'host' => $this->config->host,
                            'port' => $this->config->port,
                            'resolved_address' => $address,
                            'operation' => 'connect',
                            'socket_options' => $this->config->socketOptions,
                            'system_error_code' => $errorCode,
                        ]
                    );
                }

                $this->closeSocket($socket, false);

                throw new SocketException(
                    message: 'Socket connect failed: ' . socket_strerror($errorCode),
                    code: ExceptionCode::SOCKET_CONNECT_FAILED->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'resolved_address' => $address,
                        'operation' => 'connect',
                        'socket_options' => $this->config->socketOptions,
                        'system_error_code' => $errorCode,
                    ]
                );
            }
        } while ($result === false);

        return $socket;
    }

    /**
     * Read a timeout option (SO_SNDTIMEO / SO_RCVTIMEO) as fractional seconds.
     *
     * Both components are honoured, so a sub-second timeout expressed purely
     * through 'usec' is not silently rounded away. A zero timeout
     * (`['sec' => 0, 'usec' => 0]`) disables the timeout entirely, which is
     * what the underlying socket option means as well.
     *
     * @throws \Cassandra\Exception\SocketException
     */
    private function getTimeoutFromConfig(int $option, string $name, float $default): float {
        $value = $this->config->socketOptions[$option] ?? null;

        if ($value === null) {
            return $default;
        }

        if (!is_array($value)) {
            throw new SocketException(
                message: 'Invalid ' . $name . ' timeout',
                code: ExceptionCode::SOCKET_INVALID_CONFIG->value,
                context: [
                    $name . '_timeout' => $value,
                ]
            );
        }

        $seconds = $value['sec'] ?? 0;
        $microseconds = $value['usec'] ?? 0;

        if (!is_int($seconds) || !is_int($microseconds) || $seconds < 0 || $microseconds < 0) {
            throw new SocketException(
                message: 'Invalid ' . $name . ' timeout',
                code: ExceptionCode::SOCKET_INVALID_CONFIG->value,
                context: [
                    $name . '_timeout_sec' => $seconds,
                    $name . '_timeout_usec' => $microseconds,
                ]
            );
        }

        if ($seconds === 0 && $microseconds === 0) {
            return self::NO_TIMEOUT;
        }

        return (float) $seconds + (float) $microseconds / 1_000_000.0;
    }

    /**
     * @return array{
     *   sendTimeout: float,
     *   receiveTimeout: float,
     * }
     *
     * @throws \Cassandra\Exception\SocketException
     */
    private function getTimeoutsFromConfig(): array {
        // SocketNodeConfig fills both options in, so the fallbacks only apply
        // to a configuration that dropped them; they mirror its defaults.
        return [
            'sendTimeout' => $this->getTimeoutFromConfig(
                SO_SNDTIMEO,
                'send',
                (float) SocketNodeConfig::DEFAULT_SO_SNDTIMEO['sec']
            ),
            'receiveTimeout' => $this->getTimeoutFromConfig(
                SO_RCVTIMEO,
                'receive',
                (float) SocketNodeConfig::DEFAULT_SO_RCVTIMEO['sec']
            ),
        ];
    }

    /**
     * Resolve the configured host into the list of candidate addresses to try.
     *
     * getaddrinfo() handles IPv4 literals, IPv6 literals and DNS names uniformly and
     * reports the address family of every result, so a host that only publishes AAAA
     * records connects just as well as an A-only one. Every candidate is returned so
     * that connect() can fall back to the next address when one is unreachable.
     *
     * @return list<array{family: int, address: string}>
     *
     * @throws \Cassandra\Exception\SocketException
     */
    private function resolveHost(): array {

        $host = $this->config->host;

        // getaddrinfo() only understands the bare form of an IPv6 literal, but the
        // bracketed form is what users copy out of URLs and connection strings.
        if (strlen($host) > 2 && $host[0] === '[' && $host[-1] === ']') {
            $host = substr($host, 1, -1);
        }

        // "ai_family" is deliberately not set: PHP has no AF_UNSPEC constant, and leaving
        // the hint at its zero default is exactly AF_UNSPEC, so both families are returned.
        $addressInfoList = socket_addrinfo_lookup($host, (string) $this->config->port, [
            'ai_socktype' => SOCK_STREAM,
            'ai_protocol' => SOL_TCP,
        ]);

        $addresses = [];

        foreach ($addressInfoList === false ? [] : $addressInfoList as $addressInfo) {
            $info = socket_addrinfo_explain($addressInfo);

            $family = $info['ai_family'] ?? null;
            $socketAddress = $info['ai_addr'] ?? null;
            if (!is_int($family) || !is_array($socketAddress)) {
                continue;
            }

            $address = $socketAddress['sin6_addr'] ?? $socketAddress['sin_addr'] ?? null;
            if (!is_string($address) || $address === '') {
                continue;
            }

            $addresses[] = ['family' => $family, 'address' => $address];
        }

        if ($addresses === []) {
            throw new SocketException(
                message: 'Could not resolve host to any usable address',
                code: ExceptionCode::SOCKET_HOST_RESOLUTION_FAILED->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'socket_addrinfo_lookup',
                ]
            );
        }

        return $addresses;
    }

    /**
     * @throws \Cassandra\Exception\SocketException
     */
    private function selectSocketForRead(PhpSocket $socket, float $start, int $expectedLength, int $upperBoundaryLength, bool $waitForData, ?float $readDeadline): bool {

        do {
            $read = [ $socket ];
            $write = null;
            $except = null;

            socket_clear_error();

            if ($waitForData) {
                // Wait at most for the remaining receive timeout, so a peer that
                // stays connected but silent cannot block reads forever.
                $remaining = $this->receiveTimeout - (microtime(true) - $start);

                if ($remaining <= 0.0) {
                    // Selecting with a zero timeout would return immediately and
                    // busy-spin, so enforce the receive timeout right away.
                    $this->checkForReceiveTimeout($start, $expectedLength, $upperBoundaryLength);

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

                $selectResult = @socket_select(
                    read: $read,
                    write: $write,
                    except: $except,
                    seconds: $remainingSeconds,
                    microseconds: $remainingMicroseconds
                );
            } else {
                $selectResult = @socket_select(
                    read: $read,
                    write: $write,
                    except: $except,
                    seconds: 0
                );
            }

            if ($selectResult === false) {
                $errorCode = socket_last_error();

                if ($errorCode === SOCKET_EINTR) {
                    if ($waitForData) {
                        $this->checkForReceiveTimeout($start, $expectedLength, $upperBoundaryLength);

                        continue;
                    }

                    return false;
                }

                throw new SocketException(
                    message: 'Socket select failed: ' . socket_strerror($errorCode),
                    code: ExceptionCode::SOCKET_SELECT_FAILED->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'readAvailableDataFromSource',
                        'expectedLength' => $expectedLength,
                        'upperBoundaryLength' => $upperBoundaryLength,
                        'waitForData' => $waitForData,
                        'socket_options' => $this->config->socketOptions,
                        'system_error_code' => $errorCode,
                    ]
                );
            }

            if ($selectResult === 0) {
                if ($waitForData) {
                    $this->checkForReceiveTimeout($start, $expectedLength, $upperBoundaryLength);

                    continue;
                }

                return false;
            }

            break;

        } while (true);

        return true;
    }

    /**
     * @throws \Cassandra\Exception\SocketException
     */
    private function selectSocketForWrite(PhpSocket $socket, float $lastProgressAt): bool {

        $read = null;
        $write = [ $socket ];
        $except = null;

        // Wait at most for what is left of the stall window, so that several
        // select() calls without progress cannot add up to a multiple of the
        // configured send timeout.
        $remaining = $this->sendTimeout - (microtime(true) - $lastProgressAt);

        if ($remaining <= 0.0) {
            $this->checkForWriteTimeout($lastProgressAt);

            return false;
        }

        [$remainingSeconds, $remainingMicroseconds] = $this->splitTimeout($remaining);

        socket_clear_error();

        $selectResult = @socket_select(
            read: $read,
            write: $write,
            except: $except,
            seconds: $remainingSeconds,
            microseconds: $remainingMicroseconds
        );

        if ($selectResult === false) {
            $errorCode = socket_last_error();

            if ($errorCode === SOCKET_EINTR) {
                $this->checkForWriteTimeout($lastProgressAt);

                return false;
            }

            throw new SocketException(
                message: 'Socket select failed: ' . socket_strerror($errorCode),
                code: ExceptionCode::SOCKET_SELECT_FAILED->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'write',
                    'socket_options' => $this->config->socketOptions,
                    'system_error_code' => $errorCode,
                ]
            );
        }

        if ($selectResult === 0) {
            $this->checkForWriteTimeout($lastProgressAt);

            return false;
        }

        return true;
    }

    /**
     * Apply an option without allowing a PHP warning, native exception or
     * Error raised by ext-sockets to cross the transport boundary.
     *
     * @param array<mixed>|int|string $value
     *
     * @throws \Cassandra\Exception\SocketException
     */
    private function setSocketOption(PhpSocket $socket, int $level, int $option, array|int|string $value): void {
        set_error_handler(function (int $severity, string $message, string $file, int $line) use ($level, $option): never {
            $previous = new ErrorException($message, 0, $severity, $file, $line);

            throw new SocketException(
                message: 'Failed to set socket option',
                code: ExceptionCode::SOCKET_SET_OPTION_FAILED->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'socket_set_option',
                    'socket_option_level' => $level,
                    'socket_option' => $option,
                ],
                previous: $previous,
            );
        });

        try {
            $success = socket_set_option($socket, $level, $option, $value);
        } finally {
            restore_error_handler();
        }

        if ($success) {
            return;
        }

        $errorCode = socket_last_error($socket);

        throw new SocketException(
            message: 'Failed to set socket option: ' . socket_strerror($errorCode),
            code: ExceptionCode::SOCKET_SET_OPTION_FAILED->value,
            context: [
                'host' => $this->config->host,
                'port' => $this->config->port,
                'operation' => 'socket_set_option',
                'socket_option_level' => $level,
                'socket_option' => $option,
                'system_error_code' => $errorCode,
            ],
        );
    }

    /**
     * @throws \Cassandra\Exception\SocketException
     */
    private function throwConnectTimeout(): never {
        throw new SocketException(
            message: 'Socket connect timed out',
            code: ExceptionCode::SOCKET_TIMEOUT_DURING_CONNECT->value,
            context: [
                'host' => $this->config->host,
                'port' => $this->config->port,
                'operation' => 'connect',
                'socket_options' => $this->config->socketOptions,
            ]
        );
    }

    /**
     * Keep defensive runtime validation of the configuration's broader input
     * away from the narrower type accepted by ext-sockets.
     *
     * @return array<mixed>|int|string
     *
     * @throws \Cassandra\Exception\SocketException
     */
    private function validateSocketOptionValue(mixed $value, int $option): array|int|string {
        if (is_array($value) || is_int($value) || is_string($value)) {
            return $value;
        }

        throw new SocketException(
            message: 'Invalid socket option value',
            code: ExceptionCode::SOCKET_SET_OPTION_FAILED->value,
            context: [
                'host' => $this->config->host,
                'port' => $this->config->port,
                'operation' => 'socket_set_option',
                'socket_option_level' => SOL_SOCKET,
                'socket_option' => $option,
                'value_type' => get_debug_type($value),
            ],
        );
    }

    /**
     * @throws \Cassandra\Exception\SocketException
     */
    private function waitForConnect(PhpSocket $socket, float $start): void {

        do {
            $read = null;
            $write = [ $socket ];
            $except = null;

            // Unlike the I/O timeouts, the connect timeout is a hard deadline
            // for the whole attempt: there is no progress to measure, and an
            // unbounded connect would let an unreachable host wedge the client.
            $remaining = $this->connectTimeout - (microtime(true) - $start);

            if ($remaining <= 0.0) {
                $this->throwConnectTimeout();
            }

            [$remainingSeconds, $remainingMicroseconds] = $this->splitTimeout($remaining);

            socket_clear_error();

            $selectResult = socket_select(
                read: $read,
                write: $write,
                except: $except,
                seconds: $remainingSeconds,
                microseconds: $remainingMicroseconds
            );

            if ($selectResult === false) {
                $errorCode = socket_last_error();
                if ($errorCode === SOCKET_EINTR) {
                    // The remaining budget is re-checked at the top of the loop.
                    continue;
                }

                throw new SocketException(
                    message: 'Socket select failed: ' . socket_strerror($errorCode),
                    code: ExceptionCode::SOCKET_SELECT_FAILED->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'connect',
                        'socket_options' => $this->config->socketOptions,
                        'system_error_code' => $errorCode,
                    ]
                );
            }

            if ($selectResult === 0) {
                // select() timed out; only the remaining budget decides whether
                // that is fatal, so let the top of the loop check it.
                continue;
            }

            $errorCode = socket_get_option($socket, SOL_SOCKET, SO_ERROR);
            if ($errorCode === 0) {

                return;
            }

            if ($errorCode === false || !is_int($errorCode)) {
                throw new SocketException(
                    message: 'Socket connect failed: Unknown error',
                    code: ExceptionCode::SOCKET_CONNECT_FAILED->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'connect',
                        'socket_options' => $this->config->socketOptions,
                        'system_error_code' => 'unknown',
                    ]
                );
            }

            throw new SocketException(
                message: 'Socket connect failed: ' . socket_strerror($errorCode),
                code: ExceptionCode::SOCKET_CONNECT_FAILED->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'connect',
                    'socket_options' => $this->config->socketOptions,
                    'system_error_code' => $errorCode,
                ]
            );

        } while (true);
    }
}
