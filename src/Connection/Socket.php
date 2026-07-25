<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\SocketException;
use Socket as PhpSocket;
use Cassandra\Request\Request;

final class Socket extends NodeImplementation implements IoNode {
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

        if ($config->connectTimeoutInSeconds <= 0.0) {
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
        return clone $this->config;
    }

    /**
     * @throws \Cassandra\Exception\SocketException
     */
    #[\Override]
    public function readAvailableDataFromSource(int $expectedLength, int $upperBoundaryLength, bool $waitForData): string {

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
                    'waitForData' => $waitForData,
                ]
            );
        }

        $socket = $this->socket;

        if ($expectedLength < 1) {
            return '';
        }

        $start = microtime(true);

        if (!$this->isBlockingIo) {
            $hasData = $this->selectSocketForRead($socket, $start, $expectedLength, $upperBoundaryLength, $waitForData);
            if (!$hasData) {
                return '';
            }
        }

        $readLength = $this->isBlockingIo ? $expectedLength : max($expectedLength, $upperBoundaryLength);
        do {
            $readData = socket_read($socket, $readLength, PHP_BINARY_READ);
            if ($readData === false) {
                $errorCode = socket_last_error($socket);

                if ($errorCode === SOCKET_EINTR) {
                    if ($waitForData) {
                        $this->checkForReceiveTimeout($start, $expectedLength, $upperBoundaryLength);

                        continue;
                    }

                    return '';
                }

                if (
                    $errorCode === SOCKET_EWOULDBLOCK
                    || $errorCode === SOCKET_EAGAIN /* @phpstan-ignore identical.alwaysFalse */
                ) {
                    if ($this->isBlockingIo && $waitForData) {
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
                        'waitForData' => $waitForData,
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
        do {
            if (!$this->isBlockingIo) {
                $canWrite = $this->selectSocketForWrite($socket, $lastProgressAt);
                if (!$canWrite) {
                    continue;
                }
            }

            $bufferErrors = 0;
            do {
                $sentBytes = socket_write($socket, $data);

                if ($sentBytes === 0) {
                    $this->checkForWriteTimeout($lastProgressAt);

                    // Back to the outer loop so the socket is selected for
                    // writability again instead of spinning on socket_write().
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
     * @throws \Cassandra\Exception\SocketException
     */
    private function checkForReceiveTimeout(float $start, int $expectedLength, int $upperBoundaryLength): void {

        if (microtime(true) - $start > $this->receiveTimeout) {
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

        if (microtime(true) - $lastProgressAt > $this->sendTimeout) {
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
        socket_set_option($socket, SOL_SOCKET, SO_LINGER, [
            'l_onoff' => 1,
            'l_linger' => 1,
        ]);

        if ($shutdown) {
            socket_shutdown($socket);
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

        $socket = socket_create($addressFamily, SOCK_STREAM, SOL_TCP);
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

        socket_set_option($socket, SOL_TCP, TCP_NODELAY, 1);

        foreach ($this->config->socketOptions as $optname => $optval) {
            socket_set_option($socket, SOL_SOCKET, (int) $optname, $optval);
        }

        $this->isBlockingIo = socket_set_nonblock($socket) === false;

        $start = microtime(true);
        do {
            $result = socket_connect($socket, $address, $this->config->port);
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
    private function selectSocketForRead(PhpSocket $socket, float $start, int $expectedLength, int $upperBoundaryLength, bool $waitForData): bool {

        do {
            $read = [ $socket ];
            $write = null;
            $except = null;

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

                [$remainingSeconds, $remainingMicroseconds] = $this->splitTimeout($remaining);

                $selectResult = socket_select(
                    read: $read,
                    write: $write,
                    except: $except,
                    seconds: $remainingSeconds,
                    microseconds: $remainingMicroseconds
                );
            } else {
                $selectResult = socket_select(
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
