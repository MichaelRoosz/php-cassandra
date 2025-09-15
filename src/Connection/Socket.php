<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\SocketException;
use Socket as PhpSocket;
use Cassandra\Request\Request;

final class Socket extends Node implements IoNode {
    protected SocketNodeConfig $config;
    protected bool $isBlockingIo = false;
    protected int $receiveTimeout = 10;
    protected int $sendTimeout = 10;
    protected ?PhpSocket $socket = null;

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

        $this->sendTimeout = max(1, $this->config->socketOptions[SO_SNDTIMEO]['sec'] ?? 10);
        $this->receiveTimeout = max(1, $this->config->socketOptions[SO_RCVTIMEO]['sec'] ?? 10);

        $this->connect();
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

        if ($expectedLength < 1) {
            return '';
        }

        $start = microtime(true);

        if (!$this->isBlockingIo) {
            $hasData = $this->selectSocketForRead($start, $expectedLength, $upperBoundaryLength, $waitForData);
            if (!$hasData) {
                return '';
            }
        }

        $readLength = $this->isBlockingIo ? $expectedLength : $upperBoundaryLength;
        do {
            $readData = socket_read($this->socket, $readLength, PHP_BINARY_READ);
            if ($readData === false) {
                $errorCode = socket_last_error($this->socket);

                if ($errorCode === SOCKET_EINTR) {
                    if ($waitForData) {
                        $this->checkForReceiveTimeout($start, $expectedLength, $upperBoundaryLength);

                        continue;
                    }

                    return '';
                }

                if (
                    $errorCode === SOCKET_EWOULDBLOCK
                    || $errorCode === SOCKET_EAGAIN
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
    public function write(string $binary): void {
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

        if (strlen($binary) < 1) {
            return;
        }

        $start = microtime(true);
        do {
            if (!$this->isBlockingIo) {
                $canWrite = $this->selectSocketForWrite($start);
                if (!$canWrite) {
                    continue;
                }
            }

            $bufferErrors = 0;
            do {
                $sentBytes = socket_write($this->socket, $binary);

                if ($sentBytes === 0) {
                    $this->checkForWriteTimeout($start);

                    continue;
                }

                if ($sentBytes === false) {
                    $errorCode = socket_last_error($this->socket);

                    if (
                        $errorCode === SOCKET_EWOULDBLOCK
                        || $errorCode === SOCKET_EAGAIN
                        || $errorCode === SOCKET_EINTR
                    ) {

                        $this->checkForWriteTimeout($start);

                        continue;
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
                $binary = substr($binary, $sentBytes);

            } while ($binary);

            break;

        } while (true);
    }

    /**
     * @throws \Cassandra\Exception\SocketException
     */
    #[\Override]
    public function writeRequest(Request $request): void {
        $this->write($request->__toString());
    }

    protected function checkForReceiveTimeout(float $start, int $expectedLength, int $upperBoundaryLength): void {

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

    protected function checkForWriteTimeout(float $start): void {

        if (microtime(true) - $start > $this->sendTimeout) {
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

    protected function closeSocket(PhpSocket $socket, bool $shutdown): void {
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
     * @throws \Cassandra\Exception\SocketException
     */
    protected function connect(): void {
        if ($this->socket !== null) {
            return;
        }

        $socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
        if ($socket === false) {
            $errorCode = socket_last_error();

            throw new SocketException(
                message: 'Socket create failed: ' . socket_strerror($errorCode),
                code: ExceptionCode::SOCKET_CREATE_FAILED->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
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
            $result = socket_connect($socket, $this->config->host, $this->config->port);
            if ($result === false) {

                $errorCode = socket_last_error($socket);

                if ($errorCode === SOCKET_EISCONN) {
                    break;
                }

                if ($errorCode === SOCKET_EINTR) {

                    if (microtime(true) - $start > $this->sendTimeout) {
                        $this->closeSocket($socket, false);

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
                        'operation' => 'connect',
                        'socket_options' => $this->config->socketOptions,
                        'system_error_code' => $errorCode,
                    ]
                );
            }
        } while ($result === false);

        $this->socket = $socket;
    }

    protected function selectSocketForRead(float $start, int $expectedLength, int $upperBoundaryLength, bool $waitForData): bool {

        do {
            $read = [ $this->socket ];
            $write = null;
            $except = null;

            if ($waitForData) {
                $selectResult = socket_select(
                    read: $read,
                    write: $write,
                    except: $except,
                    seconds: null
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
                return false;
            }

            break;

        } while (true);

        return true;
    }

    protected function selectSocketForWrite(float $start): bool {

        $read = null;
        $write = [ $this->socket ];
        $except = null;

        $selectResult = socket_select(
            read: $read,
            write: $write,
            except: $except,
            seconds: $this->sendTimeout
        );

        if ($selectResult === false) {
            $errorCode = socket_last_error();

            if ($errorCode === SOCKET_EINTR) {
                $this->checkForWriteTimeout($start);

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
            $this->checkForWriteTimeout($start);

            return false;
        }

        return true;
    }

    protected function waitForConnect(PhpSocket $socket, float $start): void {

        do {
            $read = null;
            $write = [ $socket ];
            $except = null;

            $selectResult = socket_select(
                read: $read,
                write: $write,
                except: $except,
                seconds: $this->sendTimeout
            );

            if ($selectResult === false) {
                $errorCode = socket_last_error();
                if ($errorCode === SOCKET_EINTR) {

                    if (microtime(true) - $start > $this->sendTimeout) {
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
                if (microtime(true) - $start > $this->sendTimeout) {
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

                continue;
            }

            $errorCode = socket_get_option($socket, SOL_SOCKET, SO_ERROR);
            if ($errorCode === 0) {

                return;
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
