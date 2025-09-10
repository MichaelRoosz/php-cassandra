<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\SocketException;
use Socket as PhpSocket;
use Cassandra\Request\Request;

final class Socket implements NodeImplementation {
    protected SocketNodeConfig $config;

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

        $this->connect();
    }

    #[\Override]
    public function close(): void {
        if ($this->socket === null) {
            return;
        }

        $socket = $this->socket;
        $this->socket = null;

        socket_set_option($socket, SOL_SOCKET, SO_LINGER, [
            'l_onoff' => 1,
            'l_linger' => 1,
        ]);

        socket_shutdown($socket);

        socket_close($socket);
    }

    #[\Override]
    public function getConfig(): SocketNodeConfig {
        return clone $this->config;
    }

    /**
     * @throws \Cassandra\Exception\SocketException
     */
    #[\Override]
    public function read(int $length): string {
        if ($this->socket === null) {
            throw new SocketException(
                message: 'Socket transport not connected',
                code: ExceptionCode::SOCKET_NOT_CONNECTED_DURING_READ->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'read',
                    'requested_bytes' => $length,
                ]
            );
        }

        if ($length < 1) {
            return '';
        }

        $data = '';
        do {
            $readData = socket_read($this->socket, $length);

            if ($readData === false) {
                $errorCode = socket_last_error($this->socket);

                throw new SocketException(
                    message: 'Socket read failed: ' . socket_strerror($errorCode),
                    code: ExceptionCode::SOCKET_READ_FAILED->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'read',
                        'requested_bytes' => $length,
                        'bytes_read' => strlen($data),
                        'socket_options' => $this->config->socketOptions,
                        'system_error_code' => $errorCode,
                    ]
                );
            }

            if ($readData === '') {
                throw new SocketException(
                    message: 'Socket connection reset by peer.',
                    code: ExceptionCode::SOCKET_RESET_BY_PEER_DURING_READ->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'read',
                        'requested_bytes' => $length,
                        'bytes_read' => strlen($data),
                        'socket_options' => $this->config->socketOptions,
                    ]
                );
            }

            $data .= $readData;
            $length -= strlen($readData);
        } while ($length > 0);

        return $data;
    }

    /**
     * @throws \Cassandra\Exception\SocketException
     */
    #[\Override]
    public function readAvailableData(int $maxLength): string {
        if ($this->socket === null) {
            throw new SocketException(
                message: 'Socket transport not connected',
                code: ExceptionCode::SOCKET_NOT_CONNECTED_DURING_READ->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'readAvailableData',
                    'requested_bytes' => $maxLength,
                ]
            );
        }

        if ($maxLength < 1) {
            return '';
        }

        $readData = socket_read($this->socket, $maxLength);
        if ($readData === false) {
            $errorCode = socket_last_error($this->socket);

            throw new SocketException(
                message: 'Socket read failed: ' . socket_strerror($errorCode),
                code: ExceptionCode::SOCKET_READ_FAILED->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'readAvailableData',
                    'requested_bytes' => $maxLength,
                    'bytes_read' => 0,
                    'socket_options' => $this->config->socketOptions,
                    'system_error_code' => $errorCode,
                ]
            );
        }

        if ($readData === '') {
            throw new SocketException(
                message: 'Socket connection reset by peer.',
                code: ExceptionCode::SOCKET_RESET_BY_PEER_DURING_READ->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'readAvailableData',
                    'requested_bytes' => $maxLength,
                    'bytes_read' => 0,
                    'socket_options' => $this->config->socketOptions,
                ]
            );
        }

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
                    'bytes_remaining' => strlen($binary),
                ]
            );
        }

        do {
            $sentBytes = socket_write($this->socket, $binary);

            if ($sentBytes === false) {
                $errorCode = socket_last_error($this->socket);

                throw new SocketException(
                    message: 'Socket write failed: ' . socket_strerror($errorCode),
                    code: ExceptionCode::SOCKET_WRITE_FAILED->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'write',
                        'bytes_remaining' => strlen($binary),
                        'socket_options' => $this->config->socketOptions,
                        'system_error_code' => $errorCode,
                    ]
                );
            }
            $binary = substr($binary, $sentBytes);
        } while ($binary);
    }

    /**
     * @throws \Cassandra\Exception\SocketException
     */
    #[\Override]
    public function writeRequest(Request $request): void {
        $this->write($request->__toString());
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

        socket_set_block($socket);

        socket_set_option($socket, SOL_TCP, TCP_NODELAY, 1);

        foreach ($this->config->socketOptions as $optname => $optval) {
            socket_set_option($socket, SOL_SOCKET, (int) $optname, $optval);
        }

        $result = socket_connect($socket, $this->config->host, $this->config->port);
        if ($result === false) {

            $errorCode = socket_last_error($socket);

            socket_set_option($socket, SOL_SOCKET, SO_LINGER, [
                'l_onoff' => 1,
                'l_linger' => 1,
            ]);

            socket_close($socket);

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

        $this->socket = $socket;
    }
}
