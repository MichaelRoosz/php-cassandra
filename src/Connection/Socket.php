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

        socket_set_block($socket);

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
                code: ExceptionCode::SOCKET_NOT_CONNECTED_READ->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'read',
                    'requested_bytes' => $length,
                ]
            );
        }

        $data = socket_read($this->socket, $length);
        if ($data === false) {
            $errorCode = socket_last_error($this->socket);

            throw new SocketException(
                message: 'Socket read failed: ' . socket_strerror($errorCode),
                code: ExceptionCode::SOCKET_READ_FAILED->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'read',
                    'requested_bytes' => $length,
                    'bytes_read' => 0,
                    'socket_options' => $this->config->socketOptions,
                    'system_error_code' => $errorCode,
                ]
            );
        }

        if ($length > 0 && $data === '') {
            throw new SocketException(
                message: 'Socket read returned no data',
                code: ExceptionCode::SOCKET_READ_NO_DATA->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'read',
                    'requested_bytes' => $length,
                    'bytes_read' => 0,
                    'socket_options' => $this->config->socketOptions,
                ]
            );
        }

        $remainder = $length - strlen($data);

        while ($remainder > 0) {
            $readData = socket_read($this->socket, $remainder);

            if ($readData === false) {
                $errorCode = socket_last_error($this->socket);

                throw new SocketException(
                    message: 'Socket read failed: ' . socket_strerror($errorCode),
                    code: ExceptionCode::SOCKET_READ_FAILED_CONT->value,
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
                    message: 'Socket read returned no data',
                    code: ExceptionCode::SOCKET_READ_NO_DATA_CONT->value,
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
            $remainder -= strlen($readData);
        }

        return $data;
    }

    /**
     * @throws \Cassandra\Exception\SocketException
     */
    #[\Override]
    public function readOnce(int $length): string {
        if ($this->socket === null) {
            throw new SocketException(
                message: 'Socket transport not connected',
                code: ExceptionCode::SOCKET_NOT_CONNECTED_READ_ONCE->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'readOnce',
                    'requested_bytes' => $length,
                ]
            );
        }

        $data = socket_read($this->socket, $length);
        if ($data === false) {
            $errorCode = socket_last_error($this->socket);

            throw new SocketException(
                message: 'Socket read failed: ' . socket_strerror($errorCode),
                code: ExceptionCode::SOCKET_READ_ONCE_FAILED->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'readOnce',
                    'requested_bytes' => $length,
                    'bytes_read' => 0,
                    'socket_options' => $this->config->socketOptions,
                    'system_error_code' => $errorCode,
                ]
            );
        }

        if ($length > 0 && $data === '') {
            throw new SocketException(
                message: 'Socket read returned no data',
                code: ExceptionCode::SOCKET_READ_ONCE_NO_DATA->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'readOnce',
                    'requested_bytes' => $length,
                    'bytes_read' => 0,
                    'socket_options' => $this->config->socketOptions,
                ]
            );
        }

        return $data;
    }

    /**
     * @throws \Cassandra\Exception\SocketException
     */
    #[\Override]
    public function write(string $binary): void {
        if ($this->socket === null) {
            throw new SocketException(
                message: 'Socket transport not connected',
                code: ExceptionCode::SOCKET_NOT_CONNECTED_WRITE->value,
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
    protected function connect(): PhpSocket {
        if ($this->socket) {
            return $this->socket;
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

        $this->socket = $socket;

        socket_set_option($this->socket, SOL_TCP, TCP_NODELAY, 1);

        foreach ($this->config->socketOptions as $optname => $optval) {
            socket_set_option($this->socket, SOL_SOCKET, (int) $optname, $optval);
        }

        $result = socket_connect($this->socket, $this->config->host, $this->config->port);
        if ($result === false) {
            $errorCode = socket_last_error($this->socket);

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

        return $this->socket;
    }
}
