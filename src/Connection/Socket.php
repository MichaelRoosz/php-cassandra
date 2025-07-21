<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Socket as PhpSocket;
use Cassandra\Request\Request;

final class Socket implements NodeImplementation {

    protected SocketNodeConfig $config;

    protected ?PhpSocket $socket = null;

    /**
     * @throws \Cassandra\Connection\SocketException
     */
    public function __construct(
        NodeConfig $config
    ) {
        if (!($config instanceof SocketNodeConfig)) {
            throw new SocketException('Expected instance of SocketNodeConfig');
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
            'l_linger' => 1
        ]);

        socket_shutdown($socket);

        socket_close($socket);
    }

    #[\Override]
    public function getConfig(): SocketNodeConfig {
        return clone $this->config;
    }

    /**
     * @throws \Cassandra\Connection\SocketException
     */
    #[\Override]
    public function read(int $length): string {
        if ($this->socket === null) {
            throw new SocketException('not connected');
        }

        $data = socket_read($this->socket, $length);
        if ($data === false) {
            $errorCode = socket_last_error($this->socket);

            throw new SocketException(socket_strerror($errorCode), $errorCode);
        }

        if ($length > 0 && $data === '') {
            throw new SocketException('socket_read() returned no data');
        }

        $remainder = $length - strlen($data);

        while ($remainder > 0) {
            $readData = socket_read($this->socket, $remainder);

            if ($readData === false) {
                $errorCode = socket_last_error($this->socket);

                throw new SocketException(socket_strerror($errorCode), $errorCode);
            }

            if ($readData === '') {
                throw new SocketException('socket_read() returned no data');
            }

            $data .= $readData;
            $remainder -= strlen($readData);
        }

        return $data;
    }

    /**
     * @throws \Cassandra\Connection\SocketException
     */
    #[\Override]
    public function readOnce(int $length): string {
        if ($this->socket === null) {
            throw new SocketException('not connected');
        }

        $data = socket_read($this->socket, $length);
        if ($data === false) {
            $errorCode = socket_last_error($this->socket);

            throw new SocketException(socket_strerror($errorCode), $errorCode);
        }

        if ($length > 0 && $data === '') {
            throw new SocketException('socket_read() returned no data');
        }

        return $data;
    }

    /**
     * @throws \Cassandra\Connection\SocketException
     */
    #[\Override]
    public function write(string $binary): void {
        if ($this->socket === null) {
            throw new SocketException('not connected');
        }

        do {
            $sentBytes = socket_write($this->socket, $binary);

            if ($sentBytes === false) {
                $errorCode = socket_last_error($this->socket);

                throw new SocketException(socket_strerror($errorCode), $errorCode);
            }
            $binary = substr($binary, $sentBytes);
        } while ($binary);
    }

    /**
     * @throws \Cassandra\Connection\SocketException
     */
    #[\Override]
    public function writeRequest(Request $request): void {
        $this->write($request->__toString());
    }

    /**
     * @throws \Cassandra\Connection\SocketException
     */
    protected function connect(): PhpSocket {
        if ($this->socket) {
            return $this->socket;
        }

        $socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
        if ($socket === false) {
            $errorCode = socket_last_error();

            throw new SocketException(socket_strerror($errorCode), $errorCode);
        }

        $this->socket = $socket;

        socket_set_option($this->socket, SOL_TCP, TCP_NODELAY, 1);

        foreach ($this->config->socketOptions as $optname => $optval) {
            socket_set_option($this->socket, SOL_SOCKET, $optname, $optval);
        }

        $result = socket_connect($this->socket, $this->config->host, $this->config->port);
        if ($result === false) {
            $errorCode = socket_last_error($this->socket);

            //Unable to connect to Cassandra node: {$this->options['host']}:{$this->options['port']}
            throw new SocketException(socket_strerror($errorCode), $errorCode);
        }

        return $this->socket;
    }
}
