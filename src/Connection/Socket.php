<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Socket as PhpSocket;
use Cassandra\Request\Request;

/**
 * @psalm-consistent-constructor
 */
class Socket implements NodeImplementation {
    /**
     * @var array{
     *  class: string,
     *  host: ?string,
     *  port: int,
     *  username: ?string,
     *  password: ?string,
     *  socket: array<int, array<mixed>|int|string>,
     * } & array<string, mixed> $options
     */
    protected array $options = [
        'class'       => self::class,
        'host'        => null,
        'port'        => 9042,
        'username'    => null,
        'password'    => null,
        'socket'      => [
            SO_RCVTIMEO => ['sec' => 30, 'usec' => 0],
            SO_SNDTIMEO => ['sec' => 5, 'usec' => 0],
        ],
    ];
    protected ?PhpSocket $socket = null;

    /**
     * @param array{
     *  class?: string,
     *  host?: ?string,
     *  port?: int,
     *  username?: ?string,
     *  password?: ?string,
     * } & array<string, mixed> $options
     *
     * @throws \Cassandra\Connection\SocketException
     */
    public function __construct(array $options) {
        if (!isset($options['socket']) || !is_array($options['socket'])) {
            $options['socket'] = [];
        } else {
            foreach (array_keys($options['socket']) as $optname) {
                if (!is_int($optname)) {
                    throw new SocketException('Invalid socket option - must be of type int');
                }
            }
        }

        $options['socket'] += $this->options['socket'];

        /**
         * @var array{
         *  class: string,
         *  host: ?string,
         *  port: int,
         *  username: ?string,
         *  password: ?string,
         *  socket: array<int, array<mixed>|int|string>,
         * } & array<string, mixed> $mergedOptions
         */
        $mergedOptions = array_merge($this->options, $options);
        $this->options = $mergedOptions;

        $this->connect();
    }

    #[\Override]
    public function close(): void {
        if ($this->socket) {
            $socket = $this->socket;
            $this->socket = null;

            socket_set_block($socket);
            socket_set_option($socket, SOL_SOCKET, SO_LINGER, ['l_onoff' => 1, 'l_linger' => 1]);

            /** @psalm-suppress UnusedFunctionCall */
            socket_shutdown($socket);

            socket_close($socket);
        }
    }

    /**
     * @return array{
     *  class: string,
     *  host: ?string,
     *  port: int,
     *  username: ?string,
     *  password: ?string,
     *  socket: array<int, array<mixed>|int|string>,
     * } & array<string, mixed> $options
     */
    #[\Override]
    public function getOptions(): array {
        return $this->options;
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

        foreach ($this->options['socket'] as $optname => $optval) {
            socket_set_option($this->socket, SOL_SOCKET, $optname, $optval);
        }

        $result = socket_connect($this->socket, $this->options['host'] ?? 'localhost', $this->options['port']);
        if ($result === false) {
            $errorCode = socket_last_error($this->socket);

            //Unable to connect to Cassandra node: {$this->options['host']}:{$this->options['port']}
            throw new SocketException(socket_strerror($errorCode), $errorCode);
        }

        return $this->socket;
    }
}
