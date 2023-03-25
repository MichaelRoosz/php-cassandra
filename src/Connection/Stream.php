<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Request\Request;

/**
 * @psalm-consistent-constructor
 */
class Stream implements NodeImplementation {
    /**
     * @var array{
     *  class: string,
     *  host: ?string,
     *  port: int,
     *  username: ?string,
     *  password: ?string,
     *  timeout: int,
     *  connectTimeout: int,
     *  persistent: bool,
     *  ssl: array<string, mixed>,
     * } & array<string, mixed> $options
     */
    protected array $options = [
        'class'       => self::class,
        'host'        => null,
        'port'        => 9042,
        'username'    => null,
        'password'    => null,
        'timeout'     => 30,
        'connectTimeout' => 5,
        'persistent'=> false,
        'ssl' => [
            'verify_peer' => true,
            'verify_peer_name' => true,
            'allow_self_signed' => false,
        ],
    ];

    /**
     * @var ?resource $stream
     */
    protected $stream;

    /**
     * @param array{
     *  class?: string,
     *  host?: ?string,
     *  port?: int,
     *  username?: ?string,
     *  password?: ?string,
     * } & array<string, mixed> $options
     *
     * @throws \Cassandra\Connection\StreamException
     */
    public function __construct(array $options) {
        if (isset($options['timeout'])  && !is_int($options['timeout'])) {
            throw new StreamException('timeout must be an int value');
        }

        if (isset($options['connectTimeout'])  && !is_int($options['connectTimeout'])) {
            throw new StreamException('connectTimeout must be an int value');
        }

        if (isset($options['persistent'])  && !is_bool($options['persistent'])) {
            throw new StreamException('persistent must be a bool value');
        }


        if (!isset($options['ssl']) || !is_array($options['ssl'])) {
            $options['ssl'] = [];
        } else {
            foreach (array_keys($options['ssl']) as $optname) {
                if (!is_string($optname)) {
                    throw new StreamException('Invalid ssl option - must be of type string');
                }
            }
        }

        $options['ssl'] += $this->options['ssl'];

        /**
         * @var array{
         *  class: string,
         *  host: ?string,
         *  port: int,
         *  username: ?string,
         *  password: ?string,
         *  timeout: int,
         *  connectTimeout: int,
         *  persistent: bool,
         *  ssl: array<string, mixed>,
         * } & array<string, mixed> $mergedOptions
         */
        $mergedOptions = array_merge($this->options, $options);
        $this->options = $mergedOptions;

        $this->connect();
    }

    public function close(): void {
        if ($this->stream) {
            $stream = $this->stream;
            $this->stream = null;
            fclose($stream);
        }
    }

    /**
     * @return array{
     *  class: string,
     *  host: ?string,
     *  port: int,
     *  username: ?string,
     *  password: ?string,
     *  timeout: int,
     *  connectTimeout: int,
     *  persistent: bool,
     *  ssl: array<string, mixed>,
     * } & array<string, mixed>
     */
    public function getOptions(): array {
        return $this->options;
    }

    /**
     * @throws \Cassandra\Connection\StreamException
     */
    public function read(int $length): string {
        if ($this->stream === null) {
            throw new StreamException('not connected');
        }

        if ($length < 1) {
            return '';
        }

        $data = '';
        do {
            $readData = fread($this->stream, $length);

            if (feof($this->stream)) {
                throw new StreamException('Connection reset by peer');
            }

            if (stream_get_meta_data($this->stream)['timed_out']) {
                throw new StreamException('Connection timed out');
            }

            if ($readData === false || strlen($readData) == 0) {
                throw new StreamException('Unknown error');
            }

            $data .= $readData;
            $length -= strlen($readData);
        } while ($length > 0);

        return $data;
    }

    /**
     * @throws \Cassandra\Connection\StreamException
     */
    public function readOnce(int $length): string {
        if ($this->stream === null) {
            throw new StreamException('not connected');
        }

        if ($length < 1) {
            return '';
        }

        $readData = fread($this->stream, $length);

        if (feof($this->stream)) {
            throw new StreamException('Connection reset by peer');
        }

        if (stream_get_meta_data($this->stream)['timed_out']) {
            throw new StreamException('Connection timed out');
        }

        if ($readData === false || strlen($readData) == 0) {
            throw new StreamException('Unknown error');
        }

        return $readData;
    }

    /**
     * @throws \Cassandra\Connection\StreamException
     */
    public function write(string $binary): void {
        if ($this->stream === null) {
            throw new StreamException('not connected');
        }

        if (strlen($binary) < 1) {
            return;
        }

        do {
            $sentBytes = fwrite($this->stream, $binary);

            if (feof($this->stream)) {
                throw new StreamException('Connection reset by peer');
            }

            if (stream_get_meta_data($this->stream)['timed_out']) {
                throw new StreamException('Connection timed out');
            }

            if ($sentBytes === false || $sentBytes < 1) {
                throw new StreamException('Unknown error');
            }

            $binary = substr($binary, $sentBytes);
        } while ($binary);
    }

    /**
     * @throws \Cassandra\Connection\StreamException
     */
    public function writeRequest(Request $request): void {
        $this->write($request->__toString());
    }

    /**
     * @return resource
     * @throws \Cassandra\Connection\StreamException
     */
    protected function connect() {
        if ($this->stream) {
            return $this->stream;
        }

        $host = $this->options['host'] ??  'localhost';

        $context = stream_context_create();

        /** @var mixed $optval */
        foreach ($this->options['ssl'] as $optname => $optval) {
            stream_context_set_option($context, 'ssl', $optname, $optval);
        }

        $connFlag = $this->options['persistent'] ? STREAM_CLIENT_PERSISTENT : STREAM_CLIENT_CONNECT;
        $stream = stream_socket_client($host . ':' . $this->options['port'], $errorCode, $errorMessage, $this->options['connectTimeout'], $connFlag, $context);
        if ($stream === false) {
            throw new StreamException($errorMessage, $errorCode);
        }

        $this->stream = $stream;

        stream_set_timeout($this->stream, $this->options['timeout']);

        return $this->stream;
    }
}
