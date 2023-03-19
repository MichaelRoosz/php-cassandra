<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Request\Request;

/**
 * @psalm-consistent-constructor
 */
class Stream implements NodeImplementation
{
    /**
     * @var ?resource $_stream
     */
    protected $_stream;

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
     * } & array<string, mixed> $_options
     */
    protected array $_options = [
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
    public function __construct(array $options)
    {
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

        $options['ssl'] += $this->_options['ssl'];

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
        $mergedOptions = array_merge($this->_options, $options);
        $this->_options = $mergedOptions;

        $this->_connect();
    }

    /**
     * @return resource
     * @throws \Cassandra\Connection\StreamException
     */
    protected function _connect()
    {
        if ($this->_stream) {
            return $this->_stream;
        }

        $host = $this->_options['host'] ??  'localhost';

        $context = stream_context_create();

        /** @var mixed $optval */
        foreach ($this->_options['ssl'] as $optname => $optval) {
            stream_context_set_option($context, 'ssl', $optname, $optval);
        }

        $connFlag = $this->_options['persistent'] ? STREAM_CLIENT_PERSISTENT : STREAM_CLIENT_CONNECT;
        $stream = stream_socket_client($host . ':' . $this->_options['port'], $errorCode, $errorMessage, $this->_options['connectTimeout'], $connFlag, $context);
        if ($stream === false) {
            throw new StreamException($errorMessage, $errorCode);
        }

        $this->_stream = $stream;

        stream_set_timeout($this->_stream, $this->_options['timeout']);

        return $this->_stream;
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
    public function getOptions(): array
    {
        return $this->_options;
    }

    /**
     * @throws \Cassandra\Connection\StreamException
     */
    public function read(int $length): string
    {
        if ($this->_stream === null) {
            throw new StreamException('not connected');
        }

        if ($length < 1) {
            return '';
        }

        $data = '';
        do {
            $readData = fread($this->_stream, $length);

            if (feof($this->_stream)) {
                throw new StreamException('Connection reset by peer');
            }

            if (stream_get_meta_data($this->_stream)['timed_out']) {
                throw new StreamException('Connection timed out');
            }

            if ($readData === false || strlen($readData) == 0) {
                throw new StreamException("Unknown error");
            }

            $data .= $readData;
            $length -= strlen($readData);
        } while ($length > 0);

        return $data;
    }

    /**
     * @throws \Cassandra\Connection\StreamException
     */
    public function readOnce(int $length): string
    {
        if ($this->_stream === null) {
            throw new StreamException('not connected');
        }

        if ($length < 1) {
            return '';
        }

        $readData = fread($this->_stream, $length);

        if (feof($this->_stream)) {
            throw new StreamException('Connection reset by peer');
        }

        if (stream_get_meta_data($this->_stream)['timed_out']) {
            throw new StreamException('Connection timed out');
        }

        if ($readData === false || strlen($readData) == 0) {
            throw new StreamException("Unknown error");
        }

        return $readData;
    }

    /**
     * @throws \Cassandra\Connection\StreamException
     */
    public function write(string $binary): void
    {
        if ($this->_stream === null) {
            throw new StreamException('not connected');
        }

        if (strlen($binary) < 1) {
            return;
        }

        do {
            $sentBytes = fwrite($this->_stream, $binary);

            if (feof($this->_stream)) {
                throw new StreamException('Connection reset by peer');
            }

            if (stream_get_meta_data($this->_stream)['timed_out']) {
                throw new StreamException('Connection timed out');
            }

            if ($sentBytes === false || $sentBytes < 1) {
                throw new StreamException("Unknown error");
            }

            $binary = substr($binary, $sentBytes);
        } while ($binary);
    }

    /**
     * @throws \Cassandra\Connection\StreamException
     */
    public function writeRequest(Request $request): void
    {
        $this->write($request->__tostring());
    }

    public function close(): void
    {
        if ($this->_stream) {
            $stream = $this->_stream;
            $this->_stream = null;
            fclose($stream);
        }
    }
}
