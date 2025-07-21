<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Request\Request;

final class Stream implements NodeImplementation {
    protected StreamNodeConfig $config;

    /**
     * @var ?resource $stream
     */
    protected $stream = null;

    /**
     * @throws \Cassandra\Connection\StreamException
     */
    public function __construct(
        NodeConfig $config
    ) {
        if (!($config instanceof StreamNodeConfig)) {
            throw new StreamException('Expected instance of StreamNodeConfig');
        }
        $this->config = $config;

        $this->connect();
    }

    #[\Override]
    public function close(): void {
        if ($this->stream) {
            $stream = $this->stream;
            $this->stream = null;
            fclose($stream);
        }
    }

    #[\Override]
    public function getConfig(): StreamNodeConfig {
        return clone $this->config;
    }

    /**
     * @throws \Cassandra\Connection\StreamException
     */
    #[\Override]
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
    #[\Override]
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
    #[\Override]
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
    #[\Override]
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

        $context = stream_context_create(
            options: [
                'ssl' => $this->config->sslOptions,
            ]
        );

        $flags = $this->config->persistent ? STREAM_CLIENT_PERSISTENT : STREAM_CLIENT_CONNECT;

        $stream = stream_socket_client(
            address: $this->config->host . ':' . $this->config->port,
            error_code: $errorCode,
            error_message: $errorMessage,
            timeout: $this->config->connectTimeoutInSeconds,
            flags: $flags,
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

            throw new StreamException($errorMessage, $errorCode);
        }

        $this->stream = $stream;

        $timeoutSeconds = (int) floor($this->config->timeoutInSeconds);
        $timeoutMicroseconds = (int) (($this->config->timeoutInSeconds - (float) $timeoutSeconds) * 1_000_000.0);
        stream_set_timeout($this->stream, $timeoutSeconds, $timeoutMicroseconds);

        return $this->stream;
    }
}
