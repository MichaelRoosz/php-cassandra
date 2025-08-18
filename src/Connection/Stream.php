<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\ExceptionCode;
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
            throw new StreamException(
                message: 'Invalid node configuration type for Stream transport',
                code: ExceptionCode::STREAM_INVALID_CONFIG->value,
                context: [
                    'expected_class' => StreamNodeConfig::class,
                    'actual_class' => get_debug_type($config),
                ]
            );
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
            throw new StreamException(
                message: 'Stream transport not connected',
                code: ExceptionCode::STREAM_NOT_CONNECTED_READ->value,
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
            $readData = fread($this->stream, $length);

            if (feof($this->stream)) {
                throw new StreamException(
                    message: 'Stream connection reset by peer',
                    code: ExceptionCode::STREAM_RESET_BY_PEER_READ->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'read',
                        'requested_bytes' => $length,
                        'bytes_read' => strlen($data),
                        'meta' => stream_get_meta_data($this->stream),
                    ]
                );
            }

            if (stream_get_meta_data($this->stream)['timed_out']) {
                throw new StreamException(
                    message: 'Stream read timed out',
                    code: ExceptionCode::STREAM_TIMEOUT_READ->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'read',
                        'timeout_seconds' => $this->config->timeoutInSeconds,
                        'requested_bytes' => $length,
                        'bytes_read' => strlen($data),
                        'meta' => stream_get_meta_data($this->stream),
                    ]
                );
            }

            if ($readData === false || strlen($readData) == 0) {
                throw new StreamException(
                    message: 'Stream read failed',
                    code: ExceptionCode::STREAM_READ_FAILED->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'read',
                        'requested_bytes' => $length,
                        'bytes_read' => strlen($data),
                        'meta' => stream_get_meta_data($this->stream),
                    ]
                );
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
            throw new StreamException(
                message: 'Stream transport not connected',
                code: ExceptionCode::STREAM_NOT_CONNECTED_READ_ONCE->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'readOnce',
                    'requested_bytes' => $length,
                ]
            );
        }

        if ($length < 1) {
            return '';
        }

        $readData = fread($this->stream, $length);

        if (feof($this->stream)) {
            throw new StreamException(
                message: 'Stream connection reset by peer',
                code: ExceptionCode::STREAM_RESET_BY_PEER_READ_ONCE->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'readOnce',
                    'requested_bytes' => $length,
                    'meta' => stream_get_meta_data($this->stream),
                ]
            );
        }

        if (stream_get_meta_data($this->stream)['timed_out']) {
            throw new StreamException(
                message: 'Stream read timed out',
                code: ExceptionCode::STREAM_TIMEOUT_READ_ONCE->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'readOnce',
                    'timeout_seconds' => $this->config->timeoutInSeconds,
                    'requested_bytes' => $length,
                    'meta' => stream_get_meta_data($this->stream),
                ]
            );
        }

        if ($readData === false || strlen($readData) == 0) {
            throw new StreamException(
                message: 'Stream read failed',
                code: ExceptionCode::STREAM_READ_ONCE_FAILED->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'readOnce',
                    'requested_bytes' => $length,
                    'meta' => stream_get_meta_data($this->stream),
                ]
            );
        }

        return $readData;
    }

    /**
     * @throws \Cassandra\Connection\StreamException
     */
    #[\Override]
    public function write(string $binary): void {
        if ($this->stream === null) {
            throw new StreamException(
                message: 'Stream transport not connected',
                code: ExceptionCode::STREAM_NOT_CONNECTED_WRITE->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'write',
                    'bytes_remaining' => strlen($binary),
                ]
            );
        }

        if (strlen($binary) < 1) {
            return;
        }

        do {
            $sentBytes = fwrite($this->stream, $binary);

            if (feof($this->stream)) {
                throw new StreamException(
                    message: 'Stream connection reset by peer',
                    code: ExceptionCode::STREAM_RESET_BY_PEER_WRITE->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'write',
                        'bytes_remaining' => strlen($binary),
                        'meta' => stream_get_meta_data($this->stream),
                    ]
                );
            }

            if (stream_get_meta_data($this->stream)['timed_out']) {
                throw new StreamException(
                    message: 'Stream write timed out',
                    code: ExceptionCode::STREAM_TIMEOUT_WRITE->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'write',
                        'timeout_seconds' => $this->config->timeoutInSeconds,
                        'bytes_remaining' => strlen($binary),
                        'meta' => stream_get_meta_data($this->stream),
                    ]
                );
            }

            if ($sentBytes === false || $sentBytes < 1) {
                throw new StreamException(
                    message: 'Stream write failed',
                    code: ExceptionCode::STREAM_WRITE_FAILED->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'write',
                        'bytes_remaining' => strlen($binary),
                        'meta' => stream_get_meta_data($this->stream),
                    ]
                );
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

            throw new StreamException(
                message: $errorMessage,
                code: ExceptionCode::STREAM_CONNECT_FAILED->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'connect',
                    'connect_timeout_seconds' => $this->config->connectTimeoutInSeconds,
                    'persistent' => $this->config->persistent,
                    'ssl_options' => $this->config->sslOptions,
                    'system_error_code' => $errorCode,
                ]
            );
        }

        $this->stream = $stream;

        $timeoutSeconds = (int) floor($this->config->timeoutInSeconds);
        $timeoutMicroseconds = (int) (($this->config->timeoutInSeconds - (float) $timeoutSeconds) * 1_000_000.0);
        stream_set_timeout($this->stream, $timeoutSeconds, $timeoutMicroseconds);

        return $this->stream;
    }
}
