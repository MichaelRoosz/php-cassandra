<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\StreamException;
use Cassandra\Request\Request;

final class Stream extends Node implements IoNode {
    protected StreamNodeConfig $config;
    protected bool $isBlockingIo = false;
    protected int $sendTimeout = 10;

    /**
     * @var ?resource $stream
     */
    protected $stream = null;

    /**
     * @throws \Cassandra\Exception\StreamException
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

        $this->sendTimeout = max(1, (int) $config->timeoutInSeconds);

        $this->connect();
    }

    public function __destruct() {
        $this->close();
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
     * @throws \Cassandra\Exception\StreamException
     */
    #[\Override]
    public function readAvailableDataFromSource(int $expectedLength, int $upperBoundaryLength, bool $waitForData): string {

        if ($this->stream === null) {
            throw new StreamException(
                message: 'Stream transport not connected',
                code: ExceptionCode::STREAM_NOT_CONNECTED_DURING_READ->value,
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

        if (!$this->isBlockingIo) {
            $hasData = $this->selectStreamForRead($expectedLength, $upperBoundaryLength, $waitForData);
            if (!$hasData) {
                return '';
            }
        }

        $readLength = $this->isBlockingIo ? $expectedLength : $upperBoundaryLength;

        $readData = fread($this->stream, $readLength);
        if ($readData === false) {

            if (feof($this->stream)) {
                throw new StreamException(
                    message: 'Stream connection reset by peer',
                    code: ExceptionCode::STREAM_RESET_BY_PEER_DURING_READ->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'readAvailableDataFromSource',
                        'expectedLength' => $expectedLength,
                        'upperBoundaryLength' => $upperBoundaryLength,
                        'waitForData' => $waitForData,
                        'meta' => stream_get_meta_data($this->stream),
                    ]
                );
            }

            if (stream_get_meta_data($this->stream)['timed_out']) {
                throw new StreamException(
                    message: 'Stream read timed out',
                    code: ExceptionCode::STREAM_TIMEOUT_DURING_READ->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'readAvailableDataFromSource',
                        'expectedLength' => $expectedLength,
                        'upperBoundaryLength' => $upperBoundaryLength,
                        'waitForData' => $waitForData,
                        'meta' => stream_get_meta_data($this->stream),
                    ]
                );
            }

            throw new StreamException(
                message: 'Stream read failed',
                code: ExceptionCode::STREAM_READ_FAILED->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'readAvailableDataFromSource',
                    'expectedLength' => $expectedLength,
                    'upperBoundaryLength' => $upperBoundaryLength,
                    'waitForData' => $waitForData,
                    'bytes_read' => 0,
                    'meta' => stream_get_meta_data($this->stream),
                ]
            );
        }

        if ($readData === '') {
            if (feof($this->stream)) {
                throw new StreamException(
                    message: 'Stream connection reset by peer',
                    code: ExceptionCode::STREAM_RESET_BY_PEER_DURING_READ->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'readAvailableDataFromSource',
                        'expectedLength' => $expectedLength,
                        'upperBoundaryLength' => $upperBoundaryLength,
                        'waitForData' => $waitForData,
                        'meta' => stream_get_meta_data($this->stream),
                    ]
                );
            }

            if (stream_get_meta_data($this->stream)['timed_out']) {
                throw new StreamException(
                    message: 'Stream read timed out',
                    code: ExceptionCode::STREAM_TIMEOUT_DURING_READ->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'readAvailableDataFromSource',
                        'expectedLength' => $expectedLength,
                        'upperBoundaryLength' => $upperBoundaryLength,
                        'waitForData' => $waitForData,
                        'meta' => stream_get_meta_data($this->stream),
                    ]
                );
            }
        }

        return $readData;
    }

    /**
     * @throws \Cassandra\Exception\StreamException
     */
    #[\Override]
    public function write(string $binary): void {
        if ($this->stream === null) {
            throw new StreamException(
                message: 'Stream transport not connected',
                code: ExceptionCode::STREAM_NOT_CONNECTED_DURING_WRITE->value,
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

        $start = microtime(true);

        do {
            if (!$this->isBlockingIo) {
                $canWrite = $this->selectStreamForWrite($start);
                if (!$canWrite) {
                    continue;
                }
            }

            $sentBytes = fwrite($this->stream, $binary);
            if ($sentBytes === false) {

                if (feof($this->stream)) {
                    throw new StreamException(
                        message: 'Stream connection reset by peer',
                        code: ExceptionCode::STREAM_RESET_BY_PEER_DURING_WRITE->value,
                        context: [
                            'host' => $this->config->host,
                            'port' => $this->config->port,
                            'operation' => 'write',
                            'meta' => stream_get_meta_data($this->stream),
                        ]
                    );
                }

                if (stream_get_meta_data($this->stream)['timed_out']) {
                    throw new StreamException(
                        message: 'Stream write timed out',
                        code: ExceptionCode::STREAM_TIMEOUT_DURING_WRITE->value,
                        context: [
                            'host' => $this->config->host,
                            'port' => $this->config->port,
                            'operation' => 'write',
                            'send_timeout_seconds' => $this->sendTimeout,
                            'meta' => stream_get_meta_data($this->stream),
                        ]
                    );
                }

                throw new StreamException(
                    message: 'Stream write failed',
                    code: ExceptionCode::STREAM_WRITE_FAILED->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'write',
                        'meta' => stream_get_meta_data($this->stream),
                    ]
                );
            }

            if ($sentBytes === 0) {

                $this->checkForWriteTimeout($start);

                continue;
            }

            $binary = substr($binary, $sentBytes);

        } while ($binary);
    }

    /**
     * @throws \Cassandra\Exception\StreamException
     */
    #[\Override]
    public function writeRequest(Request $request): void {
        $this->write($request->__toString());
    }

    protected function checkForWriteTimeout(float $start): void {

        if (microtime(true) - $start > $this->sendTimeout) {
            throw new StreamException(
                message: 'Stream write timed out',
                code: ExceptionCode::STREAM_TIMEOUT_DURING_WRITE->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'write',
                    'send_timeout_seconds' => $this->sendTimeout,
                    'meta' => stream_get_meta_data($this->stream),
                ]
            );
        }
    }

    /**
     * @throws \Cassandra\Exception\StreamException
     */
    protected function connect(): void {
        if ($this->stream !== null) {
            return;
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

        $this->isBlockingIo = stream_set_blocking($stream, enable: false) === false;

        $timeoutSeconds = (int) floor($this->config->timeoutInSeconds);
        $timeoutMicroseconds = (int) (($this->config->timeoutInSeconds - (float) $timeoutSeconds) * 1_000_000.0);
        stream_set_timeout($stream, $timeoutSeconds, $timeoutMicroseconds);

        $this->stream = $stream;
    }

    protected function selectStreamForRead(int $expectedLength, int $upperBoundaryLength, bool $waitForData): bool {

        $read = [ $this->stream ];
        $write = null;
        $except = null;

        if ($waitForData) {
            $selectResult = stream_select(
                read: $read,
                write: $write,
                except: $except,
                seconds: null
            );
        } else {
            $selectResult = stream_select(
                read: $read,
                write: $write,
                except: $except,
                seconds: 0
            );
        }

        if ($selectResult === false) {

            if (feof($this->stream)) {
                throw new StreamException(
                    message: 'Stream connection reset by peer',
                    code: ExceptionCode::STREAM_RESET_BY_PEER_DURING_READ->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'readAvailableDataFromSource',
                        'expectedLength' => $expectedLength,
                        'upperBoundaryLength' => $upperBoundaryLength,
                        'waitForData' => $waitForData,
                        'meta' => stream_get_meta_data($this->stream),
                    ]
                );
            }

            throw new StreamException(
                message: 'Stream select failed',
                code: ExceptionCode::STREAM_SELECT_FAILED->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'readAvailableDataFromSource',
                    'expectedLength' => $expectedLength,
                    'upperBoundaryLength' => $upperBoundaryLength,
                    'waitForData' => $waitForData,
                    'bytes_read' => 0,
                    'meta' => stream_get_meta_data($this->stream),
                ]
            );
        }

        if ($selectResult === 0) {
            return false;
        }

        return true;
    }

    protected function selectStreamForWrite(float $start): bool {
        $read = null;
        $write = [ $this->stream ];
        $except = null;

        $selectResult = stream_select(
            read: $read,
            write: $write,
            except: $except,
            seconds: $this->sendTimeout,
        );

        if ($selectResult === false) {

            if (feof($this->stream)) {
                throw new StreamException(
                    message: 'Stream connection reset by peer',
                    code: ExceptionCode::STREAM_RESET_BY_PEER_DURING_WRITE->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'write',
                        'meta' => stream_get_meta_data($this->stream),
                    ]
                );
            }

            throw new StreamException(
                message: 'Stream select failed',
                code: ExceptionCode::STREAM_SELECT_FAILED->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'write',
                    'meta' => stream_get_meta_data($this->stream),
                ]
            );
        }

        if ($selectResult === 0) {
            $this->checkForWriteTimeout($start);

            return false;
        }

        return true;
    }

}
