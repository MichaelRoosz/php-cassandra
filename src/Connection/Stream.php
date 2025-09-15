<?php

declare(strict_types=1);

namespace Cassandra\Connection;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\StreamException;
use Cassandra\Request\Request;

final class Stream extends NodeImplementation implements IoNode {
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

        $stream = $this->stream;

        if ($expectedLength < 1) {
            return '';
        }

        if (!$this->isBlockingIo) {
            $hasData = $this->selectStreamForRead($stream, $expectedLength, $upperBoundaryLength, $waitForData);
            if (!$hasData) {
                return '';
            }
        }

        $readLength = $this->isBlockingIo ? $expectedLength : max($expectedLength, $upperBoundaryLength);

        $readData = fread($stream, $readLength);
        if ($readData === false) {

            if (feof($stream)) {
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
                        'meta' => stream_get_meta_data($stream),
                    ]
                );
            }

            if (stream_get_meta_data($stream)['timed_out']) {
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
                        'meta' => stream_get_meta_data($stream),
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
                    'meta' => stream_get_meta_data($stream),
                ]
            );
        }

        if ($readData === '') {
            if (feof($stream)) {
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
                        'meta' => stream_get_meta_data($stream),
                    ]
                );
            }

            if (stream_get_meta_data($stream)['timed_out']) {
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
                        'meta' => stream_get_meta_data($stream),
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
    public function write(string $data): void {
        if ($this->stream === null) {
            throw new StreamException(
                message: 'Stream transport not connected',
                code: ExceptionCode::STREAM_NOT_CONNECTED_DURING_WRITE->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'write',
                    'bytes_remaining' => strlen($data),
                ]
            );
        }

        $stream = $this->stream;

        if (strlen($data) < 1) {
            return;
        }

        $start = microtime(true);

        do {
            if (!$this->isBlockingIo) {
                $canWrite = $this->selectStreamForWrite($stream, $start);
                if (!$canWrite) {
                    continue;
                }
            }

            $sentBytes = fwrite($stream, $data);
            if ($sentBytes === false) {

                if (feof($stream)) {
                    throw new StreamException(
                        message: 'Stream connection reset by peer',
                        code: ExceptionCode::STREAM_RESET_BY_PEER_DURING_WRITE->value,
                        context: [
                            'host' => $this->config->host,
                            'port' => $this->config->port,
                            'operation' => 'write',
                            'meta' => stream_get_meta_data($stream),
                        ]
                    );
                }

                if (stream_get_meta_data($stream)['timed_out']) {
                    throw new StreamException(
                        message: 'Stream write timed out',
                        code: ExceptionCode::STREAM_TIMEOUT_DURING_WRITE->value,
                        context: [
                            'host' => $this->config->host,
                            'port' => $this->config->port,
                            'operation' => 'write',
                            'send_timeout_seconds' => $this->sendTimeout,
                            'meta' => stream_get_meta_data($stream),
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
                        'meta' => stream_get_meta_data($stream),
                    ]
                );
            }

            if ($sentBytes === 0) {

                $this->checkForWriteTimeout($stream, $start);

                continue;
            }

            $data = substr($data, $sentBytes);

        } while ($data);
    }

    /**
     * @throws \Cassandra\Exception\StreamException
     */
    #[\Override]
    public function writeRequest(Request $request): void {
        $this->write($request->__toString());
    }

    /**
     * @param resource $stream
     * 
     * @throws \Cassandra\Exception\StreamException
     */
    protected function checkForWriteTimeout($stream, float $start): void {

        if (microtime(true) - $start > $this->sendTimeout) {
            throw new StreamException(
                message: 'Stream write timed out',
                code: ExceptionCode::STREAM_TIMEOUT_DURING_WRITE->value,
                context: [
                    'host' => $this->config->host,
                    'port' => $this->config->port,
                    'operation' => 'write',
                    'send_timeout_seconds' => $this->sendTimeout,
                    'meta' => stream_get_meta_data($stream),
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

    /**
     * @param resource $stream
     * 
     * @throws \Cassandra\Exception\StreamException
     */
    protected function selectStreamForRead($stream, int $expectedLength, int $upperBoundaryLength, bool $waitForData): bool {

        $read = [ $stream ];
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

            if (feof($stream)) {
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
                        'meta' => stream_get_meta_data($stream),
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
                    'meta' => stream_get_meta_data($stream),
                ]
            );
        }

        if ($selectResult === 0) {
            return false;
        }

        return true;
    }

    /**
     * @param resource $stream
     * 
     * @throws \Cassandra\Exception\StreamException
     */
    protected function selectStreamForWrite($stream, float $start): bool {
        $read = null;
        $write = [ $stream ];
        $except = null;

        $selectResult = stream_select(
            read: $read,
            write: $write,
            except: $except,
            seconds: $this->sendTimeout,
        );

        if ($selectResult === false) {

            if (feof($stream)) {
                throw new StreamException(
                    message: 'Stream connection reset by peer',
                    code: ExceptionCode::STREAM_RESET_BY_PEER_DURING_WRITE->value,
                    context: [
                        'host' => $this->config->host,
                        'port' => $this->config->port,
                        'operation' => 'write',
                        'meta' => stream_get_meta_data($stream),
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
                    'meta' => stream_get_meta_data($stream),
                ]
            );
        }

        if ($selectResult === 0) {
            $this->checkForWriteTimeout($stream, $start);

            return false;
        }

        return true;
    }

}
