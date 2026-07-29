<?php

declare(strict_types=1);

namespace Cassandra;

use Cassandra\Exception\ExceptionCode;
use Cassandra\Exception\StatementException;

final class Statement {
    private Connection $connection;

    private Request\Request $originalRequest;

    /**
     * How many times the node has answered this statement's EXECUTE with
     * UNPREPARED and the driver has reprepared it in response. Counted so that
     * a node which never keeps the prepared statement cannot keep the pair
     * going for good — every round chains a fresh request and restarts the
     * budget, so nothing else would ever end it.
     */
    private int $repreparationCount = 0;

    private Request\Request $request;

    /**
     * The EXECUTE a node answered with UNPREPARED, kept for as long as the
     * repreparation is in flight so that the follow-up EXECUTE can be built
     * from it. The original request is not enough: an auto-prepared query
     * keeps the QUERY the caller made as its original request, and the EXECUTE
     * derived from it - the one that actually carries the prepared statement's
     * values and options - would be lost otherwise.
     */
    private ?Request\Execute $requestBeingReprepared = null;

    private ?float $requestTimeout;

    private ?Response\Response $response = null;

    private float $sentAt;

    private StatementStatus $status;

    private int $streamGeneration;

    private int $streamId;

    /**
     * @param int $streamGeneration which run of the connection's id space
     * $streamId was claimed under, kept so that giving the id back names the
     * pool it came from; see {@see \Cassandra\Connection\StreamIdPool}.
     *
     * @param ?float $requestTimeoutInSeconds an explicit override from the
     * caller, which wins over what the request's own options asked for. Null
     * falls back to those, and then to the connection default.
     */
    public function __construct(Connection $connection, int $streamId, int $streamGeneration, Request\Request $request, ?Request\Request $originalRequest = null, ?float $requestTimeoutInSeconds = null) {
        $this->connection = $connection;
        $this->streamId = $streamId;
        $this->streamGeneration = $streamGeneration;
        $this->request = $request;
        $this->originalRequest = $originalRequest ?? $request;
        $this->status = StatementStatus::CREATED;
        $this->sentAt = microtime(true);
        $this->requestTimeout = $requestTimeoutInSeconds
            ?? $this->originalRequest->getRequestTimeout()
            ?? $request->getRequestTimeout();
    }

    public function getOriginalRequest(): Request\Request {
        return $this->originalRequest;
    }

    /**
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     * @throws \Cassandra\Exception\StatementException
     * @throws \Cassandra\Exception\RequestTimeoutException
     */
    public function getPreparedResult(): Response\Result\PreparedResult {
        $response = $this->getResponse();
        if (!($response instanceof Response\Result\PreparedResult)) {
            throw new StatementException('Unexpected response type for getPreparedResult', ExceptionCode::STATEMENT_UNEXPECTED_PREPARED_RESULT->value, [
                'operation' => 'Statement::getPreparedResult',
                'expected' => Response\Result\PreparedResult::class,
                'received' => get_class($response),
                'stream_id' => $this->streamId,
            ]);
        }

        return $response;
    }

    /**
     * How many times this statement has been reprepared after an UNPREPARED
     * error, see {@see self::$repreparationCount}.
     */
    public function getRepreparationCount(): int {
        return $this->repreparationCount;
    }

    public function getRequest(): Request\Request {
        return $this->request;
    }

    /**
     * The EXECUTE currently being reprepared, see
     * {@see self::$requestBeingReprepared}.
     */
    public function getRequestBeingReprepared(): ?Request\Execute {
        return $this->requestBeingReprepared;
    }

    /**
     * The timeout this statement's request asked for, or null to use the
     * connection default.
     */
    public function getRequestTimeout(): ?float {
        return $this->requestTimeout;
    }

    /**
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     * @throws \Cassandra\Exception\StatementException
     */
    public function getResponse(): Response\Response {
        if ($this->response === null) {
            $this->response = $this->connection->getResponseForStatement($this);
        }

        if ($this->response instanceof Response\Error) {
            throw $this->response->getException();
        }

        return $this->response;
    }

    /**
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     * @throws \Cassandra\Exception\StatementException
     * @throws \Cassandra\Exception\RequestTimeoutException
     */
    public function getResult(): Response\Result {
        $response = $this->getResponse();
        if (!($response instanceof Response\Result)) {
            throw new StatementException('Unexpected response type for getResult', ExceptionCode::STATEMENT_UNEXPECTED_RESULT->value, [
                'operation' => 'Statement::getResult',
                'expected' => Response\Result::class,
                'received' => get_class($response),
                'stream_id' => $this->streamId,
            ]);
        }

        return $response;
    }

    /**
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     * @throws \Cassandra\Exception\StatementException
     * @throws \Cassandra\Exception\RequestTimeoutException
     */
    public function getRowsResult(): Response\Result\RowsResult {
        $response = $this->getResponse();
        if (!($response instanceof Response\Result\RowsResult)) {
            throw new StatementException('Unexpected response type for getRowsResult', ExceptionCode::STATEMENT_UNEXPECTED_ROWS_RESULT->value, [
                'operation' => 'Statement::getRowsResult',
                'expected' => Response\Result\RowsResult::class,
                'received' => get_class($response),
                'stream_id' => $this->streamId,
            ]);
        }

        return $response;
    }

    /**
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     * @throws \Cassandra\Exception\StatementException
     * @throws \Cassandra\Exception\RequestTimeoutException
     */
    public function getSchemaChangeResult(): Response\Result\SchemaChangeResult {
        $response = $this->getResponse();
        if (!($response instanceof Response\Result\SchemaChangeResult)) {
            throw new StatementException('Unexpected response type for getSchemaChangeResult', ExceptionCode::STATEMENT_UNEXPECTED_SCHEMA_CHANGE_RESULT->value, [
                'operation' => 'Statement::getSchemaChangeResult',
                'expected' => Response\Result\SchemaChangeResult::class,
                'received' => get_class($response),
                'stream_id' => $this->streamId,
            ]);
        }

        return $response;
    }

    /**
     * When the request was written to the node, as a microtime.
     */
    public function getSentAt(): float {
        return $this->sentAt;
    }

    /**
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     * @throws \Cassandra\Exception\StatementException
     * @throws \Cassandra\Exception\RequestTimeoutException
     */
    public function getSetKeyspaceResult(): Response\Result\SetKeyspaceResult {
        $response = $this->getResponse();
        if (!($response instanceof Response\Result\SetKeyspaceResult)) {
            throw new StatementException('Unexpected response type for getSetKeyspaceResult', ExceptionCode::STATEMENT_UNEXPECTED_SET_KEYSPACE_RESULT->value, [
                'operation' => 'Statement::getSetKeyspaceResult',
                'expected' => Response\Result\SetKeyspaceResult::class,
                'received' => get_class($response),
                'stream_id' => $this->streamId,
            ]);
        }

        return $response;
    }

    /**
     * Which run of the connection's id space {@see self::getStreamId()} was
     * claimed under, see {@see \Cassandra\Connection\StreamIdPool::$generation}.
     */
    public function getStreamGeneration(): int {
        return $this->streamGeneration;
    }

    public function getStreamId(): int {
        return $this->streamId;
    }

    /**
     * Whether this statement was given up on before it could be answered — the
     * connection was closed, a follow-up request it needed never reached the
     * node, or its answer arrived but could not be handled — in which case it
     * can never be resolved and any attempt to read its result fails
     * immediately instead of waiting for an answer that cannot arrive.
     */
    public function isAbandoned(): bool {
        return $this->status === StatementStatus::ABANDONED;
    }

    public function isAutoPreparing(): bool {
        return $this->status === StatementStatus::AUTO_PREPARING;
    }

    public function isRepreparing(): bool {
        return $this->status === StatementStatus::REPREPARING;
    }

    public function isResultReady(): bool {
        return $this->status === StatementStatus::RESULT_READY;
    }

    /**
     * Whether the client stopped waiting for this statement's answer. The
     * connection and its other statements are unaffected; this one is simply
     * finished and would have to be sent again.
     */
    public function isTimedOut(): bool {
        return $this->status === StatementStatus::TIMED_OUT;
    }

    public function isWaitingForResult(): bool {
        return $this->status === StatementStatus::WAITING_FOR_RESULT;
    }

    /**
     * The answer if it has already arrived, and null otherwise.
     *
     * Unlike {@see self::getResponse()} and {@see self::tryGetResponse()} this
     * only looks: it never reads from the connection and never raises the
     * server's error, so it can be used from inside the connection's own read
     * loops without re-entering them.
     */
    public function peekResponse(): ?Response\Response {
        return $this->response;
    }

    /**
     * Note that this statement is being reprepared once more, see
     * {@see self::$repreparationCount}.
     */
    public function recordRepreparation(): void {
        $this->repreparationCount++;
    }

    public function setRequest(Request\Request $request): void {
        $this->request = $request;
    }

    /**
     * Note the EXECUTE whose repreparation is about to be sent, see
     * {@see self::$requestBeingReprepared}.
     */
    public function setRequestBeingReprepared(?Request\Execute $request): void {
        $this->requestBeingReprepared = $request;
    }

    public function setResponse(?Response\Response $response): void {
        $this->response = $response;

        if ($response !== null) {
            $this->status = StatementStatus::RESULT_READY;
        }
    }

    /**
     * Restarts the request timeout budget, for when the request behind this
     * statement is (re)written to the node.
     */
    public function setSentAt(float $sentAt): void {
        $this->sentAt = $sentAt;
    }

    public function setStatus(StatementStatus $status): void {
        $this->status = $status;
    }

    /**
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     * @throws \Cassandra\Exception\StatementException
     * @throws \Cassandra\Exception\RequestTimeoutException
     */
    public function tryGetPreparedResult(): ?Response\Result\PreparedResult {
        $response = $this->tryGetResponse();
        if ($response === null) {
            return null;
        }

        if (!($response instanceof Response\Result\PreparedResult)) {
            throw new StatementException('Unexpected response type for tryGetPreparedResult', ExceptionCode::STATEMENT_UNEXPECTED_PREPARED_RESULT->value, [
                'operation' => 'Statement::tryGetPreparedResult',
                'expected' => Response\Result\PreparedResult::class,
                'received' => get_class($response),
                'stream_id' => $this->streamId,
            ]);
        }

        return $response;
    }

    /**
     * Non-blocking: try to fetch the response if available; returns null if not ready.
     *
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     * @throws \Cassandra\Exception\StatementException
     */
    public function tryGetResponse(): ?Response\Response {
        if ($this->response === null) {
            $this->connection->tryResolveStatement($this);
        }

        if ($this->response instanceof Response\Error) {
            throw $this->response->getException();
        }

        return $this->response;
    }

    /**
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     * @throws \Cassandra\Exception\StatementException
     * @throws \Cassandra\Exception\RequestTimeoutException
     */
    public function tryGetResult(): ?Response\Result {
        $response = $this->tryGetResponse();
        if ($response === null) {
            return null;
        }

        if (!($response instanceof Response\Result)) {
            throw new StatementException('Unexpected response type for tryGetResult', ExceptionCode::STATEMENT_UNEXPECTED_RESULT->value, [
                'operation' => 'Statement::tryGetResult',
                'expected' => Response\Result::class,
                'received' => get_class($response),
                'stream_id' => $this->streamId,
            ]);
        }

        return $response;
    }

    /**
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     * @throws \Cassandra\Exception\StatementException
     * @throws \Cassandra\Exception\RequestTimeoutException
     */
    public function tryGetRowsResult(): ?Response\Result\RowsResult {
        $response = $this->tryGetResponse();
        if ($response === null) {
            return null;
        }

        if (!($response instanceof Response\Result\RowsResult)) {
            throw new StatementException('Unexpected response type for tryGetRowsResult', ExceptionCode::STATEMENT_UNEXPECTED_ROWS_RESULT->value, [
                'operation' => 'Statement::tryGetRowsResult',
                'expected' => Response\Result\RowsResult::class,
                'received' => get_class($response),
                'stream_id' => $this->streamId,
            ]);
        }

        return $response;
    }

    /**
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     * @throws \Cassandra\Exception\StatementException
     * @throws \Cassandra\Exception\RequestTimeoutException
     */
    public function tryGetSchemaChangeResult(): ?Response\Result\SchemaChangeResult {
        $response = $this->tryGetResponse();
        if ($response === null) {
            return null;
        }

        if (!($response instanceof Response\Result\SchemaChangeResult)) {
            throw new StatementException('Unexpected response type for tryGetSchemaChangeResult', ExceptionCode::STATEMENT_UNEXPECTED_SCHEMA_CHANGE_RESULT->value, [
                'operation' => 'Statement::tryGetSchemaChangeResult',
                'expected' => Response\Result\SchemaChangeResult::class,
                'received' => get_class($response),
                'stream_id' => $this->streamId,
            ]);
        }

        return $response;
    }

    /**
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     * @throws \Cassandra\Exception\StatementException
     * @throws \Cassandra\Exception\RequestTimeoutException
     */
    public function tryGetSetKeyspaceResult(): ?Response\Result\SetKeyspaceResult {
        $response = $this->tryGetResponse();
        if ($response === null) {
            return null;
        }

        if (!($response instanceof Response\Result\SetKeyspaceResult)) {
            throw new StatementException('Unexpected response type for tryGetSetKeyspaceResult', ExceptionCode::STATEMENT_UNEXPECTED_SET_KEYSPACE_RESULT->value, [
                'operation' => 'Statement::tryGetSetKeyspaceResult',
                'expected' => Response\Result\SetKeyspaceResult::class,
                'received' => get_class($response),
                'stream_id' => $this->streamId,
            ]);
        }

        return $response;
    }

    /**
     * @throws \Cassandra\Exception\CompressionException
     * @throws \Cassandra\Exception\NodeException
     * @throws \Cassandra\Exception\RequestException
     * @throws \Cassandra\Exception\ConnectionException
     * @throws \Cassandra\Exception\ResponseException
     * @throws \Cassandra\Exception\ValueException
     * @throws \Cassandra\Exception\ValueFactoryException
     * @throws \Cassandra\Exception\ServerException
     * @throws \Cassandra\Exception\RequestTimeoutException
     * @throws \Cassandra\Exception\StatementException
     */
    public function waitForResponse(): void {
        $this->getResponse();
    }
}
