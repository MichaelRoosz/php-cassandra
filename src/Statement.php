<?php

declare(strict_types=1);

namespace Cassandra;

use Cassandra\Exception\StatementException;

final class Statement {
    protected Connection $connection;

    protected Request\Request $originalRequest;

    protected Request\Request $request;

    protected ?Response\Response $response = null;

    protected StatementStatus $status;

    protected int $streamId;

    public function __construct(Connection $connection, int $streamId, Request\Request $request, ?Request\Request $originalRequest = null) {
        $this->connection = $connection;
        $this->streamId = $streamId;
        $this->request = $request;
        $this->originalRequest = $originalRequest ?? $request;
        $this->status = StatementStatus::CREATED;
    }

    public function getOriginalRequest(): Request\Request {
        return $this->originalRequest;
    }

    /**
     * @throws \Cassandra\Exception\StatementException
     * @throws \Cassandra\Compression\Exception
     * @throws \Cassandra\Connection\NodeException
     * @throws \Cassandra\Request\Exception
     * @throws \Cassandra\Connection\Exception
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Value\Exception
     */
    public function getPreparedResult(): Response\Result\PreparedResult {
        $response = $this->getResponse();
        if (!($response instanceof Response\Result\PreparedResult)) {
            throw new StatementException('Unexpected response type for getPreparedResult', ExceptionCode::STMT_UNEXPECTED_PREPARED_RESULT->value, [
                'operation' => 'Statement::getPreparedResult',
                'expected' => Response\Result\PreparedResult::class,
                'received' => get_class($response),
                'stream_id' => $this->streamId,
            ]);
        }

        return $response;
    }

    public function getRequest(): Request\Request {
        return $this->request;
    }

    /**
     * @throws \Cassandra\Compression\Exception
     * @throws \Cassandra\Connection\NodeException
     * @throws \Cassandra\Request\Exception
     * @throws \Cassandra\Connection\Exception
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Value\Exception
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
     * @throws \Cassandra\Exception\StatementException
     * @throws \Cassandra\Compression\Exception
     * @throws \Cassandra\Connection\NodeException
     * @throws \Cassandra\Request\Exception
     * @throws \Cassandra\Connection\Exception
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Value\Exception
     */
    public function getResult(): Response\Result {
        $response = $this->getResponse();
        if (!($response instanceof Response\Result)) {
            throw new StatementException('Unexpected response type for getResult', ExceptionCode::STMT_UNEXPECTED_RESULT->value, [
                'operation' => 'Statement::getResult',
                'expected' => Response\Result::class,
                'received' => get_class($response),
                'stream_id' => $this->streamId,
            ]);
        }

        return $response;
    }

    /**
     * @throws \Cassandra\Exception\StatementException
     * @throws \Cassandra\Compression\Exception
     * @throws \Cassandra\Connection\NodeException
     * @throws \Cassandra\Request\Exception
     * @throws \Cassandra\Connection\Exception
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Value\Exception
     */
    public function getRowsResult(): Response\Result\RowsResult {
        $response = $this->getResponse();
        if (!($response instanceof Response\Result\RowsResult)) {
            throw new StatementException('Unexpected response type for getRowsResult', ExceptionCode::STMT_UNEXPECTED_ROWS_RESULT->value, [
                'operation' => 'Statement::getRowsResult',
                'expected' => Response\Result\RowsResult::class,
                'received' => get_class($response),
                'stream_id' => $this->streamId,
            ]);
        }

        return $response;
    }

    /**
     * @throws \Cassandra\Exception\StatementException
     * @throws \Cassandra\Compression\Exception
     * @throws \Cassandra\Connection\NodeException
     * @throws \Cassandra\Request\Exception
     * @throws \Cassandra\Connection\Exception
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Value\Exception
     */
    public function getSchemaChangeResult(): Response\Result\SchemaChangeResult {
        $response = $this->getResponse();
        if (!($response instanceof Response\Result\SchemaChangeResult)) {
            throw new StatementException('Unexpected response type for getSchemaChangeResult', ExceptionCode::STMT_UNEXPECTED_SCHEMA_CHANGE_RESULT->value, [
                'operation' => 'Statement::getSchemaChangeResult',
                'expected' => Response\Result\SchemaChangeResult::class,
                'received' => get_class($response),
                'stream_id' => $this->streamId,
            ]);
        }

        return $response;
    }

    /**
     * @throws \Cassandra\Exception\StatementException
     * @throws \Cassandra\Compression\Exception
     * @throws \Cassandra\Connection\NodeException
     * @throws \Cassandra\Request\Exception
     * @throws \Cassandra\Connection\Exception
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Value\Exception
     */
    public function getSetKeyspaceResult(): Response\Result\SetKeyspaceResult {
        $response = $this->getResponse();
        if (!($response instanceof Response\Result\SetKeyspaceResult)) {
            throw new StatementException('Unexpected response type for getSetKeyspaceResult', ExceptionCode::STMT_UNEXPECTED_SET_KEYSPACE_RESULT->value, [
                'operation' => 'Statement::getSetKeyspaceResult',
                'expected' => Response\Result\SetKeyspaceResult::class,
                'received' => get_class($response),
                'stream_id' => $this->streamId,
            ]);
        }

        return $response;
    }

    public function getStreamId(): int {
        return $this->streamId;
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

    public function isWaitingForResult(): bool {
        return $this->status === StatementStatus::WAITING_FOR_RESULT;
    }

    public function setRequest(Request\Request $request): void {
        $this->request = $request;
    }

    public function setResponse(?Response\Response $response): void {
        $this->response = $response;

        if ($response !== null) {
            $this->status = StatementStatus::RESULT_READY;
        }
    }

    public function setStatus(StatementStatus $status): void {
        $this->status = $status;
    }

    /**
     * @throws \Cassandra\Compression\Exception
     * @throws \Cassandra\Connection\NodeException
     * @throws \Cassandra\Request\Exception
     * @throws \Cassandra\Connection\Exception
     * @throws \Cassandra\Response\Exception
     * @throws \Cassandra\Value\Exception
     */
    public function waitForResponse(): void {
        $this->getResponse();
    }
}
