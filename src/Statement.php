<?php

declare(strict_types=1);

namespace Cassandra;

final class Statement {
    protected Connection $connection;

    protected bool $isRepreparing = false;

    protected ?Request\Request $originalRequest = null;

    protected Request\Request $request;

    protected ?Response\Response $response = null;

    protected int $streamId;

    public function __construct(Connection $connection, int $streamId, Request\Request $request) {
        $this->connection = $connection;
        $this->streamId = $streamId;
        $this->request = $request;
        $this->originalRequest = $request;
    }

    public function getOriginalRequest(): ?Request\Request {
        return $this->originalRequest;
    }

    /**
     * @throws \Cassandra\Exception
     * @throws \Cassandra\Response\Exception
     */
    public function getPreparedResult(): Response\Result\PreparedResult {
        $response = $this->getResponse();
        if (!($response instanceof Response\Result\PreparedResult)) {
            throw new Exception('received unexpected response type: ' . get_class($response), 0, [
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
     * @throws \Cassandra\Exception
     * @throws \Cassandra\Response\Exception
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
     * @throws \Cassandra\Exception
     * @throws \Cassandra\Response\Exception
     */
    public function getResult(): Response\Result {
        $response = $this->getResponse();
        if (!($response instanceof Response\Result)) {
            throw new Exception('received unexpected response type: ' . get_class($response), 0, [
                'expected' => Response\Result::class,
                'received' => get_class($response),
                'stream_id' => $this->streamId,
            ]);
        }

        return $response;
    }

    /**
     * @throws \Cassandra\Exception
     * @throws \Cassandra\Response\Exception
     */
    public function getRowsResult(): Response\Result\RowsResult {
        $response = $this->getResponse();
        if (!($response instanceof Response\Result\RowsResult)) {
            throw new Exception('received unexpected response type: ' . get_class($response), 0, [
                'expected' => Response\Result\RowsResult::class,
                'received' => get_class($response),
                'stream_id' => $this->streamId,
            ]);
        }

        return $response;
    }

    /**
     * @throws \Cassandra\Exception
     * @throws \Cassandra\Response\Exception
     */
    public function getSchemaChangeResult(): Response\Result\SchemaChangeResult {
        $response = $this->getResponse();
        if (!($response instanceof Response\Result\SchemaChangeResult)) {
            throw new Exception('received unexpected response type: ' . get_class($response), 0, [
                'expected' => Response\Result\SchemaChangeResult::class,
                'received' => get_class($response),
                'stream_id' => $this->streamId,
            ]);
        }

        return $response;
    }

    /**
     * @throws \Cassandra\Exception
     * @throws \Cassandra\Response\Exception
     */
    public function getSetKeyspaceResult(): Response\Result\SetKeyspaceResult {
        $response = $this->getResponse();
        if (!($response instanceof Response\Result\SetKeyspaceResult)) {
            throw new Exception('received unexpected response type: ' . get_class($response), 0, [
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

    public function isRepreparing(): bool {
        return $this->isRepreparing;
    }

    public function setIsRepreparing(bool $isRepreparing): void {
        $this->isRepreparing = $isRepreparing;
    }

    public function setRequest(Request\Request $request): void {
        $this->request = $request;
    }

    public function setResponse(?Response\Response $response): void {
        $this->response = $response;
    }
}
