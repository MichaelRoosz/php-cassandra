<?php

declare(strict_types=1);

namespace Cassandra;

class Statement {
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
