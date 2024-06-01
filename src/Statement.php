<?php

declare(strict_types=1);

namespace Cassandra;

class Statement {
    protected Connection $connection;

    protected ?Response\Result $prevoiousResult = null;

    protected ?Response\Response $response = null;

    protected int $streamId;

    public function __construct(Connection $connection, int $streamId) {
        $this->connection = $connection;
        $this->streamId = $streamId;
    }

    /**
     * @throws \Cassandra\Exception
     * @throws \Cassandra\Response\Exception
     */
    public function getResponse(): Response\Response {
        if ($this->response === null) {
            $this->response = $this->connection->getResponse($this->streamId);
        }

        if ($this->response instanceof Response\Error) {
            throw $this->response->getException();
        }

        if ($this->response instanceof Response\Result && $this->prevoiousResult !== null) {
            $this->response->setPreviousResult($this->prevoiousResult);
            $this->prevoiousResult = null;
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
            throw new Exception('received invalid response');
        }

        return $response;
    }

    public function setPreviousResult(Response\Result $prevoiousResult): void {
        $this->prevoiousResult = $prevoiousResult;
    }

    public function setResponse(Response\Response $response): void {
        $this->response = $response;
    }
}
